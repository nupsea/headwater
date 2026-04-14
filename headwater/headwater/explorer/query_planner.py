"""Query planner -- translates natural language questions to SQL via schema graph.

Architecture:
1. **Tokenize** the question and extract content words (strip stop/analytical words).
2. **Detect intent** -- what kind of analysis? (count, average, trend, top, etc.)
3. **Resolve entities** -- map terms to tables and columns via SchemaGraph.
4. **Plan joins** -- if resolved entities span multiple tables, find FK paths.
5. **Build SQL** -- assemble SELECT/FROM/JOIN/GROUP BY/ORDER BY from the plan.

The planner uses SchemaGraph for all entity resolution, which means:
- Cross-table columns are found naturally (county in a lookup table)
- Join paths are computed via BFS, not ad-hoc pattern matching
- Column roles (metric/dimension/temporal) drive correct aggregation
- Confidence scoring prevents silently wrong SQL

This replaces the ad-hoc _heuristic_sql approach with structured planning.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import duckdb

from headwater.core.models import GeneratedModel
from headwater.explorer.schema_graph import (
    ROLE_DIMENSION,
    ROLE_METRIC,
    ROLE_TEMPORAL,
    JoinStep,
    SchemaGraph,
    _split_name,
    _stem,
)
from headwater.explorer.utils import resolve_table_ref

logger = logging.getLogger(__name__)


# ── Intent constants ─────────────────────────────────────────────────────────

INTENT_COUNT = "count"
INTENT_AVERAGE = "average"
INTENT_SUM = "sum"
INTENT_TREND = "trend"
INTENT_TOP = "top"
INTENT_DISTRIBUTION = "distribution"
INTENT_LIST = "list"
INTENT_BREAKDOWN = "breakdown"

# Word sets for intent detection (order matters -- first match wins for ties)
_INTENT_SIGNALS: list[tuple[str, set[str]]] = [
    (INTENT_TREND, {
        "trend", "trends", "trending", "trended",
        "changing", "changed", "growth", "growing",
        "increasing", "decreasing", "over",
    }),
    (INTENT_TOP, {
        "top", "highest", "lowest", "most", "least", "worst", "best",
        "largest", "smallest", "which",
    }),
    (INTENT_COUNT, {
        "how", "many", "count", "number", "per",
    }),
    (INTENT_AVERAGE, {"average", "avg", "mean"}),
    (INTENT_SUM, {"sum", "total", "cumulative"}),
    (INTENT_DISTRIBUTION, {"distribution", "spread", "histogram", "range", "percentile"}),
    (INTENT_LIST, {"show", "list", "display", "browse"}),
    (INTENT_BREAKDOWN, {"breakdown", "break", "across", "between"}),
]

_STOP_WORDS: frozenset[str] = frozenset({
    "what", "is", "the", "a", "an", "are", "how", "do", "does", "which",
    "where", "when", "who", "in", "on", "by", "for", "to", "of", "and",
    "or", "from", "with", "that", "this", "there", "have", "has", "was",
    "were", "be", "been", "being", "my", "your", "their", "its",
    "we", "our", "us", "i", "me", "more", "less", "than", "not", "no",
    "one", "ones", "other", "each", "every", "any", "some", "all",
    "give", "tell", "can", "could", "would", "should", "please", "just",
    "about", "also",
})

_ANALYTICAL_WORDS: frozenset[str] = frozenset({
    "average", "avg", "mean", "sum", "total", "count", "max", "min",
    "median", "rate", "ratio", "percent", "percentage",
    "trend", "trends", "compare", "comparison",
    "increase", "decrease", "change", "changes",
    "distribution", "breakdown", "across", "between", "per",
    "over", "time", "daily", "weekly", "monthly", "yearly",
    "top", "bottom", "highest", "lowest", "most", "least",
    "show", "list", "get", "find", "display",
    "number", "many", "much", "often",
    "changing", "changed", "growing", "growth",
    "increasing", "decreasing",
    "worst", "best", "largest", "smallest",
})


# ── Query plan ───────────────────────────────────────────────────────────────


@dataclass
class ResolvedColumn:
    """A column resolved to a specific table with its role."""

    table_name: str
    column_name: str
    role: str


@dataclass
class QueryPlan:
    """Structured representation of a query before SQL generation."""

    primary_table: str
    intent: str
    measures: list[ResolvedColumn] = field(default_factory=list)
    dimensions: list[ResolvedColumn] = field(default_factory=list)
    time_axis: ResolvedColumn | None = None
    joins: list[JoinStep] = field(default_factory=list)
    limit: int | None = 20
    confidence: float = 0.0
    warnings: list[str] = field(default_factory=list)


# ── Query planner ────────────────────────────────────────────────────────────


class QueryPlanner:
    """Translates a natural language question into a SQL query.

    Uses SchemaGraph for entity resolution and join-path finding.
    """

    def __init__(
        self,
        graph: SchemaGraph,
        con: duckdb.DuckDBPyConnection | None = None,
        models: list[GeneratedModel] | None = None,
    ) -> None:
        self.graph = graph
        self.con = con
        self.models = models or []

    def plan_sql(self, question: str) -> str | None:
        """Build a SQL query from a natural language question.

        Returns the SQL string, or None if the question can't be mapped
        to the schema with sufficient confidence.
        """
        plan = self._build_plan(question)
        if plan is None:
            return None
        if plan.confidence < 0.3:
            logger.info(
                "Planner confidence %.2f too low for: %s", plan.confidence, question,
            )
            return None
        return self._plan_to_sql(plan)

    # ── Plan construction ────────────────────────────────────────────────

    def _build_plan(self, question: str) -> QueryPlan | None:
        """Parse question -> resolve entities -> assemble plan.

        Key design: the question is split into **subject** (the entity being
        queried) and **predicate** (grouping dimensions, metrics, filters)
        around structural prepositions like "per", "by", "across".

        "complaints per county"  -> subject=["complaints"], predicate=["county"]
        "average AQI by state"   -> subject=["aqi"],        predicate=["state"]
        "top counties by score"  -> subject=["counties"],    predicate=["score"]

        The subject drives **table** resolution. The predicate drives **column**
        resolution. This prevents grouping-dimension words like "county" from
        hijacking the primary table selection.
        """
        q_lower = question.lower().strip().rstrip("?")
        tokens = _tokenize(q_lower)
        intent = _detect_intent(tokens)
        content_words = _extract_content_words(tokens)

        if not content_words:
            return None

        # Step 0: Split subject vs predicate at structural prepositions
        subject_words, predicate_words = _split_subject_predicate(tokens)

        # Step 1: Resolve primary table from SUBJECT words only
        # (fall back to all content words if subject yields nothing)
        primary_table = self._resolve_primary_table(subject_words)
        if primary_table is None:
            primary_table = self._resolve_primary_table(content_words)
        if primary_table is None:
            return None

        # Step 2: Identify content words that refer to the table (vs columns)
        table_words = set(_split_name(primary_table))
        tnode = self.graph.tables[primary_table]
        if tnode.info.domain:
            table_words.update(tnode.info.domain.lower().split())

        # Column candidates = predicate words + any non-table subject words
        col_words = [
            w for w in content_words
            if w not in table_words and not _stems_overlap(w, table_words)
        ]

        # Step 3: Resolve columns from the question
        dimensions, metrics, temporal = self._resolve_columns(
            col_words, content_words, primary_table, intent, tokens,
        )

        # Step 4: Find joins for cross-table columns.
        # _resolve_joins mutates the lists to drop unreachable columns.
        all_resolved = dimensions + metrics
        joins = self._resolve_joins(primary_table, all_resolved)
        # Sync back: remove any columns that were dropped
        reachable = set((c.table_name, c.column_name) for c in all_resolved)
        dimensions[:] = [d for d in dimensions if (d.table_name, d.column_name) in reachable]
        metrics[:] = [m for m in metrics if (m.table_name, m.column_name) in reachable]

        # Step 5: Assemble plan
        plan = QueryPlan(
            primary_table=primary_table,
            intent=intent,
            measures=metrics,
            dimensions=dimensions,
            time_axis=temporal,
            joins=joins,
        )

        # Apply defaults when the question didn't mention specific columns
        self._apply_defaults(plan)

        # Score confidence
        plan.confidence = self._score_confidence(plan, col_words, content_words)

        return plan

    # ── Entity resolution ────────────────────────────────────────────────

    def _resolve_primary_table(self, content_words: list[str]) -> str | None:
        """Find the table that best matches the question terms.

        Uses two signals:
        1. Table name / domain / description matches (from resolve_table)
        2. Column name matches (if a word matches a column, the column's table
           gets a score boost -- "value by sensor type" implies "readings")
        """
        table_scores: dict[str, float] = {}

        for word in content_words:
            # Signal 1: direct table name matching
            for match in self.graph.resolve_table(word):
                table_scores[match.table_name] = (
                    table_scores.get(match.table_name, 0) + match.score
                )

            # Signal 2: column name matching -> boosts the column's table
            for match in self.graph.resolve_column(word):
                if match.column_name is not None and match.score >= 6:
                    table_scores[match.table_name] = (
                        table_scores.get(match.table_name, 0) + match.score * 0.5
                    )

        if table_scores:
            best = max(table_scores, key=table_scores.get)  # type: ignore[arg-type]
            if table_scores[best] >= 5:
                return best

        # Fallback: try multi-word table name matching
        combined = "_".join(content_words)
        for tname in self.graph.tables:
            if tname in combined or combined in tname:
                return tname

        return None

    def _resolve_columns(
        self,
        col_words: list[str],
        all_content_words: list[str],
        primary_table: str,
        intent: str,
        all_tokens: list[str] | None = None,
    ) -> tuple[list[ResolvedColumn], list[ResolvedColumn], ResolvedColumn | None]:
        """Resolve column references from question words.

        Three resolution passes:
        1. Single-word matches from col_words (table-name words removed)
        2. Compound name matches -- adjacent tokens joined with underscore
           to match multi-word column names like "max_aqi", "good_days"
        3. Fallback to all_content_words when passes 1-2 yield nothing

        Returns (dimensions, metrics, temporal).
        """
        dimensions: list[ResolvedColumn] = []
        metrics: list[ResolvedColumn] = []
        temporal: ResolvedColumn | None = None
        used_words: set[str] = set()

        # First pass: single-word matches from col_words
        for word in col_words:
            resolved = self._try_resolve_word(
                word, primary_table, dimensions, metrics, temporal,
            )
            if resolved is not None:
                temporal = resolved
                used_words.add(word)
            elif word in {c.info.name.lower() for t in self.graph.tables.values()
                          for c in t.columns.values()}:
                used_words.add(word)

        # Second pass: compound column names from ALL tokens (including
        # analytical words like "max", "good", "median").  This handles
        # "average max AQI by state" -> "max_aqi" column.
        if not metrics:
            compound_tokens = all_tokens if all_tokens else _tokenize(
                " ".join(all_content_words)
            )
            compounds = _build_compound_candidates(
                compound_tokens, primary_table, self.graph,
            )
            for compound, words in compounds:
                matches = self.graph.resolve_column(
                    compound, preferred_table=primary_table,
                )
                if not matches or matches[0].score < 10:
                    continue
                best = matches[0]
                if best.column_name is None:
                    continue
                resolved_col = ResolvedColumn(
                    best.table_name, best.column_name, best.role or "",
                )
                if best.role == ROLE_METRIC:
                    metrics.append(resolved_col)
                    used_words.update(words)
                elif best.role == ROLE_DIMENSION and not dimensions:
                    dimensions.append(resolved_col)
                    used_words.update(words)

        # Third pass: fallback to all_content_words
        if not dimensions and not metrics and not temporal:
            for word in all_content_words:
                if word in used_words:
                    continue
                resolved = self._try_resolve_word(
                    word, primary_table, dimensions, metrics, temporal,
                    min_score=6,
                )
                if resolved is not None:
                    temporal = resolved

        return dimensions, metrics, temporal

    def _try_resolve_word(
        self,
        word: str,
        primary_table: str,
        dimensions: list[ResolvedColumn],
        metrics: list[ResolvedColumn],
        temporal: ResolvedColumn | None,
        min_score: float = 0,
    ) -> ResolvedColumn | None:
        """Try to resolve a single word to a column. Returns new temporal if matched.

        Appends to dimensions/metrics in place. Returns a ResolvedColumn
        only when a temporal match was found (so caller can update temporal).
        """
        matches = self.graph.resolve_column(word, preferred_table=primary_table)
        if not matches:
            return None

        best = matches[0]
        if best.column_name is None or best.score < min_score:
            return None

        resolved = ResolvedColumn(
            best.table_name, best.column_name, best.role or "",
        )
        if best.role == ROLE_TEMPORAL:
            if temporal is None:
                return resolved
        elif best.role == ROLE_METRIC:
            metrics.append(resolved)
        elif best.role == ROLE_DIMENSION:
            dimensions.append(resolved)
        else:
            # Unknown role -- guess from intent context
            dimensions.append(resolved)
        return None

    def _resolve_joins(
        self,
        primary_table: str,
        resolved_cols: list[ResolvedColumn],
    ) -> list[JoinStep]:
        """Find join paths for cross-table columns. Drop unreachable columns.

        If a resolved column lives in another table and no FK path exists,
        the column is removed from the plan (mutates the list in place)
        rather than generating broken SQL that references a column not
        in the FROM clause.
        """
        joins: list[JoinStep] = []
        joined_tables: set[str] = {primary_table}
        unreachable: list[ResolvedColumn] = []

        for col in resolved_cols:
            if col.table_name == primary_table or col.table_name in joined_tables:
                continue

            path = self.graph.find_join_path(primary_table, col.table_name)
            if path is not None:
                for step in path:
                    if step.to_table not in joined_tables:
                        joins.append(step)
                        joined_tables.add(step.to_table)
            else:
                unreachable.append(col)

        # Remove unreachable columns from the plan
        for col in unreachable:
            if col in resolved_cols:
                resolved_cols.remove(col)
                logger.info(
                    "Dropped column %s.%s -- no join path from %s",
                    col.table_name, col.column_name, primary_table,
                )

        return joins

    # ── Defaults ─────────────────────────────────────────────────────────

    def _apply_defaults(self, plan: QueryPlan) -> None:
        """Fill in defaults when the question didn't mention specific columns.

        For example, "complaints per county" resolves county as dimension
        but no metric -- default to COUNT(*).  "AQI trend" resolves no
        temporal column -- pick the best one from the table.
        """
        # Trend intent needs a temporal axis
        if plan.intent == INTENT_TREND and plan.time_axis is None:
            temps = self.graph.get_temporals(plan.primary_table)
            if temps:
                t = temps[0]
                plan.time_axis = ResolvedColumn(t.table_name, t.info.name, ROLE_TEMPORAL)

        # If we have dimensions but no metrics, and intent needs a metric
        # (AVERAGE, SUM), add the best metric from the primary table.
        # For COUNT/TOP/BREAKDOWN, implicit COUNT(*) is added in SQL generation.
        if (
            plan.dimensions
            and not plan.measures
            and plan.intent in (INTENT_AVERAGE, INTENT_SUM)
        ):
                metric = self.graph.get_best_metric(plan.primary_table)
                if metric:
                    plan.measures.append(
                        ResolvedColumn(
                            metric.table_name, metric.info.name, ROLE_METRIC,
                        )
                    )

        # If we have metrics but no dimensions, add the best dimension
        if plan.measures and not plan.dimensions and plan.intent != INTENT_TREND:
            dim = self.graph.get_best_dimension(plan.primary_table)
            if dim:
                plan.dimensions.append(
                    ResolvedColumn(dim.table_name, dim.info.name, ROLE_DIMENSION)
                )

        # If we have nothing resolved, fall back to table's best columns
        if not plan.dimensions and not plan.measures and plan.time_axis is None:
            dim = self.graph.get_best_dimension(plan.primary_table)
            metric = self.graph.get_best_metric(plan.primary_table)
            if dim:
                plan.dimensions.append(
                    ResolvedColumn(dim.table_name, dim.info.name, ROLE_DIMENSION)
                )
            if metric:
                plan.measures.append(
                    ResolvedColumn(metric.table_name, metric.info.name, ROLE_METRIC)
                )

        # Trend without metric: add the best metric
        if plan.intent == INTENT_TREND and plan.time_axis and not plan.measures:
            metric = self.graph.get_best_metric(plan.primary_table)
            if metric:
                plan.measures.append(
                    ResolvedColumn(metric.table_name, metric.info.name, ROLE_METRIC)
                )

    # ── Confidence scoring ───────────────────────────────────────────────

    def _score_confidence(
        self,
        plan: QueryPlan,
        col_words: list[str],
        content_words: list[str],
    ) -> float:
        """Score how confident we are in the plan.

        Higher = more confident.  Below 0.3 the planner declines to produce SQL.
        """
        score = 0.0

        # Having a primary table is baseline
        if plan.primary_table:
            score += 0.3

        # Resolved columns from the question = strong signal
        has_explicit = bool(plan.dimensions) or bool(plan.measures) or plan.time_axis is not None
        if has_explicit:
            score += 0.3

        # Check how many col_words actually matched something
        if col_words:
            resolved_tables_and_cols = set()
            for rc in plan.dimensions + plan.measures:
                resolved_tables_and_cols.update(_split_name(rc.column_name))
            if plan.time_axis:
                resolved_tables_and_cols.update(_split_name(plan.time_axis.column_name))

            matched = sum(
                1 for w in col_words
                if any(_stem(w) & _stem(rw) for rw in resolved_tables_and_cols)
            )
            if col_words:
                match_ratio = matched / len(col_words)
                score += match_ratio * 0.3

        # Joins resolved successfully
        cross_table_cols = [
            c for c in plan.dimensions + plan.measures
            if c.table_name != plan.primary_table
        ]
        if cross_table_cols and plan.joins:
            score += 0.1
        elif cross_table_cols and not plan.joins:
            score -= 0.2  # needed a join but couldn't find one
            plan.warnings.append(
                "Column(s) in other table(s) but no join path found"
            )

        return min(score, 1.0)

    # ── SQL generation ───────────────────────────────────────────────────

    def _plan_to_sql(self, plan: QueryPlan) -> str:
        """Generate SQL from a QueryPlan."""
        primary_ref = self._resolve_ref(plan.primary_table)

        # ── Build FROM clause with JOINs ─────────────────────────────
        aliases: dict[str, str] = {}
        use_aliases = bool(plan.joins)

        if use_aliases:
            aliases[plan.primary_table] = "t0"
            from_clause = f"{primary_ref} t0"
            for i, step in enumerate(plan.joins, 1):
                alias = f"t{i}"
                aliases[step.to_table] = alias
                join_ref = self._resolve_ref(step.to_table)
                # Determine which alias has the from-side column
                from_alias = aliases.get(step.from_table, "t0")
                from_clause += (
                    f' JOIN {join_ref} {alias}'
                    f' ON {from_alias}."{step.from_column}" = {alias}."{step.to_column}"'
                )
        else:
            from_clause = primary_ref

        # ── Build SELECT and GROUP BY ────────────────────────────────
        select_parts: list[str] = []
        group_parts: list[str] = []

        # Time axis
        if plan.time_axis:
            time_ref = self._col_ref(plan.time_axis, aliases, use_aliases)
            time_expr = self._time_expression(plan.time_axis, time_ref)
            select_parts.append(time_expr)
            group_parts.append(time_expr.split(" AS ")[0] if " AS " in time_expr else time_expr)

        # Dimensions
        for dim in plan.dimensions:
            col_ref = self._col_ref(dim, aliases, use_aliases)
            select_parts.append(col_ref)
            group_parts.append(col_ref)

        # Measures / aggregations
        has_explicit_agg = False
        if plan.measures:
            for m in plan.measures:
                col_ref = self._col_ref(m, aliases, use_aliases)
                agg = self._agg_for_intent(plan.intent)
                select_parts.append(
                    f'ROUND({agg}({col_ref}), 2) AS {agg.lower()}_{m.column_name}'
                )
                has_explicit_agg = True

        # Implicit COUNT(*) for count/top/breakdown intents without explicit metrics
        if not plan.measures and plan.intent in (
            INTENT_COUNT, INTENT_TOP, INTENT_BREAKDOWN, INTENT_LIST,
        ):
            select_parts.append("COUNT(*) AS total")
            has_explicit_agg = True

        # If we have group columns + metrics, add COUNT for context
        if plan.measures and group_parts:
            select_parts.append("COUNT(*) AS records")
            has_explicit_agg = True

        # ── Assemble query ──────────────────────────────────���────────
        sql = f"SELECT {', '.join(select_parts)} FROM {from_clause}"

        if group_parts and has_explicit_agg:
            sql += f" GROUP BY {', '.join(group_parts)}"

        # ORDER BY
        sql += self._order_clause(plan, select_parts)

        # LIMIT
        if plan.limit:
            sql += f" LIMIT {plan.limit}"

        return sql

    # ── SQL helpers ──────────────────────────────────────────────────────

    def _resolve_ref(self, table_name: str) -> str:
        """Resolve a table name to its schema-qualified reference."""
        if self.con is not None:
            return resolve_table_ref(table_name, self.con, self.models)
        # No connection -- prefer executed mart, else staging
        for m in self.models:
            if (
                m.model_type == "mart"
                and table_name in m.source_tables
                and m.status == "executed"
            ):
                return f"marts.{m.name}"
        return f"staging.stg_{table_name}"

    def _col_ref(
        self,
        col: ResolvedColumn,
        aliases: dict[str, str],
        use_aliases: bool,
    ) -> str:
        """Build a column reference, with alias prefix if joins are present."""
        if use_aliases and col.table_name in aliases:
            return f'{aliases[col.table_name]}."{col.column_name}"'
        return f'"{col.column_name}"'

    def _time_expression(self, col: ResolvedColumn, col_ref: str) -> str:
        """Build a time expression based on column dtype."""
        # Look up the actual column info
        tnode = self.graph.tables.get(col.table_name)
        dtype = ""
        if tnode and col.column_name in tnode.columns:
            dtype = tnode.columns[col.column_name].info.dtype.lower()

        if dtype == "timestamp":
            return f"DATE_TRUNC('month', {col_ref}) AS period"
        if dtype == "date":
            return f"CAST({col_ref} AS DATE) AS period"
        # Name-pattern temporal (year, month, etc.) -- use raw value
        return col_ref

    def _agg_for_intent(self, intent: str) -> str:
        """Return the aggregation function for an intent."""
        if intent == INTENT_SUM:
            return "SUM"
        if intent == INTENT_COUNT:
            return "AVG"  # COUNT goes via COUNT(*); metric aggregation defaults to AVG
        return "AVG"

    def _order_clause(self, plan: QueryPlan, select_parts: list[str]) -> str:
        """Build ORDER BY clause."""
        # Trend queries order by time
        if plan.time_axis:
            time_col = plan.time_axis.column_name
            # Check if we used a period alias
            if any("AS period" in p for p in select_parts):
                return " ORDER BY period"
            return f' ORDER BY "{time_col}"'

        # Top/count queries order by aggregate DESC
        if plan.intent in (INTENT_TOP, INTENT_COUNT, INTENT_BREAKDOWN):
            if plan.measures:
                m = plan.measures[0]
                agg = self._agg_for_intent(plan.intent).lower()
                return f" ORDER BY {agg}_{m.column_name} DESC"
            if "COUNT(*) AS total" in " ".join(select_parts):
                return " ORDER BY total DESC"

        # Average queries order by the aggregate
        if plan.intent == INTENT_AVERAGE and plan.measures:
            m = plan.measures[0]
            return f" ORDER BY avg_{m.column_name} DESC"

        return ""


# ── Tokenization and intent detection ────────────────────────────────────────


def _tokenize(question: str) -> list[str]:
    """Tokenize a question into lowercase words, stripping punctuation."""
    return re.sub(r"[^a-z0-9 ]", "", question.lower()).split()


def _extract_content_words(tokens: list[str]) -> list[str]:
    """Extract words that might refer to data entities (not operations)."""
    return [w for w in tokens if w not in _STOP_WORDS and w not in _ANALYTICAL_WORDS]


def _detect_intent(tokens: list[str]) -> str:
    """Detect the analytical intent from question tokens.

    Returns the intent with the strongest signal. Falls back to INTENT_BREAKDOWN.
    """
    token_set = set(tokens)
    best_intent = INTENT_BREAKDOWN
    best_score = 0

    for intent, signal_words in _INTENT_SIGNALS:
        overlap = token_set & signal_words
        if len(overlap) > best_score:
            best_score = len(overlap)
            best_intent = intent

    # Special case: "X per Y" is count even if "per" alone is weak
    if "per" in token_set and best_intent not in (INTENT_TREND, INTENT_AVERAGE, INTENT_SUM):
        best_intent = INTENT_COUNT

    # Special case: "over time" strongly indicates trend
    if "over" in token_set and "time" in token_set:
        best_intent = INTENT_TREND

    return best_intent


# Structural prepositions that separate subject from predicate/grouping
_SPLIT_PREPOSITIONS = {"per", "by", "across", "for", "within", "grouped"}


def _split_subject_predicate(tokens: list[str]) -> tuple[list[str], list[str]]:
    """Split question tokens into subject (entity) and predicate (grouping).

    Splits at the first structural preposition ("per", "by", "across", etc.).
    Words before the split are subject (drive table selection).
    Words after are predicate (drive column selection).

    "complaints per county"  -> (["complaints"],  ["county"])
    "average aqi by state"   -> (["average", "aqi"], ["state"])
    "how many readings"      -> (["how", "many", "readings"], [])

    When no preposition is found, all tokens go to subject.
    """
    for i, token in enumerate(tokens):
        if token in _SPLIT_PREPOSITIONS:
            subject = tokens[:i]
            predicate = tokens[i + 1:]
            # Extract content words from each side
            noise = _STOP_WORDS | _ANALYTICAL_WORDS
            subj_content = [w for w in subject if w not in noise]
            pred_content = [w for w in predicate if w not in noise]
            return subj_content, pred_content

    # No preposition found -- all words are subject
    all_content = [w for w in tokens if w not in _STOP_WORDS and w not in _ANALYTICAL_WORDS]
    return all_content, []


def _build_compound_candidates(
    tokens: list[str],
    primary_table: str,
    graph: SchemaGraph,
) -> list[tuple[str, list[str]]]:
    """Build compound column name candidates from adjacent tokens.

    "average max aqi by state" -> tries "max_aqi", "aqi_by", etc.
    Only returns candidates that exactly match an existing column name
    in the graph to avoid noise.

    Returns list of (compound_name, [source_words]).
    """
    # Collect all known column names for quick membership check
    known_cols: set[str] = set()
    for tnode in graph.tables.values():
        for cname in tnode.columns:
            known_cols.add(cname.lower())

    candidates: list[tuple[str, list[str]]] = []
    for i in range(len(tokens) - 1):
        # Try 2-word compounds
        compound = f"{tokens[i]}_{tokens[i + 1]}"
        if compound in known_cols:
            candidates.append((compound, [tokens[i], tokens[i + 1]]))
        # Try 3-word compounds
        if i + 2 < len(tokens):
            compound3 = f"{tokens[i]}_{tokens[i + 1]}_{tokens[i + 2]}"
            if compound3 in known_cols:
                candidates.append((compound3, tokens[i : i + 3]))

    return candidates


def _stems_overlap(word: str, word_set: set[str]) -> bool:
    """Check if a word's stems overlap with any word in the set."""
    word_stems = _stem(word)
    return any(_stem(w) & word_stems for w in word_set)
