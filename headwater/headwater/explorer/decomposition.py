"""Query decomposition engine -- resolves NL questions against the semantic catalog.

Three-strategy resolution (in order):
  1. Keyword: stem-match question tokens against catalog names and synonyms
  2. Embedding: vector similarity search via LanceDB
  3. LLM: (future) complex multi-entity reasoning

If fully resolved -> deterministic SQL generation from catalog ontology.
If ambiguous -> returns options for user disambiguation.
If outside catalog -> reports unmatched concepts.

The decomposer never generates SQL from an LLM. SQL is always
deterministic from the catalog definitions: metric expressions,
dimension columns, join paths. This is the MetricFlow insight.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from headwater.core.models import (
    DecompositionResult,
    DimensionDefinition,
    DimensionMatch,
    DimensionOption,
    EntityDefinition,
    MetricDefinition,
    MetricMatch,
    SemanticCatalog,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stop words and analytical intent words
# ---------------------------------------------------------------------------

_STOP_WORDS: frozenset[str] = frozenset(
    {
        "a",
        "an",
        "the",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "shall",
        "should",
        "may",
        "might",
        "can",
        "could",
        "must",
        "i",
        "me",
        "my",
        "we",
        "us",
        "our",
        "you",
        "your",
        "he",
        "she",
        "it",
        "they",
        "them",
        "their",
        "its",
        "this",
        "that",
        "these",
        "those",
        "what",
        "which",
        "who",
        "whom",
        "how",
        "where",
        "when",
        "why",
        "and",
        "or",
        "but",
        "if",
        "then",
        "else",
        "so",
        "because",
        "of",
        "in",
        "on",
        "at",
        "to",
        "for",
        "with",
        "from",
        "by",
        "about",
        "into",
        "through",
        "during",
        "before",
        "after",
        "above",
        "below",
        "between",
        "under",
        "over",
        "all",
        "each",
        "every",
        "both",
        "few",
        "more",
        "most",
        "some",
        "any",
        "no",
        "not",
        "only",
        "same",
        "than",
        "too",
        "very",
        "show",
        "give",
        "tell",
        "get",
        "find",
        "list",
        "display",
        "see",
        "please",
        "let",
    }
)

_INTENT_WORDS: dict[str, str] = {
    "count": "count",
    "total": "count",
    "number": "count",
    "how many": "count",
    "average": "avg",
    "avg": "avg",
    "mean": "avg",
    "sum": "sum",
    "total amount": "sum",
    "maximum": "max",
    "max": "max",
    "highest": "max",
    "top": "max",
    "minimum": "min",
    "min": "min",
    "lowest": "min",
}

_FILTER_PATTERNS = re.compile(
    r"\b(?:for|where|with|only|just|in|of)\s+(.+?)(?:\s+(?:by|per|grouped|across|over)\b|$)",
    re.IGNORECASE,
)

_GROUP_BY_PATTERNS = re.compile(
    r"\b(?:by|per|grouped\s+by|broken?\s+down\s+by|across|over)\s+(.+?)(?:\s+(?:for|where|with)\b|$)",
    re.IGNORECASE,
)

# Stemming (simple suffix removal)
_STEM_SUFFIXES = ("ies", "ing", "tion", "ment", "ness", "able", "ous", "ful", "ed", "ly", "es", "s")


def _stem(word: str) -> str:
    """Simple suffix-stripping stemmer."""
    w = word.lower()
    for suffix in _STEM_SUFFIXES:
        if w.endswith(suffix) and len(w) - len(suffix) >= 3:
            return w[: -len(suffix)]
    return w


def _tokenize(text: str) -> list[str]:
    """Extract content words from text, lowercased."""
    words = re.findall(r"[a-zA-Z_]+", text.lower())
    return [w for w in words if w not in _STOP_WORDS and len(w) > 1]


# ---------------------------------------------------------------------------
# QueryDecomposer
# ---------------------------------------------------------------------------


class QueryDecomposer:
    """Decomposes NL queries against a SemanticCatalog.

    Usage:
        decomposer = QueryDecomposer(catalog)
        result = decomposer.decompose("complaints by county", vector_store=vs)
    """

    def __init__(self, catalog: SemanticCatalog) -> None:
        self.catalog = catalog
        self._name_index: dict[str, list[tuple[str, str, str]]] = {}
        self._build_index()

    def _build_index(self) -> None:
        """Build a stem-to-catalog-entry index for fast keyword lookup."""
        for m in self.catalog.metrics:
            for token in _name_tokens(m.name, m.display_name, m.synonyms):
                self._name_index.setdefault(token, []).append(("metric", m.name, m.table))

        for d in self.catalog.dimensions:
            for token in _name_tokens(d.name, d.display_name, d.synonyms):
                self._name_index.setdefault(token, []).append(("dimension", d.name, d.table))

        for e in self.catalog.entities:
            for token in _name_tokens(e.name, e.display_name, e.synonyms):
                self._name_index.setdefault(token, []).append(("entity", e.name, e.table))

        logger.debug("Decomposer index: %d stems", len(self._name_index))

    def decompose(
        self,
        question: str,
        vector_store: Any | None = None,
        project_id: str | None = None,
    ) -> DecompositionResult:
        """Decompose a natural language question into catalog components.

        Args:
            question: The user's question.
            vector_store: Optional VectorStore for embedding-based resolution.
            project_id: Optional project filter for vector search.

        Returns:
            DecompositionResult with status resolved/options/outside_scope.
        """
        logger.info("Decomposing: %r", question)
        tokens = _tokenize(question)
        stems = [_stem(t) for t in tokens]
        logger.debug("Tokens: %s, stems: %s", tokens, stems)

        # Detect aggregation intent
        intent = _detect_intent(question)
        logger.debug("Detected intent: %s", intent)

        # Strategy A: keyword resolution
        entity_matches = self._keyword_resolve_entities(stems)
        metric_matches = self._keyword_resolve_metrics(stems, tokens, intent)
        dim_matches = self._keyword_resolve_dimensions(stems, tokens)

        # Strategy B: embedding resolution (fills gaps)
        if vector_store and (not metric_matches or not dim_matches):
            logger.debug("Trying embedding resolution for gaps")
            self._embedding_resolve(
                question,
                tokens,
                vector_store,
                project_id,
                metric_matches,
                dim_matches,
                entity_matches,
            )

        # Resolve entity from matches
        entity = self._pick_entity(entity_matches, metric_matches, dim_matches)

        # If no metrics found, default to COUNT(*) for the entity
        if not metric_matches and entity:
            entity_def = self._get_entity(entity)
            if entity_def and entity_def.metrics:
                count_metric_name = next(
                    (mn for mn in entity_def.metrics if mn.endswith("_count")),
                    entity_def.metrics[0],
                )
                m = self._get_metric(count_metric_name)
                if m:
                    metric_matches.append(
                        MetricMatch(
                            metric_name=m.name,
                            display_name=m.display_name,
                            expression=m.expression,
                            table=m.table,
                            confidence=0.7,
                            strategy="keyword",
                        )
                    )

        # Check for ambiguous dimensions
        ambiguous = _find_ambiguous_dimensions(dim_matches, self.catalog.dimensions)
        if ambiguous:
            logger.info("Ambiguous dimensions detected, returning options")
            return DecompositionResult(
                status="options",
                entity=entity,
                metrics=metric_matches,
                options=ambiguous,
                explanation=(
                    "The query could refer to multiple dimensions. "
                    "Please select which one you mean."
                ),
                confidence=0.4,
                resolution_mode="catalog",
            )

        # Check for outside_scope
        unmatched = _find_unmatched(tokens, stems, metric_matches, dim_matches, entity_matches)
        if not metric_matches and not dim_matches and unmatched:
            logger.info("Outside scope: unmatched terms %s", unmatched)
            return DecompositionResult(
                status="outside_scope",
                outside_catalog=unmatched,
                explanation=(f"These concepts are not in the dataset: {', '.join(unmatched)}"),
                confidence=0.0,
            )

        # Build SQL if resolved
        sql = None
        warnings: list[str] = []
        if metric_matches:
            sql, warnings = self._build_sql(entity, metric_matches, dim_matches)

        if sql:
            explanation = _build_explanation(entity, metric_matches, dim_matches, warnings)
            suggestions = self._build_suggestions(entity, metric_matches, dim_matches)
            confidence = _compute_confidence(metric_matches, dim_matches)
            logger.info(
                "Resolved: entity=%s, %d metrics, %d dimensions, confidence=%.2f",
                entity,
                len(metric_matches),
                len(dim_matches),
                confidence,
            )
            return DecompositionResult(
                status="resolved",
                entity=entity,
                metrics=metric_matches,
                dimensions=dim_matches,
                sql=sql,
                explanation=explanation,
                warnings=warnings,
                suggestions=suggestions,
                confidence=confidence,
                resolution_mode="catalog",
            )

        # Partial resolution -- return what we have
        logger.info(
            "Partial resolution: entity=%s, metrics=%d, dims=%d",
            entity,
            len(metric_matches),
            len(dim_matches),
        )
        return DecompositionResult(
            status="resolved" if metric_matches else "outside_scope",
            entity=entity,
            metrics=metric_matches,
            dimensions=dim_matches,
            explanation="Partially resolved from catalog.",
            outside_catalog=unmatched if not metric_matches else [],
            confidence=_compute_confidence(metric_matches, dim_matches) * 0.5,
            resolution_mode="catalog",
        )

    # -------------------------------------------------------------------
    # Keyword resolution
    # -------------------------------------------------------------------

    def _keyword_resolve_entities(
        self,
        stems: list[str],
    ) -> list[tuple[str, float]]:
        """Find entity matches by stem lookup."""
        matches: list[tuple[str, float]] = []
        seen: set[str] = set()
        for stem in stems:
            for entry_type, name, _table in self._name_index.get(stem, []):
                if entry_type == "entity" and name not in seen:
                    seen.add(name)
                    matches.append((name, 0.9))
        return matches

    def _keyword_resolve_metrics(
        self,
        stems: list[str],
        tokens: list[str],
        intent: str | None,
    ) -> list[MetricMatch]:
        """Find metric matches by stem lookup."""
        matches: list[MetricMatch] = []
        seen: set[str] = set()
        for stem in stems:
            for entry_type, name, _table in self._name_index.get(stem, []):
                if entry_type == "metric" and name not in seen:
                    m = self._get_metric(name)
                    if m:
                        seen.add(name)
                        matches.append(
                            MetricMatch(
                                metric_name=m.name,
                                display_name=m.display_name,
                                expression=m.expression,
                                table=m.table,
                                confidence=0.85,
                                strategy="keyword",
                            )
                        )
        # If intent detected but no metric matched, find one matching the intent
        if intent and not matches:
            for m in self.catalog.metrics:
                if m.agg_type == intent and m.name not in seen:
                    seen.add(m.name)
                    matches.append(
                        MetricMatch(
                            metric_name=m.name,
                            display_name=m.display_name,
                            expression=m.expression,
                            table=m.table,
                            confidence=0.6,
                            strategy="keyword",
                        )
                    )
                    break
        return matches

    def _keyword_resolve_dimensions(
        self,
        stems: list[str],
        tokens: list[str],
    ) -> list[DimensionMatch]:
        """Find dimension matches by stem lookup."""
        matches: list[DimensionMatch] = []
        seen: set[str] = set()
        for stem in stems:
            for entry_type, name, _table in self._name_index.get(stem, []):
                if entry_type == "dimension" and name not in seen:
                    d = self._get_dimension(name)
                    if d:
                        seen.add(name)
                        matches.append(
                            DimensionMatch(
                                dimension_name=d.name,
                                display_name=d.display_name,
                                column=d.column,
                                table=d.table,
                                join_path=d.join_path,
                                confidence=0.85,
                                strategy="keyword",
                                is_filter=False,
                            )
                        )
        return matches

    # -------------------------------------------------------------------
    # Embedding resolution (Strategy B)
    # -------------------------------------------------------------------

    def _embedding_resolve(
        self,
        question: str,
        tokens: list[str],
        vector_store: Any,
        project_id: str | None,
        metric_matches: list[MetricMatch],
        dim_matches: list[DimensionMatch],
        entity_matches: list[tuple[str, float]],
    ) -> None:
        """Use vector similarity to fill gaps in keyword resolution."""
        matched_names = {m.metric_name for m in metric_matches}
        matched_dims = {d.dimension_name for d in dim_matches}

        results = vector_store.search(
            question,
            project_id=project_id,
            limit=5,
        )

        for r in results:
            dist = r.get("_distance", 1.0)
            if dist > 1.0:
                continue  # Too far

            name = r["name"]
            entry_type = r["entry_type"]
            confidence = max(0.3, 1.0 - dist)

            if entry_type == "metric" and name not in matched_names:
                m = self._get_metric(name)
                if m:
                    matched_names.add(name)
                    metric_matches.append(
                        MetricMatch(
                            metric_name=m.name,
                            display_name=m.display_name,
                            expression=m.expression,
                            table=m.table,
                            confidence=round(confidence, 3),
                            strategy="embedding",
                        )
                    )
                    logger.debug(
                        "Embedding matched metric: %s (distance=%.3f)",
                        name,
                        dist,
                    )

            elif entry_type == "dimension" and name not in matched_dims:
                d = self._get_dimension(name)
                if d:
                    matched_dims.add(name)
                    dim_matches.append(
                        DimensionMatch(
                            dimension_name=d.name,
                            display_name=d.display_name,
                            column=d.column,
                            table=d.table,
                            join_path=d.join_path,
                            confidence=round(confidence, 3),
                            strategy="embedding",
                            is_filter=False,
                        )
                    )
                    logger.debug(
                        "Embedding matched dimension: %s (distance=%.3f)",
                        name,
                        dist,
                    )

            elif entry_type == "entity":
                if not any(e[0] == name for e in entity_matches):
                    entity_matches.append((name, confidence))

    # -------------------------------------------------------------------
    # Entity resolution
    # -------------------------------------------------------------------

    def _pick_entity(
        self,
        entity_matches: list[tuple[str, float]],
        metric_matches: list[MetricMatch],
        dim_matches: list[DimensionMatch],
    ) -> str | None:
        """Pick the best entity from matches and metric/dimension tables."""
        # Direct entity match wins
        if entity_matches:
            entity_matches.sort(key=lambda x: x[1], reverse=True)
            return entity_matches[0][0]

        # Infer from metric table
        if metric_matches:
            table = metric_matches[0].table
            entity = self._get_entity_by_table(table)
            if entity:
                return entity.name

        # Infer from dimension table
        if dim_matches:
            table = dim_matches[0].table
            entity = self._get_entity_by_table(table)
            if entity:
                return entity.name

        return None

    # -------------------------------------------------------------------
    # SQL generation (deterministic from catalog)
    # -------------------------------------------------------------------

    def _build_sql(
        self,
        entity: str | None,
        metrics: list[MetricMatch],
        dimensions: list[DimensionMatch],
    ) -> tuple[str | None, list[str]]:
        """Build SQL deterministically from resolved catalog components.

        Returns (sql_string, warnings_list).
        """
        if not metrics:
            return None, []

        warnings: list[str] = []
        primary_table = metrics[0].table

        # Collect all tables involved
        tables_needed: dict[str, str] = {primary_table: primary_table}
        for d in dimensions:
            if d.table != primary_table:
                tables_needed[d.table] = d.table

        # Build SELECT
        select_parts: list[str] = []
        group_by_parts: list[str] = []

        # Add dimension columns
        for d in dimensions:
            if d.is_filter:
                continue
            col_ref = f'"{d.table}"."{d.column}"'
            alias = d.display_name.lower().replace(" ", "_")
            select_parts.append(f'{col_ref} AS "{alias}"')
            group_by_parts.append(col_ref)

        # Add metric expressions
        for m in metrics:
            alias = m.display_name.lower().replace(" ", "_")
            # Prefix expression columns with table if needed
            expr = m.expression
            select_parts.append(f'{expr} AS "{alias}"')

        if not select_parts:
            return None, warnings

        # Build FROM + JOINs
        from_clause = f'"{primary_table}"'
        join_clauses: list[str] = []

        for d in dimensions:
            if d.table == primary_table:
                continue
            if d.join_path:
                join_info = _parse_join_path(d.join_path)
                if join_info:
                    from_t, from_c, to_t, to_c = join_info
                    # Check nullable join
                    dim_def = self._get_dimension(d.dimension_name)
                    join_type = "LEFT JOIN" if (dim_def and dim_def.join_nullable) else "JOIN"
                    join_clause = (
                        f'{join_type} "{to_t}" ON "{from_t}"."{from_c}" = "{to_t}"."{to_c}"'
                    )
                    if join_clause not in join_clauses:
                        join_clauses.append(join_clause)
                    if dim_def and dim_def.join_nullable:
                        warnings.append(
                            f"Using LEFT JOIN for {d.display_name} "
                            f"({from_t}.{from_c} has low referential integrity)"
                        )

        # Build WHERE for filter dimensions
        where_parts: list[str] = []
        for d in dimensions:
            if d.is_filter and d.filter_value:
                col_ref = f'"{d.table}"."{d.column}"'
                where_parts.append(f"{col_ref} = '{d.filter_value}'")

        # Check metric columns for NULL warnings
        for m in metrics:
            metric_def = self._get_metric(m.metric_name)
            if metric_def and metric_def.column:
                for dim in self.catalog.dimensions:
                    if dim.table == m.table:
                        break
                # Check if description mentions NULL
                if metric_def.description and "NULL" in metric_def.description:
                    warnings.append(
                        f"{metric_def.display_name}: some values are NULL "
                        f"(aggregation excludes NULLs)"
                    )

        # Assemble
        sql = f"SELECT {', '.join(select_parts)}\nFROM {from_clause}"
        if join_clauses:
            sql += "\n" + "\n".join(join_clauses)
        if where_parts:
            sql += "\nWHERE " + " AND ".join(where_parts)
        if group_by_parts:
            sql += "\nGROUP BY " + ", ".join(group_by_parts)
            sql += "\nORDER BY " + select_parts[-1].split(" AS ")[0] + " DESC"

        logger.debug("Generated SQL:\n%s", sql)
        return sql, warnings

    # -------------------------------------------------------------------
    # Follow-up suggestions
    # -------------------------------------------------------------------

    def _build_suggestions(
        self,
        entity: str | None,
        metrics: list[MetricMatch],
        dimensions: list[DimensionMatch],
    ) -> list[str]:
        """Generate follow-up suggestions from catalog cross-products."""
        suggestions: list[str] = []
        if not entity:
            return suggestions

        entity_def = self._get_entity(entity)
        if not entity_def:
            return suggestions

        used_dims = {d.dimension_name for d in dimensions}
        used_metrics = {m.metric_name for m in metrics}

        # Suggest other dimensions for the same metric
        for dim_name in entity_def.dimensions:
            if dim_name in used_dims:
                continue
            d = self._get_dimension(dim_name)
            if d and d.confidence >= 0.5:
                m_display = metrics[0].display_name if metrics else "count"
                suggestions.append(f"{m_display} by {d.display_name}")
                if len(suggestions) >= 3:
                    break

        # Suggest other metrics for the same dimensions
        for metric_name in entity_def.metrics:
            if metric_name in used_metrics:
                continue
            m = self._get_metric(metric_name)
            if m and m.confidence >= 0.5 and dimensions:
                d_display = dimensions[0].display_name
                suggestions.append(f"{m.display_name} by {d_display}")
                if len(suggestions) >= 5:
                    break

        return suggestions[:5]

    # -------------------------------------------------------------------
    # Catalog lookups
    # -------------------------------------------------------------------

    def _get_metric(self, name: str) -> MetricDefinition | None:
        return next((m for m in self.catalog.metrics if m.name == name), None)

    def _get_dimension(self, name: str) -> DimensionDefinition | None:
        return next((d for d in self.catalog.dimensions if d.name == name), None)

    def _get_entity(self, name: str) -> EntityDefinition | None:
        return next((e for e in self.catalog.entities if e.name == name), None)

    def _get_entity_by_table(self, table: str) -> EntityDefinition | None:
        return next((e for e in self.catalog.entities if e.table == table), None)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _name_tokens(name: str, display_name: str, synonyms: list[str]) -> set[str]:
    """Extract stemmed tokens from a catalog entry for indexing."""
    tokens: set[str] = set()
    for part in re.split(r"[_\s]+", name.lower()):
        if part and len(part) > 1:
            tokens.add(_stem(part))
            tokens.add(part)
    for part in re.split(r"[_\s]+", display_name.lower()):
        if part and len(part) > 1:
            tokens.add(_stem(part))
            tokens.add(part)
    for syn in synonyms:
        for part in re.split(r"[_\s]+", syn.lower()):
            if part and len(part) > 1:
                tokens.add(_stem(part))
                tokens.add(part)
    return tokens


def _detect_intent(question: str) -> str | None:
    """Detect aggregation intent from the question."""
    q_lower = question.lower()
    for phrase, intent in _INTENT_WORDS.items():
        if phrase in q_lower:
            return intent
    return None


def _find_ambiguous_dimensions(
    matches: list[DimensionMatch],
    all_dims: list[DimensionDefinition],
) -> list[DimensionOption]:
    """Check if matched dimensions are ambiguous (multiple dims for same concept)."""
    if len(matches) <= 1:
        return []

    # Check if multiple dims share the same stem (e.g. zone_geography and zone_type)
    stem_groups: dict[str, list[DimensionMatch]] = {}
    for m in matches:
        stem = _stem(m.column)
        stem_groups.setdefault(stem, []).append(m)

    options: list[DimensionOption] = []
    for _stem_key, group in stem_groups.items():
        if len(group) > 1:
            for dm in group:
                d = next((d for d in all_dims if d.name == dm.dimension_name), None)
                if d:
                    options.append(
                        DimensionOption(
                            dimension_name=d.name,
                            display_name=d.display_name,
                            description=d.description,
                            sample_values=d.sample_values[:5],
                            confidence=dm.confidence,
                        )
                    )

    return options


def _find_unmatched(
    tokens: list[str],
    stems: list[str],
    metrics: list[MetricMatch],
    dims: list[DimensionMatch],
    entities: list[tuple[str, float]],
) -> list[str]:
    """Find question tokens that didn't match anything in the catalog."""
    matched_stems: set[str] = set()

    for m in metrics:
        for part in re.split(r"[_\s]+", m.metric_name.lower()):
            matched_stems.add(_stem(part))
            matched_stems.add(part)
    for d in dims:
        for part in re.split(r"[_\s]+", d.dimension_name.lower()):
            matched_stems.add(_stem(part))
            matched_stems.add(part)
    for e_name, _ in entities:
        for part in re.split(r"[_\s]+", e_name.lower()):
            matched_stems.add(_stem(part))
            matched_stems.add(part)

    # Also count intent words as matched
    for word in _INTENT_WORDS:
        for part in word.split():
            matched_stems.add(_stem(part))
            matched_stems.add(part)

    unmatched = []
    for token, stem in zip(tokens, stems, strict=False):
        if stem not in matched_stems and token not in matched_stems:
            unmatched.append(token)

    return unmatched


def _parse_join_path(join_path: str) -> tuple[str, str, str, str] | None:
    """Parse 'from_table.from_col -> to_table.to_col' into components."""
    match = re.match(r"(\w+)\.(\w+)\s*->\s*(\w+)\.(\w+)", join_path)
    if match:
        return match.group(1), match.group(2), match.group(3), match.group(4)
    return None


def _build_explanation(
    entity: str | None,
    metrics: list[MetricMatch],
    dimensions: list[DimensionMatch],
    warnings: list[str],
) -> str:
    """Build a human-readable explanation of how the query was resolved."""
    parts = []
    if metrics:
        m_names = ", ".join(m.display_name for m in metrics)
        parts.append(f"Computed {m_names}")
    if dimensions:
        non_filter = [d for d in dimensions if not d.is_filter]
        filters = [d for d in dimensions if d.is_filter]
        if non_filter:
            d_names = ", ".join(d.display_name for d in non_filter)
            parts.append(f"grouped by {d_names}")
        if filters:
            f_names = ", ".join(f"{d.display_name} = {d.filter_value}" for d in filters)
            parts.append(f"filtered to {f_names}")
    if entity:
        parts.append(f"from {entity}")

    explanation = " ".join(parts) + "."
    if warnings:
        explanation += " Note: " + "; ".join(warnings)
    return explanation


def _compute_confidence(
    metrics: list[MetricMatch],
    dimensions: list[DimensionMatch],
) -> float:
    """Compute overall confidence from matched components."""
    if not metrics and not dimensions:
        return 0.0
    confs = [m.confidence for m in metrics] + [d.confidence for d in dimensions]
    return round(sum(confs) / len(confs), 3)
