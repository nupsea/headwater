"""Schema graph -- queryable in-memory representation of discovered metadata.

The foundation layer for NL-to-SQL. Builds from DiscoveryResult and provides:

- **Entity resolution**: map natural language terms to tables and columns with
  confidence scores, using name matching, stem matching, description matching,
  and top-value matching.
- **Join path finding**: BFS shortest path between any two tables via declared
  or inferred foreign key relationships.
- **Column role classification**: every column is assigned a role (metric,
  dimension, temporal, identifier, text, geographic) using semantic types,
  profiling data, and the shared classification rules.
- **Cross-table column lookup**: find a column by name across ALL tables,
  not just the primary table -- essential for questions like "complaints per
  county" where county may live in a different table.

Built once per ask() call; cheap enough to construct on every request since
it's pure in-memory indexing over the already-loaded DiscoveryResult.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field

from headwater.core.classification import is_dimension_column, is_metric_column
from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    DiscoveryResult,
    GeneratedModel,
    Relationship,
    TableInfo,
)

# ── Column roles ─────────────────────────────────────────────────────────────

ROLE_METRIC = "metric"
ROLE_DIMENSION = "dimension"
ROLE_TEMPORAL = "temporal"
ROLE_IDENTIFIER = "identifier"
ROLE_GEOGRAPHIC = "geographic"
ROLE_TEXT = "text"
ROLE_UNKNOWN = "unknown"

_TEMPORAL_DTYPES = ("timestamp", "date", "time", "datetime")
# Temporal name patterns -- match standalone temporal words and suffixes,
# but NOT metric names like "days_with_aqi", "good_days", "unhealthy_days".
# Uses word boundaries and specific suffix patterns to avoid false positives.
_TEMPORAL_NAME_RE = re.compile(
    r"("
    r"date|_date|date_"    # date as standalone/prefix/suffix
    r"|timestamp|datetime"  # explicit temporal types
    r"|_time$|^time_"       # time as suffix/prefix (not "time" embedded)
    r"|^month$|_month$|^month_"  # month standalone/suffix/prefix
    r"|^year$|_year$|^year_"     # year standalone/suffix/prefix
    r"|^day$|_day$|^day_"        # day standalone/suffix/prefix (not "days_with_aqi")
    r"|^week$|_week$|^week_"     # week
    r"|^quarter$|_quarter$"      # quarter
    r"|^period$|_period$"        # period
    r"|_at$|_ts$"                # common timestamp suffixes
    r")",
    re.IGNORECASE,
)
_ID_NAME_RE = re.compile(
    r"(_id$|_key$|_fk$|_pk$|^id$|^key$|^uuid$)", re.IGNORECASE,
)


# ── Data classes ─────────────────────────────────────────────────────────────


@dataclass
class ColumnNode:
    """A column with its classification and profiling data."""

    table_name: str
    info: ColumnInfo
    profile: ColumnProfile | None
    role: str  # one of ROLE_* constants


@dataclass
class TableNode:
    """A table with pre-classified column lists."""

    info: TableInfo
    columns: dict[str, ColumnNode]  # col_name -> ColumnNode
    metrics: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    temporals: list[str] = field(default_factory=list)


@dataclass
class EntityMatch:
    """A schema entity that matched a natural language term."""

    table_name: str
    column_name: str | None  # None = table-level match
    score: float
    match_type: str  # "exact", "stem", "description", "value", "semantic_type"
    role: str | None = None  # column role, if column match


@dataclass
class JoinStep:
    """One hop in a join path between two tables."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    rel_type: str


# ── Schema graph ─────────────────────────────────────────────────────────────


class SchemaGraph:
    """Queryable index over all discovered tables, columns, and relationships.

    Constructed from a DiscoveryResult.  Every entity-resolution and
    join-path query runs against in-memory dictionaries -- no I/O.
    """

    def __init__(
        self,
        discovery: DiscoveryResult,
        models: list[GeneratedModel] | None = None,
        reviewed_tables: set[str] | None = None,
    ) -> None:
        self.tables: dict[str, TableNode] = {}
        self.relationships: list[Relationship] = list(discovery.relationships)
        self._models = models or []
        self._reviewed_tables = reviewed_tables

        # Build profile index
        profile_index: dict[tuple[str, str], ColumnProfile] = {
            (p.table_name, p.column_name): p for p in discovery.profiles
        }

        # ── Build table nodes with classified columns ────────────────────
        for table in discovery.tables:
            # Gate: only index reviewed tables when filter is provided
            if reviewed_tables is not None and table.name not in reviewed_tables:
                continue
            cols: dict[str, ColumnNode] = {}
            metrics: list[str] = []
            dims: list[str] = []
            temps: list[str] = []

            for col in table.columns:
                profile = profile_index.get((table.name, col.name))
                role = _classify_role(col, profile)
                cols[col.name] = ColumnNode(table.name, col, profile, role)

                if role == ROLE_METRIC:
                    metrics.append(col.name)
                elif role == ROLE_DIMENSION:
                    dims.append(col.name)
                elif role == ROLE_TEMPORAL:
                    temps.append(col.name)

            self.tables[table.name] = TableNode(
                info=table,
                columns=cols,
                metrics=metrics,
                dimensions=dims,
                temporals=temps,
            )

        # ── Build indexes for fast lookup ────────────────────────────────

        # column_name (lowercase) -> list of ColumnNodes across all tables
        self._col_by_name: dict[str, list[ColumnNode]] = defaultdict(list)

        # word -> list of table names that contain that word
        self._word_to_tables: dict[str, list[str]] = defaultdict(list)

        # word -> list of (table_name, col_name) tuples
        self._word_to_cols: dict[str, list[tuple[str, str]]] = defaultdict(list)

        # value (from top_values) -> list of (table_name, col_name)
        self._value_index: dict[str, list[tuple[str, str]]] = defaultdict(list)

        for tname, tnode in self.tables.items():
            # Index table name words
            for word in _split_name(tname):
                self._word_to_tables[word].append(tname)
            if tnode.info.domain:
                for word in tnode.info.domain.lower().split():
                    self._word_to_tables[word].append(tname)

            for cname, cnode in tnode.columns.items():
                # Index by exact lowercase name
                self._col_by_name[cname.lower()].append(cnode)

                # Index column name words
                for word in _split_name(cname):
                    self._word_to_cols[word].append((tname, cname))

                # Index top values for value-based resolution
                if cnode.profile and cnode.profile.top_values:
                    for val, _ in cnode.profile.top_values:
                        for word in val.lower().split():
                            if len(word) >= 3:
                                self._value_index[word].append((tname, cname))

        # ── Build adjacency for BFS join-path finding ────────────────────
        self._adjacency: dict[str, list[tuple[str, Relationship]]] = defaultdict(list)
        for rel in self.relationships:
            self._adjacency[rel.from_table].append((rel.to_table, rel))
            self._adjacency[rel.to_table].append((rel.from_table, rel))

    # ── Table resolution ─────────────────────────────────────────────────

    def resolve_table(self, term: str) -> list[EntityMatch]:
        """Find tables whose name, domain, or description matches *term*.

        Returns matches sorted by descending score.
        """
        matches: list[EntityMatch] = []
        term_lower = term.lower()
        stems = _stem(term_lower)

        for tname, tnode in self.tables.items():
            table_words = set(_split_name(tname))
            score = 0.0
            match_type = ""

            # Exact word match in table name
            if term_lower in table_words:
                score = 10
                match_type = "exact"
            # Stem match against table name words
            elif any(_stem(tw) & stems for tw in table_words):
                score = 7
                match_type = "stem"
            # Domain match
            elif tnode.info.domain and term_lower in tnode.info.domain.lower():
                score = 5
                match_type = "description"
            # Description match
            elif tnode.info.description and term_lower in tnode.info.description.lower():
                score = 4
                match_type = "description"

            if score > 0:
                matches.append(EntityMatch(tname, None, score, match_type))

        return sorted(matches, key=lambda m: -m.score)

    # ── Column resolution ────────────────────────────────────────────────

    def resolve_column(
        self,
        term: str,
        preferred_table: str | None = None,
    ) -> list[EntityMatch]:
        """Find columns matching *term* across **all** tables.

        Columns in *preferred_table* get a score boost so that same-table
        matches outrank cross-table matches when both exist.

        Returns matches sorted by descending score.
        """
        matches: list[EntityMatch] = []
        term_lower = term.lower()
        stems = _stem(term_lower)

        for tname, tnode in self.tables.items():
            table_boost = 5.0 if tname == preferred_table else 0.0

            for cname, cnode in tnode.columns.items():
                col_lower = cname.lower()
                col_words = set(_split_name(cname))
                score = 0.0
                match_type = ""

                # Full column name match (e.g. "county" == "county")
                if term_lower == col_lower:
                    score = 12 + table_boost
                    match_type = "exact"
                # Exact word match within compound name (e.g. "type" in "complaint_type")
                elif term_lower in col_words:
                    score = 10 + table_boost
                    match_type = "exact"
                # Stem match (e.g. "complaints" ~ "complaint_type")
                elif any(_stem(cw) & stems for cw in col_words):
                    score = 6 + table_boost
                    match_type = "stem"
                # Description match
                elif cnode.info.description and term_lower in cnode.info.description.lower():
                    score = 4 + table_boost
                    match_type = "description"
                # Semantic type match
                elif cnode.info.semantic_type and term_lower in cnode.info.semantic_type.lower():
                    score = 3 + table_boost
                    match_type = "semantic_type"

                if score > 0:
                    matches.append(
                        EntityMatch(tname, cname, score, match_type, role=cnode.role)
                    )

        # Value-index matches (user might reference a data value, e.g. "Brooklyn")
        if term_lower in self._value_index:
            for tname, cname in self._value_index[term_lower]:
                cnode = self.tables[tname].columns.get(cname)
                if cnode:
                    table_boost = 5.0 if tname == preferred_table else 0.0
                    matches.append(
                        EntityMatch(
                            tname, cname, 2 + table_boost, "value", role=cnode.role,
                        )
                    )

        return sorted(matches, key=lambda m: -m.score)

    # ── Cross-table column search ────────────────────────────────────────

    def find_column_anywhere(
        self,
        term: str,
        role_filter: str | None = None,
    ) -> list[EntityMatch]:
        """Find a column across all tables, optionally filtered by role.

        Unlike resolve_column, this has no preferred-table bias -- useful for
        finding where a column lives when the user's question doesn't name
        the table (e.g. "per county" when county is in a lookup table).
        """
        matches = self.resolve_column(term, preferred_table=None)
        if role_filter:
            matches = [m for m in matches if m.role == role_filter]
        return matches

    # ── Join path finding ────────────────────────────────────────────────

    def find_join_path(
        self,
        from_table: str,
        to_table: str,
        max_hops: int = 3,
    ) -> list[JoinStep] | None:
        """BFS shortest join path between two tables via relationships.

        Returns a list of JoinSteps, or None if no path exists within
        *max_hops*.  Returns an empty list if from == to.
        """
        if from_table == to_table:
            return []
        if from_table not in self.tables or to_table not in self.tables:
            return None

        visited: set[str] = {from_table}
        queue: list[tuple[str, list[JoinStep]]] = [(from_table, [])]

        while queue:
            current, path = queue.pop(0)
            if len(path) >= max_hops:
                continue

            for neighbor, rel in self._adjacency.get(current, []):
                if neighbor in visited:
                    continue
                visited.add(neighbor)

                # Build join step with correct column direction
                if rel.from_table == current:
                    step = JoinStep(
                        current, rel.from_column,
                        neighbor, rel.to_column,
                        rel.type,
                    )
                else:
                    step = JoinStep(
                        current, rel.to_column,
                        neighbor, rel.from_column,
                        rel.type,
                    )

                new_path = path + [step]
                if neighbor == to_table:
                    return new_path
                queue.append((neighbor, new_path))

        return None

    # ── Column accessors ─────────────────────────────────────────────────

    def get_columns_by_role(self, table_name: str, role: str) -> list[ColumnNode]:
        """Return all columns of a given role from a table."""
        tnode = self.tables.get(table_name)
        if not tnode:
            return []
        return [c for c in tnode.columns.values() if c.role == role]

    def get_best_metric(self, table_name: str) -> ColumnNode | None:
        """Return the first metric column for a table, or None."""
        cols = self.get_columns_by_role(table_name, ROLE_METRIC)
        return cols[0] if cols else None

    def get_best_dimension(self, table_name: str) -> ColumnNode | None:
        """Return the best dimension column for a table.

        Prefers human-readable names over codes, and columns whose name
        appears in the table name (table affinity).
        """
        dims = self.get_columns_by_role(table_name, ROLE_DIMENSION)
        if not dims:
            return None

        table_words = set(_split_name(table_name))

        def _rank(c: ColumnNode) -> tuple[int, int]:
            col_words = set(_split_name(c.info.name))
            affinity = 0 if (col_words & table_words) else 1
            name = c.info.name.lower()
            if any(s in name for s in ("_name", "name_", "label", "description")):
                display = 0
            elif any(s in name for s in ("_code", "code_", "_num", "_id", "_key")):
                display = 2
            else:
                display = 1
            return (affinity, display)

        return sorted(dims, key=_rank)[0]

    def get_temporals(self, table_name: str) -> list[ColumnNode]:
        """Return temporal columns, preferring real date/timestamp dtypes."""
        cols = self.get_columns_by_role(table_name, ROLE_TEMPORAL)
        # Sort: actual date/timestamp dtypes first, name-pattern matches second
        def _temporal_rank(c: ColumnNode) -> int:
            return 0 if any(c.info.dtype.lower().startswith(t) for t in _TEMPORAL_DTYPES) else 1

        return sorted(cols, key=_temporal_rank)


# ── Column role classification ───────────────────────────────────────────────


def _classify_role(col: ColumnInfo, profile: ColumnProfile | None) -> str:
    """Classify a column's analytical role using all available metadata.

    Priority order mirrors how a data analyst would think:
    1. Explicit identifiers (PK, name patterns, semantic type)
    2. Temporal columns (dtype or name pattern)
    3. Geographic columns
    4. Text/PII columns
    5. Metrics (numeric, aggregatable)
    6. Dimensions (categorical, groupable)
    7. Unknown
    """
    # ── Identifiers (highest priority) ───────────────────────────────
    if col.is_primary_key:
        return ROLE_IDENTIFIER
    if _ID_NAME_RE.search(col.name):
        return ROLE_IDENTIFIER
    if col.semantic_type in ("id", "foreign_key", "primary_key"):
        return ROLE_IDENTIFIER
    # High-uniqueness numeric columns are likely surrogate IDs
    if (
        profile is not None
        and profile.uniqueness_ratio is not None
        and profile.uniqueness_ratio >= 0.8
        and any(t in (profile.dtype or col.dtype).lower() for t in ("int", "bigint"))
    ):
        return ROLE_IDENTIFIER

    # ── Explicit semantic type overrides (before name-pattern checks) ──
    # If the enrichment pipeline or human explicitly tagged a column as
    # metric/dimension/geographic, respect that over name-pattern heuristics.
    # This prevents "days_with_aqi" (semantic_type="metric") from being
    # misclassified as temporal just because the name contains "day".
    from headwater.core.classification import METRIC_SEMANTIC_TYPES

    if col.semantic_type in METRIC_SEMANTIC_TYPES:
        dtype = (profile.dtype if profile else col.dtype).lower()
        if any(t in dtype for t in ("int", "float", "double", "decimal", "numeric", "real")):
            return ROLE_METRIC
    if col.semantic_type == "dimension":
        return ROLE_DIMENSION

    # ── Temporal ─────────────────────────────────────────────────────
    if any(col.dtype.lower().startswith(t) for t in _TEMPORAL_DTYPES):
        return ROLE_TEMPORAL
    if col.semantic_type == "temporal":
        return ROLE_TEMPORAL
    # Name-pattern temporal (year, month, etc.) with non-text dtype
    if _TEMPORAL_NAME_RE.search(col.name) and col.dtype.lower() not in ("varchar", "text"):
        return ROLE_TEMPORAL

    # ── Geographic ───────────────────────────────────────────────────
    if col.semantic_type == "geographic":
        return ROLE_GEOGRAPHIC

    # ── Text / PII ───────────────────────────────────────────────────
    if col.semantic_type in ("text", "pii"):
        return ROLE_TEXT

    # ── Metric vs Dimension (use shared classification) ──────────────
    if is_metric_column(col, profile):
        return ROLE_METRIC
    if is_dimension_column(col, profile):
        return ROLE_DIMENSION

    return ROLE_UNKNOWN


# ── String utilities ─────────────────────────────────────────────────────────


def _split_name(name: str) -> list[str]:
    """Split a snake_case name into lowercase words."""
    return [w for w in name.lower().replace("_", " ").split() if w]


def _stem(word: str) -> set[str]:
    """Generate plausible stems for matching.

    Not a full stemmer -- handles the common English suffixes that appear
    in BI questions vs schema names.
    """
    w = word.lower()
    stems = {w}

    if w.endswith("ies") and len(w) > 4:
        stems.add(w[:-3] + "y")
    if w.endswith("es") and len(w) > 3:
        stems.add(w[:-2])
    if w.endswith("s") and not w.endswith("ss") and len(w) > 3:
        stems.add(w[:-1])
    if w.endswith("ing") and len(w) > 5:
        stems.add(w[:-3])
        stems.add(w[:-3] + "e")
    if w.endswith("ed") and len(w) > 4:
        stems.add(w[:-2])
        stems.add(w[:-1])
        if w.endswith("ied"):
            stems.add(w[:-3] + "y")
    if w.endswith("ly") and len(w) > 4:
        stems.add(w[:-2])

    return stems
