"""Rule-based heuristic descriptions -- fallback when LLM is unavailable."""

from __future__ import annotations

import math
import re
from collections import defaultdict

from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    ColumnSemanticDetail,
    Relationship,
    TableInfo,
    TableSemanticDetail,
)

# Common column-name suffixes that carry no domain signal
_NOISE_TOKENS = frozenset(
    {
        "id",
        "date",
        "name",
        "type",
        "code",
        "at",
        "status",
        "flag",
        "count",
        "rate",
        "value",
        "number",
        "description",
        "notes",
        "created",
        "updated",
        "is",
        "has",
        "pct",
    }
)

# Column name patterns -> semantic type
_SEMANTIC_TYPE_PATTERNS: list[tuple[str, str]] = [
    (r".*_id$", "id"),
    (r"^id$", "id"),
    (r".*email.*", "pii"),
    (r".*phone.*", "pii"),
    (r".*ssn.*", "pii"),
    (r".*name$", "dimension"),
    (r".*_name$", "dimension"),
    (r".*_type($|_\d+$)", "dimension"),  # complaint_type, complaint_type_311
    (r".*type$", "dimension"),
    (r".*category.*", "dimension"),
    (r".*status.*", "dimension"),
    # Common standalone dimension words (administrative, organizational)
    (r"^county$", "dimension"),
    (r"^borough$", "dimension"),
    (r"^city$", "dimension"),
    (r"^state$", "dimension"),
    (r"^country$", "dimension"),
    (r"^region$", "dimension"),
    (r"^district$", "dimension"),
    (r"^ward$", "dimension"),
    (r"^province$", "dimension"),
    (r"^department$", "dimension"),
    (r"^zone$", "dimension"),
    (r"^sector$", "dimension"),
    (r"^priority$", "dimension"),
    (r"^severity$", "dimension"),
    (r"^channel$", "dimension"),
    (r"^source$", "dimension"),
    (r"^platform$", "dimension"),
    (r"^tier$", "dimension"),
    (r"^level$", "dimension"),
    (r"^class$", "dimension"),
    (r"^group$", "dimension"),
    (r"^gender$", "dimension"),
    (r"^race$", "dimension"),
    (r"^ethnicity$", "dimension"),
    # Codes, flags, indicators -- numeric but NOT metrics (must precede metric patterns)
    (r".*_code$", "dimension"),
    (r".*code$", "dimension"),
    (r".*flag$", "dimension"),
    (r".*indicator$", "dimension"),
    (r".*number$", "dimension"),
    (r".*_num$", "dimension"),  # site_num
    (r".*_no$", "dimension"),  # complaint_no
    (r".*_tract$|^tract$", "dimension"),  # census_tract
    (r".*_board$|^board$", "dimension"),  # community_board
    # Temporal singletons
    (r"^year$", "temporal"),
    (r"^month$", "temporal"),
    (r".*date.*", "temporal"),
    (r".*_at$", "temporal"),
    (r".*timestamp.*", "temporal"),
    (r".*_count$|^count$", "metric"),
    (r".*score.*", "metric"),
    (r".*_rate$|^rate$", "metric"),
    (r".*amount.*", "metric"),
    (r".*budget.*", "metric"),
    (r".*pct_.*", "metric"),
    (r".*_value$|^value$", "metric"),
    (r".*_measure$|.*measure_.*", "metric"),
    (r".*latitude.*", "geographic"),
    (r".*longitude.*", "geographic"),
    (r"^lat$", "geographic"),
    (r"^lon$", "geographic"),
    (r".*address.*", "geographic"),
    (r".*description.*", "text"),
    (r".*narrative.*", "text"),
    (r".*notes.*", "text"),
]


def generate_table_description(table: TableInfo) -> str:
    """Generate a heuristic description for a table based on its name and columns."""
    col_names = [c.name for c in table.columns]
    name = table.name

    # Simple template-based descriptions
    desc_parts = [f"Contains {table.row_count} records"]
    if any("_id" in c for c in col_names):
        fk_cols = [c for c in col_names if c.endswith("_id") and c != f"{name[:-1]}_id"]
        if fk_cols:
            refs = ", ".join(c.replace("_id", "") for c in fk_cols[:3])
            desc_parts.append(f"with references to {refs}")

    return f"{_humanize_name(name)} table. {' '.join(desc_parts)}."


def classify_domain(table: TableInfo) -> str:
    """Standalone fallback -- returns 'General'. Use classify_domains() instead."""
    return "General"


def classify_domains(
    tables: list[TableInfo],
    relationships: list[Relationship],
) -> dict[str, str]:
    """Classify every table into a domain using relationship and vocabulary signals.

    Returns ``{table_name: domain_label}`` for all tables.

    Tier 1: tables connected by FK relationships are grouped into clusters.
    Tier 2: unconnected tables with >= 3 shared meaningful column tokens are merged.
    """
    if not tables:
        return {}

    table_names = {t.name for t in tables}

    # --- Tier 1: relationship graph connected components ---
    adj: dict[str, set[str]] = defaultdict(set)
    for rel in relationships:
        if rel.from_table in table_names and rel.to_table in table_names:
            adj[rel.from_table].add(rel.to_table)
            adj[rel.to_table].add(rel.from_table)

    visited: set[str] = set()
    clusters: list[set[str]] = []

    def _bfs(start: str) -> set[str]:
        queue = [start]
        component: set[str] = {start}
        while queue:
            node = queue.pop()
            for neighbour in adj[node]:
                if neighbour not in component:
                    component.add(neighbour)
                    queue.append(neighbour)
        return component

    for tname in table_names:
        if tname not in visited and tname in adj:
            component = _bfs(tname)
            visited.update(component)
            clusters.append(component)

    # Singletons (no relationships)
    unconnected = table_names - visited

    # --- Tier 2: column-vocabulary similarity for unconnected tables ---
    table_lookup = {t.name: t for t in tables}

    def _meaningful_tokens(table: TableInfo) -> set[str]:
        tokens: set[str] = set()
        for col in table.columns:
            for part in col.name.lower().split("_"):
                if part and part not in _NOISE_TOKENS:
                    tokens.add(part)
        # Include table-name tokens too
        for part in table.name.lower().split("_"):
            if part and part not in _NOISE_TOKENS:
                tokens.add(part)
        return tokens

    if unconnected:
        token_map = {name: _meaningful_tokens(table_lookup[name]) for name in unconnected}
        unconnected_list = sorted(unconnected)
        remaining = set(unconnected_list)
        for i, t1 in enumerate(unconnected_list):
            if t1 not in remaining:
                continue
            group: set[str] = {t1}
            for t2 in unconnected_list[i + 1 :]:
                if t2 not in remaining:
                    continue
                shared = token_map[t1] & token_map[t2]
                if len(shared) >= 3:
                    group.add(t2)
            if len(group) > 1:
                remaining -= group
                clusters.append(group)
            else:
                remaining.discard(t1)
                clusters.append(group)

    # --- Label each cluster ---
    result: dict[str, str] = {}
    for cluster in clusters:
        label = _derive_cluster_label(cluster, table_lookup)
        for tname in cluster:
            result[tname] = label

    # Safety net: any table still missing (shouldn't happen)
    for t in tables:
        if t.name not in result:
            result[t.name] = _humanize_name(t.name)

    return result


def _derive_cluster_label(
    cluster: set[str],
    table_lookup: dict[str, TableInfo],
) -> str:
    """Derive a human-readable domain label for a cluster of tables."""
    if len(cluster) == 1:
        name = next(iter(cluster))
        return _humanize_name(name)

    # Try to find a common prefix/root across table names
    names = sorted(cluster)
    prefix = _common_prefix_token(names)
    if prefix:
        return _humanize_name(prefix) + " & Related"

    # Fallback: use the largest table's name
    largest = max(cluster, key=lambda n: table_lookup[n].row_count)
    return _humanize_name(largest) + " & Related"


def _common_prefix_token(names: list[str]) -> str | None:
    """Find the longest shared leading token across a list of snake_case names.

    E.g. ["aqs_sites", "aqs_monitors", "aqs_daily"] -> "aqs"
    """
    token_lists = [n.lower().split("_") for n in names]
    if not token_lists:
        return None

    # Check if all names share the same first token
    first_tokens = {tl[0] for tl in token_lists if tl}
    if len(first_tokens) == 1:
        return first_tokens.pop()
    return None


def generate_column_description(col_name: str, table_name: str) -> str:
    """Generate a heuristic description for a column."""
    human = _humanize_name(col_name)

    if col_name.endswith("_id"):
        prefix = col_name[:-3]
        if _is_likely_pk(col_name, table_name):
            return f"Unique identifier for the {prefix}"
        return f"Reference to {prefix}"

    if col_name.endswith("_date") or col_name.endswith("_at"):
        return f"Timestamp of {human.lower()}"

    if col_name.startswith("pct_"):
        return f"Percentage: {human.lower()}"

    if col_name.startswith("is_") or col_name.startswith("has_"):
        return f"Flag indicating whether {human.lower()}"

    return human


def classify_semantic_type(col_name: str) -> str | None:
    """Classify a column's semantic type from its name."""
    lower = col_name.lower()
    for pattern, sem_type in _SEMANTIC_TYPE_PATTERNS:
        if re.match(pattern, lower):
            return sem_type
    return None


# Confidence levels for different classification sources
_CONF_EXACT_ID_PATTERN = 0.9  # _id$, ^id$
_CONF_SPECIFIC_NAME_PATTERN = 0.85  # .*email.*, .*_count$
_CONF_BROAD_NAME_PATTERN = 0.7  # .*type$, .*name$
_CONF_STANDALONE_DIM = 0.8  # ^county$, ^state$ etc
_CONF_PROFILE_OVERRIDE = 0.75  # Profile-based refinement overrides name
_CONF_PROFILE_INFER = 0.5  # Profile-based inference with no name match
_CONF_NO_SIGNAL = 0.0  # Nothing matched

# Patterns grouped by confidence level
_HIGH_CONF_PATTERNS = {
    r".*_id$",
    r"^id$",
    r".*email.*",
    r".*phone.*",
    r".*ssn.*",
    r".*_count$|^count$",
    r".*score.*",
    r".*_rate$|^rate$",
    r".*amount.*",
    r".*budget.*",
    r".*latitude.*",
    r".*longitude.*",
}
_STANDALONE_DIM_PATTERNS = {
    r"^county$",
    r"^borough$",
    r"^city$",
    r"^state$",
    r"^country$",
    r"^region$",
    r"^district$",
    r"^ward$",
    r"^province$",
    r"^department$",
    r"^zone$",
    r"^sector$",
    r"^priority$",
    r"^severity$",
    r"^channel$",
    r"^source$",
    r"^platform$",
    r"^tier$",
    r"^level$",
    r"^class$",
    r"^group$",
    r"^gender$",
    r"^race$",
    r"^ethnicity$",
}


def classify_semantic_type_with_confidence(col_name: str) -> tuple[str | None, float]:
    """Classify a column's semantic type from its name with confidence score.

    Returns (semantic_type, confidence).
    """
    lower = col_name.lower()
    for pattern, sem_type in _SEMANTIC_TYPE_PATTERNS:
        if re.match(pattern, lower):
            if pattern in _HIGH_CONF_PATTERNS:
                return sem_type, _CONF_EXACT_ID_PATTERN
            if pattern in _STANDALONE_DIM_PATTERNS:
                return sem_type, _CONF_STANDALONE_DIM
            return sem_type, _CONF_BROAD_NAME_PATTERN
    return None, _CONF_NO_SIGNAL


def _refine_semantic_type(
    col: ColumnInfo,
    profile: ColumnProfile,
    row_count: int,
) -> str | None:
    """Refine semantic type using profiling statistics.

    Called AFTER name-pattern classification. Can override or fill in
    NULL semantic_type based on cardinality and uniqueness.
    """
    current = col.semantic_type
    dtype = col.dtype.lower()
    is_numeric = any(t in dtype for t in ("int", "float", "double", "decimal"))

    # Rule 1: High uniqueness overrides "dimension" -> "id"
    # e.g. complaint_number matched .*number$ but has 0.95 uniqueness
    if current == "dimension" and is_numeric and profile.uniqueness_ratio > 0.9:
        return "id"

    # Rule 2: NULL type + numeric + very high uniqueness -> "id"
    if current is None and is_numeric and profile.uniqueness_ratio > 0.9:
        return "id"

    # Rule 3: NULL type + numeric + continuous distribution -> "metric"
    # If it has stddev > 0 and a meaningful range, it's continuous data,
    # not categorical -- even if cardinality is low (e.g. hazardous_days
    # has only 10 distinct values but is clearly a count metric).
    if (
        current is None
        and is_numeric
        and profile.mean is not None
        and profile.stddev is not None
        and profile.stddev > 0
        and profile.min_value is not None
        and profile.max_value is not None
        and profile.max_value > profile.min_value
    ):
        return "metric"

    # Rule 4: NULL type + low cardinality + NOT numeric -> "dimension"
    dim_threshold = max(2, min(200, int(math.sqrt(row_count))))
    if current is None and profile.distinct_count <= dim_threshold and not is_numeric:
        return "dimension"

    # Rule 5: NULL type + low cardinality + numeric but no variance -> "dimension"
    # (e.g. status codes with 5 distinct integer values and no stddev)
    if (
        current is None
        and is_numeric
        and profile.distinct_count <= dim_threshold
        and (profile.stddev is None or profile.stddev == 0)
    ):
        return "dimension"

    # Rule 6: NULL type + numeric + moderate cardinality + has mean -> "metric"
    if (
        current is None
        and is_numeric
        and profile.distinct_count > dim_threshold
        and profile.mean is not None
    ):
        return "metric"

    # Rule 7: NULL type + low cardinality + non-numeric -> "dimension"
    if current is None and profile.distinct_count <= dim_threshold:
        return "dimension"

    return current


def _compute_confidence(
    col: ColumnInfo,
    profile: ColumnProfile | None,
    name_confidence: float,
    row_count: int,
) -> float:
    """Compute confidence as a weighted combination of multiple signals.

    Signals:
    1. Name pattern match (0.0 to 0.9) -- from classify_semantic_type_with_confidence
    2. Dtype agreement -- does the dtype support the assigned role?
    3. Distribution shape -- how clearly metric-like or dimension-like is the profile?
    4. Uniqueness distance from thresholds -- how far from the decision boundary?

    Returns a score from 0.0 to 1.0 that reflects genuine certainty.
    """
    if profile is None:
        # No profile data -- rely solely on name pattern
        return name_confidence

    dtype = col.dtype.lower()
    is_numeric = any(t in dtype for t in ("int", "float", "double", "decimal"))
    dim_threshold = max(2, min(200, int(math.sqrt(row_count)))) if row_count > 0 else 10
    final_type = col.semantic_type

    # Start with name confidence as base
    score = name_confidence

    if name_confidence > 0:
        # Name pattern matched -- add dtype agreement bonus
        if final_type == "metric" and is_numeric and profile.mean is not None:
            score = min(score + 0.1, 1.0)
        elif final_type == "dimension" and not is_numeric:
            score = min(score + 0.05, 1.0)
        elif final_type == "id" and is_numeric and profile.uniqueness_ratio > 0.9:
            score = min(score + 0.1, 1.0)
        return score

    # No name pattern -- profile-only inference. Confidence depends on
    # how far the data is from the decision boundaries.

    # High uniqueness -> ID (very clear signal)
    if is_numeric and profile.uniqueness_ratio > 0.9:
        # Further from 0.9 threshold = higher confidence
        return 0.7 + min(0.2, (profile.uniqueness_ratio - 0.9) * 2)

    # Low cardinality -> dimension
    if profile.distinct_count <= dim_threshold:
        ratio = profile.distinct_count / dim_threshold if dim_threshold > 0 else 0
        # Very low relative cardinality = high confidence dimension
        if ratio < 0.2:
            return 0.75
        if ratio < 0.5:
            return 0.65
        # Near the threshold = low confidence
        return 0.45

    # Numeric with mean -> metric
    if is_numeric and profile.mean is not None:
        signals = 0.0
        # Cardinality well above the dimension threshold = metric signal
        if dim_threshold > 0:
            cardinality_ratio = profile.distinct_count / dim_threshold
            if cardinality_ratio > 3:
                signals += 0.15
            elif cardinality_ratio > 1.5:
                signals += 0.08
        # Has variance (stddev != 0) = continuous metric signal
        if profile.stddev is not None and profile.stddev > 0:
            signals += 0.1
        # Has a reasonable range = metric signal
        if (
            profile.min_value is not None
            and profile.max_value is not None
            and profile.max_value > profile.min_value
        ):
            signals += 0.05
        return 0.55 + signals

    return 0.3  # Genuinely unclear


def enrich_tables(
    tables: list[TableInfo],
    profiles: list[ColumnProfile],
    relationships: list[Relationship],
) -> list[TableInfo]:
    """Enrich tables with heuristic descriptions, domains, semantic types, and confidence.

    Locked tables preserve their existing descriptions. Locked columns preserve
    their descriptions, semantic types, roles, and confidence.

    After individual column classification, applies sibling consistency:
    columns in the same table with similar names and distributions are
    aligned to the majority role.
    """
    import logging

    from headwater.explorer.schema_graph import _classify_role

    _log = logging.getLogger(__name__)

    # Compute domain labels for all unlocked tables in one pass
    domain_map = classify_domains(tables, relationships)

    # Build profile index for fast lookup
    profile_index: dict[tuple[str, str], ColumnProfile] = {
        (p.table_name, p.column_name): p for p in profiles
    }

    enriched: list[TableInfo] = []
    for table in tables:
        locked_col_count = sum(1 for c in table.columns if c.locked)
        if locked_col_count:
            _log.info(
                "Skipped enrichment for %d locked column(s) in table %s",
                locked_col_count,
                table.name,
            )

        if not table.locked:
            table.description = generate_table_description(table)
            table.domain = domain_map.get(table.name, "General")

        # Pass 1: individual column classification
        for col in table.columns:
            if col.locked:
                continue
            col.description = generate_column_description(col.name, table.name)
            sem_type, name_conf = classify_semantic_type_with_confidence(col.name)
            col.semantic_type = sem_type
            col.confidence = name_conf

            profile = profile_index.get((table.name, col.name))
            if profile is not None and table.row_count > 0:
                col.semantic_type = _refine_semantic_type(
                    col,
                    profile,
                    table.row_count,
                )
                col.confidence = _compute_confidence(
                    col,
                    profile,
                    name_conf,
                    table.row_count,
                )

            col.role = _classify_role(col, profile)

        # Pass 2: sibling consistency -- align columns with similar names
        _apply_sibling_consistency(table, profile_index)

        enriched.append(table)

    return enriched


def _apply_sibling_consistency(
    table: TableInfo,
    profile_index: dict[tuple[str, str], ColumnProfile],
) -> None:
    """Align sibling columns to the majority role when they share a name pattern.

    Groups columns by shared tokens (e.g. "days_with_aqi", "good_days",
    "hazardous_days" all share "days"). If a group has >= 3 members and
    the majority share a role, dissenting columns with LOW confidence
    are flipped to match.

    This prevents "moderate_days" = dimension while "good_days" = metric
    just because distinct_count crossed an arbitrary threshold.
    """
    # Only act on unlocked, non-high-confidence columns
    unlocked = [c for c in table.columns if not c.locked and c.confidence < 0.8]
    if len(unlocked) < 3:
        return

    # Group by shared name tokens
    token_groups: dict[str, list[ColumnInfo]] = defaultdict(list)
    for col in unlocked:
        tokens = col.name.lower().replace("_", " ").split()
        for token in tokens:
            if len(token) >= 3 and token not in _NOISE_TOKENS:
                token_groups[token].append(col)

    for _token, group in token_groups.items():
        if len(group) < 3:
            continue

        # Count roles in this group
        role_counts: dict[str, int] = defaultdict(int)
        for col in group:
            if col.role:
                role_counts[col.role] += 1

        if not role_counts:
            continue

        majority_role = max(role_counts, key=role_counts.get)  # type: ignore[arg-type]
        majority_count = role_counts[majority_role]
        total = len(group)

        # Only align if majority is clear (>= 60%)
        if majority_count / total < 0.6:
            continue

        # Align dissenters
        for col in group:
            if col.role != majority_role and col.confidence < 0.75:
                dtype = col.dtype.lower()
                is_numeric = any(t in dtype for t in ("int", "float", "double", "decimal"))

                # Only align if dtype is compatible with the majority role
                if majority_role == "metric" and is_numeric:
                    col.role = "metric"
                    col.semantic_type = "metric"
                    # Boost confidence because sibling agreement is a signal
                    col.confidence = max(col.confidence, 0.65)
                elif majority_role == "dimension":
                    col.role = "dimension"
                    col.semantic_type = "dimension"
                    col.confidence = max(col.confidence, 0.65)


# Confidence threshold below which columns are flagged for review
CONFIDENCE_REVIEW_THRESHOLD = 0.7


def generate_clarifying_questions(
    tables: list[TableInfo],
    profiles: list[ColumnProfile],
) -> dict[str, list[str]]:
    """Generate table-level clarifying questions based on classification analysis.

    Instead of asking about each column individually, groups related columns
    and asks coherent questions about the system's understanding of the table.

    Returns {table_name: [question, ...]} for tables needing review.
    """
    profile_index: dict[tuple[str, str], ColumnProfile] = {
        (p.table_name, p.column_name): p for p in profiles
    }

    questions: dict[str, list[str]] = {}
    for table in tables:
        table_qs: list[str] = []
        if all(c.locked for c in table.columns):
            continue

        # Group columns by role
        role_groups: dict[str, list[ColumnInfo]] = defaultdict(list)
        unclassified: list[ColumnInfo] = []
        low_conf: list[ColumnInfo] = []

        for col in table.columns:
            if col.locked:
                continue
            if col.role is None or col.semantic_type is None:
                unclassified.append(col)
            elif col.confidence < CONFIDENCE_REVIEW_THRESHOLD:
                low_conf.append(col)
                role_groups[col.role].append(col)
            else:
                role_groups[col.role].append(col)

        # Table-level comprehension summary
        metrics = role_groups.get("metric", [])
        dims = role_groups.get("dimension", [])
        ids = role_groups.get("identifier", [])

        # Question 1: Are the metrics correct? (grouped)
        low_conf_metrics = [c for c in metrics if c.confidence < CONFIDENCE_REVIEW_THRESHOLD]
        if low_conf_metrics:
            names = ", ".join(c.name for c in low_conf_metrics)
            if len(low_conf_metrics) > 3:
                sample = ", ".join(c.name for c in low_conf_metrics[:3])
                names = f"{sample} and {len(low_conf_metrics) - 3} more"
            table_qs.append(
                f"Detected {len(metrics)} metric column(s) for aggregation "
                f"(AVG/SUM/COUNT). Uncertain about: {names}. "
                f"Are these numeric measures to aggregate, or categorical codes?"
            )

        # Question 2: Are the dimensions correct?
        low_conf_dims = [c for c in dims if c.confidence < CONFIDENCE_REVIEW_THRESHOLD]
        if low_conf_dims:
            names = ", ".join(c.name for c in low_conf_dims)
            if len(low_conf_dims) > 3:
                sample = ", ".join(c.name for c in low_conf_dims[:3])
                names = f"{sample} and {len(low_conf_dims) - 3} more"
            table_qs.append(
                f"Detected {len(dims)} dimension column(s) for grouping (GROUP BY). "
                f"Uncertain about: {names}. "
                f"Are these categorical values for slicing data?"
            )

        # Question 3: Unclassified columns
        if unclassified:
            names = ", ".join(c.name for c in unclassified)
            table_qs.append(
                f"Could not classify {len(unclassified)} column(s): {names}. "
                f"Please assign roles (metric, dimension, identifier, or other)."
            )

        # Question 4: Inconsistent siblings
        # Find groups where similar columns got different roles
        _check_sibling_inconsistency(table, low_conf, profile_index, table_qs)

        # Question 5: Missing primary key
        has_pk = any(c.is_primary_key for c in table.columns)
        if not has_pk and ids:
            candidates = ", ".join(c.name for c in ids[:3])
            table_qs.append(
                f"No primary key defined. Possible candidates: {candidates}. "
                f"Which column uniquely identifies each row?"
            )
        elif not has_pk and not ids:
            table_qs.append(
                "No primary key detected. Which column (or combination) "
                "uniquely identifies each row?"
            )

        if table_qs:
            questions[table.name] = table_qs

    return questions


def _check_sibling_inconsistency(
    table: TableInfo,
    low_conf: list[ColumnInfo],
    profile_index: dict[tuple[str, str], ColumnProfile],
    table_qs: list[str],
) -> None:
    """Add a question if sibling columns (shared token) have different roles."""
    if len(low_conf) < 2:
        return

    # Group low-confidence columns by shared tokens
    token_cols: dict[str, list[ColumnInfo]] = defaultdict(list)
    for col in low_conf:
        tokens = col.name.lower().replace("_", " ").split()
        for token in tokens:
            if len(token) >= 3 and token not in _NOISE_TOKENS:
                token_cols[token].append(col)

    for token, cols in token_cols.items():
        if len(cols) < 2:
            continue
        roles = {c.role for c in cols if c.role}
        if len(roles) > 1:
            names = ", ".join(c.name for c in cols)
            role_summary = ", ".join(
                f"{r}: {sum(1 for c in cols if c.role == r)}" for r in sorted(roles)
            )
            table_qs.append(
                f"Columns sharing '{token}' have mixed roles ({role_summary}): "
                f"{names}. Should these all be the same role?"
            )
            break  # One inconsistency question per table is enough


def build_domain_map(tables: list[TableInfo]) -> dict[str, list[str]]:
    """Build a domain -> table_names mapping from enriched tables."""
    domains: dict[str, list[str]] = {}
    for t in tables:
        domain = t.domain or "General"
        domains.setdefault(domain, []).append(t.name)
    return domains


def _humanize_name(name: str) -> str:
    """Convert snake_case to Title Case."""
    return name.replace("_", " ").title()


def _is_likely_pk(col_name: str, table_name: str) -> bool:
    """Check if this looks like a primary key for this table."""
    if col_name == "id":
        return True
    prefix = col_name[:-3] if col_name.endswith("_id") else ""
    if table_name.endswith("s") and table_name[:-1] == prefix:
        return True
    return table_name == prefix


# ---------------------------------------------------------------------------
# Deep semantic descriptions (heuristic-only)
# ---------------------------------------------------------------------------


def generate_deep_table_description(
    table: TableInfo,
    profiles: list[ColumnProfile],
    relationships: list[Relationship],
    companion_context: str | None = None,
) -> TableSemanticDetail:
    """Generate a structured semantic description using heuristics only.

    Produces row_semantics, column_groups, temporal_grain, key_dimensions,
    key_metrics, and per-column ColumnSemanticDetail from classification
    data already computed by enrich_tables().
    """

    profile_index = {p.column_name: p for p in profiles if p.table_name == table.name}

    # Classify columns by role
    dims = [c for c in table.columns if c.role == "dimension"]
    metrics = [c for c in table.columns if c.role == "metric"]
    temporals = [c for c in table.columns if c.role == "temporal"]
    identifiers = [c for c in table.columns if c.role == "identifier"]
    geos = [c for c in table.columns if c.role == "geographic"]
    texts = [c for c in table.columns if c.role == "text"]

    # --- Row semantics ---
    row_semantics = _infer_row_semantics(table, dims, metrics, temporals, relationships)

    # --- Temporal grain ---
    temporal_grain = _infer_temporal_grain(temporals, profile_index)

    # --- Column groups ---
    column_groups: dict[str, list[str]] = {}
    if identifiers:
        column_groups["identifiers"] = [c.name for c in identifiers]
    if dims:
        column_groups["dimensions"] = [c.name for c in dims]
    if metrics:
        column_groups["metrics"] = [c.name for c in metrics]
    if temporals:
        column_groups["temporal"] = [c.name for c in temporals]
    if geos:
        column_groups["geographic"] = [c.name for c in geos]
    if texts:
        column_groups["text_fields"] = [c.name for c in texts]

    # --- Per-column semantic detail ---
    semantic_columns: dict[str, ColumnSemanticDetail] = {}
    for col in table.columns:
        profile = profile_index.get(col.name)
        semantic_columns[col.name] = _build_column_semantic_detail(col, profile, table)

    # --- Narrative ---
    narrative = _build_heuristic_narrative(
        table,
        dims,
        metrics,
        temporals,
        geos,
        relationships,
    )

    # --- Business process ---
    business_process = None
    if table.domain and table.domain != "General":
        business_process = f"Part of the {table.domain} domain"
        if relationships:
            related = {r.to_table for r in relationships if r.from_table == table.name}
            related |= {r.from_table for r in relationships if r.to_table == table.name}
            if related:
                business_process += f", connected to {', '.join(sorted(related)[:3])}"

    return TableSemanticDetail(
        narrative=narrative,
        row_semantics=row_semantics,
        business_process=business_process,
        temporal_grain=temporal_grain,
        key_dimensions=[c.name for c in dims],
        key_metrics=[c.name for c in metrics],
        column_groups=column_groups,
        semantic_columns=semantic_columns,
        companion_context=companion_context,
        inference_confidence=0.4,  # Heuristic-only = lower confidence
    )


def _infer_row_semantics(
    table: TableInfo,
    dims: list[ColumnInfo],
    metrics: list[ColumnInfo],
    temporals: list[ColumnInfo],
    relationships: list[Relationship],
) -> str:
    """Infer what each row in the table represents."""
    name = _humanize_name(table.name)
    # Singular form of table name for row description
    singular = name.rstrip("s") if name.endswith("s") and len(name) > 2 else name

    parts = [f"Each row represents a {singular.lower()} record"]

    if temporals:
        temporal_names = ", ".join(c.name for c in temporals[:2])
        parts.append(f"with temporal context ({temporal_names})")

    if dims:
        dim_names = ", ".join(c.name for c in dims[:3])
        suffix = f" and {len(dims) - 3} more" if len(dims) > 3 else ""
        parts.append(f"categorized by {dim_names}{suffix}")

    if metrics:
        metric_names = ", ".join(c.name for c in metrics[:3])
        suffix = f" and {len(metrics) - 3} more" if len(metrics) > 3 else ""
        parts.append(f"measuring {metric_names}{suffix}")

    # FK references give entity context
    fk_targets = set()
    for rel in relationships:
        if rel.from_table == table.name:
            fk_targets.add(rel.to_table)
    if fk_targets:
        refs = ", ".join(sorted(fk_targets)[:3])
        parts.append(f"linked to {refs}")

    return ", ".join(parts) + "."


def _infer_temporal_grain(
    temporals: list[ColumnInfo],
    profile_index: dict[str, ColumnProfile],
) -> str | None:
    """Infer temporal grain from temporal column profiles."""
    if not temporals:
        return None

    for col in temporals:
        profile = profile_index.get(col.name)
        if profile is None:
            continue

        # Check column name for grain hints
        lower = col.name.lower()
        if "year" in lower:
            return "yearly"
        if "month" in lower:
            return "monthly"
        if "week" in lower:
            return "weekly"

        # Check date range vs distinct count
        if profile.min_date and profile.max_date and profile.distinct_count > 0:
            from datetime import datetime

            try:
                min_dt = datetime.fromisoformat(profile.min_date)
                max_dt = datetime.fromisoformat(profile.max_date)
                span_days = (max_dt - min_dt).days
                if span_days <= 0:
                    continue

                # Ratio of distinct dates to span gives grain estimate
                ratio = profile.distinct_count / span_days
                if ratio > 0.8:
                    return "daily"
                if ratio > 0.2:
                    return "weekly"
                if ratio > 0.02:
                    return "monthly"
                return "yearly"
            except (ValueError, TypeError):
                continue

    return "event-based"


def _build_column_semantic_detail(
    col: ColumnInfo,
    profile: ColumnProfile | None,
    table: TableInfo,
) -> ColumnSemanticDetail:
    """Build a ColumnSemanticDetail from heuristic analysis."""

    business_desc = col.description  # Start with existing heuristic description

    # Enhance with context from profile
    data_quality_notes = None
    if profile is not None:
        quality_parts = []
        if profile.null_rate > 0.05:
            quality_parts.append(f"{profile.null_rate:.0%} null values")
        if profile.uniqueness_ratio > 0.99:
            quality_parts.append("nearly all values are unique")
        elif profile.distinct_count <= 5:
            quality_parts.append(f"only {profile.distinct_count} distinct values")
        if quality_parts:
            data_quality_notes = "; ".join(quality_parts)

    # Determine semantic group based on role
    group_map = {
        "identifier": "identifiers",
        "dimension": "dimensions",
        "metric": "measurements",
        "temporal": "temporal",
        "geographic": "geographic",
        "text": "text_fields",
    }
    semantic_group = group_map.get(col.role or "")

    # Example interpretation for metrics
    example_interpretation = None
    if col.role == "metric" and profile is not None and profile.mean is not None:
        example_interpretation = f"Typical value around {profile.mean:.1f}"
        if profile.min_value is not None and profile.max_value is not None:
            example_interpretation += (
                f", ranging from {profile.min_value:.1f} to {profile.max_value:.1f}"
            )

    return ColumnSemanticDetail(
        business_description=business_desc,
        data_quality_notes=data_quality_notes,
        semantic_group=semantic_group,
        example_interpretation=example_interpretation,
    )


def _build_heuristic_narrative(
    table: TableInfo,
    dims: list[ColumnInfo],
    metrics: list[ColumnInfo],
    temporals: list[ColumnInfo],
    geos: list[ColumnInfo],
    relationships: list[Relationship],
) -> str:
    """Build a multi-sentence narrative about the table from heuristics."""
    name = _humanize_name(table.name)
    parts = [f"The {name} table contains {table.row_count:,} records."]

    if metrics and dims:
        parts.append(
            f"It captures {len(metrics)} measurement(s) "
            f"across {len(dims)} categorical dimension(s)."
        )
    elif metrics:
        parts.append(f"It tracks {len(metrics)} measurement(s).")
    elif dims:
        parts.append(f"It contains {len(dims)} categorical dimension(s).")

    if temporals:
        parts.append("The data has a temporal component, enabling time-based analysis.")

    if geos:
        parts.append("Geographic information is present, supporting spatial analysis.")

    # Relationship context
    related_tables = set()
    for rel in relationships:
        if rel.from_table == table.name:
            related_tables.add(rel.to_table)
        elif rel.to_table == table.name:
            related_tables.add(rel.from_table)
    if related_tables:
        refs = ", ".join(sorted(related_tables)[:4])
        parts.append(f"Related to: {refs}.")

    if table.domain and table.domain != "General":
        parts.append(f"Domain: {table.domain}.")

    return " ".join(parts)
