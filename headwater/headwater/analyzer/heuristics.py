"""Rule-based heuristic descriptions -- fallback when LLM is unavailable."""

from __future__ import annotations

import re
from collections import defaultdict

from headwater.core.models import ColumnProfile, Relationship, TableInfo

# Common column-name suffixes that carry no domain signal
_NOISE_TOKENS = frozenset({
    "id", "date", "name", "type", "code", "at", "status", "flag",
    "count", "rate", "value", "number", "description", "notes",
    "created", "updated", "is", "has", "pct",
})

# Column name patterns -> semantic type
_SEMANTIC_TYPE_PATTERNS: list[tuple[str, str]] = [
    (r".*_id$", "id"),
    (r"^id$", "id"),
    (r".*email.*", "pii"),
    (r".*phone.*", "pii"),
    (r".*ssn.*", "pii"),
    (r".*name$", "dimension"),
    (r".*_name$", "dimension"),
    (r".*type$", "dimension"),
    (r".*_type$", "dimension"),
    (r".*category.*", "dimension"),
    (r".*status.*", "dimension"),
    # Codes, flags, indicators -- numeric but NOT metrics (must precede metric patterns)
    (r".*_code$", "dimension"),
    (r".*code$", "dimension"),
    (r".*flag$", "dimension"),
    (r".*indicator$", "dimension"),
    (r".*number$", "dimension"),
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
            for t2 in unconnected_list[i + 1:]:
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


def enrich_tables(
    tables: list[TableInfo],
    profiles: list[ColumnProfile],
    relationships: list[Relationship],
) -> list[TableInfo]:
    """Enrich tables with heuristic descriptions, domains, and semantic types.

    Locked tables preserve their existing descriptions. Locked columns preserve
    their descriptions and semantic types.
    """
    import logging

    _log = logging.getLogger(__name__)

    # Compute domain labels for all unlocked tables in one pass
    domain_map = classify_domains(tables, relationships)

    enriched: list[TableInfo] = []
    for table in tables:
        locked_col_count = sum(1 for c in table.columns if c.locked)
        if locked_col_count:
            _log.info(
                "Skipped enrichment for %d locked column(s) in table %s",
                locked_col_count, table.name,
            )

        if not table.locked:
            table.description = generate_table_description(table)
            table.domain = domain_map.get(table.name, "General")
        for col in table.columns:
            if col.locked:
                continue  # Preserve human-approved description
            col.description = generate_column_description(col.name, table.name)
            col.semantic_type = classify_semantic_type(col.name)
        enriched.append(table)

    # Build domain groupings
    return enriched


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
