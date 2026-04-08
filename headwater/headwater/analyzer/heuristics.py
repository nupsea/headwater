"""Rule-based heuristic descriptions -- fallback when LLM is unavailable."""

from __future__ import annotations

import re

from headwater.core.models import ColumnProfile, Relationship, TableInfo

# Domain keywords for table-level domain classification
_DOMAIN_KEYWORDS: dict[str, list[str]] = {
    "Environmental Monitoring": [
        "sensor", "reading", "monitor", "air", "water", "quality",
        "pm25", "ozone", "turbidity", "calibration",
    ],
    "Public Health": [
        "incident", "patient", "health", "disease", "exposure",
        "respiratory", "severity", "symptom", "outcome",
    ],
    "Facility & Inspection": [
        "inspection", "violation", "score", "inspector", "compliance",
        "food", "facility", "permit",
    ],
    "Community Engagement": [
        "complaint", "report", "citizen", "priority", "resolution",
        "filed", "acknowledged",
    ],
    "Programs & Interventions": [
        "program", "budget", "enrollment", "funding", "intervention",
        "abatement", "resilience",
    ],
    "Geography & Demographics": [
        "zone", "population", "income", "poverty", "census",
        "area", "demographic", "housing",
    ],
    "Infrastructure": [
        "site", "location", "address", "facility", "commissioned",
        "latitude", "longitude",
    ],
}

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
    (r".*date.*", "temporal"),
    (r".*_at$", "temporal"),
    (r".*timestamp.*", "temporal"),
    (r".*count.*", "metric"),
    (r".*score.*", "metric"),
    (r".*rate.*", "metric"),
    (r".*amount.*", "metric"),
    (r".*budget.*", "metric"),
    (r".*pct_.*", "metric"),
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
    """Classify a table into a business domain based on column names."""
    col_text = " ".join(c.name.lower() for c in table.columns) + " " + table.name.lower()

    best_domain = "General"
    best_score = 0
    for domain, keywords in _DOMAIN_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in col_text)
        if score > best_score:
            best_score = score
            best_domain = domain

    return best_domain


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
    """Enrich tables with heuristic descriptions, domains, and semantic types."""
    enriched: list[TableInfo] = []
    for table in tables:
        table.description = generate_table_description(table)
        table.domain = classify_domain(table)
        for col in table.columns:
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
