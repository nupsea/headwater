"""Semantic catalog builder -- generates ontology from discovery results.

Extracts metrics, dimensions, and entities from enriched tables, profiles,
and relationships. Builds synonym families, computes join paths, and
populates the LanceDB embedding index.

Works at three tiers:
  Tier 0 (heuristic): patterns + profiles + relationships (no LLM)
  Tier 1 (local LLM): + synonym expansion, richer descriptions
  Tier 2 (cloud LLM): + deep semantic inference, domain-aware synonyms
"""

from __future__ import annotations

import logging
import re
from datetime import datetime

from headwater.core.classification import is_dimension_column, is_metric_column
from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    DimensionDefinition,
    DiscoveryResult,
    EntityDefinition,
    MetricDefinition,
    Relationship,
    SemanticCatalog,
    TableInfo,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Built-in synonym families (~60 entries covering common BI vocabulary)
# ---------------------------------------------------------------------------

_GEOGRAPHIC_SYNONYMS: dict[str, list[str]] = {
    "county": ["borough", "district", "parish", "zone", "area", "neighborhood", "region"],
    "city": ["town", "municipality", "metro"],
    "state": ["province", "territory"],
    "country": ["nation"],
    "address": ["location", "street"],
    "zip": ["postal_code", "zipcode"],
}

_TEMPORAL_SYNONYMS: dict[str, list[str]] = {
    "date": ["day", "when"],
    "month": ["period", "month_of"],
    "year": ["fiscal_year", "calendar_year"],
    "quarter": ["q1", "q2", "q3", "q4"],
}

_CATEGORICAL_SYNONYMS: dict[str, list[str]] = {
    "type": ["kind", "category", "classification", "class"],
    "status": ["state", "condition", "phase", "stage"],
    "priority": ["severity", "urgency", "level"],
    "source": ["origin", "channel", "medium"],
    "result": ["outcome", "finding", "conclusion"],
}

_AGG_INTENT_WORDS = {"count", "total", "sum", "average", "avg", "mean", "max", "min"}

# Map from column name pattern to agg_type
_AGG_TYPE_PATTERNS: list[tuple[str, str]] = [
    (r".*_count$|^count$|^total$|^num_", "count"),
    (r".*_sum$|^sum_", "sum"),
    (r".*_avg$|^avg_|.*_average$|^mean_", "avg"),
    (r".*_rate$|.*_ratio$|.*_pct$|^pct_", "avg"),
    (r".*_score$|.*score$", "avg"),
    (r".*_amount$|.*_value$|.*_cost$|.*_price$|.*_budget$", "sum"),
    (r".*_min$", "min"),
    (r".*_max$", "max"),
]

# ---------------------------------------------------------------------------
# Slug / display name helpers
# ---------------------------------------------------------------------------

_SNAKE_RE = re.compile(r"[_\s]+")


def _to_display_name(snake: str) -> str:
    """Convert snake_case to Title Case display name."""
    return _SNAKE_RE.sub(" ", snake).strip().title()


def _metric_name(table: str, col_name: str, agg: str) -> str:
    """Generate a canonical metric name."""
    # e.g. complaints + COUNT(*) -> complaint_count
    singular = table.rstrip("s") if table.endswith("s") else table
    if agg == "count" and col_name == "*":
        return f"{singular}_count"
    return f"{agg}_{table}_{col_name}"


def _infer_agg_type(col: ColumnInfo) -> str:
    """Infer aggregation type from column name patterns."""
    name_lower = col.name.lower()
    for pattern, agg in _AGG_TYPE_PATTERNS:
        if re.match(pattern, name_lower):
            return agg
    # Default: avg for most numeric columns
    return "avg"


# ---------------------------------------------------------------------------
# Synonym expansion
# ---------------------------------------------------------------------------


def _expand_synonyms(name: str, description: str | None, sample_values: list[str]) -> list[str]:
    """Generate synonym list for a dimension from name, description, and values."""
    synonyms: set[str] = set()
    name_lower = name.lower()
    tokens = set(re.split(r"[_\s]+", name_lower))

    # Check built-in synonym families
    for families in (_GEOGRAPHIC_SYNONYMS, _TEMPORAL_SYNONYMS, _CATEGORICAL_SYNONYMS):
        for key, syns in families.items():
            if key in tokens or key in name_lower:
                synonyms.update(syns)
                synonyms.add(key)
            for syn in syns:
                if syn in tokens or syn in name_lower:
                    synonyms.update(syns)
                    synonyms.add(key)
                    break

    # Add individual name tokens as synonyms (if meaningful)
    noise = {"id", "of", "the", "is", "in", "at", "by", "to", "for", "and", "or"}
    for token in tokens:
        if token not in noise and len(token) > 2:
            synonyms.add(token)

    # Remove the dimension name itself
    synonyms.discard(name_lower)
    synonyms.discard(name)

    return sorted(synonyms)


# ---------------------------------------------------------------------------
# Core catalog building
# ---------------------------------------------------------------------------


def build_catalog(
    discovery: DiscoveryResult,
    relationships: list[Relationship] | None = None,
) -> SemanticCatalog:
    """Build a SemanticCatalog from an enriched DiscoveryResult.

    This is the heuristic (Tier 0) builder. It uses:
    - Column roles and semantic types from enrichment
    - Column profiles (cardinality, top_values, null_rate)
    - FK relationships for join paths and dimension detection
    - Built-in synonym families

    Returns a SemanticCatalog with proposed metrics, dimensions, and entities.
    """
    rels = relationships if relationships is not None else discovery.relationships

    # Build lookup maps
    profile_map = _build_profile_map(discovery.profiles)
    rel_map = _build_rel_map(rels)
    pk_tables = _find_pk_tables(discovery.tables)

    metrics: list[MetricDefinition] = []
    dimensions: list[DimensionDefinition] = []
    entities: list[EntityDefinition] = []

    for table in discovery.tables:
        table_metrics = _extract_metrics(table, profile_map)
        table_dimensions = _extract_dimensions(table, profile_map, rel_map, pk_tables)

        metrics.extend(table_metrics)
        dimensions.extend(table_dimensions)

        # Build entity if table has metrics (fact table candidate)
        if table_metrics or _is_fact_table(table, rel_map):
            entity = _build_entity(table, table_metrics, table_dimensions, rel_map, profile_map)
            entities.append(entity)

    # Cross-table dimension linking: add FK-reachable dimensions to entities
    _link_cross_table_dimensions(entities, dimensions, rel_map)

    # Compute overall confidence
    total = len(metrics) + len(dimensions) + len(entities)
    if total > 0:
        avg_conf = (
            (sum(m.confidence for m in metrics) + sum(d.confidence for d in dimensions))
            / (len(metrics) + len(dimensions))
            if (len(metrics) + len(dimensions)) > 0
            else 0.5
        )
    else:
        avg_conf = 0.0

    catalog = SemanticCatalog(
        metrics=metrics,
        dimensions=dimensions,
        entities=entities,
        generated_at=datetime.now(),
        generation_source="heuristic",
        confidence=round(avg_conf, 3),
    )

    logger.info(
        "Built catalog: %d metrics, %d dimensions, %d entities (confidence=%.2f)",
        len(metrics),
        len(dimensions),
        len(entities),
        catalog.confidence,
    )
    return catalog


# ---------------------------------------------------------------------------
# Metric extraction
# ---------------------------------------------------------------------------


def _extract_metrics(
    table: TableInfo,
    profile_map: dict[str, dict[str, ColumnProfile]],
) -> list[MetricDefinition]:
    """Extract metric definitions from a table's columns."""
    logger.debug("Extracting metrics from table %s (%d columns)", table.name, len(table.columns))
    metrics: list[MetricDefinition] = []
    table_profiles = profile_map.get(table.name, {})

    # Always add COUNT(*) for tables with meaningful row counts
    if table.row_count > 0:
        singular = table.name.rstrip("s") if table.name.endswith("s") else table.name
        metrics.append(
            MetricDefinition(
                name=f"{singular}_count",
                display_name=f"Total {_to_display_name(table.name)}",
                description=f"Count of all {table.name} records",
                expression="COUNT(*)",
                column=None,
                table=table.name,
                agg_type="count",
                synonyms=[f"number of {table.name}", f"total {table.name}", f"{singular} total"],
                confidence=0.95,
                source="heuristic",
            )
        )

    for col in table.columns:
        profile = table_profiles.get(col.name)
        if not is_metric_column(col, profile):
            continue

        agg_type = _infer_agg_type(col)
        name = f"{agg_type}_{table.name}_{col.name}"
        display = f"{_to_display_name(agg_type)} {_to_display_name(col.name)}"

        # Build expression
        if agg_type == "count":
            expression = f'COUNT("{col.name}")'
        elif agg_type == "count_distinct":
            expression = f'COUNT(DISTINCT "{col.name}")'
        else:
            expression = f'{agg_type.upper()}("{col.name}")'

        # Null-aware: if column has significant nulls, note in description
        desc_parts = [col.description or f"{col.name} from {table.name}"]
        if profile and profile.null_rate > 0.1:
            desc_parts.append(f"Note: {profile.null_rate:.0%} of values are NULL")

        confidence = col.confidence if col.confidence > 0 else 0.5

        metrics.append(
            MetricDefinition(
                name=name,
                display_name=display,
                description=". ".join(desc_parts),
                expression=expression,
                column=col.name,
                table=table.name,
                agg_type=agg_type,
                synonyms=_metric_synonyms(col.name, agg_type),
                confidence=round(confidence, 3),
                source="heuristic",
            )
        )

    logger.debug("Table %s: extracted %d metrics", table.name, len(metrics))
    return metrics


def _metric_synonyms(col_name: str, agg_type: str) -> list[str]:
    """Generate synonyms for a metric."""
    tokens = re.split(r"[_\s]+", col_name.lower())
    syns: set[str] = set()
    for t in tokens:
        if t not in {"id", "of", "the", "is"} and len(t) > 2:
            syns.add(t)
    if agg_type == "avg":
        syns.update(["average", "mean"])
    elif agg_type == "sum":
        syns.update(["total", "sum"])
    elif agg_type == "count":
        syns.update(["count", "number", "total"])
    return sorted(syns)


# ---------------------------------------------------------------------------
# Dimension extraction
# ---------------------------------------------------------------------------


def _extract_dimensions(
    table: TableInfo,
    profile_map: dict[str, dict[str, ColumnProfile]],
    rel_map: dict[str, list[Relationship]],
    pk_tables: set[str],
) -> list[DimensionDefinition]:
    """Extract dimension definitions from a table's columns."""
    logger.debug("Extracting dimensions from table %s", table.name)
    dimensions: list[DimensionDefinition] = []
    table_profiles = profile_map.get(table.name, {})

    for col in table.columns:
        profile = table_profiles.get(col.name)

        # Skip FK columns -- they become join paths, not dimensions themselves
        if col.semantic_type == "foreign_key":
            continue
        # Skip IDs and PKs
        if col.is_primary_key or col.semantic_type in ("id", "primary_key"):
            continue

        if not is_dimension_column(col, profile):
            continue

        # Get sample values from profile
        sample_values: list[str] = []
        cardinality = 0
        if profile:
            cardinality = profile.distinct_count
            if profile.top_values:
                sample_values = [v for v, _ in profile.top_values[:8]]

        # Determine join path if this table is reachable via FK
        join_path = None
        join_nullable = False
        # Check if other tables have FK pointing to this table
        for rel in rel_map.get(table.name, []):
            if rel.to_table == table.name:
                join_path = f"{rel.from_table}.{rel.from_column} -> {rel.to_table}.{rel.to_column}"
                join_nullable = rel.referential_integrity < 0.5

        name = f"{table.name}_{col.name}"
        synonyms = _expand_synonyms(col.name, col.description, sample_values)
        confidence = col.confidence if col.confidence > 0 else 0.5

        dimensions.append(
            DimensionDefinition(
                name=name,
                display_name=_to_display_name(col.name),
                description=col.description or f"{col.name} from {table.name}",
                column=col.name,
                table=table.name,
                dtype=col.dtype,
                synonyms=synonyms,
                sample_values=sample_values,
                cardinality=cardinality,
                confidence=round(confidence, 3),
                source="heuristic",
                join_path=join_path,
                join_nullable=join_nullable,
            )
        )

    logger.debug("Table %s: extracted %d dimensions", table.name, len(dimensions))
    return dimensions


# ---------------------------------------------------------------------------
# Entity extraction
# ---------------------------------------------------------------------------


def _build_entity(
    table: TableInfo,
    table_metrics: list[MetricDefinition],
    table_dimensions: list[DimensionDefinition],
    rel_map: dict[str, list[Relationship]],
    profile_map: dict[str, dict[str, ColumnProfile]],
) -> EntityDefinition:
    """Build an entity definition for a fact table."""
    # Infer temporal grain from temporal columns
    temporal_grain = None
    for col in table.columns:
        if col.role == "temporal" or col.semantic_type == "temporal":
            profile = profile_map.get(table.name, {}).get(col.name)
            if profile and profile.min_date and profile.max_date:
                temporal_grain = _infer_temporal_grain(profile, table.row_count)
            break

    # Row semantics from semantic_detail or heuristic
    row_semantics = ""
    if table.semantic_detail and table.semantic_detail.row_semantics:
        row_semantics = table.semantic_detail.row_semantics
    else:
        singular = table.name.rstrip("s") if table.name.endswith("s") else table.name
        row_semantics = f"Each row represents one {singular} record"

    description = table.description or f"Table {table.name}"
    if table.semantic_detail and table.semantic_detail.narrative:
        description = table.semantic_detail.narrative

    return EntityDefinition(
        name=table.name,
        display_name=_to_display_name(table.name),
        description=description,
        table=table.name,
        row_semantics=row_semantics,
        metrics=[m.name for m in table_metrics],
        dimensions=[d.name for d in table_dimensions],
        temporal_grain=temporal_grain,
        synonyms=[table.name.rstrip("s")] if table.name.endswith("s") else [],
    )


def _link_cross_table_dimensions(
    entities: list[EntityDefinition],
    all_dimensions: list[DimensionDefinition],
    rel_map: dict[str, list[Relationship]],
) -> None:
    """Add FK-reachable dimensions from other tables to entities."""
    dim_by_table: dict[str, list[str]] = {}
    for d in all_dimensions:
        dim_by_table.setdefault(d.table, []).append(d.name)

    linked_count = 0
    for entity in entities:
        # Find tables reachable via FK from this entity's table
        for rel in rel_map.get(entity.table, []):
            if rel.from_table == entity.table:
                target_dims = dim_by_table.get(rel.to_table, [])
                for dim_name in target_dims:
                    if dim_name not in entity.dimensions:
                        entity.dimensions.append(dim_name)
                        linked_count += 1
    if linked_count:
        logger.debug("Linked %d cross-table dimensions to entities", linked_count)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_profile_map(
    profiles: list[ColumnProfile],
) -> dict[str, dict[str, ColumnProfile]]:
    """Build {table_name: {column_name: ColumnProfile}} lookup."""
    result: dict[str, dict[str, ColumnProfile]] = {}
    for p in profiles:
        result.setdefault(p.table_name, {})[p.column_name] = p
    return result


def _build_rel_map(
    relationships: list[Relationship],
) -> dict[str, list[Relationship]]:
    """Build {table_name: [relationships]} lookup (both directions)."""
    result: dict[str, list[Relationship]] = {}
    for r in relationships:
        result.setdefault(r.from_table, []).append(r)
        result.setdefault(r.to_table, []).append(r)
    return result


def _find_pk_tables(tables: list[TableInfo]) -> set[str]:
    """Find tables that have at least one PK column (likely dimension tables)."""
    result: set[str] = set()
    for t in tables:
        if any(c.is_primary_key for c in t.columns):
            result.add(t.name)
    return result


def _is_fact_table(
    table: TableInfo,
    rel_map: dict[str, list[Relationship]],
) -> bool:
    """Heuristic: a fact table has outgoing FKs to dimension tables."""
    rels = rel_map.get(table.name, [])
    outgoing = [r for r in rels if r.from_table == table.name]
    return len(outgoing) >= 1 and table.row_count >= 10


def _infer_temporal_grain(profile: ColumnProfile, row_count: int) -> str:
    """Infer temporal grain from a date column profile."""
    if not profile.min_date or not profile.max_date:
        return "none"

    try:
        from datetime import datetime as dt

        min_d = dt.fromisoformat(profile.min_date[:10])
        max_d = dt.fromisoformat(profile.max_date[:10])
        span_days = (max_d - min_d).days
    except (ValueError, TypeError):
        return "none"

    if span_days == 0:
        return "snapshot"

    # Ratio of rows to days gives approximate grain
    if row_count <= 0:
        return "none"

    rows_per_day = row_count / max(span_days, 1)

    if rows_per_day >= 10:
        return "event-based"
    if rows_per_day >= 0.8:
        return "daily"
    if rows_per_day >= 0.2:
        return "weekly"
    if rows_per_day >= 0.03:
        return "monthly"
    return "yearly"


# ---------------------------------------------------------------------------
# LanceDB indexing
# ---------------------------------------------------------------------------


def index_catalog(
    catalog: SemanticCatalog,
    project_id: str,
    vector_store,
) -> int:
    """Index all catalog entries in LanceDB for semantic search.

    Returns number of entries indexed.
    """
    entries: list[dict] = []

    for m in catalog.metrics:
        text = (
            f"{m.display_name}. {m.description}. "
            f"Expression: {m.expression}. Table: {m.table}. "
            f"Synonyms: {', '.join(m.synonyms)}."
        )
        entries.append(
            {
                "id": f"metric_{m.name}",
                "entry_type": "metric",
                "name": m.name,
                "display_name": m.display_name,
                "text": text,
            }
        )

    for d in catalog.dimensions:
        values_str = ", ".join(d.sample_values[:5]) if d.sample_values else ""
        text = (
            f"{d.display_name}. {d.description}. "
            f"Column: {d.column} in {d.table}. "
            f"Synonyms: {', '.join(d.synonyms)}. "
            f"Values: {values_str}."
        )
        entries.append(
            {
                "id": f"dimension_{d.name}",
                "entry_type": "dimension",
                "name": d.name,
                "display_name": d.display_name,
                "text": text,
            }
        )

    for e in catalog.entities:
        text = (
            f"{e.display_name}. {e.description}. "
            f"Row semantics: {e.row_semantics}. "
            f"Metrics: {', '.join(e.metrics)}. "
            f"Dimensions: {', '.join(e.dimensions)}."
        )
        entries.append(
            {
                "id": f"entity_{e.name}",
                "entry_type": "entity",
                "name": e.name,
                "display_name": e.display_name,
                "text": text,
            }
        )

    count = vector_store.index_entries(project_id, entries)
    logger.info("Indexed %d catalog entries in LanceDB for project %s", count, project_id)
    return count
