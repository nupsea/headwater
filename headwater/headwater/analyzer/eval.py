"""Catalog quality evaluation -- scores the semantic catalog.

Measures:
  - Coverage: what fraction of analytical columns are in the catalog
  - SQL validity: do all metric expressions parse in DuckDB
  - Synonym coverage: do built-in synonym families cover key dimensions
  - Ambiguity detection: warn when multiple dimensions share synonyms
  - Overall confidence: weighted composite of the above
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import duckdb

from headwater.core.models import (
    ColumnProfile,
    MetricDefinition,
    SemanticCatalog,
    TableInfo,
)

logger = logging.getLogger(__name__)


@dataclass
class CatalogEvaluation:
    """Result of evaluating a semantic catalog's quality."""

    # Coverage
    total_analytical_columns: int = 0
    columns_in_catalog: int = 0
    coverage: float = 0.0

    # SQL validity
    total_expressions: int = 0
    valid_expressions: int = 0
    sql_validity: float = 0.0
    invalid_expressions: list[dict[str, str]] = field(default_factory=list)

    # Synonym quality
    dimensions_with_synonyms: int = 0
    total_dimensions: int = 0
    synonym_coverage: float = 0.0

    # Ambiguity
    ambiguous_synonyms: list[dict[str, list[str]]] = field(default_factory=list)

    # Overall
    confidence: float = 0.0
    warnings: list[str] = field(default_factory=list)


def evaluate_catalog(
    catalog: SemanticCatalog,
    tables: list[TableInfo],
    profiles: list[ColumnProfile],
) -> CatalogEvaluation:
    """Evaluate catalog quality across multiple dimensions.

    Args:
        catalog: The semantic catalog to evaluate.
        tables: The source tables (for coverage calculation).
        profiles: Column profiles (for analytical column identification).

    Returns:
        CatalogEvaluation with scores and warnings.
    """
    ev = CatalogEvaluation()

    _eval_coverage(ev, catalog, tables, profiles)
    _eval_sql_validity(ev, catalog, tables)
    _eval_synonyms(ev, catalog)
    _eval_ambiguity(ev, catalog)
    _compute_overall(ev)

    logger.info(
        "Catalog evaluation: coverage=%.2f, sql_validity=%.2f, synonym_coverage=%.2f, overall=%.2f",
        ev.coverage,
        ev.sql_validity,
        ev.synonym_coverage,
        ev.confidence,
    )
    return ev


# ---------------------------------------------------------------------------
# Coverage: what fraction of analytical columns are represented
# ---------------------------------------------------------------------------


def _eval_coverage(
    ev: CatalogEvaluation,
    catalog: SemanticCatalog,
    tables: list[TableInfo],
    profiles: list[ColumnProfile],
) -> None:
    """Compute what fraction of analytical columns appear in the catalog."""
    from headwater.core.classification import is_dimension_column, is_metric_column

    profile_map: dict[tuple[str, str], ColumnProfile] = {
        (p.table_name, p.column_name): p for p in profiles
    }

    # Identify all analytical columns (metrics + dimensions)
    analytical_cols: set[tuple[str, str]] = set()
    for table in tables:
        for col in table.columns:
            prof = profile_map.get((table.name, col.name))
            if is_metric_column(col, prof) or is_dimension_column(col, prof):
                analytical_cols.add((table.name, col.name))

    # Columns represented in catalog
    catalog_cols: set[tuple[str, str]] = set()
    for m in catalog.metrics:
        if m.column:
            catalog_cols.add((m.table, m.column))
    for d in catalog.dimensions:
        catalog_cols.add((d.table, d.column))

    ev.total_analytical_columns = len(analytical_cols)
    ev.columns_in_catalog = len(analytical_cols & catalog_cols)
    ev.coverage = (
        ev.columns_in_catalog / ev.total_analytical_columns
        if ev.total_analytical_columns > 0
        else 0.0
    )
    logger.debug(
        "Coverage: %d/%d analytical columns in catalog (%.0f%%)",
        ev.columns_in_catalog,
        ev.total_analytical_columns,
        ev.coverage * 100,
    )


# ---------------------------------------------------------------------------
# SQL validity: do metric expressions parse in DuckDB
# ---------------------------------------------------------------------------


def _eval_sql_validity(
    ev: CatalogEvaluation,
    catalog: SemanticCatalog,
    tables: list[TableInfo],
) -> None:
    """Check that all metric expressions are valid SQL in DuckDB."""
    if not catalog.metrics:
        ev.sql_validity = 1.0
        return

    # Create an in-memory DuckDB with stub tables for validation
    con = duckdb.connect(":memory:")
    try:
        for table in tables:
            cols = ", ".join(f'"{c.name}" {_duckdb_type(c.dtype)}' for c in table.columns)
            if cols:
                con.execute(f'CREATE TABLE "{table.name}" ({cols})')

        ev.total_expressions = len(catalog.metrics)
        ev.valid_expressions = 0

        for metric in catalog.metrics:
            if _validate_expression(con, metric):
                ev.valid_expressions += 1
            else:
                ev.invalid_expressions.append(
                    {
                        "metric": metric.name,
                        "expression": metric.expression,
                        "table": metric.table,
                    }
                )
    finally:
        con.close()

    ev.sql_validity = (
        ev.valid_expressions / ev.total_expressions if ev.total_expressions > 0 else 1.0
    )
    if ev.invalid_expressions:
        logger.warning(
            "SQL validation: %d/%d expressions invalid: %s",
            len(ev.invalid_expressions),
            ev.total_expressions,
            [e["metric"] for e in ev.invalid_expressions],
        )
    else:
        logger.debug("SQL validation: all %d expressions valid", ev.total_expressions)


def _validate_expression(con: duckdb.DuckDBPyConnection, metric: MetricDefinition) -> bool:
    """Try to parse a metric expression as a SELECT against its table."""
    try:
        sql = f'SELECT {metric.expression} FROM "{metric.table}"'
        # Use EXPLAIN to validate without executing
        con.execute(f"EXPLAIN {sql}")
        return True
    except Exception:
        return False


def _duckdb_type(dtype: str) -> str:
    """Map a column dtype string to a DuckDB-compatible type."""
    d = dtype.lower()
    if "int" in d:
        return "BIGINT"
    if "float" in d or "double" in d or "decimal" in d or "numeric" in d or "real" in d:
        return "DOUBLE"
    if "bool" in d:
        return "BOOLEAN"
    if "date" in d and "time" not in d:
        return "DATE"
    if "timestamp" in d or "datetime" in d:
        return "TIMESTAMP"
    if "time" in d:
        return "TIME"
    return "VARCHAR"


# ---------------------------------------------------------------------------
# Synonym coverage
# ---------------------------------------------------------------------------


def _eval_synonyms(ev: CatalogEvaluation, catalog: SemanticCatalog) -> None:
    """Check what fraction of dimensions have synonyms."""
    ev.total_dimensions = len(catalog.dimensions)
    ev.dimensions_with_synonyms = sum(1 for d in catalog.dimensions if d.synonyms)
    ev.synonym_coverage = (
        ev.dimensions_with_synonyms / ev.total_dimensions if ev.total_dimensions > 0 else 0.0
    )


# ---------------------------------------------------------------------------
# Ambiguity detection
# ---------------------------------------------------------------------------


def _eval_ambiguity(ev: CatalogEvaluation, catalog: SemanticCatalog) -> None:
    """Detect dimensions that share synonyms (potential disambiguation needed)."""
    # Build synonym -> dimensions mapping
    synonym_to_dims: dict[str, list[str]] = {}
    for d in catalog.dimensions:
        for syn in d.synonyms:
            syn_lower = syn.lower()
            synonym_to_dims.setdefault(syn_lower, []).append(d.name)

    for syn, dims in synonym_to_dims.items():
        if len(dims) > 1:
            ev.ambiguous_synonyms.append({"synonym": syn, "dimensions": dims})
            ev.warnings.append(f"Ambiguous synonym '{syn}' matches dimensions: {', '.join(dims)}")
    if ev.ambiguous_synonyms:
        logger.info(
            "Ambiguity: %d shared synonyms detected across dimensions",
            len(ev.ambiguous_synonyms),
        )


# ---------------------------------------------------------------------------
# Overall confidence
# ---------------------------------------------------------------------------


def _compute_overall(ev: CatalogEvaluation) -> None:
    """Compute weighted overall confidence score."""
    # Weights: coverage is most important, then SQL validity,
    # then synonyms. Ambiguity penalizes.
    coverage_weight = 0.40
    sql_weight = 0.30
    synonym_weight = 0.20
    ambiguity_weight = 0.10

    # Ambiguity penalty: more ambiguous synonyms = lower score
    ambiguity_score = max(0.0, 1.0 - len(ev.ambiguous_synonyms) * 0.1)

    ev.confidence = round(
        ev.coverage * coverage_weight
        + ev.sql_validity * sql_weight
        + ev.synonym_coverage * synonym_weight
        + ambiguity_score * ambiguity_weight,
        3,
    )

    # Add coverage warning
    if ev.coverage < 0.5:
        ev.warnings.append(
            f"Low catalog coverage: {ev.coverage:.0%} of analytical columns represented"
        )
    if ev.sql_validity < 1.0:
        ev.warnings.append(
            f"{len(ev.invalid_expressions)} metric expression(s) failed SQL validation"
        )
