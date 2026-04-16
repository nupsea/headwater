"""Insights API -- aggregated KPIs and data quality metrics for the UI."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

from headwater.api.routes.project import _compute_maturity, _compute_progress

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/insights")
async def get_insights(request: Request):
    """Compute aggregated data insights from the discovery and quality pipeline."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    logger.info(
        "Computing insights: %d tables, %d profiles, %d relationships",
        len(discovery.tables),
        len(discovery.profiles),
        len(discovery.relationships),
    )

    profiles = discovery.profiles
    tables = discovery.tables
    relationships = discovery.relationships
    contracts = pipeline["contracts"]
    quality_report = pipeline["quality_report"]
    exec_results = pipeline["execution_results"]
    staging_models = pipeline["staging_models"]
    mart_models = pipeline["mart_models"]

    # --- Overall metrics ---
    total_rows = sum(t.row_count for t in tables)
    total_columns = sum(len(t.columns) for t in tables)
    total_cells = sum(t.row_count * len(t.columns) for t in tables)

    # --- Completeness ---
    total_nulls = sum(p.null_count for p in profiles)
    table_rows = {t.name: t.row_count for t in tables}
    profiled_cells = sum(table_rows.get(p.table_name, 0) for p in profiles)
    completeness_pct = (
        ((profiled_cells - total_nulls) / profiled_cells * 100) if profiled_cells > 0 else 100.0
    )

    # --- Per-table health ---
    table_health = []
    for t in tables:
        t_profiles = [p for p in profiles if p.table_name == t.name]
        if not t_profiles:
            table_health.append(
                {
                    "name": t.name,
                    "row_count": t.row_count,
                    "column_count": len(t.columns),
                    "domain": t.domain,
                    "description": t.description,
                    "completeness": 100.0,
                    "avg_null_rate": 0.0,
                    "pk_columns": [c.name for c in t.columns if c.is_primary_key],
                    "fk_columns": [],
                    "has_relationships": False,
                }
            )
            continue

        avg_null = sum(p.null_rate for p in t_profiles) / len(t_profiles)
        completeness = (1 - avg_null) * 100

        fk_cols = [
            {"column": r.from_column, "references": f"{r.to_table}.{r.to_column}"}
            for r in relationships
            if r.from_table == t.name
        ]
        has_rels = any(r.from_table == t.name or r.to_table == t.name for r in relationships)

        table_health.append(
            {
                "name": t.name,
                "row_count": t.row_count,
                "column_count": len(t.columns),
                "domain": t.domain,
                "description": t.description,
                "completeness": round(completeness, 1),
                "avg_null_rate": round(avg_null * 100, 1),
                "pk_columns": [c.name for c in t.columns if c.is_primary_key],
                "fk_columns": fk_cols,
                "has_relationships": has_rels,
            }
        )

    # --- Column issues (sorted by severity) ---
    column_issues = []
    for p in profiles:
        issues = []
        if p.null_rate > 0.05:
            issues.append(
                {
                    "type": "high_null_rate",
                    "severity": "error" if p.null_rate > 0.2 else "warning",
                    "message": f"{p.null_rate * 100:.1f}% null values",
                    "detail": f"{p.null_count} of {table_rows.get(p.table_name, 0)} rows",
                }
            )
        if p.uniqueness_ratio == 1.0 and p.distinct_count > 1 and not p.column_name.endswith("_id"):
            issues.append(
                {
                    "type": "unexpected_uniqueness",
                    "severity": "info",
                    "message": "100% unique values -- possible natural key",
                    "detail": f"{p.distinct_count} distinct values",
                }
            )
        if p.distinct_count == 1 and p.null_rate == 0:
            issues.append(
                {
                    "type": "constant_column",
                    "severity": "warning",
                    "message": "Single constant value -- consider removing",
                    "detail": "No variation in this column",
                }
            )
        if p.null_rate == 0 and p.distinct_count == 0:
            issues.append(
                {
                    "type": "empty_column",
                    "severity": "error",
                    "message": "Column appears empty",
                    "detail": "No non-null values found",
                }
            )
        if issues:
            column_issues.append(
                {
                    "table": p.table_name,
                    "column": p.column_name,
                    "dtype": p.dtype,
                    "issues": issues,
                }
            )

    sev_order = {"error": 0, "warning": 1, "info": 2}
    column_issues.sort(key=lambda x: min(sev_order.get(i["severity"], 3) for i in x["issues"]))

    # --- Null rate analysis ---
    null_analysis = []
    for p in profiles:
        if p.null_rate > 0:
            null_analysis.append(
                {
                    "table": p.table_name,
                    "column": p.column_name,
                    "null_rate": round(p.null_rate * 100, 2),
                    "null_count": p.null_count,
                    "total_rows": table_rows.get(p.table_name, 0),
                }
            )
    null_analysis.sort(key=lambda x: x["null_rate"], reverse=True)

    # --- Uniqueness analysis ---
    uniqueness_analysis = []
    for p in profiles:
        uniqueness_analysis.append(
            {
                "table": p.table_name,
                "column": p.column_name,
                "uniqueness_ratio": round(p.uniqueness_ratio * 100, 1),
                "distinct_count": p.distinct_count,
                "is_pk_candidate": p.uniqueness_ratio == 1.0 and p.distinct_count > 1,
            }
        )

    # --- Pattern detection summary ---
    patterns_found = []
    for p in profiles:
        if p.detected_pattern:
            patterns_found.append(
                {
                    "table": p.table_name,
                    "column": p.column_name,
                    "pattern": p.detected_pattern,
                }
            )

    # --- Relationship map ---
    rel_map = []
    for r in relationships:
        rel_map.append(
            {
                "from_table": r.from_table,
                "from_column": r.from_column,
                "to_table": r.to_table,
                "to_column": r.to_column,
                "type": r.type,
                "confidence": round(r.confidence * 100),
                "integrity": round(r.referential_integrity * 100),
            }
        )

    # --- Domain summary ---
    domains = {}
    for t in tables:
        d = t.domain or "Unclassified"
        if d not in domains:
            domains[d] = {"tables": [], "total_rows": 0}
        domains[d]["tables"].append(t.name)
        domains[d]["total_rows"] += t.row_count

    # --- Data profile (concrete, defensible metrics) ---
    data_profile = _compute_data_profile(
        tables, profiles, relationships, completeness_pct, quality_report
    )

    # --- Workflow state ---
    workflow = _compute_workflow_state(
        tables,
        profiles,
        relationships,
        contracts,
        staging_models,
        mart_models,
        exec_results,
        quality_report,
        column_issues,
    )

    # --- Advisory actions (what to do next) ---
    advisory_actions = _compute_advisory_actions(
        tables,
        profiles,
        relationships,
        contracts,
        staging_models,
        mart_models,
        exec_results,
        quality_report,
        column_issues,
    )

    # --- Quality summary ---
    quality_summary = None
    if quality_report:
        quality_summary = {
            "total": quality_report.total_contracts,
            "passed": quality_report.passed,
            "failed": quality_report.failed,
            "pass_rate": (
                round(quality_report.passed / quality_report.total_contracts * 100, 1)
                if quality_report.total_contracts > 0
                else 0
            ),
        }

    # --- Model suggestions ---
    model_suggestions = _compute_model_suggestions(tables, profiles, relationships, pipeline)

    # --- Catalog health (v2) ---
    try:
        catalog_health = _compute_catalog_health(request, discovery)
    except Exception:
        logger.exception("Failed to compute catalog health")
        catalog_health = {
            "metrics_total": 0,
            "metrics_confirmed": 0,
            "dimensions_total": 0,
            "dimensions_confirmed": 0,
            "entities_total": 0,
            "catalog_confidence": 0.0,
            "catalog_coverage": 0.0,
            "maturity": "raw",
            "maturity_score": 0.0,
        }

    return {
        "data_profile": data_profile,
        "workflow": workflow,
        "advisory_actions": advisory_actions,
        "overview": {
            "total_tables": len(tables),
            "total_columns": total_columns,
            "total_rows": total_rows,
            "total_cells": total_cells,
            "total_relationships": len(relationships),
            "completeness_pct": round(completeness_pct, 1),
            "total_profiles": len(profiles),
            "total_contracts": len(contracts),
        },
        "domains": domains,
        "table_health": table_health,
        "column_issues": column_issues,
        "null_analysis": null_analysis,
        "uniqueness_analysis": uniqueness_analysis,
        "patterns_found": patterns_found,
        "relationship_map": rel_map,
        "quality_summary": quality_summary,
        "model_suggestions": model_suggestions,
        "catalog_health": catalog_health,
    }


# ---------------------------------------------------------------------------
# Data Profile -- concrete numbers, each with a plain-English definition
# ---------------------------------------------------------------------------


def _compute_data_profile(tables, profiles, relationships, completeness_pct, quality_report):
    """Return concrete, defensible metrics -- no composite scores."""

    # Completeness: % of non-null cells across all profiled columns
    completeness = round(min(completeness_pct, 100.0), 1)

    # PK coverage: how many tables have at least one identified primary key
    tables_with_pk = sum(1 for t in tables if any(c.is_primary_key for c in t.columns))
    pk_coverage = {
        "tables_with_pk": tables_with_pk,
        "total_tables": len(tables),
        "description": "Tables with an identified primary key",
    }

    # FK integrity: average referential integrity across all detected relationships
    if relationships:
        avg_integrity = sum(r.referential_integrity for r in relationships) / len(relationships)
        fk_integrity = {
            "avg_integrity_pct": round(avg_integrity * 100, 1),
            "total_relationships": len(relationships),
            "description": "Average % of foreign key values that resolve to a parent record",
        }
    else:
        fk_integrity = {
            "avg_integrity_pct": None,
            "total_relationships": 0,
            "description": "No foreign key relationships detected",
        }

    # Quality pass rate: how many auto-generated quality contracts pass
    if quality_report and quality_report.total_contracts > 0:
        quality = {
            "passed": quality_report.passed,
            "total": quality_report.total_contracts,
            "pass_rate_pct": round(quality_report.passed / quality_report.total_contracts * 100, 1),
            "description": "Auto-generated quality contracts that pass",
        }
    else:
        quality = {
            "passed": 0,
            "total": 0,
            "pass_rate_pct": None,
            "description": "Quality contracts not yet evaluated",
        }

    # Columns with issues: high nulls, constants, etc.
    high_null_cols = sum(1 for p in profiles if p.null_rate > 0.05)
    constant_cols = sum(1 for p in profiles if p.distinct_count == 1 and p.null_rate == 0)

    return {
        "completeness_pct": completeness,
        "pk_coverage": pk_coverage,
        "fk_integrity": fk_integrity,
        "quality": quality,
        "high_null_columns": high_null_cols,
        "constant_columns": constant_cols,
        "total_columns_profiled": len(profiles),
    }


# ---------------------------------------------------------------------------
# Workflow state -- where is this dataset in the journey?
# ---------------------------------------------------------------------------

_PHASES = [
    {"key": "discovery", "label": "Discovered"},
    {"key": "profiling", "label": "Profiled"},
    {"key": "review", "label": "Schema Reviewed"},
    {"key": "modeling", "label": "Modeled"},
    {"key": "quality", "label": "Quality Baselined"},
]


def _compute_workflow_state(
    tables,
    profiles,
    relationships,
    contracts,
    staging_models,
    mart_models,
    exec_results,
    quality_report,
    column_issues,
):
    """Determine which workflow phase the dataset is in."""
    phases = []

    # Phase 1: Discovery -- did we find tables?
    discovered = len(tables) > 0
    phases.append(
        {
            "key": "discovery",
            "label": "Discovered",
            "status": "complete" if discovered else "pending",
            "detail": f"{len(tables)} tables found" if discovered else "Not started",
        }
    )

    # Phase 2: Profiling -- did we profile columns?
    profiled = len(profiles) > 0
    phases.append(
        {
            "key": "profiling",
            "label": "Profiled",
            "status": "complete" if profiled else "pending",
            "detail": (
                f"{len(profiles)} columns, {len(relationships)} relationships"
                if profiled
                else "Not started"
            ),
        }
    )

    # Phase 3: Schema Review -- are there unresolved issues?
    # Issues that need human attention: tables without PKs, high-null columns, isolated tables
    tables_no_pk = [t.name for t in tables if not any(c.is_primary_key for c in t.columns)]
    connected = set()
    for r in relationships:
        connected.add(r.from_table)
        connected.add(r.to_table)
    error_issues = [
        ci for ci in column_issues if any(i["severity"] == "error" for i in ci["issues"])
    ]

    review_blockers = len(tables_no_pk) + len(error_issues)
    if not profiled:
        review_status = "pending"
        review_detail = "Profiling must complete first"
    elif review_blockers == 0:
        review_status = "complete"
        review_detail = "No blocking schema issues"
    else:
        review_status = "active"
        parts = []
        if tables_no_pk:
            parts.append(f"{len(tables_no_pk)} tables missing PKs")
        if error_issues:
            parts.append(f"{len(error_issues)} columns with critical issues")
        review_detail = ", ".join(parts)

    phases.append(
        {
            "key": "review",
            "label": "Schema Reviewed",
            "status": review_status,
            "detail": review_detail,
        }
    )

    # Phase 4: Modeling -- are models generated and reviewed?
    total_models = len(staging_models) + len(mart_models)
    pending_marts = [m for m in mart_models if m.status == "proposed"]
    executed = [r for r in exec_results if r.success]

    if total_models == 0:
        model_status = "pending"
        model_detail = "No models generated yet"
    elif pending_marts:
        model_status = "active"
        model_detail = f"{len(pending_marts)} mart model(s) awaiting review"
    elif len(executed) > 0:
        model_status = "complete"
        model_detail = f"{len(executed)} models materialized"
    else:
        model_status = "active"
        model_detail = f"{total_models} models generated, none executed"

    phases.append(
        {
            "key": "modeling",
            "label": "Modeled",
            "status": model_status,
            "detail": model_detail,
        }
    )

    # Phase 5: Quality -- are contracts evaluated?
    if not quality_report:
        quality_status = "pending"
        quality_detail = "Quality checks not yet run"
    elif quality_report.failed > 0:
        quality_status = "active"
        quality_detail = (
            f"{quality_report.failed} of {quality_report.total_contracts} contracts failed"
        )
    else:
        quality_status = "complete"
        quality_detail = f"All {quality_report.total_contracts} contracts pass"

    phases.append(
        {
            "key": "quality",
            "label": "Quality Baselined",
            "status": quality_status,
            "detail": quality_detail,
        }
    )

    # Current phase = first non-complete phase, or last phase if all complete
    current = "quality"
    for p in phases:
        if p["status"] != "complete":
            current = p["key"]
            break

    return {
        "phases": phases,
        "current_phase": current,
    }


# ---------------------------------------------------------------------------
# Advisory actions -- "what should I do next?"
# ---------------------------------------------------------------------------


def _compute_advisory_actions(
    tables,
    profiles,
    relationships,
    contracts,
    staging_models,
    mart_models,
    exec_results,
    quality_report,
    column_issues,
):
    """Generate prioritized, actionable recommendations grouped by phase."""
    actions = []

    # --- Schema Review actions ---

    # Tables without primary keys
    tables_no_pk = [t.name for t in tables if not any(c.is_primary_key for c in t.columns)]
    if tables_no_pk:
        actions.append(
            {
                "phase": "review",
                "priority": "blocking",
                "title": f"Confirm primary keys for {len(tables_no_pk)} table(s)",
                "detail": (
                    f"{', '.join(tables_no_pk[:4])}"
                    + (f" and {len(tables_no_pk) - 4} more" if len(tables_no_pk) > 4 else "")
                    + " -- without PKs, deduplication and joins may be unreliable"
                ),
                "link": "/discovery",
            }
        )

    # Isolated tables
    connected = set()
    for r in relationships:
        connected.add(r.from_table)
        connected.add(r.to_table)
    isolated = [t.name for t in tables if t.name not in connected]
    if isolated:
        actions.append(
            {
                "phase": "review",
                "priority": "recommended",
                "title": f"Verify {len(isolated)} isolated table(s)",
                "detail": (
                    f"{', '.join(isolated[:4])}"
                    + (f" and {len(isolated) - 4} more" if len(isolated) > 4 else "")
                    + " -- no foreign key relationships detected. "
                    "Confirm these are standalone or identify missing links"
                ),
                "link": "/discovery",
            }
        )

    # --- Data Cleanup actions ---

    # High null columns
    high_null = [p for p in profiles if p.null_rate > 0.05]
    if high_null:
        error_level = [p for p in high_null if p.null_rate > 0.2]
        worst = max(high_null, key=lambda p: p.null_rate)
        if error_level:
            actions.append(
                {
                    "phase": "cleanup",
                    "priority": "blocking",
                    "title": f"{len(error_level)} column(s) are >20% null",
                    "detail": (
                        f"Worst: {worst.table_name}.{worst.column_name} "
                        f"at {worst.null_rate * 100:.0f}% -- decide whether to drop, "
                        "fill with defaults, or flag as legitimately sparse"
                    ),
                    "link": "/quality",
                }
            )
        elif len(high_null) > 0:
            actions.append(
                {
                    "phase": "cleanup",
                    "priority": "recommended",
                    "title": f"Review {len(high_null)} column(s) with >5% nulls",
                    "detail": (
                        f"Worst: {worst.table_name}.{worst.column_name} "
                        f"at {worst.null_rate * 100:.0f}% -- decide which need defaults "
                        "vs. which are legitimately optional"
                    ),
                    "link": "/quality",
                }
            )

    # Low FK integrity
    weak_fks = [r for r in relationships if r.referential_integrity < 0.95]
    if weak_fks:
        worst_fk = min(weak_fks, key=lambda r: r.referential_integrity)
        actions.append(
            {
                "phase": "cleanup",
                "priority": "recommended",
                "title": f"{len(weak_fks)} relationship(s) have weak referential integrity",
                "detail": (
                    f"Lowest: {worst_fk.from_table}.{worst_fk.from_column} -> "
                    f"{worst_fk.to_table}.{worst_fk.to_column} at "
                    f"{worst_fk.referential_integrity * 100:.0f}% -- "
                    "JOINs in mart models may silently drop rows"
                ),
                "link": "/quality",
            }
        )

    # --- Modeling actions ---

    pending_marts = [m for m in mart_models if m.status == "proposed"]
    if pending_marts:
        actions.append(
            {
                "phase": "modeling",
                "priority": "blocking",
                "title": f"Review {len(pending_marts)} mart model(s)",
                "detail": (
                    f"{', '.join(m.name for m in pending_marts[:3])}"
                    + (f" and {len(pending_marts) - 3} more" if len(pending_marts) > 3 else "")
                    + " -- mart models encode business logic and need human approval"
                ),
                "link": "/models",
            }
        )

    # Tables not covered by any mart
    if mart_models:
        mart_sources = set()
        for m in mart_models:
            mart_sources.update(m.source_tables)
        uncovered = [t.name for t in tables if t.name not in mart_sources]
        if uncovered:
            actions.append(
                {
                    "phase": "modeling",
                    "priority": "informational",
                    "title": f"{len(uncovered)} table(s) not referenced in any mart model",
                    "detail": (
                        f"{', '.join(uncovered[:4])}"
                        + (f" and {len(uncovered) - 4} more" if len(uncovered) > 4 else "")
                        + " -- consider whether analytical models should include them"
                    ),
                    "link": "/models",
                }
            )

    # --- Quality actions ---

    if quality_report and quality_report.failed > 0:
        actions.append(
            {
                "phase": "quality",
                "priority": "blocking",
                "title": f"{quality_report.failed} quality contract(s) failed",
                "detail": (
                    f"{quality_report.passed} of {quality_report.total_contracts} pass -- "
                    "review failures to decide whether data needs fixing or the contract "
                    "expectations should be adjusted"
                ),
                "link": "/quality",
            }
        )

    if contracts and quality_report:
        observing = [c for c in contracts if c.status == "observing"]
        if observing:
            actions.append(
                {
                    "phase": "quality",
                    "priority": "informational",
                    "title": f"{len(observing)} contract(s) in observation mode",
                    "detail": (
                        "Contracts are tracking violations without enforcing -- "
                        "review results to decide which to promote to enforcement"
                    ),
                    "link": "/quality",
                }
            )

    # --- Success signals (important -- show progress, not just problems) ---

    null_free = []
    for t in tables:
        t_profiles = [p for p in profiles if p.table_name == t.name]
        if t_profiles and all(p.null_rate == 0 for p in t_profiles):
            null_free.append(t.name)
    if null_free:
        actions.append(
            {
                "phase": "review",
                "priority": "success",
                "title": f"{len(null_free)} table(s) are 100% complete",
                "detail": ", ".join(null_free[:5]),
                "link": "/discovery",
            }
        )

    if quality_report and quality_report.failed == 0 and quality_report.total_contracts > 0:
        actions.append(
            {
                "phase": "quality",
                "priority": "success",
                "title": f"All {quality_report.total_contracts} quality contracts pass",
                "detail": "Quality baseline established -- data meets all auto-generated expectations",  # noqa: E501
                "link": "/quality",
            }
        )

    if exec_results:
        ok = sum(1 for r in exec_results if r.success)
        if ok == len(exec_results) and ok > 0:
            total_time = sum(r.execution_time_ms for r in exec_results)
            actions.append(
                {
                    "phase": "modeling",
                    "priority": "success",
                    "title": f"All {ok} models materialized successfully",
                    "detail": f"Total execution time: {total_time:.0f}ms",
                    "link": "/models",
                }
            )

    # Sort: blocking first, then recommended, then informational, success last
    priority_order = {"blocking": 0, "recommended": 1, "informational": 2, "success": 3}
    actions.sort(key=lambda a: priority_order.get(a["priority"], 9))

    return actions


# ---------------------------------------------------------------------------
# Model suggestions (unchanged)
# ---------------------------------------------------------------------------


def _compute_model_suggestions(tables, profiles, relationships, pipeline):
    """Generate suggestions for model improvements."""
    suggestions = []
    mart_models = pipeline.get("mart_models", [])

    # Check for tables not covered by any mart
    mart_sources = set()
    for m in mart_models:
        mart_sources.update(m.source_tables)
    uncovered = [t.name for t in tables if t.name not in mart_sources]
    if uncovered:
        suggestions.append(
            {
                "type": "coverage",
                "title": f"{len(uncovered)} source table(s) not used in any mart model",
                "detail": (
                    f"Tables {', '.join(uncovered)} are not directly referenced. "
                    "Consider whether analytical models should include them."
                ),
            }
        )

    # Suggest dedup for tables with low uniqueness on ID columns
    for p in profiles:
        if p.column_name.endswith("_id") and p.uniqueness_ratio < 1.0 and p.uniqueness_ratio > 0:
            suggestions.append(
                {
                    "type": "dedup",
                    "title": f"{p.table_name}.{p.column_name} is not fully unique",
                    "detail": (
                        f"Uniqueness: {p.uniqueness_ratio * 100:.1f}%. "
                        "Staging model should include deduplication logic."
                    ),
                }
            )

    # Suggest relationship validation for low-integrity FKs
    for r in relationships:
        if r.referential_integrity < 0.95:
            suggestions.append(
                {
                    "type": "integrity",
                    "title": (
                        f"Weak referential integrity: "
                        f"{r.from_table}.{r.from_column} -> {r.to_table}.{r.to_column}"
                    ),
                    "detail": (
                        f"Only {r.referential_integrity * 100:.0f}% of FK values found in PK. "
                        "JOINs in mart models may drop rows."
                    ),
                }
            )

    # Suggest quality observations for proposed marts
    for m in mart_models:
        if m.status == "proposed" and m.questions:
            suggestions.append(
                {
                    "type": "review",
                    "title": f"{m.name} has {len(m.questions)} open question(s)",
                    "detail": m.questions[0],
                }
            )

    return suggestions


# ---------------------------------------------------------------------------
# Catalog health (v2)
# ---------------------------------------------------------------------------


def _compute_catalog_health(request: Request, discovery) -> dict:
    """Return catalog health metrics for the insights dashboard."""
    store = request.app.state.metadata_store
    pipeline = request.app.state.pipeline
    source = getattr(discovery, "source", None)
    if source is None:
        logger.warning("Discovery has no source attribute -- cannot compute catalog health")
        raise ValueError("Discovery missing source")
    source_name = source.name
    logger.info("Computing catalog health for source '%s'", source_name)

    metrics = store.get_catalog_metrics(source_name)
    dimensions = store.get_catalog_dimensions(source_name)
    entities = store.get_catalog_entities(source_name)

    # Progress and maturity
    progress = _compute_progress(discovery, pipeline, store, source_name)
    maturity, maturity_score = _compute_maturity(progress)

    # Project info
    project = store.get_project(source_name)
    catalog_confidence = project.get("catalog_confidence", 0.0) if project else 0.0

    return {
        "metrics_total": len(metrics),
        "metrics_confirmed": sum(1 for m in metrics if m.get("status") == "confirmed"),
        "dimensions_total": len(dimensions),
        "dimensions_confirmed": sum(1 for d in dimensions if d.get("status") == "confirmed"),
        "entities_total": len(entities),
        "catalog_confidence": catalog_confidence,
        "catalog_coverage": progress["catalog_coverage"],
        "maturity": maturity,
        "maturity_score": maturity_score,
    }
