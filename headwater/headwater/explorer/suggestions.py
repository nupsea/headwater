"""Suggestion engine -- auto-generates BI-oriented questions from metadata.

Generates business-level questions that a data analyst or decision-maker would ask,
not engineering-level column queries. Sources from: mart definitions, semantic column
analysis, cross-entity relationships, and quality findings.
"""

from __future__ import annotations

from headwater.core.models import (
    ColumnProfile,
    ContractCheckResult,
    ContractRule,
    DiscoveryResult,
    GeneratedModel,
    Relationship,
    SuggestedQuestion,
    TableInfo,
)


def generate_suggestions(
    discovery: DiscoveryResult,
    models: list[GeneratedModel] | None = None,
    contracts: list[ContractRule] | None = None,
    quality_results: list[ContractCheckResult] | None = None,
) -> list[SuggestedQuestion]:
    """Generate suggested NL questions from all available metadata.

    Priority order: mart questions first (highest BI value), then cross-entity,
    then domain-specific, then quality investigations.
    """
    questions: list[SuggestedQuestion] = []

    questions.extend(_from_mart_models(models or []))
    questions.extend(_from_relationships(discovery.tables, discovery.relationships))
    questions.extend(_from_domain_analysis(discovery.tables, discovery.profiles))
    questions.extend(_from_quality_findings(contracts or [], quality_results or []))

    return questions


# ---------------------------------------------------------------------------
# Mart-derived questions (highest BI value)
# ---------------------------------------------------------------------------

# These are the best questions -- they map to pre-built analytical models
# that encode real business logic. Show them whenever the mart exists,
# regardless of approval status, so users see the system's analytical value.
_MART_QUESTIONS: dict[str, list[dict[str, str]]] = {
    "mart_air_quality_daily": [
        {
            "q": "Which zones consistently exceed unhealthy air quality thresholds?",
            "cat": "Air Quality",
            "sql": (
                "SELECT zone_name, aqi_category, COUNT(*) AS days "
                "FROM marts.mart_air_quality_daily "
                "WHERE aqi_category IS NOT NULL "
                "GROUP BY zone_name, aqi_category ORDER BY zone_name"
            ),
        },
        {
            "q": "How are PM2.5 levels trending across zones over time?",
            "cat": "Air Quality",
            "sql": (
                "SELECT reading_date, zone_name, AVG(avg_value) AS avg_pm25 "
                "FROM marts.mart_air_quality_daily "
                "WHERE sensor_type = 'pm25' "
                "GROUP BY reading_date, zone_name ORDER BY reading_date"
            ),
        },
        {
            "q": "Which monitoring sites report the worst air quality?",
            "cat": "Air Quality",
            "sql": (
                "SELECT site_name, zone_name, sensor_type, "
                "AVG(avg_value) AS mean_value, MAX(max_value) AS peak_value, "
                "COUNT(*) AS days_monitored "
                "FROM marts.mart_air_quality_daily "
                "GROUP BY site_name, zone_name, sensor_type "
                "ORDER BY mean_value DESC LIMIT 15"
            ),
        },
    ],
    "mart_incident_summary": [
        {
            "q": "Are public health incidents increasing or decreasing over time?",
            "cat": "Public Health",
            "sql": (
                "SELECT DATE_TRUNC('month', CAST(i.date_reported AS DATE)) AS report_month, "
                "i.severity, COUNT(*) AS total "
                "FROM staging.stg_incidents i "
                "GROUP BY report_month, i.severity ORDER BY report_month"
            ),
        },
        {
            "q": "Which communities face the highest incident rates per capita?",
            "cat": "Public Health",
            "sql": (
                "SELECT z.name AS zone_name, z.population, "
                "COUNT(*) AS total_incidents, "
                "ROUND(COUNT(*) * 1000.0 / z.population, 2) AS per_1k "
                "FROM staging.stg_incidents i "
                "JOIN staging.stg_zones z ON i.zone_id = z.zone_id "
                "GROUP BY z.name, z.population ORDER BY per_1k DESC"
            ),
        },
        {
            "q": "Do lower-income zones experience more health incidents?",
            "cat": "Public Health",
            "sql": (
                "SELECT z.name AS zone_name, z.pct_below_poverty, "
                "z.environmental_risk_score, "
                "COUNT(*) AS total_incidents, "
                "SUM(CASE WHEN i.outcome = 'hospitalized' THEN 1 ELSE 0 END) "
                "AS hospitalizations "
                "FROM staging.stg_incidents i "
                "JOIN staging.stg_zones z ON i.zone_id = z.zone_id "
                "GROUP BY z.name, z.pct_below_poverty, z.environmental_risk_score "
                "ORDER BY z.pct_below_poverty DESC"
            ),
        },
    ],
    "mart_inspection_scores": [
        {
            "q": "Which zones have the lowest inspection pass rates?",
            "cat": "Inspections",
            "sql": (
                "SELECT zone_name, site_type, total_inspections, "
                "pass_rate_pct, avg_score, total_critical "
                "FROM marts.mart_inspection_scores "
                "ORDER BY pass_rate_pct ASC"
            ),
        },
        {
            "q": "What site types are most likely to have critical violations?",
            "cat": "Inspections",
            "sql": (
                "SELECT site_type, SUM(total_critical) AS critical_violations, "
                "SUM(total_violations) AS total_violations, "
                "ROUND(AVG(pass_rate_pct), 1) AS avg_pass_rate "
                "FROM marts.mart_inspection_scores "
                "GROUP BY site_type ORDER BY critical_violations DESC"
            ),
        },
    ],
    "mart_complaint_response": [
        {
            "q": "Are we meeting response time SLAs across priority levels?",
            "cat": "Complaints",
            "sql": (
                "SELECT priority, "
                "ROUND(AVG(avg_acknowledgment_days), 1) AS avg_ack_days, "
                "ROUND(AVG(avg_resolution_days), 1) AS avg_resolve_days, "
                "SUM(total_complaints) AS total, "
                "ROUND(AVG(resolution_rate_pct), 1) AS resolution_rate "
                "FROM marts.mart_complaint_response "
                "GROUP BY priority ORDER BY priority"
            ),
        },
        {
            "q": "Which communities wait the longest for complaint resolution?",
            "cat": "Complaints",
            "sql": (
                "SELECT zone_name, "
                "ROUND(AVG(avg_acknowledgment_days), 1) AS avg_ack_days, "
                "ROUND(AVG(avg_resolution_days), 1) AS avg_resolve_days, "
                "SUM(total_complaints) AS total, "
                "SUM(open_count) AS still_open "
                "FROM marts.mart_complaint_response "
                "GROUP BY zone_name ORDER BY avg_resolve_days DESC"
            ),
        },
    ],
    "mart_program_effectiveness": [
        {
            "q": "Are intervention programs associated with lower incident rates?",
            "cat": "Programs",
            "sql": (
                "SELECT program_name, program_type, zone_name, "
                "incidents_per_1k, budget_usd, program_status "
                "FROM marts.mart_program_effectiveness "
                "WHERE program_status = 'active' "
                "ORDER BY incidents_per_1k ASC"
            ),
        },
        {
            "q": "How is program budget distributed relative to community risk?",
            "cat": "Programs",
            "sql": (
                "SELECT program_name, budget_usd, zone_name, "
                "environmental_risk_score, incidents_per_1k "
                "FROM marts.mart_program_effectiveness "
                "WHERE program_status = 'active' "
                "ORDER BY budget_usd DESC"
            ),
        },
    ],
}


def _from_mart_models(models: list[GeneratedModel]) -> list[SuggestedQuestion]:
    """Generate BI questions from mart model definitions.

    Shows questions whenever a mart model exists (proposed, approved, or executed).
    The SQL targets the marts schema -- queries work once a mart is materialized.
    """
    questions: list[SuggestedQuestion] = []

    known_marts = {m.name for m in models if m.model_type == "mart"}

    for model_name, qs in _MART_QUESTIONS.items():
        if model_name not in known_marts:
            continue
        source_model = next((m for m in models if m.name == model_name), None)
        tables = source_model.source_tables if source_model else []
        for q in qs:
            questions.append(
                SuggestedQuestion(
                    question=q["q"],
                    source="mart",
                    category=q["cat"],
                    relevant_tables=tables,
                    sql_hint=q["sql"],
                )
            )

    return questions


# ---------------------------------------------------------------------------
# Cross-entity questions (from detected relationships)
# ---------------------------------------------------------------------------

# Pre-built BI questions for known entity pairs in the environmental health domain
_RELATIONSHIP_QUESTIONS: dict[tuple[str, str], list[dict[str, str]]] = {
    ("readings", "sites"): [
        {
            "q": "Which monitoring sites report the highest average sensor readings?",
            "cat": "Environmental Monitoring",
            "sql": (
                "SELECT s.name AS site_name, s.site_type, "
                "AVG(r.value) AS avg_reading, COUNT(*) AS total_readings "
                "FROM staging.stg_readings r "
                "JOIN staging.stg_sites s ON r.site_id = s.site_id "
                "GROUP BY s.name, s.site_type "
                "ORDER BY avg_reading DESC LIMIT 15"
            ),
        },
    ],
    ("readings", "sensors"): [
        {
            "q": "What is the average reading by sensor type and operational status?",
            "cat": "Environmental Monitoring",
            "sql": (
                "SELECT sn.sensor_type, sn.status, "
                "AVG(r.value) AS avg_reading, COUNT(*) AS measurements "
                "FROM staging.stg_readings r "
                "JOIN staging.stg_sensors sn ON r.sensor_id = sn.sensor_id "
                "GROUP BY sn.sensor_type, sn.status "
                "ORDER BY avg_reading DESC"
            ),
        },
    ],
    ("inspections", "sites"): [
        {
            "q": "Which sites consistently fail inspections?",
            "cat": "Facility & Inspection",
            "sql": (
                "SELECT s.name AS site_name, s.site_type, "
                "COUNT(*) AS total_inspections, "
                "ROUND(AVG(i.score), 1) AS avg_score, "
                "SUM(CASE WHEN i.result = 'fail' THEN 1 ELSE 0 END) AS failures, "
                "SUM(i.critical_violations) AS critical_violations "
                "FROM staging.stg_inspections i "
                "JOIN staging.stg_sites s ON i.site_id = s.site_id "
                "GROUP BY s.name, s.site_type "
                "ORDER BY avg_score ASC LIMIT 15"
            ),
        },
    ],
    ("incidents", "zones"): [
        {
            "q": "How do incident rates compare across zones by severity?",
            "cat": "Public Health",
            "sql": (
                "SELECT z.name AS zone_name, z.population, i.severity, "
                "COUNT(*) AS incidents, "
                "ROUND(COUNT(*) * 1000.0 / z.population, 2) AS per_1k "
                "FROM staging.stg_incidents i "
                "JOIN staging.stg_zones z ON i.zone_id = z.zone_id "
                "GROUP BY z.name, z.population, i.severity "
                "ORDER BY per_1k DESC"
            ),
        },
    ],
    ("complaints", "zones"): [
        {
            "q": "Which neighborhoods have the most unresolved complaints?",
            "cat": "Community Engagement",
            "sql": (
                "SELECT z.name AS zone_name, c.category, "
                "COUNT(*) AS total_complaints, "
                "SUM(CASE WHEN c.status = 'open' THEN 1 ELSE 0 END) AS open, "
                "SUM(CASE WHEN c.status = 'resolved' THEN 1 ELSE 0 END) AS resolved "
                "FROM staging.stg_complaints c "
                "JOIN staging.stg_zones z ON c.zone_id = z.zone_id "
                "GROUP BY z.name, c.category "
                "ORDER BY open DESC LIMIT 20"
            ),
        },
    ],
    ("sites", "zones"): [
        {
            "q": "How are monitoring sites distributed across zones?",
            "cat": "Infrastructure",
            "sql": (
                "SELECT z.name AS zone_name, z.population, s.site_type, "
                "COUNT(*) AS site_count "
                "FROM staging.stg_sites s "
                "JOIN staging.stg_zones z ON s.zone_id = z.zone_id "
                "GROUP BY z.name, z.population, s.site_type "
                "ORDER BY zone_name"
            ),
        },
    ],
}


def _from_relationships(
    tables: list[TableInfo],
    relationships: list[Relationship],
) -> list[SuggestedQuestion]:
    """Generate cross-entity BI questions from detected relationships."""
    questions: list[SuggestedQuestion] = []
    table_names = {t.name for t in tables}
    seen_pairs: set[tuple[str, str]] = set()

    for rel in relationships:
        if rel.from_table not in table_names or rel.to_table not in table_names:
            continue

        # Normalize pair order for lookup
        pair = (rel.from_table, rel.to_table)
        pair_rev = (rel.to_table, rel.from_table)
        if pair in seen_pairs or pair_rev in seen_pairs:
            continue
        seen_pairs.add(pair)

        # Try both orderings against the pre-built question map
        matched = _RELATIONSHIP_QUESTIONS.get(pair) or _RELATIONSHIP_QUESTIONS.get(pair_rev)
        if matched:
            for q in matched:
                questions.append(
                    SuggestedQuestion(
                        question=q["q"],
                        source="relationship",
                        category=q["cat"],
                        relevant_tables=[rel.from_table, rel.to_table],
                        sql_hint=q["sql"],
                    )
                )

    return questions


# ---------------------------------------------------------------------------
# Domain-specific questions (from column semantics)
# ---------------------------------------------------------------------------

# Instead of raw column templates, generate domain-aware BI questions
# based on what we know about the table's role and column types.
_DOMAIN_QUESTIONS: dict[str, list[dict[str, str]]] = {
    "inspections": [
        {
            "q": "How have inspection scores trended over time?",
            "cat": "Facility & Inspection",
            "sql": (
                "SELECT CAST(inspection_date AS DATE) AS inspection_day, "
                "ROUND(AVG(score), 1) AS avg_score, COUNT(*) AS inspections "
                "FROM staging.stg_inspections "
                "GROUP BY inspection_day ORDER BY inspection_day"
            ),
        },
        {
            "q": "Which inspectors have the highest violation discovery rate?",
            "cat": "Facility & Inspection",
            "sql": (
                "SELECT inspector_name, COUNT(*) AS inspections, "
                "ROUND(AVG(score), 1) AS avg_score, "
                "SUM(violation_count) AS total_violations, "
                "ROUND(AVG(violation_count), 1) AS avg_violations_per_inspection "
                "FROM staging.stg_inspections "
                "GROUP BY inspector_name "
                "ORDER BY avg_violations_per_inspection DESC"
            ),
        },
        {
            "q": "Are complaint-driven inspections finding more violations than routine ones?",
            "cat": "Facility & Inspection",
            "sql": (
                "SELECT inspection_type, COUNT(*) AS inspections, "
                "ROUND(AVG(score), 1) AS avg_score, "
                "ROUND(AVG(violation_count), 1) AS avg_violations, "
                "ROUND(AVG(critical_violations), 1) AS avg_critical "
                "FROM staging.stg_inspections "
                "GROUP BY inspection_type ORDER BY avg_violations DESC"
            ),
        },
    ],
    "complaints": [
        {
            "q": "What are the most common complaint categories?",
            "cat": "Community Engagement",
            "sql": (
                "SELECT category, priority, COUNT(*) AS total, "
                "SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) AS resolved, "
                "SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open "
                "FROM staging.stg_complaints "
                "GROUP BY category, priority "
                "ORDER BY total DESC"
            ),
        },
    ],
    "programs": [
        {
            "q": "How is budget allocated across active programs?",
            "cat": "Programs & Interventions",
            "sql": (
                "SELECT name AS program_name, type, status, budget_usd "
                "FROM staging.stg_programs "
                "ORDER BY budget_usd DESC"
            ),
        },
    ],
    "zones": [
        {
            "q": "Which zones have the highest environmental risk scores?",
            "cat": "Geography & Demographics",
            "sql": (
                "SELECT name AS zone_name, population, "
                "median_household_income, pct_below_poverty, "
                "environmental_risk_score "
                "FROM staging.stg_zones "
                "ORDER BY environmental_risk_score DESC"
            ),
        },
        {
            "q": "Is there a demographic pattern in environmental risk?",
            "cat": "Geography & Demographics",
            "sql": (
                "SELECT name AS zone_name, "
                "pct_below_poverty, pct_minority, "
                "environmental_risk_score, median_household_income "
                "FROM staging.stg_zones "
                "ORDER BY environmental_risk_score DESC"
            ),
        },
    ],
    "readings": [
        {
            "q": "What are the daily average readings by sensor type?",
            "cat": "Environmental Monitoring",
            "sql": (
                "SELECT sensor_type, CAST(\"timestamp\" AS DATE) AS reading_day, "
                "ROUND(AVG(value), 2) AS avg_reading, COUNT(*) AS measurements "
                "FROM staging.stg_readings "
                "GROUP BY sensor_type, reading_day "
                "ORDER BY sensor_type, reading_day"
            ),
        },
    ],
    "incidents": [
        {
            "q": "What types of health incidents are most common?",
            "cat": "Public Health",
            "sql": (
                "SELECT incident_type, severity, "
                "COUNT(*) AS total, "
                "SUM(CASE WHEN outcome = 'hospitalized' THEN 1 ELSE 0 END) AS hospitalizations "
                "FROM staging.stg_incidents "
                "GROUP BY incident_type, severity "
                "ORDER BY total DESC"
            ),
        },
    ],
}


def _from_domain_analysis(
    tables: list[TableInfo],
    profiles: list[ColumnProfile],
) -> list[SuggestedQuestion]:
    """Generate domain-aware BI questions based on known table roles."""
    questions: list[SuggestedQuestion] = []
    table_names = {t.name for t in tables}

    for table_name, qs in _DOMAIN_QUESTIONS.items():
        if table_name not in table_names:
            continue
        for q in qs:
            questions.append(
                SuggestedQuestion(
                    question=q["q"],
                    source="semantic",
                    category=q["cat"],
                    relevant_tables=[table_name],
                    sql_hint=q["sql"],
                )
            )

    return questions


# ---------------------------------------------------------------------------
# Quality investigation questions
# ---------------------------------------------------------------------------


def _from_quality_findings(
    contracts: list[ContractRule],
    results: list[ContractCheckResult],
) -> list[SuggestedQuestion]:
    """Generate investigation questions from quality contract failures."""
    questions: list[SuggestedQuestion] = []
    failed_ids = {r.rule_id for r in results if not r.passed}

    for rule in contracts:
        if rule.id not in failed_ids:
            continue

        if rule.rule_type == "not_null" and rule.column_name:
            questions.append(
                SuggestedQuestion(
                    question=(
                        f"Why are there missing values in "
                        f"{_humanize_model(rule.model_name)} {rule.column_name}?"
                    ),
                    source="quality",
                    category="Data Quality Investigation",
                    relevant_tables=[rule.model_name],
                    sql_hint=(
                        f"SELECT * FROM {rule.model_name} "
                        f'WHERE "{rule.column_name}" IS NULL LIMIT 20'
                    ),
                )
            )
        elif rule.rule_type == "cardinality" and rule.column_name:
            questions.append(
                SuggestedQuestion(
                    question=(
                        f"What unexpected {rule.column_name} values appeared in "
                        f"{_humanize_model(rule.model_name)}?"
                    ),
                    source="quality",
                    category="Data Quality Investigation",
                    relevant_tables=[rule.model_name],
                    sql_hint=(
                        f'SELECT "{rule.column_name}", COUNT(*) AS cnt '
                        f"FROM {rule.model_name} "
                        f'GROUP BY "{rule.column_name}" ORDER BY cnt DESC'
                    ),
                )
            )
        elif rule.rule_type == "unique" and rule.column_name:
            questions.append(
                SuggestedQuestion(
                    question=(
                        f"Which {rule.column_name} records have duplicates in "
                        f"{_humanize_model(rule.model_name)}?"
                    ),
                    source="quality",
                    category="Data Quality Investigation",
                    relevant_tables=[rule.model_name],
                    sql_hint=(
                        f'SELECT "{rule.column_name}", COUNT(*) AS cnt '
                        f"FROM {rule.model_name} "
                        f'GROUP BY "{rule.column_name}" HAVING cnt > 1 '
                        f"ORDER BY cnt DESC LIMIT 20"
                    ),
                )
            )

    return questions


def _humanize_model(model_name: str) -> str:
    """Convert staging.stg_readings -> readings."""
    name = model_name
    if "." in name:
        name = name.split(".")[-1]
    if name.startswith("stg_"):
        name = name[4:]
    return name
