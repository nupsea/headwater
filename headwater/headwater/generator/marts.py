"""Mart model generator -- domain-specific analytical models with clarifying questions.

Mart models encode business logic and require individual human review. Never batch-approved.
"""

from __future__ import annotations

from headwater.core.models import DiscoveryResult, GeneratedModel

# Each mart definition: name, description, sql_template, assumptions, questions, required_tables
_MART_DEFINITIONS: list[dict] = [
    {
        "name": "mart_air_quality_daily",
        "description": (
            "Daily air quality averages by site and zone. "
            "Aggregates PM2.5 and ozone readings with AQI category classification."
        ),
        "required_tables": {"readings", "sensors", "sites", "zones"},
        "sql": """-- Mart: Daily Air Quality by Site and Zone
-- REQUIRES REVIEW: Contains business logic for AQI classification.

CREATE OR REPLACE TABLE {{target_schema}}.mart_air_quality_daily AS
WITH daily_readings AS (
    SELECT
        r.site_id,
        r.sensor_type,
        CAST(r."timestamp" AS DATE) AS reading_date,
        AVG(r.value) AS avg_value,
        MIN(r.value) AS min_value,
        MAX(r.value) AS max_value,
        COUNT(*) AS reading_count
    FROM staging.stg_readings r
    WHERE r.qc_flag = 'valid'
    GROUP BY r.site_id, r.sensor_type, CAST(r."timestamp" AS DATE)
)
SELECT
    dr.reading_date,
    dr.site_id,
    s.name AS site_name,
    s.zone_id,
    z.name AS zone_name,
    z.type AS zone_type,
    dr.sensor_type,
    dr.avg_value,
    dr.min_value,
    dr.max_value,
    dr.reading_count,
    CASE
        WHEN dr.sensor_type = 'pm25' AND dr.avg_value <= 12.0 THEN 'Good'
        WHEN dr.sensor_type = 'pm25' AND dr.avg_value <= 35.4 THEN 'Moderate'
        WHEN dr.sensor_type = 'pm25' AND dr.avg_value <= 55.4 THEN 'Unhealthy for Sensitive'
        WHEN dr.sensor_type = 'pm25' AND dr.avg_value > 55.4 THEN 'Unhealthy'
        WHEN dr.sensor_type = 'ozone' AND dr.avg_value <= 0.054 THEN 'Good'
        WHEN dr.sensor_type = 'ozone' AND dr.avg_value <= 0.070 THEN 'Moderate'
        ELSE NULL
    END AS aqi_category
FROM daily_readings dr
JOIN staging.stg_sites s ON dr.site_id = s.site_id
JOIN staging.stg_zones z ON s.zone_id = z.zone_id""",
        "assumptions": [
            "Only 'valid' QC flag readings are included (maintenance/suspect/invalid excluded)",
            "AQI breakpoints follow EPA standards for PM2.5 (24-hr) and ozone (8-hr)",
            "Daily average is used as the aggregation metric",
        ],
        "questions": [
            "Should 'suspect' QC flag readings be included with a flag, or excluded entirely?",
            "Are the AQI breakpoints correct for your jurisdiction?",
            "Should this include all sensor types, or only air quality sensors "
            "(pm25, ozone, no2, co, so2)?",
        ],
    },
    {
        "name": "mart_incident_summary",
        "description": (
            "Public health incident summary by type, severity, zone, and month. "
            "Includes demographic overlay from zone data."
        ),
        "required_tables": {"incidents", "zones"},
        "sql": """-- Mart: Incident Summary by Zone and Month
-- REQUIRES REVIEW: Aggregation and demographic overlay.

CREATE OR REPLACE TABLE {{target_schema}}.mart_incident_summary AS
SELECT
    DATE_TRUNC('month', CAST(i.date_reported AS DATE)) AS report_month,
    i.zone_id,
    z.name AS zone_name,
    z.type AS zone_type,
    z.population,
    z.median_household_income,
    z.pct_below_poverty,
    z.pct_minority,
    z.environmental_risk_score,
    i.incident_type,
    i.severity,
    COUNT(*) AS incident_count,
    COUNT(DISTINCT i.incident_id) AS unique_incidents,
    AVG(i.patient_age) AS avg_patient_age,
    SUM(CASE WHEN i.outcome = 'hospitalized' THEN 1 ELSE 0 END) AS hospitalizations,
    ROUND(COUNT(*) * 1000.0 / z.population, 2) AS incidents_per_1k_population
FROM staging.stg_incidents i
JOIN staging.stg_zones z ON i.zone_id = z.zone_id
GROUP BY ALL""",
        "assumptions": [
            "Incident rate is calculated per 1,000 population using zone-level census data",
            "Monthly aggregation uses date_reported, not date_onset",
            "All severity levels are included in the aggregate",
        ],
        "questions": [
            "Should incident rate be calculated per 1K or per 10K population?",
            "Should this use date_reported or date_onset for temporal grouping?",
            "Should we filter out any severity levels (e.g., 'mild') from the summary?",
        ],
    },
    {
        "name": "mart_inspection_scores",
        "description": (
            "Inspection pass rates and violation breakdown by site type and zone."
        ),
        "required_tables": {"inspections", "sites", "zones"},
        "sql": """-- Mart: Inspection Scores by Site Type and Zone
-- REQUIRES REVIEW: Pass rate calculation and violation counting.

CREATE OR REPLACE TABLE {{target_schema}}.mart_inspection_scores AS
SELECT
    s.zone_id,
    z.name AS zone_name,
    s.site_type,
    COUNT(*) AS total_inspections,
    AVG(i.score) AS avg_score,
    SUM(CASE WHEN i.result = 'pass' THEN 1 ELSE 0 END) AS pass_count,
    SUM(CASE WHEN i.result = 'conditional_pass' THEN 1 ELSE 0 END) AS conditional_count,
    SUM(CASE WHEN i.result = 'fail' THEN 1 ELSE 0 END) AS fail_count,
    ROUND(
        SUM(CASE WHEN i.result = 'pass' THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 1
    ) AS pass_rate_pct,
    SUM(i.violation_count) AS total_violations,
    SUM(i.critical_violations) AS total_critical,
    AVG(i.duration_minutes) AS avg_duration_minutes
FROM staging.stg_inspections i
JOIN staging.stg_sites s ON i.site_id = s.site_id
JOIN staging.stg_zones z ON s.zone_id = z.zone_id
GROUP BY s.zone_id, z.name, s.site_type""",
        "assumptions": [
            "Pass rate includes only 'pass' results (not conditional_pass)",
            "All inspection types are aggregated together",
            "Violation counts come from the pre-computed columns, not the violations array",
        ],
        "questions": [
            "Should conditional_pass count toward the pass rate?",
            "Should inspection types be broken out separately (routine vs complaint-driven)?",
            "Is there a minimum score threshold that defines a passing inspection?",
        ],
    },
    {
        "name": "mart_complaint_response",
        "description": (
            "Complaint resolution times and status breakdown by category, priority, and zone."
        ),
        "required_tables": {"complaints", "zones"},
        "sql": """-- Mart: Complaint Response Times
-- REQUIRES REVIEW: Resolution time calculation.

CREATE OR REPLACE TABLE {{target_schema}}.mart_complaint_response AS
SELECT
    c.zone_id,
    z.name AS zone_name,
    c.category,
    c.priority,
    COUNT(*) AS total_complaints,
    SUM(CASE WHEN c.status = 'resolved' THEN 1 ELSE 0 END) AS resolved_count,
    SUM(CASE WHEN c.status = 'open' THEN 1 ELSE 0 END) AS open_count,
    ROUND(
        SUM(CASE WHEN c.status = 'resolved' THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 1
    ) AS resolution_rate_pct,
    AVG(
        CASE WHEN c.resolution_date IS NOT NULL
        THEN DATEDIFF('day', CAST(c.date_filed AS DATE), CAST(c.resolution_date AS DATE))
        END
    ) AS avg_resolution_days,
    AVG(
        DATEDIFF('day', CAST(c.date_filed AS DATE), CAST(c.date_acknowledged AS DATE))
    ) AS avg_acknowledgment_days
FROM staging.stg_complaints c
JOIN staging.stg_zones z ON c.zone_id = z.zone_id
GROUP BY c.zone_id, z.name, c.category, c.priority""",
        "assumptions": [
            "Resolution time is measured from date_filed to resolution_date",
            "Acknowledgment time is measured from date_filed to date_acknowledged",
            "Only resolved complaints contribute to avg_resolution_days",
        ],
        "questions": [
            "Is resolution time measured from filing or from acknowledgment?",
            "Should 'closed_no_action' be counted as resolved?",
            "Are there SLA targets for acknowledgment or resolution times?",
        ],
    },
    {
        "name": "mart_program_effectiveness",
        "description": (
            "Program enrollment and incident correlation by target zone. "
            "Helps evaluate whether intervention programs correlate with reduced incidents."
        ),
        "required_tables": {"programs", "incidents", "zones"},
        "sql": """-- Mart: Program Effectiveness Indicators
-- REQUIRES REVIEW: Correlation model is observational, not causal.

CREATE OR REPLACE TABLE {{target_schema}}.mart_program_effectiveness AS
WITH zone_incidents AS (
    SELECT
        zone_id,
        incident_type,
        COUNT(*) AS incident_count
    FROM staging.stg_incidents
    GROUP BY zone_id, incident_type
)
SELECT
    p.program_id,
    p.name AS program_name,
    p.type AS program_type,
    p.budget_usd,
    p.status AS program_status,
    z.zone_id,
    z.name AS zone_name,
    z.population,
    z.environmental_risk_score,
    zi.incident_type,
    COALESCE(zi.incident_count, 0) AS zone_incident_count,
    ROUND(
        COALESCE(zi.incident_count, 0) * 1000.0 / z.population, 2
    ) AS incidents_per_1k
FROM staging.stg_programs p,
UNNEST(p.target_zones) AS t(zone_id)
JOIN staging.stg_zones z ON t.zone_id = z.zone_id
LEFT JOIN zone_incidents zi ON z.zone_id = zi.zone_id""",
        "assumptions": [
            "Program target_zones is an array field that gets unnested",
            "Incident counts are total (not filtered by program-relevant type)",
            "This is observational correlation, not causal attribution",
        ],
        "questions": [
            "Should incidents be filtered to types relevant to each program?",
            "Should we compare target zones vs non-target zones for the same period?",
            "Is the UNNEST syntax correct for your target_zones array structure?",
        ],
    },
]


def generate_mart_models(
    discovery: DiscoveryResult,
    target_schema: str = "marts",
) -> list[GeneratedModel]:
    """Generate mart SQL models based on discovered tables and domains.

    Each mart is proposed with assumptions and clarifying questions.
    Status is always 'proposed' -- never auto-approved.
    """
    available_tables = {t.name for t in discovery.tables}
    models: list[GeneratedModel] = []

    for defn in _MART_DEFINITIONS:
        # Only propose marts whose required tables are present
        if not defn["required_tables"].issubset(available_tables):
            continue

        sql = defn["sql"].replace("{{target_schema}}", target_schema)

        models.append(
            GeneratedModel(
                name=defn["name"],
                model_type="mart",
                sql=sql.strip(),
                description=defn["description"],
                source_tables=sorted(defn["required_tables"]),
                depends_on=[f"stg_{t}" for t in sorted(defn["required_tables"])],
                status="proposed",
                assumptions=defn["assumptions"],
                questions=defn["questions"],
            )
        )

    return models
