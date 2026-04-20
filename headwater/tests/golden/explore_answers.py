"""Golden answers -- manually verified ground truth from the sample dataset.

Every value here was computed by running raw SQL against the 8 sample JSON files
loaded into DuckDB and manually verified. These values are the correctness
benchmark for all explore tests.

Sample data: 8 tables, 59,912 total rows, environmental health domain.
Data version: data/sample/ as of 2026-04-18.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Category A: Single-table aggregations
# ---------------------------------------------------------------------------

TOTAL_COMPLAINTS = 3000
AVG_READING_VALUE = 34.94  # tolerance +/- 0.5
DISTINCT_ZONES = 25  # zones table has 25 rows
MAX_READING_VALUE = 1084.0
NULL_RATE_COMPLAINTS_COUNTY = 0.0  # county column has zero nulls

# ---------------------------------------------------------------------------
# Category B: Multi-table joins
# ---------------------------------------------------------------------------

# JOIN complaints -> zones ON zone_id, GROUP BY zone, ORDER BY cnt DESC LIMIT 1
COMPLAINTS_PER_ZONE_TOP1 = ("Z09", 203)  # zone Z09 "Southview" has 203 complaints

# JOIN readings -> sensors ON sensor_id, GROUP BY sensor_type, AVG(value)
# Top 3 by avg value (descending)
READINGS_AVG_BY_SENSOR_TYPE_TOP3 = [
    ("conductivity", 608.34),  # tolerance +/- 1.0
    ("humidity", 65.74),
    ("noise", 62.44),
]

# COUNT DISTINCT sites that have at least one inspection
SITES_WITH_INSPECTIONS = 452

# Programs with no linked incidents (via incidents.linked_program_id)
# 7 programs have zero incident links
PROGRAMS_WITHOUT_INCIDENTS_COUNT = 7

# Zone with the most sensors (JOIN sensors -> sites -> zones)
ZONE_WITH_MOST_SENSORS = ("Z03", 65)  # "Old Mill Quarter", 65 sensors

# ---------------------------------------------------------------------------
# Category C: Temporal patterns
# ---------------------------------------------------------------------------

# Readings AVG(value) grouped by month -- August (month 8) is highest
READINGS_HIGHEST_AVG_MONTH = 8  # August
READINGS_HIGHEST_AVG_VALUE = 40.37  # tolerance +/- 0.5

# Complaints trend: H1 (Jan-Jun) = 1497, H2 (Jul-Dec) = 1503 -- stable
COMPLAINTS_TREND = "stable"

# First incident date (MIN of date_reported)
FIRST_INCIDENT_DATE = "2024-01-01"

# Average readings per day: 49302 readings / 366 distinct days
READINGS_PER_DAY_AVG = 134.7  # tolerance +/- 1.0

# Complaints seasonal: monthly counts range 228-270, no clear peak
# The month with the most complaints (for reference):
COMPLAINTS_PEAK_MONTH_COUNT = 270  # tolerance: check max monthly count

# ---------------------------------------------------------------------------
# Category D: Statistical properties
# ---------------------------------------------------------------------------

READINGS_VALUE_STDDEV = 68.70  # sample stddev, tolerance +/- 0.5
READINGS_VALUE_SKEWNESS_SIGN = "positive"  # strongly positive (8.37)
COMPLAINTS_ZONE_CARDINALITY = 35  # 25 valid + 10 orphan zone_ids
READINGS_NULL_RATE_PCT = 0.48  # tolerance +/- 0.05

# ---------------------------------------------------------------------------
# Category E: Cross-cutting correctness
# ---------------------------------------------------------------------------

# Sum of all row counts across 8 tables
TOTAL_ROWS_ALL_TABLES = 59912

# Tables with date/time columns (7 of 8; zones has none)
TABLES_WITH_TEMPORAL_COLUMNS = sorted([
    "complaints", "incidents", "inspections", "programs",
    "readings", "sensors", "sites",
])

# Column with highest null rate across entire dataset
HIGHEST_NULL_COLUMN = ("incidents", "linked_program_id", 85.86)  # tolerance +/- 0.5

# Top 3 zones by complaint count
TOP3_ZONES_BY_COMPLAINTS = [
    ("Z09", 203),  # Southview
    ("Z08", 193),  # Eastside
    ("Z18", 189),  # Fairview
]

# Table row counts for reference
TABLE_ROW_COUNTS = {
    "complaints": 3000,
    "incidents": 5000,
    "inspections": 1243,
    "programs": 10,
    "readings": 49302,
    "sensors": 832,
    "sites": 500,
    "zones": 25,
}
