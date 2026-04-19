"""Verify golden answers against raw DuckDB queries on sample data.

Run: cd headwater && uv run python tests/golden/verify_golden.py

Prints a comparison table for manual review. Every answer must match
before we freeze the golden answer set.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

# Add project root to path so we can import golden answers
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tests.golden.explore_answers import (
    AVG_READING_VALUE,
    COMPLAINTS_PER_ZONE_TOP1,
    COMPLAINTS_ZONE_CARDINALITY,
    DISTINCT_ZONES,
    FIRST_INCIDENT_DATE,
    HIGHEST_NULL_COLUMN,
    MAX_READING_VALUE,
    PROGRAMS_WITHOUT_INCIDENTS_COUNT,
    READINGS_HIGHEST_AVG_MONTH,
    READINGS_HIGHEST_AVG_VALUE,
    READINGS_NULL_RATE_PCT,
    READINGS_PER_DAY_AVG,
    READINGS_VALUE_STDDEV,
    SITES_WITH_INSPECTIONS,
    TABLE_ROW_COUNTS,
    TOP3_ZONES_BY_COMPLAINTS,
    TOTAL_COMPLAINTS,
    TOTAL_ROWS_ALL_TABLES,
)

SAMPLE_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "sample"


def load_sample_data(con: duckdb.DuckDBPyConnection) -> None:
    """Load all 8 JSON files into DuckDB in 'env_health' schema."""
    con.execute("CREATE SCHEMA IF NOT EXISTS env_health")
    for f in SAMPLE_DIR.glob("*.json"):
        table_name = f.stem
        con.execute(
            f"CREATE TABLE env_health.{table_name} AS "
            f"SELECT * FROM read_json_auto('{f}', maximum_object_size=10485760)"
        )


def run_checks(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str, str, bool]]:
    """Run all golden answer queries. Returns list of (name, expected, actual, match)."""
    results: list[tuple[str, str, str, bool]] = []

    def check(name: str, expected, actual, tolerance=None):
        if tolerance and isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
            match = abs(expected - actual) <= tolerance
        else:
            match = expected == actual
        results.append((name, str(expected), str(actual), match))

    # A: Single-table aggregations
    r = con.execute("SELECT COUNT(*) FROM env_health.complaints").fetchone()
    check("total_complaints", TOTAL_COMPLAINTS, r[0])

    r = con.execute("SELECT AVG(value) FROM env_health.readings").fetchone()
    check("avg_reading_value", AVG_READING_VALUE, round(r[0], 2), tolerance=0.5)

    r = con.execute("SELECT COUNT(DISTINCT zone_id) FROM env_health.zones").fetchone()
    check("distinct_zones", DISTINCT_ZONES, r[0])

    r = con.execute("SELECT MAX(value) FROM env_health.readings").fetchone()
    check("max_reading_value", MAX_READING_VALUE, r[0])

    # B: Multi-table joins
    r = con.execute(
        "SELECT zone_id, COUNT(*) AS cnt FROM env_health.complaints "
        "GROUP BY zone_id ORDER BY cnt DESC LIMIT 1"
    ).fetchone()
    check("complaints_per_zone_top1", COMPLAINTS_PER_ZONE_TOP1, (r[0], r[1]))

    r = con.execute(
        "SELECT COUNT(DISTINCT i.site_id) FROM env_health.inspections i "
        "JOIN env_health.sites s ON i.site_id = s.site_id"
    ).fetchone()
    check("sites_with_inspections", SITES_WITH_INSPECTIONS, r[0])

    r = con.execute(
        "SELECT COUNT(*) FROM env_health.programs p "
        "WHERE p.program_id NOT IN "
        "(SELECT DISTINCT linked_program_id FROM env_health.incidents "
        " WHERE linked_program_id IS NOT NULL)"
    ).fetchone()
    check("programs_without_incidents", PROGRAMS_WITHOUT_INCIDENTS_COUNT, r[0])

    # C: Temporal
    r = con.execute(
        "SELECT EXTRACT(MONTH FROM timestamp::TIMESTAMP) AS m, AVG(value) AS avg_val "
        "FROM env_health.readings WHERE value IS NOT NULL "
        "GROUP BY m ORDER BY avg_val DESC LIMIT 1"
    ).fetchone()
    check("readings_highest_avg_month", READINGS_HIGHEST_AVG_MONTH, int(r[0]))
    check("readings_highest_avg_value", READINGS_HIGHEST_AVG_VALUE, round(r[1], 2), tolerance=0.5)

    r = con.execute(
        "SELECT MIN(date_reported) FROM env_health.incidents"
    ).fetchone()
    check("first_incident_date", FIRST_INCIDENT_DATE, str(r[0]))

    r = con.execute(
        "SELECT COUNT(*)::DOUBLE / COUNT(DISTINCT timestamp::DATE) "
        "FROM env_health.readings"
    ).fetchone()
    check("readings_per_day_avg", READINGS_PER_DAY_AVG, round(r[0], 1), tolerance=1.0)

    # D: Statistical
    r = con.execute("SELECT STDDEV(value) FROM env_health.readings").fetchone()
    check("readings_stddev", READINGS_VALUE_STDDEV, round(r[0], 2), tolerance=0.5)

    r = con.execute(
        "SELECT COUNT(DISTINCT zone_id) FROM env_health.complaints"
    ).fetchone()
    check("complaints_zone_cardinality", COMPLAINTS_ZONE_CARDINALITY, r[0])

    r = con.execute(
        "SELECT 100.0 * SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*) "
        "FROM env_health.readings"
    ).fetchone()
    check("readings_null_rate_pct", READINGS_NULL_RATE_PCT, round(r[0], 2), tolerance=0.05)

    # E: Cross-cutting
    total = 0
    for table, expected_count in TABLE_ROW_COUNTS.items():
        r = con.execute(f"SELECT COUNT(*) FROM env_health.{table}").fetchone()
        total += r[0]
        check(f"row_count_{table}", expected_count, r[0])
    check("total_rows_all_tables", TOTAL_ROWS_ALL_TABLES, total)

    # Top 3 zones by complaints
    rows = con.execute(
        "SELECT zone_id, COUNT(*) AS cnt FROM env_health.complaints "
        "GROUP BY zone_id ORDER BY cnt DESC LIMIT 3"
    ).fetchall()
    actual_top3 = [(r[0], r[1]) for r in rows]
    check("top3_zones_by_complaints", TOP3_ZONES_BY_COMPLAINTS, actual_top3)

    # Highest null column
    r = con.execute(
        "SELECT 100.0 * SUM(CASE WHEN linked_program_id IS NULL THEN 1 ELSE 0 END) "
        "/ COUNT(*) FROM env_health.incidents"
    ).fetchone()
    check(
        "highest_null_column_pct",
        HIGHEST_NULL_COLUMN[2],
        round(r[0], 2),
        tolerance=0.5,
    )

    return results


def main():
    con = duckdb.connect(":memory:")
    print("Loading sample data...")
    load_sample_data(con)
    print("Running checks...\n")

    results = run_checks(con)
    con.close()

    # Print table
    print(f"{'Check':<35} {'Expected':<30} {'Actual':<30} {'Match'}")
    print("-" * 130)
    passed = 0
    for name, expected, actual, match in results:
        status = "OK" if match else "FAIL"
        print(f"{name:<35} {expected:<30} {actual:<30} {status}")
        if match:
            passed += 1

    print(f"\n{passed}/{len(results)} checks passed.")
    if passed < len(results):
        print("SOME CHECKS FAILED -- review and fix golden answers before freezing.")
        sys.exit(1)
    else:
        print("All golden answers verified.")


if __name__ == "__main__":
    main()
