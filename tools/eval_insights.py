"""End-to-end insight-quality evaluation on fresh synthetic datasets.

Generates datasets the engine has never seen (different domains, realistic
distributions, joins, flags, timestamps, empty-ish columns), runs the FULL
pipeline (ingest -> frame -> LLM question generation -> recompute: draft,
execute, readiness, findings), then grades the output structurally:

  G1  every drafted SQL executes without error
  G2  every ranking/segment result has >1 row AND variance in the measure
      (or honestly states "No variation") — never a fake "highest"
  G3  trend questions produce multi-period series, not a coverage line
  G4  chart specs exist for ranking/trend answers (bar/line)
  G5  at least one cross-table (JOIN) question per dataset
  G6  no coverage filler ("records in scope") on chart-shaped questions
  G7  no question ranks by a flag/identifier (concept grounding held)
  G8  sort direction matches the question's wording

Usage:  uv run python ../tools/eval_insights.py [--dataset ecommerce|fleet|all]
Run from headwater/ (the Python project root). Requires Ollama for question
generation; grading itself is deterministic.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

# ── Dataset generators (synthetic, domain-diverse, never seen by the engine) ──


def gen_ecommerce(path: Path, seed: int = 7) -> dict:
    """Online retail: customers, products, orders (joins, money, flags, time)."""
    rng = random.Random(seed)
    con = duckdb.connect(str(path))
    segments = ["consumer", "small_business", "enterprise"]
    regions = ["north", "south", "east", "west", "central"]
    categories = ["electronics", "home", "apparel", "sports", "beauty", "grocery"]
    channels = ["web", "mobile_app", "marketplace", "phone"]

    con.execute("CREATE TABLE customers (customer_id INTEGER, segment VARCHAR, region VARCHAR, signup_date DATE, is_loyalty_member INTEGER)")
    base = datetime(2023, 1, 1)
    for i in range(800):
        con.execute(
            "INSERT INTO customers VALUES (?, ?, ?, ?, ?)",
            (
                i,
                rng.choices(segments, weights=[70, 22, 8])[0],
                rng.choice(regions),
                (base + timedelta(days=rng.randint(0, 700))).date(),
                int(rng.random() < 0.35),
            ),
        )

    con.execute("CREATE TABLE products (product_id INTEGER, category VARCHAR, unit_price DOUBLE, is_discontinued INTEGER)")
    for i in range(120):
        cat = rng.choice(categories)
        price = {
            "electronics": rng.uniform(80, 1200), "home": rng.uniform(15, 300),
            "apparel": rng.uniform(10, 150), "sports": rng.uniform(20, 400),
            "beauty": rng.uniform(5, 90), "grocery": rng.uniform(2, 40),
        }[cat]
        con.execute("INSERT INTO products VALUES (?, ?, ?, ?)", (i, cat, round(price, 2), int(rng.random() < 0.1)))

    con.execute(
        "CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, product_id INTEGER, "
        "order_date TIMESTAMP, quantity INTEGER, order_amount DOUBLE, sales_channel VARCHAR, was_returned INTEGER)"
    )
    for i in range(12_000):
        cid = rng.randint(0, 799)
        pid = rng.randint(0, 119)
        price = con.execute("SELECT unit_price FROM products WHERE product_id=?", (pid,)).fetchone()[0]
        qty = rng.choices([1, 2, 3, 5], weights=[70, 18, 8, 4])[0]
        # Seasonal + channel effects so trends/rankings have real structure.
        day = rng.randint(0, 700)
        season_boost = 1.6 if (day % 365) > 320 else 1.0
        channel = rng.choices(channels, weights=[45, 30, 20, 5])[0]
        amount = round(price * qty * season_boost * rng.uniform(0.9, 1.1), 2)
        con.execute(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (i, cid, pid, base + timedelta(days=day, hours=rng.randint(6, 23)), qty, amount,
             channel, int(rng.random() < (0.12 if channel == "marketplace" else 0.04))),
        )
    con.close()
    return {
        "name": "shopnova",
        "goal": "Understand revenue drivers and customer purchasing behavior across regions and channels",
        "tables": ["customers", "products", "orders"],
        "rels": [
            ("orders", "customer_id", "customers", "customer_id"),
            ("orders", "product_id", "products", "product_id"),
        ],
    }


def gen_fleet(path: Path, seed: int = 21) -> dict:
    """Logistics fleet: vehicles, drivers, trips (durations, fuel, incidents)."""
    rng = random.Random(seed)
    con = duckdb.connect(str(path))
    vtypes = ["box_truck", "van", "semi", "refrigerated"]
    depots = ["harborview", "eastgate", "midtown", "airport_park"]

    con.execute("CREATE TABLE vehicles (vehicle_id INTEGER, vehicle_type VARCHAR, depot VARCHAR, model_year INTEGER, is_electric INTEGER)")
    for i in range(60):
        con.execute(
            "INSERT INTO vehicles VALUES (?, ?, ?, ?, ?)",
            (i, rng.choice(vtypes), rng.choice(depots), rng.randint(2015, 2024), int(rng.random() < 0.2)),
        )

    con.execute("CREATE TABLE drivers (driver_id INTEGER, home_depot VARCHAR, hire_date DATE, safety_rating DOUBLE)")
    base = datetime(2022, 6, 1)
    for i in range(90):
        con.execute(
            "INSERT INTO drivers VALUES (?, ?, ?, ?)",
            (i, rng.choice(depots), (base + timedelta(days=rng.randint(0, 600))).date(),
             round(rng.uniform(2.5, 5.0), 2)),
        )

    con.execute(
        "CREATE TABLE trips (trip_id INTEGER, vehicle_id INTEGER, driver_id INTEGER, "
        "departed_at TIMESTAMP, trip_minutes DOUBLE, distance_km DOUBLE, fuel_liters DOUBLE, "
        "delay_minutes DOUBLE, had_incident INTEGER)"
    )
    for i in range(9_000):
        vid = rng.randint(0, 59)
        vt = con.execute("SELECT vehicle_type FROM vehicles WHERE vehicle_id=?", (vid,)).fetchone()[0]
        dist = {"box_truck": rng.uniform(20, 220), "van": rng.uniform(5, 120),
                "semi": rng.uniform(150, 900), "refrigerated": rng.uniform(40, 400)}[vt]
        speed = rng.uniform(38, 65)
        minutes = dist / speed * 60 * rng.uniform(0.95, 1.25)
        day = rng.randint(0, 540)
        # Winter delays so trends move; depot effects so rankings differentiate.
        delay = max(0.0, rng.gauss(8 if (day % 365) < 60 else 3, 6))
        con.execute(
            "INSERT INTO trips VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (i, vid, rng.randint(0, 89), base + timedelta(days=day, hours=rng.randint(4, 22)),
             round(minutes, 1), round(dist, 1), round(dist * rng.uniform(0.18, 0.42), 1),
             round(delay, 1), int(rng.random() < 0.015)),
        )
    con.close()
    return {
        "name": "routepulse",
        "goal": "Find where delivery delays and fuel costs concentrate across the fleet and depots",
        "tables": ["vehicles", "drivers", "trips"],
        "rels": [
            ("trips", "vehicle_id", "vehicles", "vehicle_id"),
            ("trips", "driver_id", "drivers", "driver_id"),
        ],
    }


# ── Pipeline run ───────────────────────────────────────────────────────────────


def run_dataset(gen, label: str) -> list[str]:
    workdir = Path(tempfile.mkdtemp(prefix=f"hw_eval_{label}_"))
    (workdir / "hw").mkdir(parents=True, exist_ok=True)
    os.environ["HEADWATER_DATA_DIR"] = str(workdir / "hw")
    # Late imports: settings bind to the env var above.
    from headwater.core.config import get_settings

    get_settings.cache_clear()

    from headwater.core.store import HeadwaterStore
    from headwater.services.h2_pipeline import recompute_project
    from headwater.services.h2_project import frame_project, propose_relevance
    from headwater.services.h2_source import discover_and_persist, ingest_tables

    db = workdir / f"{label}.duckdb"
    meta = gen(db)
    settings = get_settings()
    settings.reasoning_engine = True

    store = HeadwaterStore(Path(os.environ["HEADWATER_DATA_DIR"]) / "h2_metadata.db")
    store.init()
    discover_and_persist(str(db), store=store, source_type="duckdb", name=meta["name"])
    ingest_tables(store, meta["name"], meta["tables"])
    for ft, fc, tt, tc in meta["rels"]:
        store.insert_relationship(meta["name"], ft, fc, tt, tc, "foreign_key", 1.0, 1.0)

    pid = f"eval_{meta['name']}"
    frame_project(
        store=store, project_id=pid, source_name=meta["name"], display_name=pid,
        goal_statement=meta["goal"], selected_tables=meta["tables"], settings=settings,
    )
    propose_relevance(store=store, project_id=pid)
    recompute_project(store, pid, settings=settings)

    # Grade the answers the UI would receive (rows + findings come from
    # finalize, never the stored artifacts — those hold no raw rows).
    from headwater.services.h2_pipeline import finalize_project_answers

    final = finalize_project_answers(store, pid, settings=settings, run_judge=False)
    failures = grade(store, pid, meta, final.answers)
    store.close()
    return failures


def grade(store, pid: str, meta: dict, answers) -> list[str]:
    from headwater.services.h2_insight import _HIGH_INTENT, _LOW_INTENT

    failures: list[str] = []
    questions = {q["id"]: q for q in store.list_questions(pid)}
    print(f"\n=== {meta['name']}: {len(answers)} answers ===")
    cross_table = 0
    for a in answers:
        q = questions.get(a.question_id, {})
        title = a.question_title or ""
        sql = a.sql_text or ""
        chart = a.chart_spec or {}
        finding = a.finding_headline or ""
        needed = q.get("question", {}).get("needed_columns") or []
        tables = {c.rsplit(".", 1)[0] for c in needed if "." in c}
        cross_table += len(tables) > 1

        print(f"\n[{a.question_id.rsplit(':', 1)[-1]}] {title}")
        print(f"  cols={needed} chart={chart.get('type')} rows={a.row_count} state={a.state}")
        if finding:
            print(f"  finding: {finding} {a.finding_support}")

        if a.execution_error:
            failures.append(f"G1 {a.question_id}: SQL error: {a.execution_error}")
            continue
        ctype = chart.get("type")
        if ctype == "bar":
            y = chart.get("y")
            vals = [r.get(y) for r in a.rows if isinstance(r.get(y), (int, float))]
            if len(vals) < 2:
                failures.append(f"G2 {a.question_id}: ranking with {len(vals)} usable rows")
            elif len(set(vals)) == 1 and "No variation" not in finding:
                failures.append(f"G2 {a.question_id}: zero-variance ranking not flagged")
            if _HIGH_INTENT.search(title) and not _LOW_INTENT.search(title):
                if " ASC" in sql.split("ORDER BY")[-1]:
                    failures.append(f"G8 {a.question_id}: 'highest' question sorted ASC")
            if "records in scope" in finding:
                failures.append(f"G6 {a.question_id}: coverage filler on a ranking")
        elif ctype == "line":
            if a.row_count < 2:
                failures.append(f"G3 {a.question_id}: trend with {a.row_count} period(s)")
            if "records in scope" in finding:
                failures.append(f"G6 {a.question_id}: coverage filler on a trend")
        elif sql and a.row_count > 0 and ctype not in ("bar", "line", "table"):
            failures.append(f"G4 {a.question_id}: no chart spec for an executed answer")
        # G7: no flag/id measures
        for col, role in (q.get("question", {}).get("col_roles") or {}).items():
            name = col.rsplit(".", 1)[-1].lower()
            if role == "measure" and (name.endswith("_flag") or name.endswith("_id")):
                failures.append(f"G7 {a.question_id}: {col} used as a measure")
    if cross_table == 0:
        failures.append("G5: no cross-table question generated")
    # Breadth: a goal over a multi-table source deserves more than a couple of
    # rankings — require a minimum set and at least one temporal view.
    if len(answers) < 5:
        failures.append(f"G9: only {len(answers)} question(s) — expected >= 5")
    if not any((a.chart_spec or {}).get("type") == "line" for a in answers):
        failures.append("G10: no trend (line) question despite timestamp columns")
    print(f"\ncross-table questions: {cross_table}/{len(answers)}")
    return failures


def main() -> int:
    import logging

    logging.basicConfig(
        level=logging.INFO, format="%(name)s %(levelname)s: %(message)s"
    )
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="all", choices=["ecommerce", "fleet", "all"])
    args = ap.parse_args()
    gens = {"ecommerce": gen_ecommerce, "fleet": gen_fleet}
    selected = gens if args.dataset == "all" else {args.dataset: gens[args.dataset]}

    all_failures: dict[str, list[str]] = {}
    for label, gen in selected.items():
        all_failures[label] = run_dataset(gen, label)

    print("\n" + "=" * 60)
    ok = True
    for label, fails in all_failures.items():
        if fails:
            ok = False
            print(f"FAIL {label}: {len(fails)} issue(s)")
            for f in fails:
                print(f"  - {f}")
        else:
            print(f"PASS {label}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
