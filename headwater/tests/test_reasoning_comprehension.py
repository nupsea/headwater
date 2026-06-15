"""Engine comprehension: schema brief, proposal verification, ontology propose.

Covers the fixes for the 'three thin single-table questions' failure: dotted
(schema-qualified) table refs parse correctly everywhere, descriptions reach the
model, cross-table proposals verify against real relationships, regeneration
avoids restating kept questions, and the ontology accepts only shape-compatible
LLM concept proposals.
"""

from __future__ import annotations

import pytest

from headwater.core.store import HeadwaterStore
from headwater.knowledge import make_projection
from headwater.knowledge.ontology import ColumnStats, compatible_concept
from headwater.reasoning.nodes.llm_propose import (
    _table_of,
    build_schema_brief,
    propose_and_verify,
)


class ScriptedProvider:
    """Returns a fixed JSON payload, recording the prompt it saw."""

    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.prompts: list[str] = []

    async def analyze(self, prompt: str, system: str) -> dict:
        self.prompts.append(prompt)
        return self.payload


@pytest.fixture()
def subscription_store(tmp_path):
    """A schema-qualified two-table source with a confirmed relationship."""
    store = HeadwaterStore(tmp_path / "h2_metadata.db")
    store.init()
    store.upsert_source("wh", "redshift", None, "redshift://stub:5439/dev")
    dim, fct = "data.dim_subscription", "data.fct_subscription"
    store.upsert_table("wh", dim, schema_name="data", row_count=1000, selected=True)
    store.upsert_table("wh", fct, schema_name="data", row_count=5000, selected=True)
    cols = {
        dim: [
            ("subscription_id", "varchar", "Unique identifier for the subscription."),
            ("subscription_name", "varchar", "Product name of the subscription plan."),
            ("payment_method_type", "varchar", "How the customer pays."),
            ("cancel_date", "date", "Date the subscription was cancelled."),
        ],
        fct: [
            ("subscription_id", "varchar", "Join key to the subscription dimension."),
            ("mrr_amount", "double", "Monthly recurring revenue in dollars."),
            ("event_date", "date", "Date of the subscription event."),
        ],
    }
    for t, cc in cols.items():
        for i, (name, dtype, desc) in enumerate(cc):
            store.upsert_column(
                "wh", t, name, dtype, description=desc, ordinal=i
            )
    store.insert_relationship(
        "wh", fct, "subscription_id", dim, "subscription_id", "foreign_key", 1.0, 1.0
    )
    store.con.execute(
        "INSERT INTO projects (id, slug, display_name) VALUES ('p', 'p', 'P')"
    )
    store.upsert_project_source("p", "wh", selected_tables=[dim, fct])
    try:
        yield store
    finally:
        store.close()


def _projection(store):
    class _S:  # settings stand-in for make_projection
        reasoning_engine = True

    return make_projection(_S(), store)



def _apply_ontology(store, projection):
    """Mirror production: ontology.map populates concepts before any propose."""
    from headwater.reasoning.nodes.ontology_map import OntologyMapNode
    from headwater.reasoning.types import NodeCtx, ProjectState

    state = ProjectState("p", store, projection)
    result = OntologyMapNode().verify({}, state, NodeCtx(settings=None, llm=None))
    projection.apply(result.facts)

def test_table_of_handles_schema_qualified_refs():
    assert _table_of("data.dim_subscription.cancel_date") == "data.dim_subscription"
    assert _table_of("events.duration") == "events"
    assert _table_of("bare") == ""


def test_schema_brief_carries_descriptions_rows_and_relationships(subscription_store):
    projection = _projection(subscription_store)
    # Populate the projection with concepts so the brief has classified columns.
    from headwater.reasoning.nodes.ontology_map import OntologyMapNode
    from headwater.reasoning.types import NodeCtx, ProjectState

    state = ProjectState("p", subscription_store, projection)
    result = OntologyMapNode().verify({}, state, NodeCtx(settings=None, llm=None))
    projection.apply(result.facts)

    brief = build_schema_brief(subscription_store, "p", projection)
    assert "Monthly recurring revenue" in brief  # descriptions reach the model
    assert "1,000 rows" in brief
    assert "RELATIONSHIPS" in brief
    assert "data.fct_subscription.subscription_id" in brief


def test_cross_table_proposals_verify_against_relationships(subscription_store):
    projection = _projection(subscription_store)
    _apply_ontology(subscription_store, projection)
    provider = ScriptedProvider(
        {
            "questions": [
                {  # valid cross-table: relationship exists
                    "title": "Which subscription plans drive the most revenue?",
                    "intent": "ranking",
                    "measure": "data.fct_subscription.mrr_amount",
                    "dimension": "data.dim_subscription.subscription_name",
                    "reason": "Revenue by plan.",
                },
                {  # hallucinated column -> dropped
                    "title": "Bogus",
                    "intent": "ranking",
                    "measure": "data.fct_subscription.invented",
                    "dimension": "data.dim_subscription.subscription_name",
                },
                {  # trend on a time anchor
                    "title": "How does revenue change over time?",
                    "intent": "trend",
                    "measure": "data.fct_subscription.mrr_amount",
                    "dimension": "data.fct_subscription.event_date",
                },
            ]
        }
    )
    specs = propose_and_verify(
        subscription_store,
        "p",
        projection=projection,
        provider=provider,
        goal_text="Understand consumer behavior from subscriptions",
    )
    titles = [s["title"] for s in specs]
    assert "Which subscription plans drive the most revenue?" in titles
    assert "Bogus" not in titles
    cross = next(s for s in specs if "revenue" in s["title"].lower() and "plans" in s["title"])
    assert set(cross["needed_columns"]) == {
        "data.fct_subscription.mrr_amount",
        "data.dim_subscription.subscription_name",
    }


def test_avoid_titles_suppresses_restatements(subscription_store):
    projection = _projection(subscription_store)
    _apply_ontology(subscription_store, projection)
    provider = ScriptedProvider(
        {
            "questions": [
                {
                    "title": "Which subscription plans drive the most revenue?",
                    "intent": "ranking",
                    "measure": "data.fct_subscription.mrr_amount",
                    "dimension": "data.dim_subscription.subscription_name",
                }
            ]
        }
    )
    specs = propose_and_verify(
        subscription_store,
        "p",
        projection=projection,
        provider=provider,
        goal_text="goal",
        avoid_titles=["Which subscription plans drive the most revenue?"],
    )
    assert specs == []
    assert "ALREADY ASKED" in provider.prompts[0]


def test_ontology_accepts_only_shape_compatible_proposals():
    numeric = ColumnStats(ref="t.mrr", dtype="double", distinct=100, total=1000)
    text = ColumnStats(ref="t.plan", dtype="varchar", distinct=5, total=1000)
    assert compatible_concept("Measure", numeric)
    assert not compatible_concept("Measure", text)  # text can't be a Measure
    assert compatible_concept("Dimension", text)
    assert not compatible_concept("TimeAnchor", text)
    assert compatible_concept(
        "TimeAnchor", ColumnStats(ref="t.start_date", dtype="varchar", distinct=9, total=10)
    )  # time-like NAME allows a text date column


def test_ontology_map_applies_verified_llm_proposals(subscription_store):
    from headwater.reasoning.nodes.ontology_map import OntologyMapNode
    from headwater.reasoning.types import NodeCtx, ProjectState

    projection = _projection(subscription_store)
    state = ProjectState("p", subscription_store, projection)
    node = OntologyMapNode()
    proposal = {
        "assignments": {
            # compatible: numeric -> Measure with a unit
            "data.fct_subscription.mrr_amount": {"concept": "Measure", "unit": "amount"},
            # incompatible: text column proposed as Measure -> rejected
            "data.dim_subscription.payment_method_type": {"concept": "Measure"},
        }
    }
    result = node.verify(proposal, state, NodeCtx(settings=None, llm=None))
    projection.apply(result.facts)
    nodes = {n.props.get("ref"): n for n in projection.nodes_of_type("Measure")}
    assert "data.fct_subscription.mrr_amount" in nodes
    assert nodes["data.fct_subscription.mrr_amount"].props.get("unit") == "amount"
    assert "data.dim_subscription.payment_method_type" not in nodes


def test_relationship_upsert_replaces_instead_of_duplicating(subscription_store):
    # Confirming the same relationship again must not create a second row.
    subscription_store.insert_relationship(
        "wh",
        "data.fct_subscription",
        "subscription_id",
        "data.dim_subscription",
        "subscription_id",
        "foreign_key",
        1.0,
        1.0,
    )
    rels = subscription_store.get_relationships("wh")
    assert len(rels) == 1
    assert rels[0]["confidence"] == 1.0


def test_drafted_sql_quotes_schema_qualified_tables(subscription_store):
    from headwater.services.h2_answer import _q, _resolve_col_info, _validate_identifier

    assert _validate_identifier("data.dim_subscription")
    assert _q("data.dim_subscription") == '"data"."dim_subscription"'
    info = _resolve_col_info("data.dim_subscription.cancel_date", {}, None)
    assert info["table"] == "data.dim_subscription"
    assert info["column"] == "cancel_date"


def test_flag_columns_are_dimensions_not_measures():
    from headwater.knowledge.ontology import classify_column

    flag = ColumnStats(ref="t.downgrade_flag", dtype="integer", distinct=2, total=1000)
    assert classify_column(flag).concept == "Dimension"
    assert not compatible_concept("Measure", flag)
    named = ColumnStats(ref="t.is_active", dtype="integer", distinct=0, total=0)
    assert classify_column(named).concept == "Dimension"
    real = ColumnStats(ref="t.mrr_amount", dtype="double", distinct=400, total=1000)
    assert classify_column(real).concept == "Measure"


def test_evidence_outranks_flag_name_shape():
    """Statistics decide; the name morpheme is only a no-stats fallback.

    'active_devices' matches the flag regex but has 400 distinct values — it
    is a count (Measure). Conversely an oddly-named two-valued column is a
    flag even though no name cue fires.
    """
    from headwater.knowledge.ontology import classify_column

    count_col = ColumnStats(ref="t.active_devices", dtype="integer", distinct=400, total=5000)
    assert classify_column(count_col).concept == "Measure"
    assert compatible_concept("Measure", count_col)
    odd_flag = ColumnStats(ref="t.xyzzy", dtype="integer", distinct=2, total=5000)
    assert classify_column(odd_flag).concept == "Dimension"
    assert not compatible_concept("Measure", odd_flag)


def test_ranking_sort_direction_follows_question_wording():
    from headwater.services.h2_answer import _build_sql_and_chart

    col_info = [
        {"ref": "t.plan", "table": "t", "column": "plan", "role": "categorical",
         "role_class": "category", "dtype": "varchar", "safe": True,
         "resource_defined": False, "derivation_format": None},
        {"ref": "t.mrr", "table": "t", "column": "mrr", "role": "measure",
         "role_class": "measure", "dtype": "double", "safe": True,
         "resource_defined": False, "derivation_format": None},
    ]
    sql_hi, _ = _build_sql_and_chart(
        "ranking", col_info, "s", [], {}, title="Which plan has the highest mrr?"
    )
    assert "DESC" in sql_hi  # highest -> top of a descending sort
    sql_lo, _ = _build_sql_and_chart(
        "ranking", col_info, "s", [], {}, title="Which plan has the lowest mrr?"
    )
    assert "ASC" in sql_lo


def test_count_question_drafts_count_sql():
    from headwater.services.h2_answer import _build_sql_and_chart

    col_info = [
        {"ref": "t.plan", "table": "t", "column": "plan", "role": "categorical",
         "role_class": "category", "dtype": "varchar", "safe": True,
         "resource_defined": False, "derivation_format": None},
    ]
    sql, chart = _build_sql_and_chart(
        "ranking", col_info, "s", [], {}, title="Which plan is most popular?"
    )
    assert "COUNT(*)" in sql and "GROUP BY" in sql
    assert chart["y"] == "record_count"


def test_temporal_without_measure_counts_per_period():
    from headwater.services.h2_answer import _build_sql_and_chart

    col_info = [
        {"ref": "t.created_at", "table": "t", "column": "created_at",
         "role": "event_ts", "role_class": "timestamp", "dtype": "timestamp",
         "safe": True, "resource_defined": False, "derivation_format": None},
    ]
    sql, chart = _build_sql_and_chart(
        "temporal", col_info, "s", [], {}, title="Trend of signups over time"
    )
    assert "COUNT(*)" in sql and "GROUP BY period" in sql
    assert chart == {"type": "line", "x": "period", "y": "event_count"}


def test_zero_variance_ranking_states_no_variation():
    from headwater.services.h2_insight import summarize_answer

    rows = [{"plan": p, "avg_flag": 0.0} for p in ("a", "b", "c")]
    f = summarize_answer(
        chart_spec={"type": "bar", "x": "plan", "y": "avg_flag"},
        columns=["plan", "avg_flag"],
        rows=rows,
        title="Which plan has the highest flag?",
    )
    assert f is not None
    assert "No variation" in f.headline
    assert "highest" not in f.headline
