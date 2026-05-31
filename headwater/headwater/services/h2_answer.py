"""Headwater 2 S13 — Grounded Answer Drafting.

Generates SQL drafts and chart specs from curated questions using semantic roles,
selected scope, and profiled grain.  This is NOT NL-to-SQL — queries are built
from safe templates keyed by question type and column role, not from free-form
language.

Safety rules:
  - Every identifier (table, column) passes a whitelist regex before use.
  - Only columns that exist in the project source catalog are referenced.
  - Fan-out joins are rejected when relationship confidence < 0.80.
  - Answers remain Draft unless all readiness contracts pass (state == "certified").
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from headwater.core.store import HeadwaterStore

# Question type tags encoded in the question ID suffix
_TEMPORAL_TAGS = {"when-worst", "temporal", "trend", "over-time"}
_SEGMENT_TAGS = {"which-segment", "segment", "breakdown", "highest", "lowest"}
_RANKING_TAGS = {"entity-ranking", "ranking", "best", "worst"}
_COVERAGE_TAGS = {"coverage", "summary", "overview"}

# Column roles mapped to SQL behaviour
_TIMESTAMP_ROLES = {"event_ts", "start_ts", "end_ts", "time_anchor", "temporal"}
_MEASURE_ROLES = {"measure", "duration", "quantity", "metric", "amount"}
_CATEGORY_ROLES = {"categorical", "code", "flag", "category"}
_IDENTITY_ROLES = {"identifier", "foreign_key", "entity", "primary_key"}

_NUMERIC_DTYPES = {
    "int", "int8", "int16", "int32", "int64", "integer",
    "float", "float32", "float64", "double", "decimal",
    "numeric", "real", "bigint", "smallint",
}
_TIMESTAMP_DTYPES = {"timestamp", "date", "datetime", "timestamptz"}

_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")

_SAFE_AGG_FUNCS = {"AVG", "SUM", "COUNT", "MIN", "MAX"}


@dataclass(slots=True)
class AnswerDraft:
    question_id: str
    question_title: str
    state: str  # certified, draft, cannot_answer
    sql_text: str | None
    chart_spec: dict[str, Any]
    confidence: float
    caveats: list[str] = field(default_factory=list)
    source_snapshot_id: str | None = None


@dataclass(slots=True)
class ProjectAnswers:
    project_id: str
    answers: list[AnswerDraft] = field(default_factory=list)

    @property
    def certified_count(self) -> int:
        return sum(1 for a in self.answers if a.state == "certified")

    @property
    def draft_count(self) -> int:
        return sum(1 for a in self.answers if a.state == "draft")

    @property
    def cannot_answer_count(self) -> int:
        return sum(1 for a in self.answers if a.state == "cannot_answer")


def draft_project_answers(
    store: HeadwaterStore,
    project_id: str,
) -> ProjectAnswers:
    """Generate SQL drafts and chart specs for all project questions."""
    project = store.get_project(project_id)
    if project is None:
        raise ValueError(f"Project '{project_id}' is not registered.")

    project_sources = store.get_project_sources(project_id)
    if not project_sources:
        raise ValueError(f"Project '{project_id}' has no linked source.")

    source_name = project_sources[0]["source_name"]
    snapshot = store.get_latest_source_snapshot(source_name)
    snapshot_id = snapshot["id"] if snapshot else None

    questions = [
        q for q in store.list_questions(project_id) if q.get("status") != "dropped"
    ]
    claims = store.list_semantic_claims(project_id)
    relationships = store.get_relationships(source_name)

    col_role_map = _build_col_role_map(claims, store, source_name)
    rel_confidence = _build_rel_confidence_map(relationships)

    result = ProjectAnswers(project_id=project_id)

    for question in questions:
        verdict_id = f"{question['id']}:verdict:latest"
        verdict = store.get_readiness_verdict(verdict_id)

        draft = _draft_answer(
            question=question,
            verdict=verdict,
            col_role_map=col_role_map,
            rel_confidence=rel_confidence,
            source_name=source_name,
            snapshot_id=snapshot_id,
        )

        store.upsert_answer_artifact(
            f"{question['id']}:answer:latest",
            question_id=question["id"],
            sql_text=draft.sql_text,
            chart_spec=draft.chart_spec,
            state=draft.state,
            certified_at=None,  # set when state transitions to certified
            source_snapshot_id=snapshot_id,
        )
        result.answers.append(draft)

    return result


def _draft_answer(
    question: dict[str, Any],
    verdict: dict[str, Any] | None,
    col_role_map: dict[str, dict[str, Any]],
    rel_confidence: dict[tuple[str, str], float],
    source_name: str,
    snapshot_id: str | None,
) -> AnswerDraft:
    qid = question["id"]
    title = question.get("title", qid)
    answerability = question.get("answerability", "answerable")
    q_payload = question.get("question") or {}
    needed_cols = list(q_payload.get("needed_columns") or [])

    if answerability == "cannot_answer":
        return AnswerDraft(
            question_id=qid,
            question_title=title,
            state="cannot_answer",
            sql_text=None,
            chart_spec={},
            confidence=0.0,
            caveats=[q_payload.get("reason") or ""],
            source_snapshot_id=snapshot_id,
        )

    state = "certified" if (verdict and verdict.get("state") == "certified") else "draft"
    readiness_pct = int(verdict["readiness_pct"]) if verdict else 0
    confidence = round(readiness_pct / 100.0, 2)

    question_type = _detect_question_type(qid, title)
    caveats: list[str] = []

    explicit_roles: dict[str, str] = q_payload.get("col_roles") or {}
    col_info = [_resolve_col_info(c, col_role_map, explicit_roles) for c in needed_cols]
    invalid = [c for c in col_info if not c["safe"]]
    if invalid:
        caveats.append(
            f"Unsafe identifier(s) excluded: {', '.join(c['ref'] for c in invalid)}"
        )
        col_info = [c for c in col_info if c["safe"]]

    # Fan-out guard: warn if a required join has low confidence
    join_caveats = _check_join_safety(col_info, rel_confidence)
    caveats.extend(join_caveats)

    sql, chart_spec = _build_sql_and_chart(question_type, col_info, source_name, caveats)

    return AnswerDraft(
        question_id=qid,
        question_title=title,
        state=state,
        sql_text=sql,
        chart_spec=chart_spec,
        confidence=confidence,
        caveats=caveats,
        source_snapshot_id=snapshot_id,
    )


# ── SQL and chart building ────────────────────────────────────────────────────

def _build_sql_and_chart(
    question_type: str,
    col_info: list[dict[str, Any]],
    source_name: str,
    caveats: list[str],
) -> tuple[str | None, dict[str, Any]]:
    ts_cols = [c for c in col_info if c["role_class"] == "timestamp"]
    measure_cols = [c for c in col_info if c["role_class"] == "measure"]
    cat_cols = [c for c in col_info if c["role_class"] == "category"]
    id_cols = [c for c in col_info if c["role_class"] == "identity"]

    if not col_info:
        return None, {}

    # Pick a primary table from the first column
    primary_table = col_info[0]["table"]

    if question_type == "temporal" and ts_cols and measure_cols:
        return _temporal_sql(primary_table, ts_cols[0], measure_cols[0], caveats)

    # Segmentation and ranking both group by a categorical or identity column.
    # Ranking sorts ASC (find the lowest), segmentation sorts DESC (find the highest).
    group_col = cat_cols[0] if cat_cols else (id_cols[0] if id_cols else None)
    if question_type in ("segment", "ranking") and group_col and measure_cols:
        sort_asc = question_type == "ranking"
        return _segmentation_sql(primary_table, group_col, measure_cols[0], caveats,
                                 sort_asc=sort_asc)

    if question_type == "coverage":
        return _coverage_sql(primary_table, col_info, caveats)

    # Fallback: coverage summary when type doesn't match available columns
    return _coverage_sql(primary_table, col_info, caveats)


def _temporal_sql(
    table: str,
    ts_col: dict[str, Any],
    measure_col: dict[str, Any],
    caveats: list[str],
) -> tuple[str, dict[str, Any]]:
    t = _q(table)
    tc = _q(ts_col["column"])
    mc = _q(measure_col["column"])
    m_alias = _safe_alias(f"avg_{measure_col['column']}")
    ts_expr = _ts_trunc_expr(ts_col)
    measure_expr = _measure_agg_expr(measure_col, caveats)

    sql = (
        f"SELECT\n"
        f"    {ts_expr} AS period,\n"
        f"    {measure_expr} AS {m_alias},\n"
        f"    COUNT(*) AS record_count\n"
        f"FROM {t}\n"
        f"WHERE {tc} IS NOT NULL\n"
        f"  AND {mc} IS NOT NULL\n"
        f"GROUP BY period\n"
        f"ORDER BY period ASC"
    )
    chart = {"type": "line", "x": "period", "y": m_alias, "record_count": "record_count"}
    return sql, chart


def _segmentation_sql(
    table: str,
    cat_col: dict[str, Any],
    measure_col: dict[str, Any],
    caveats: list[str],
    *,
    sort_asc: bool = False,
) -> tuple[str, dict[str, Any]]:
    t = _q(table)
    cc = _q(cat_col["column"])
    c_alias = _safe_alias(cat_col["column"])
    m_alias = _safe_alias(f"avg_{measure_col['column']}")
    measure_expr = _measure_agg_expr(measure_col, caveats)
    order_dir = "ASC" if sort_asc else "DESC"

    sql = (
        f"SELECT\n"
        f"    {cc} AS {c_alias},\n"
        f"    {measure_expr} AS {m_alias},\n"
        f"    COUNT(*) AS record_count\n"
        f"FROM {t}\n"
        f"WHERE {cc} IS NOT NULL\n"
        f"GROUP BY {cc}\n"
        f"ORDER BY {m_alias} {order_dir}\n"
        f"LIMIT 20"
    )
    chart = {"type": "bar", "x": c_alias, "y": m_alias, "record_count": "record_count"}
    return sql, chart


def _coverage_sql(
    table: str,
    col_info: list[dict[str, Any]],
    caveats: list[str],
) -> tuple[str, dict[str, Any]]:
    t = _q(table)
    ts_cols = [c for c in col_info if c["role_class"] == "timestamp"]
    id_cols = [c for c in col_info if c["role_class"] == "identity"]

    select_parts = ["    COUNT(*) AS total_records"]
    if id_cols:
        select_parts.append(
            f"    COUNT(DISTINCT {_q(id_cols[0]['column'])}) AS unique_entities"
        )
    if ts_cols:
        tc = _q(ts_cols[0]["column"])
        select_parts.append(f"    MIN({tc}) AS earliest")
        select_parts.append(f"    MAX({tc}) AS latest")

    rows_joined = ",\n".join(select_parts)
    sql = f"SELECT\n{rows_joined}\nFROM {t}"
    return sql, {"type": "table"}


# ── Identifier and expression helpers ────────────────────────────────────────

def _q(name: str) -> str:
    """Quote a single identifier."""
    return f'"{name}"'


def _safe_alias(name: str) -> str:
    """Convert a column name to a safe SQL alias."""
    return re.sub(r"[^a-z0-9_]", "_", name.lower())[:64]


def _ts_trunc_expr(col: dict[str, Any]) -> str:
    c = _q(col["column"])
    if col["dtype"] in _TIMESTAMP_DTYPES:
        return f"DATE_TRUNC('day', {c})"
    # varchar timestamp — cast first; add caveat externally
    return f"CAST({c} AS DATE)"


def _measure_agg_expr(col: dict[str, Any], caveats: list[str]) -> str:
    c = _q(col["column"])
    if col["dtype"] in _NUMERIC_DTYPES:
        return f"AVG({c})"
    # varchar measure (e.g. HH:MM duration strings) — flag as needing cast
    caveats.append(
        f'"{col["column"]}" is varchar; cast to numeric before aggregating '
        f"(e.g. EXTRACT(EPOCH FROM CAST({col['column']} AS INTERVAL)) / 60)."
    )
    return f"AVG(TRY_CAST({c} AS DOUBLE))"


def _validate_identifier(name: str) -> bool:
    return bool(_IDENTIFIER_RE.match(name.lower()))


# ── Column resolution ─────────────────────────────────────────────────────────

def _resolve_col_info(
    col_ref: str,
    col_role_map: dict[str, dict[str, Any]],
    explicit_roles: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Resolve a 'table.column' string to its role class and dtype.

    explicit_roles: from the question payload's col_roles field — the proposal engine
    set these when it created the question, so they are authoritative.  The heuristic
    dtype/name classification is only used as a fallback when the explicit role is absent.
    """
    parts = col_ref.split(".", 1)
    if len(parts) == 2:
        table, column = parts
    else:
        table, column = "", parts[0]

    safe = _validate_identifier(table) and _validate_identifier(column)
    role_info = col_role_map.get(col_ref.lower(), {})
    dtype = (role_info.get("dtype") or "varchar").lower()

    # Prefer the explicit role stored in the question payload.
    explicit = (explicit_roles or {}).get(col_ref)
    role = explicit or role_info.get("semantic_role") or role_info.get("semantic_type") or ""
    role_class = _classify_role(role, dtype)

    return {
        "ref": col_ref,
        "table": table,
        "column": column,
        "role": role,
        "role_class": role_class,
        "dtype": dtype,
        "safe": safe,
        "resource_defined": role_info.get("resource_defined", False),
    }


def _classify_role(role: str, dtype: str) -> str:
    role_l = role.lower()
    dtype_l = dtype.lower()

    # Actual timestamp dtype always wins.
    if dtype_l in _TIMESTAMP_DTYPES:
        return "timestamp"

    # Temporal role: use as timestamp axis only when the dtype confirms it.
    # Varchar columns with a temporal role are ambiguous (date-as-string vs duration-
    # as-string); explicit_roles in _resolve_col_info handles the disambiguation.
    if role_l in _TIMESTAMP_ROLES:
        return "timestamp"

    if role_l in _MEASURE_ROLES:
        return "measure"
    if role_l in _CATEGORY_ROLES:
        return "category"
    if role_l in _IDENTITY_ROLES:
        return "identity"
    # Numeric dtype is a reliable fallback for measures.
    if dtype_l in _NUMERIC_DTYPES:
        return "measure"
    return "other"


def _build_col_role_map(
    claims: list[dict[str, Any]],
    store: HeadwaterStore,
    source_name: str,
) -> dict[str, dict[str, Any]]:
    """Build a col_ref → {semantic_role, dtype, resource_defined} map.

    Relevance claims are the primary source.  Column dtype from the store fills
    in what the claims don't cover.
    """
    role_map: dict[str, dict[str, Any]] = {}

    # Seed from store columns (dtype)
    for table in store.get_tables(source_name):
        for col in store.get_columns(source_name, table["name"]):
            key = f"{table['name']}.{col['name']}".lower()
            role_map[key] = {
                "semantic_role": col.get("semantic_type") or "",
                "dtype": col.get("dtype") or "varchar",
                "resource_defined": bool(col.get("locked")),
            }

    # Override with relevance claims (have semantic_role)
    for claim in claims:
        if claim.get("claim_type") != "relevance":
            continue
        table = claim.get("table_name") or ""
        col = claim.get("column_name") or ""
        if not table or not col:
            continue
        key = f"{table}.{col}".lower()
        existing = role_map.get(key, {})
        role_map[key] = {
            **existing,
            "semantic_role": (
                claim.get("claim", {}).get("semantic_role")
                or existing.get("semantic_role", "")
            ),
            "resource_defined": bool(claim.get("claim", {}).get("selected")),
        }

    return role_map


def _build_rel_confidence_map(
    relationships: list[dict[str, Any]],
) -> dict[tuple[str, str], float]:
    result: dict[tuple[str, str], float] = {}
    for rel in relationships:
        key = (rel.get("from_table", ""), rel.get("to_table", ""))
        existing = result.get(key, 0.0)
        result[key] = max(existing, float(rel.get("confidence") or 0.0))
    return result


def _check_join_safety(
    col_info: list[dict[str, Any]],
    rel_confidence: dict[tuple[str, str], float],
    min_confidence: float = 0.80,
) -> list[str]:
    tables = {c["table"] for c in col_info if c["table"]}
    if len(tables) <= 1:
        return []
    caveats = []
    tables_list = sorted(tables)
    for i, t1 in enumerate(tables_list):
        for t2 in tables_list[i + 1:]:
            conf = rel_confidence.get((t1, t2)) or rel_confidence.get((t2, t1)) or 0.0
            if conf < min_confidence:
                caveats.append(
                    f"Join {t1} → {t2} has low relationship confidence ({conf:.0%}); "
                    "verify join key before running."
                )
    return caveats


# ── Question type detection ───────────────────────────────────────────────────

def _detect_question_type(question_id: str, title: str) -> str:
    # Suffix is authoritative — it encodes the type at question-creation time.
    # Title hints are a fallback for questions created before explicit typing was added.
    suffix = question_id.rsplit(":", 1)[-1].lower()
    if suffix in _TEMPORAL_TAGS:
        return "temporal"
    if suffix in _RANKING_TAGS:
        return "ranking"
    if suffix in _SEGMENT_TAGS:
        return "segment"
    if suffix in _COVERAGE_TAGS:
        return "coverage"

    title_l = title.lower()
    if any(t in title_l for t in ("over time", "trend", "change", "peak", "hour", "day", "week")):
        return "temporal"
    if any(t in title_l for t in ("lowest", "highest", "best", "worst", "ranking")):
        return "ranking"
    if any(t in title_l for t in ("which ", "segment", "breakdown", "by ", "category")):
        return "segment"
    if any(t in title_l for t in ("coverage", "summary", "how many", "count")):
        return "coverage"
    return "segment"  # generic fallback
