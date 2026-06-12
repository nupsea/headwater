"""Headwater 2 S14 — Continuous Certification.

Re-evaluates readiness contracts against the current source state and
auto-demotes certified questions when any contract now fails.

The trust badge is alive: it revokes itself honestly with a reason and a
reference to the source snapshot that caused the failure.

Design:
  - evaluate_and_certify() is the main entry point.  It runs all contracts
    for every project question and compares the outcome to the stored verdict.
  - A certified question that now has a failing contract is marked "demoted".
  - Each demotion persists a revocation decision and opens a high-priority
    Resolve card that names the prior certified snapshot and the breaking
    contracts.
  - Snapshot diff compares profile data (null rates, distinct counts,
    top-value vocabulary) and relationship confidence between two snapshots.
    The diff feeds into the contract re-evaluation for richer explanations.

Vocabulary drift:  a new code value in a locked enum claim, or a previously
known code value disappearing, is reported as a definition_consistent failure.

Null rate drift:  a column whose null rate exceeds the structural_integrity
threshold triggers a demotion if that column is needed by a certified question.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from headwater.core.store import HeadwaterStore
from headwater.services.h2_readiness import evaluate_question

_HIGH_NULL_THRESHOLD = 0.50


@dataclass(slots=True)
class ProfileDrift:
    table_name: str
    column_name: str
    drift_kind: str  # "null_rate_increase", "vocab_new_value", "vocab_value_missing"
    from_value: Any
    to_value: Any
    description: str


@dataclass(slots=True)
class SnapshotDiff:
    source_name: str
    from_snapshot_id: str | None
    to_snapshot_id: str | None
    profile_drifts: list[ProfileDrift] = field(default_factory=list)
    has_changes: bool = False


@dataclass(slots=True)
class DemotionRecord:
    question_id: str
    question_title: str
    prior_state: str
    prior_snapshot_id: str | None
    prior_updated_at: str | None
    to_snapshot_id: str | None
    breaking_contracts: list[str]
    drift_summary: str


@dataclass(slots=True)
class CertificationReport:
    project_id: str
    source_snapshot_id: str | None
    snapshot_diff: SnapshotDiff | None
    demotions: list[DemotionRecord] = field(default_factory=list)
    newly_certified: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)


def evaluate_and_certify(
    store: HeadwaterStore,
    project_id: str,
) -> CertificationReport:
    """Re-evaluate all project questions against current source state.

    Certified questions whose contracts now fail are demoted.
    Previously-draft questions that now pass all contracts become certified.
    Each state change is persisted and explained in a Resolve card.
    """
    project = store.get_project(project_id)
    if project is None:
        raise ValueError(f"Project '{project_id}' is not registered.")

    project_sources = store.get_project_sources(project_id)
    if not project_sources:
        raise ValueError(f"Project '{project_id}' has no linked source.")

    source_name = project_sources[0]["source_name"]
    snapshot = store.get_latest_source_snapshot(source_name)
    snapshot_id = snapshot["id"] if snapshot else None

    # Load current profiles and claims
    profiles = store.get_profiles(source_name)
    claims = store.list_semantic_claims(project_id)
    questions = store.list_questions(project_id)
    resolve_items = store.list_resolve_items(project_id)

    from headwater.services.h2_readiness import (
        _FANOUT_RI_THRESHOLD,
        _columns_with_satisfying_claim,
    )

    profile_map = {
        f"{p['table_name']}.{p['column_name']}": p["profile"] for p in profiles
    }
    # Mirror evaluate_project_readiness exactly — certify re-checks the SAME
    # contract inputs, otherwise a freshly certified question demotes on the
    # next pass for reasons readiness never saw (a false revocation).
    satisfied_cols = _columns_with_satisfying_claim(claims)
    high_priority_open: set[str] = {
        f"{r['payload'].get('table','')}.{r['payload'].get('column','')}"
        for r in resolve_items
        if r.get("priority") == "high" and r.get("status") == "open"
        if r.get("payload", {}).get("column")
    } - satisfied_cols
    conflicting_cols: set[str] = _find_conflicting_claims(claims)
    unmapped_code_cols: set[str] = {
        f"{r['payload'].get('table','')}.{r['payload'].get('column','')}"
        for r in resolve_items
        if r.get("issue_kind") == "enum_mapping_needed" and r.get("status") == "open"
        if r.get("payload", {}).get("column")
    } - satisfied_cols
    weak_joins: dict[tuple[str, str], float] = {}
    for rel in store.get_relationships(source_name):
        ri = rel.get("referential_integrity")
        if ri is not None and float(ri) < _FANOUT_RI_THRESHOLD:
            weak_joins[(rel["from_table"], rel["to_table"])] = float(ri)
    profiled_tables = {p["table_name"] for p in profiles}
    empty_tables: set[str] = {
        t["name"]
        for t in store.get_tables(source_name)
        if int(t.get("row_count") or 0) == 0 and t["name"] in profiled_tables
    }

    # Compute snapshot diff for enriched explanations
    prior_profiles = _load_prior_profiles(store, source_name, snapshot_id)
    diff = _compute_diff(source_name, prior_profiles, profiles, snapshot_id)

    report = CertificationReport(
        project_id=project_id,
        source_snapshot_id=snapshot_id,
        snapshot_diff=diff,
    )

    for question in questions:
        prior_verdict_id = f"{question['id']}:verdict:latest"
        prior_verdict = store.get_readiness_verdict(prior_verdict_id)
        prior_state = prior_verdict["state"] if prior_verdict else None

        # The stored EDA insight contract is evidence, not derived state — pass
        # it through or every certified question would demote to "unknown".
        stored_contracts = store.list_readiness_contracts(question["id"])
        eda_contract = next(
            (
                c
                for c in stored_contracts
                if c["contract_type"] == "insight_confidence"
                and (c.get("evidence") or {}).get("status") != "unknown"
            ),
            None,
        )
        new_result = evaluate_question(
            question=question,
            profile_map=profile_map,
            high_priority_open=high_priority_open,
            conflicting_cols=conflicting_cols,
            snapshot_id=snapshot_id,
            eda_contract=eda_contract,
            unmapped_code_cols=unmapped_code_cols,
            weak_joins=weak_joins,
            empty_tables=empty_tables,
        )

        # Persist updated verdict and contracts
        _persist_verdict_and_contracts(store, new_result, question)

        if prior_state == "certified" and new_result.state != "certified":
            # Demotion: the trust badge revokes itself
            breaking = [c.contract_type for c in new_result.contracts if not c.passed]
            drift_explanation = _build_drift_explanation(breaking, diff, question)

            demoted_verdict_id = f"{question['id']}:verdict:latest"
            store.upsert_readiness_verdict(
                demoted_verdict_id,
                question_id=question["id"],
                state="demoted",
                readiness_pct=new_result.readiness_pct,
                trust_bucket="gaps",
                summary=_demotion_summary(
                    prior_verdict, snapshot_id, breaking, drift_explanation
                ),
                source_snapshot_id=snapshot_id,
            )
            record = DemotionRecord(
                question_id=question["id"],
                question_title=question.get("title", question["id"]),
                prior_state="certified",
                prior_snapshot_id=(
                    prior_verdict.get("source_snapshot_id") if prior_verdict else None
                ),
                prior_updated_at=(
                    prior_verdict.get("updated_at") if prior_verdict else None
                ),
                to_snapshot_id=snapshot_id,
                breaking_contracts=breaking,
                drift_summary=drift_explanation,
            )
            report.demotions.append(record)
            _persist_demotion_decision(store, question, record, prior_verdict)
            _create_revocation_resolve_card(store, project_id, record)

        elif prior_state != "certified" and new_result.state == "certified":
            report.newly_certified.append(question["id"])
        else:
            report.unchanged.append(question["id"])

    return report


# ── Snapshot diff ─────────────────────────────────────────────────────────────

def compute_snapshot_diff(
    store: HeadwaterStore,
    source_name: str,
) -> SnapshotDiff:
    """Compute a diff between the last two known snapshots for a source."""
    snapshots = store.con.execute(
        """
        SELECT id FROM source_snapshots
         WHERE source_name = ?
         ORDER BY captured_at DESC, id DESC
         LIMIT 2
        """,
        (source_name,),
    ).fetchall()

    if len(snapshots) < 2:
        return SnapshotDiff(
            source_name=source_name,
            from_snapshot_id=None,
            to_snapshot_id=snapshots[0]["id"] if snapshots else None,
        )

    to_id = snapshots[0]["id"]
    from_id = snapshots[1]["id"]

    to_profiles = {
        f"{p['table_name']}.{p['column_name']}": p
        for p in _profiles_for_snapshot(store, source_name, to_id)
    }
    from_profiles = {
        f"{p['table_name']}.{p['column_name']}": p
        for p in _profiles_for_snapshot(store, source_name, from_id)
    }
    return _compute_diff(source_name, from_profiles, to_profiles, to_id, from_id)


def _compute_diff(
    source_name: str,
    from_profiles: dict[str, Any],
    to_profiles: list[dict[str, Any]] | dict[str, Any],
    to_snapshot_id: str | None,
    from_snapshot_id: str | None = None,
) -> SnapshotDiff:
    if isinstance(to_profiles, list):
        to_map = {f"{p['table_name']}.{p['column_name']}": p for p in to_profiles}
    else:
        to_map = to_profiles

    drifts: list[ProfileDrift] = []
    for key, to_p in to_map.items():
        if key not in from_profiles:
            continue
        from_p = from_profiles[key]
        to_prof = to_p.get("profile") or {}
        from_prof = (from_p.get("profile") or {}) if isinstance(from_p, dict) else {}

        table_name = to_p["table_name"]
        col_name = to_p["column_name"]

        # Null rate increase
        from_null = float(from_prof.get("null_rate") or 0.0)
        to_null = float(to_prof.get("null_rate") or 0.0)
        if to_null >= _HIGH_NULL_THRESHOLD and to_null > from_null + 0.05:
            drifts.append(ProfileDrift(
                table_name=table_name,
                column_name=col_name,
                drift_kind="null_rate_increase",
                from_value=from_null,
                to_value=to_null,
                description=(
                    f"`{key}` null rate increased from "
                    f"{from_null:.0%} to {to_null:.0%}."
                ),
            ))

        # Vocabulary drift: new or missing top values
        from_vals = {str(v[0]) for v in (from_prof.get("top_values") or []) if v}
        to_vals = {str(v[0]) for v in (to_prof.get("top_values") or []) if v}
        new_vals = to_vals - from_vals
        gone_vals = from_vals - to_vals
        if new_vals:
            drifts.append(ProfileDrift(
                table_name=table_name,
                column_name=col_name,
                drift_kind="vocab_new_value",
                from_value=sorted(from_vals),
                to_value=sorted(new_vals),
                description=(
                    f"`{key}` has new code value(s): {', '.join(sorted(new_vals))}."
                ),
            ))
        if gone_vals:
            drifts.append(ProfileDrift(
                table_name=table_name,
                column_name=col_name,
                drift_kind="vocab_value_missing",
                from_value=sorted(gone_vals),
                to_value=sorted(to_vals),
                description=(
                    f"`{key}` lost code value(s): {', '.join(sorted(gone_vals))}."
                ),
            ))

    return SnapshotDiff(
        source_name=source_name,
        from_snapshot_id=from_snapshot_id,
        to_snapshot_id=to_snapshot_id,
        profile_drifts=drifts,
        has_changes=bool(drifts),
    )


def _load_prior_profiles(
    store: HeadwaterStore,
    source_name: str,
    current_snapshot_id: str | None,
) -> dict[str, Any]:
    prior = store.con.execute(
        """
        SELECT id FROM source_snapshots
         WHERE source_name = ? AND id != ?
         ORDER BY captured_at DESC, id DESC
         LIMIT 1
        """,
        (source_name, current_snapshot_id or ""),
    ).fetchone()
    if prior is None:
        return {}
    return {
        f"{p['table_name']}.{p['column_name']}": p
        for p in _profiles_for_snapshot(store, source_name, prior["id"])
    }


def _profiles_for_snapshot(
    store: HeadwaterStore,
    source_name: str,
    snapshot_id: str,
) -> list[dict[str, Any]]:
    import json

    rows = store.con.execute(
        """
        SELECT * FROM profiles
         WHERE source_name = ? AND snapshot_id = ?
         ORDER BY table_name, column_name
        """,
        (source_name, snapshot_id),
    ).fetchall()
    items = [dict(row) for row in rows]
    for item in items:
        item["profile"] = json.loads(item.pop("profile_json") or "{}")
    return items


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_conflicting_claims(claims: list[dict[str, Any]]) -> set[str]:
    from collections import defaultdict

    col_claims: dict[str, list[str]] = defaultdict(list)
    for c in claims:
        if c.get("table_name") and c.get("column_name"):
            key = f"{c['table_name']}.{c['column_name']}"
            col_claims[key].append(c.get("status", ""))
    return {
        key
        for key, statuses in col_claims.items()
        if "locked" in statuses and len(set(statuses)) > 1
    }


def _build_drift_explanation(
    breaking_contracts: list[str],
    diff: SnapshotDiff,
    question: dict[str, Any],
) -> str:
    needed = set(question.get("question", {}).get("needed_columns") or [])
    if not diff.profile_drifts:
        return f"Contracts failed: {', '.join(breaking_contracts)}."
    relevant = [
        d.description
        for d in diff.profile_drifts
        if any(d.table_name in col and d.column_name in col for col in needed)
        or any(f"{d.table_name}.{d.column_name}" in col for col in needed)
    ]
    if not relevant:
        relevant = [d.description for d in diff.profile_drifts[:2]]
    if relevant:
        return "; ".join(relevant)
    return f"Contracts failed: {', '.join(breaking_contracts)}."


def _demotion_summary(
    prior_verdict: dict[str, Any] | None,
    new_snapshot_id: str | None,
    breaking: list[str],
    drift: str,
) -> str:
    prior_snap = prior_verdict.get("source_snapshot_id") if prior_verdict else None
    prior_date = (prior_verdict.get("updated_at") or "")[:10] if prior_verdict else "unknown"
    lines = [
        f"Demoted from certified (was: snapshot `{prior_snap}`, {prior_date}).",
        f"Current snapshot: `{new_snapshot_id}`.",
        f"Breaking contracts: {', '.join(breaking)}.",
    ]
    if drift:
        lines.append(f"Drift detected: {drift}")
    return " ".join(lines)


def _persist_verdict_and_contracts(
    store: HeadwaterStore,
    result: Any,
    question: dict[str, Any],
) -> None:
    from headwater.services.h2_readiness import _trust_bucket

    verdict_id = f"{question['id']}:verdict:latest"
    store.upsert_readiness_verdict(
        verdict_id,
        question_id=question["id"],
        state=result.state,
        readiness_pct=result.readiness_pct,
        trust_bucket=_trust_bucket(result.state, result.readiness_pct),
        summary=result.summary,
        source_snapshot_id=result.source_snapshot_id,
    )
    for contract in result.contracts:
        store.upsert_readiness_contract(
            f"{question['id']}:contract:{contract.contract_type}",
            question_id=question["id"],
            contract_type=contract.contract_type,
            passed=contract.passed,
            note=contract.note,
            evidence=contract.evidence,
            snapshot_id=result.source_snapshot_id,
        )


def _persist_demotion_decision(
    store: HeadwaterStore,
    question: dict[str, Any],
    record: DemotionRecord,
    prior_verdict: dict[str, Any] | None,
) -> None:
    store.record_decision(
        artifact_type="question",
        artifact_id=question["id"],
        action="demoted",
        reason=record.drift_summary,
        payload={
            "prior_state": record.prior_state,
            "prior_snapshot_id": record.prior_snapshot_id,
            "prior_updated_at": record.prior_updated_at,
            "to_snapshot_id": record.to_snapshot_id,
            "breaking_contracts": record.breaking_contracts,
        },
    )


def _create_revocation_resolve_card(
    store: HeadwaterStore,
    project_id: str,
    record: DemotionRecord,
) -> None:
    card_id = f"{project_id}:revocation:{record.question_id}"
    prior_snap = record.prior_snapshot_id or "unknown"
    prior_date = (record.prior_updated_at or "")[:10] or "unknown"
    body = (
        f'Question "{record.question_title}" was certified under snapshot '
        f"`{prior_snap}` ({prior_date}). "
        f"It has been demoted because: {record.drift_summary} "
        f"Run `hw2 readiness` to see the updated contract state, then "
        f"`hw2 resource add` or correct the source data to restore certification."
    )
    store.upsert_resolve_item(
        card_id,
        project_id=project_id,
        question_id=record.question_id,
        issue_kind="certification_revoked",
        title=f'Certification revoked: "{record.question_title}"',
        body=body,
        priority="high",
        status="open",
        payload={
            "prior_snapshot_id": record.prior_snapshot_id,
            "to_snapshot_id": record.to_snapshot_id,
            "breaking_contracts": record.breaking_contracts,
            "drift_summary": record.drift_summary,
        },
    )
