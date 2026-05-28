"""Headwater 2 project framing helpers."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import yaml

from headwater.core.config import HeadwaterSettings
from headwater.core.store import HeadwaterStore
from headwater.services.h2_project_relevance import (
    propose_relevance as _propose_relevance,
)
from headwater.services.h2_project_types import (
    H2ProjectGoal,
    H2ProjectSpec,
    project_spec_path,
)
from headwater.services.h2_project_types import (
    goal_payload as _goal_payload,
)
from headwater.services.h2_project_types import (
    slugify as _slugify,
)


def frame_project(
    *,
    store: HeadwaterStore,
    project_id: str,
    source_name: str,
    display_name: str,
    goal_statement: str,
    selected_tables: list[str] | None = None,
    decision: str | None = None,
    target_metric: str | None = None,
    entities: list[str] | None = None,
    time_horizon: str | None = None,
    resources: list[str] | None = None,
    settings: HeadwaterSettings | None = None,
) -> H2ProjectSpec:
    source = store.get_source(source_name)
    if source is None:
        raise ValueError(f"Source '{source_name}' is not registered in the H2 store.")

    latest_snapshot = store.get_latest_source_snapshot(source_name)
    snapshot_id = latest_snapshot["id"] if latest_snapshot else source.get("latest_snapshot_id")
    goal = H2ProjectGoal(
        statement=goal_statement,
        decision=decision,
        target_metric=target_metric,
        entities=list(entities or []),
        time_horizon=time_horizon,
        notes=[],
    )
    spec = H2ProjectSpec(
        project_id=project_id,
        display_name=display_name,
        source_name=source_name,
        goal=goal,
        selected_tables=list(selected_tables or []),
        resources=list(resources or []),
        source_snapshot_id=snapshot_id,
    )
    store.upsert_project(
        project_id,
        slug=_slugify(project_id or display_name),
        display_name=display_name,
        description=decision or "",
        goal=_goal_payload(goal),
    )
    store.upsert_project_source(
        project_id,
        source_name,
        selected_tables=spec.selected_tables,
        scope={
            "source_snapshot_id": snapshot_id,
            "decision": decision,
            "target_metric": target_metric,
            "entities": list(entities or []),
            "time_horizon": time_horizon,
        },
    )
    _write_project_spec(spec, settings=settings)
    _bootstrap_profile_claims(store, project_id, source_name, snapshot_id)
    return spec


def propose_relevance(
    *,
    store: HeadwaterStore,
    project_id: str,
):
    return _propose_relevance(store=store, project_id=project_id)


def _write_project_spec(
    spec: H2ProjectSpec,
    *,
    settings: HeadwaterSettings | None = None,
) -> Path:
    settings = settings or HeadwaterSettings()
    settings.ensure_dirs()
    projects_dir = settings.data_dir / "projects"
    projects_dir.mkdir(parents=True, exist_ok=True)
    path = project_spec_path(settings, spec.project_id)
    path.write_text(yaml.safe_dump(_asdict(spec), sort_keys=False), encoding="utf-8")
    return path


def _asdict(spec: H2ProjectSpec) -> dict[str, Any]:
    return asdict(spec)


# ── Profile-based claim bootstrapping ────────────────────────────────────────

_CODE_MAX_DISTINCT = 30
_CODE_MAX_AVG_LEN = 4.0
_CODE_MAX_UNIQUENESS = 0.05


def _bootstrap_profile_claims(
    store: HeadwaterStore,
    project_id: str,
    source_name: str,
    snapshot_id: str | None,
) -> int:
    """Auto-create proposed semantic claims from profile statistics at project creation.

    Detects code-like columns (short varchar, few distinct values, low uniqueness)
    and creates a proposed enum_mapping claim with the known code values but empty
    meanings.  The user fills in meanings via resource intake (hw2 resource add)
    or by editing the claim directly.

    Returns the number of bootstrap claims created.
    """
    profiles = store.get_profiles(source_name)
    existing_claim_ids = {c["id"] for c in store.list_semantic_claims(project_id)}
    created = 0

    for p in profiles:
        profile = p.get("profile") or {}
        dtype = profile.get("dtype", "").lower()
        if dtype not in ("varchar", "text", "string", "category"):
            continue
        distinct = int(profile.get("distinct_count") or 0)
        avg_len = float(profile.get("avg_length") or 0.0)
        uniqueness = float(profile.get("uniqueness_ratio") or 0.0)
        top_values = profile.get("top_values") or []

        if not (
            distinct >= 2
            and distinct <= _CODE_MAX_DISTINCT
            and avg_len <= _CODE_MAX_AVG_LEN
            and uniqueness <= _CODE_MAX_UNIQUENESS
            and top_values
        ):
            continue

        table_name = p["table_name"]
        col_name = p["column_name"]
        claim_id = f"{project_id}:bootstrap:{table_name}.{col_name}:enum_mapping"
        if claim_id in existing_claim_ids:
            continue

        # Codes are known from profile; meanings are empty — user fills them in.
        known_codes = {str(v[0]): "" for v in top_values[:8] if v}
        store.upsert_semantic_claim(
            claim_id,
            project_id=project_id,
            source_name=source_name,
            scope_type="column",
            table_name=table_name,
            column_name=col_name,
            claim_type="enum_mapping",
            claim={
                "value": known_codes,
                "note": "Codes detected from profile. Add meanings via resource intake.",
                "snapshot_id": snapshot_id,
            },
            status="proposed",
            confidence=0.30,
            source="bootstrap:profile",
            locked=False,
        )
        created += 1

    return created
