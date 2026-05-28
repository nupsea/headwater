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
