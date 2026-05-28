"""Shared H2 project types and naming helpers."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

from headwater.core.config import HeadwaterSettings

_TEMPORAL_ROLE_PREFIXES = {"event_ts", "start_ts", "end_ts", "time_anchor"}
_MEASURE_ROLES = {"measure", "duration", "quantity", "metric"}
_CATEGORY_ROLES = {"categorical", "code", "flag"}
_WORKFLOW_HINTS = {
    "activity",
    "action",
    "event",
    "step",
    "stage",
    "phase",
    "status",
    "workflow",
    "process",
}
_RESOURCE_HINTS = {
    "resource",
    "device",
    "room",
    "site",
    "location",
    "zone",
    "modality",
    "sensor",
    "channel",
    "team",
    "staff",
    "capacity",
    "throughput",
}
_GEOGRAPHIC_HINTS = {
    "latitude",
    "longitude",
    "lat",
    "lon",
    "centroid",
    "coordinate",
    "coords",
    "x_coord",
    "y_coord",
}
_GOAL_TIME_HINTS = {
    "time",
    "hour",
    "day",
    "week",
    "month",
    "trend",
    "change",
    "peak",
    "wait",
    "schedule",
    "season",
    "lifecycle",
}
_GOAL_SEGMENT_HINTS = {
    "segment",
    "group",
    "type",
    "category",
    "class",
    "bucket",
    "breakdown",
}
_GOAL_WORKFLOW_HINTS = {
    "workflow",
    "process",
    "step",
    "flow",
    "registration",
    "throughput",
    "bottleneck",
    "utilization",
    "coverage",
}
_GOAL_QUALITY_HINTS = {
    "quality",
    "freshness",
    "null",
    "missing",
    "error",
    "drift",
    "stale",
    "trust",
}
_TIME_NAME_HINTS = {
    "date",
    "time",
    "timestamp",
    "hour",
    "day",
    "week",
    "month",
    "created",
    "updated",
    "acknowledged",
}
_CATEGORY_NAME_HINTS = {
    "type",
    "category",
    "status",
    "result",
    "priority",
    "source",
    "mode",
    "kind",
}
_WORKFLOW_NAME_HINTS = {
    "activity",
    "step",
    "stage",
    "phase",
    "event",
    "workflow",
}


@dataclass(slots=True)
class H2ProjectGoal:
    """Structured framing input for a project."""

    statement: str
    decision: str | None = None
    target_metric: str | None = None
    entities: list[str] = field(default_factory=list)
    time_horizon: str | None = None
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class H2ProjectSpec:
    """Persisted project framing spec."""

    project_id: str
    display_name: str
    source_name: str
    goal: H2ProjectGoal
    selected_tables: list[str] = field(default_factory=list)
    resources: list[str] = field(default_factory=list)
    source_snapshot_id: str | None = None


@dataclass(slots=True)
class H2RelevantColumn:
    """Column relevance score for the project goal."""

    table_name: str
    column_name: str
    semantic_role: str | None
    score: float
    reason: str
    selected: bool = False


@dataclass(slots=True)
class H2QuestionProposal:
    """Question proposed during project relevance."""

    question_id: str
    title: str
    answerability: str
    reason: str
    needed_columns: list[str] = field(default_factory=list)
    confidence: float = 0.0


@dataclass(slots=True)
class H2RelevanceResult:
    """Project relevance summary."""

    project_id: str
    source_name: str
    source_snapshot_id: str | None
    selected_tables: list[str] = field(default_factory=list)
    relevant_columns: list[H2RelevantColumn] = field(default_factory=list)
    proposed_questions: list[H2QuestionProposal] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def project_spec_path(settings: HeadwaterSettings, project_id: str) -> Path:
    return settings.data_dir / "projects" / f"{project_id}.yaml"


def goal_payload(goal: H2ProjectGoal) -> dict:
    return asdict(goal)


def friendly_name(value: str) -> str:
    value = value.replace("_", " ").strip()
    value = re.sub(r"\s+", " ", value)
    return value or "metric"


def looks_code_like(column_name: str) -> bool:
    normalized = column_name.lower()
    return bool(
        re.search(r"(^id$|_id$|code$|key$|type$|mode$|class$)", normalized) or len(normalized) <= 3
    )


def slugify(value: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else "-" for ch in value.strip())
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized.strip("-") or "project"


def candidate_parts(candidate: H2RelevantColumn | str) -> tuple[str | None, str]:
    if isinstance(candidate, H2RelevantColumn):
        return candidate.table_name, candidate.column_name
    if "." in candidate:
        table_name, column_name = candidate.split(".", 1)
        return table_name, column_name
    return None, candidate
