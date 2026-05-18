"""Typed runtime state for active pipeline artifacts."""

from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PipelineRuntimeState(MutableMapping[str, Any]):
    """In-process pipeline state with a mapping-compatible surface.

    This remains a runtime cache, not a durable source of truth. The mapping
    behavior exists for compatibility while routes and services are migrated
    away from raw dict access.
    """

    discovery: Any = None
    catalog: Any = None
    staging_models: list[Any] = field(default_factory=list)
    mart_models: list[Any] = field(default_factory=list)
    contracts: list[Any] = field(default_factory=list)
    execution_results: list[Any] = field(default_factory=list)
    quality_report: Any = None
    graph_store: Any = None
    vector_store: Any = None
    project: Any = None
    source_names: list[str] = field(default_factory=list)
    table_names: list[str] | None = None

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    def __delitem__(self, key: str) -> None:
        setattr(self, key, None)

    def __iter__(self) -> Iterator[str]:
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "discovery": self.discovery,
            "catalog": self.catalog,
            "staging_models": self.staging_models,
            "mart_models": self.mart_models,
            "contracts": self.contracts,
            "execution_results": self.execution_results,
            "quality_report": self.quality_report,
            "graph_store": self.graph_store,
            "vector_store": self.vector_store,
            "project": self.project,
            "source_names": self.source_names,
            "table_names": self.table_names,
        }

    @classmethod
    def from_mapping(cls, values: dict[str, Any]) -> PipelineRuntimeState:
        return cls(
            discovery=values.get("discovery"),
            catalog=values.get("catalog"),
            staging_models=list(values.get("staging_models", [])),
            mart_models=list(values.get("mart_models", [])),
            contracts=list(values.get("contracts", [])),
            execution_results=list(values.get("execution_results", [])),
            quality_report=values.get("quality_report"),
            graph_store=values.get("graph_store"),
            vector_store=values.get("vector_store"),
            project=values.get("project"),
            source_names=list(values.get("source_names", [])),
            table_names=values.get("table_names"),
        )

    def clear_for_source(self, source_name: str) -> None:
        active_source = self.active_source_name()
        if active_source != source_name:
            return
        self.discovery = None
        self.catalog = None
        self.staging_models = []
        self.mart_models = []
        self.contracts = []
        self.execution_results = []
        self.quality_report = None
        self.graph_store = None
        self.vector_store = None
        self.project = None
        self.source_names = []
        self.table_names = None

    def active_source_name(self) -> str | None:
        if self.source_names:
            return self.source_names[0]
        return getattr(getattr(self.discovery, "source", None), "name", None)


def get_runtime_state(app_or_request) -> PipelineRuntimeState:
    """Return the current runtime state object from a FastAPI app or request."""
    state = app_or_request.app.state if hasattr(app_or_request, "app") else app_or_request.state
    runtime_state = getattr(state, "pipeline_state", None)
    if runtime_state is None:
        legacy = getattr(state, "pipeline", None)
        if isinstance(legacy, PipelineRuntimeState):
            runtime_state = legacy
        elif isinstance(legacy, dict):
            runtime_state = PipelineRuntimeState.from_mapping(legacy)
            state.pipeline = runtime_state
        else:
            runtime_state = PipelineRuntimeState()
        state.pipeline_state = runtime_state
    return runtime_state


def set_runtime_state(app_or_request, runtime_state: PipelineRuntimeState) -> PipelineRuntimeState:
    """Persist the runtime state object onto a FastAPI app or request."""
    state = app_or_request.app.state if hasattr(app_or_request, "app") else app_or_request.state
    state.pipeline_state = runtime_state
    # Keep the legacy attribute alive while routes still reference it directly.
    state.pipeline = runtime_state
    return runtime_state
