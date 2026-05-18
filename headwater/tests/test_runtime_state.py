"""Tests for the typed pipeline runtime state."""

from __future__ import annotations

from types import SimpleNamespace

from headwater.core.runtime_state import (
    PipelineRuntimeState,
    get_runtime_state,
    set_runtime_state,
)


def test_runtime_state_behaves_like_mapping():
    state = PipelineRuntimeState()
    state["contracts"] = ["c1"]
    state["quality_report"] = {"failed": 0}

    assert state["contracts"] == ["c1"]
    assert state.get("quality_report") == {"failed": 0}
    assert "contracts" in list(state)


def test_set_runtime_state_keeps_legacy_alias():
    app = SimpleNamespace(state=SimpleNamespace())
    runtime_state = PipelineRuntimeState(contracts=["c1"])

    set_runtime_state(app, runtime_state)

    assert app.state.pipeline_state is runtime_state
    assert app.state.pipeline is runtime_state


def test_get_runtime_state_upgrades_legacy_dict():
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    request.app.state.pipeline = {"contracts": ["c1"], "staging_models": []}

    runtime_state = get_runtime_state(request)

    assert isinstance(runtime_state, PipelineRuntimeState)
    assert runtime_state.contracts == ["c1"]
    assert request.app.state.pipeline is runtime_state


def test_clear_for_source_resets_matching_active_source():
    discovery = SimpleNamespace(source=SimpleNamespace(name="src"))
    runtime_state = PipelineRuntimeState(
        discovery=discovery,
        catalog={"name": "catalog"},
        staging_models=["stg"],
        mart_models=["mart"],
        contracts=["contract"],
        execution_results=["result"],
        quality_report={"failed": 0},
        source_names=["src"],
        table_names=["t1"],
    )

    runtime_state.clear_for_source("src")

    assert runtime_state.discovery is None
    assert runtime_state.catalog is None
    assert runtime_state.staging_models == []
    assert runtime_state.contracts == []
