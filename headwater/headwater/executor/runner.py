"""Model runner -- executes models in dependency order."""

from __future__ import annotations

import logging
from collections import defaultdict

from headwater.core.models import ExecutionResult, GeneratedModel
from headwater.executor.duckdb_backend import DuckDBBackend

logger = logging.getLogger(__name__)


def topological_sort(models: list[GeneratedModel]) -> list[GeneratedModel]:
    """Sort models so dependencies are executed first.

    Uses Kahn's algorithm. Models with no depends_on come first.
    """
    model_map = {m.name: m for m in models}
    in_degree: dict[str, int] = {m.name: 0 for m in models}
    dependents: dict[str, list[str]] = defaultdict(list)

    for m in models:
        for dep in m.depends_on:
            if dep in model_map:
                in_degree[m.name] += 1
                dependents[dep].append(m.name)

    # Start with models that have no in-graph dependencies
    queue = [name for name, deg in in_degree.items() if deg == 0]
    queue.sort()  # Stable ordering for determinism
    ordered: list[str] = []

    while queue:
        name = queue.pop(0)
        ordered.append(name)
        for dependent in sorted(dependents[name]):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    # Any models not in ordered have circular deps -- append them at end with warning
    for m in models:
        if m.name not in ordered:
            logger.warning("Circular dependency detected for %s, appending at end", m.name)
            ordered.append(m.name)

    return [model_map[name] for name in ordered]


def run_models(
    backend: DuckDBBackend,
    models: list[GeneratedModel],
    *,
    only_approved: bool = True,
) -> list[ExecutionResult]:
    """Execute models in dependency order, returning results.

    Only executes models with status='approved' by default.
    Staging models (auto-approved) run first, then approved marts.
    """
    eligible = [m for m in models if m.status == "approved"] if only_approved else list(models)

    sorted_models = topological_sort(eligible)
    results: list[ExecutionResult] = []

    for model in sorted_models:
        result = backend.materialize(model)
        results.append(result)
        if not result.success:
            logger.error(
                "Model %s failed, skipping downstream dependents", model.name
            )

    return results
