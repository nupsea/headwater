"""Staging model generator -- mechanical transformations, no business logic."""

from __future__ import annotations

import re
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from headwater.core.models import GeneratedModel, TableInfo

_TEMPLATE_DIR = Path(__file__).parent / "templates"


def generate_staging_models(
    tables: list[TableInfo],
    source_schema: str,
    target_schema: str = "staging",
) -> list[GeneratedModel]:
    """Generate staging SQL models for all source tables.

    Staging models are auto-approved: they perform only mechanical
    transformations (rename, cast, deduplicate) with no business logic.
    """
    env = Environment(loader=FileSystemLoader(str(_TEMPLATE_DIR)), keep_trailing_newline=True)
    template = env.get_template("staging.sql.j2")

    models: list[GeneratedModel] = []
    for table in tables:
        model_name = f"stg_{table.name}"
        columns = _build_column_mappings(table)

        sql = template.render(
            model_name=model_name,
            source_schema=source_schema,
            source_table=table.name,
            target_schema=target_schema,
            description=table.description or f"Staging layer for {table.name}",
            columns=columns,
        )

        models.append(
            GeneratedModel(
                name=model_name,
                model_type="staging",
                sql=sql.strip(),
                description=f"Staging layer for {table.name}. "
                f"Renames to snake_case, explicit casts, adds load timestamp.",
                source_tables=[table.name],
                status="approved",  # Auto-approved
            )
        )

    return models


def _build_column_mappings(table: TableInfo) -> list[dict]:
    """Build column mappings for the staging template."""
    columns = []
    for col in table.columns:
        target_name = _to_snake_case(col.name)
        expression = f'"{col.name}"'

        # Add explicit casts for clarity
        if col.dtype == "timestamp":
            expression = f'CAST("{col.name}" AS TIMESTAMP)'
        elif col.dtype == "date":
            expression = f'CAST("{col.name}" AS DATE)'

        columns.append(
            {
                "expression": expression,
                "target_name": target_name,
            }
        )

    # Add metadata column
    columns.append(
        {
            "expression": "CURRENT_TIMESTAMP",
            "target_name": "_loaded_at",
        }
    )

    return columns


def _to_snake_case(name: str) -> str:
    """Convert a column name to snake_case (most are already)."""
    # Insert underscore before uppercase letters
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", name)
    return s.lower()
