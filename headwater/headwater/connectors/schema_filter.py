"""Reusable schema and table filtering component.

This module provides a standalone ``SchemaTableFilter`` that any connector can
use to include/exclude schemas and tables by name or glob pattern.  It is
intentionally decoupled from connector internals so it can be shared across
Redshift, Snowflake, Postgres, or any future connector that needs scoped
discovery.

Usage::

    from headwater.connectors.schema_filter import SchemaTableFilter

    sf = SchemaTableFilter.from_config({
        "include_schemas": ["analytics", "reporting"],
        "exclude_tables": ["*_tmp", "*_backup"],
    })
    filtered = sf.filter_tables(all_tables)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from fnmatch import fnmatch

logger = logging.getLogger(__name__)


@dataclass
class SchemaTableFilter:
    """Configurable include/exclude filter for schemas and tables.

    Rules
    -----
    * If ``include_schemas`` is non-empty only those schemas are accepted.
    * ``exclude_schemas`` always wins over ``include_schemas``.
    * Table patterns use :func:`fnmatch` (Unix shell glob): ``*``, ``?``,
      ``[seq]``, ``[!seq]``.
    * All matching is **case-insensitive** by default (``case_sensitive=False``).
    """

    include_schemas: list[str] = field(default_factory=list)
    exclude_schemas: list[str] = field(default_factory=list)
    include_tables: list[str] = field(default_factory=list)
    exclude_tables: list[str] = field(default_factory=list)
    case_sensitive: bool = False

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, config: dict | None) -> SchemaTableFilter:
        """Build a filter from a source ``config`` dict.

        Recognised keys (all optional):
        ``include_schemas``, ``exclude_schemas``, ``include_tables``,
        ``exclude_tables``, ``case_sensitive``.
        """
        if not config:
            return cls()
        return cls(
            include_schemas=_as_list(config.get("include_schemas")),
            exclude_schemas=_as_list(config.get("exclude_schemas")),
            include_tables=_as_list(config.get("include_tables")),
            exclude_tables=_as_list(config.get("exclude_tables")),
            case_sensitive=bool(config.get("case_sensitive", False)),
        )

    @classmethod
    def from_query_params(cls, params: dict[str, str]) -> SchemaTableFilter:
        """Build a filter from URI query-string parameters.

        Comma-separated values are split automatically::

            ?include_schemas=analytics,reporting&exclude_tables=*_tmp
        """
        return cls(
            include_schemas=_split_csv(params.get("include_schemas")),
            exclude_schemas=_split_csv(params.get("exclude_schemas")),
            include_tables=_split_csv(params.get("include_tables")),
            exclude_tables=_split_csv(params.get("exclude_tables")),
        )

    # ------------------------------------------------------------------
    # Predicates
    # ------------------------------------------------------------------

    @property
    def is_empty(self) -> bool:
        """True when the filter imposes no restrictions."""
        return not any(
            [self.include_schemas, self.exclude_schemas,
             self.include_tables, self.exclude_tables]
        )

    def accepts_schema(self, schema: str) -> bool:
        """Return True if *schema* passes the include/exclude rules."""
        norm = self._norm(schema)
        exclude_patterns = self._schema_only_patterns(self.exclude_schemas)
        if exclude_patterns and any(fnmatch(norm, pattern) for pattern in exclude_patterns):
            return False
        include_patterns = self._schema_only_patterns(self.include_schemas)
        if include_patterns:
            return any(fnmatch(norm, pattern) for pattern in include_patterns)
        return True

    def accepts_table(self, table_name: str, schema: str | None = None) -> bool:
        """Return True if *table_name* passes the include/exclude glob rules.

        Patterns may target either the bare table name (``orders``) or the
        qualified form (``analytics.orders``) when a schema is available.
        """
        norm = self._norm(table_name)
        candidates = [norm]
        if schema:
            candidates.append(self._norm(f"{schema}.{table_name}"))
        qualified_exclude_patterns = self._qualified_patterns(self.exclude_schemas)
        if qualified_exclude_patterns:
            for pattern in qualified_exclude_patterns:
                if any(fnmatch(candidate, pattern) for candidate in candidates):
                    return False
        if self.exclude_tables:
            for pattern in self._norm_list(self.exclude_tables):
                if any(fnmatch(candidate, pattern) for candidate in candidates):
                    return False
        include_patterns = [
            *self._qualified_patterns(self.include_schemas),
            *self._norm_list(self.include_tables),
        ]
        if include_patterns:
            return any(
                any(fnmatch(candidate, pattern) for candidate in candidates)
                for pattern in include_patterns
            )
        return True

    # ------------------------------------------------------------------
    # Bulk helpers
    # ------------------------------------------------------------------

    def filter_schemas(self, schemas: list[str]) -> list[str]:
        """Return only schemas that pass the filter."""
        result = [s for s in schemas if self.accepts_schema(s)]
        skipped = len(schemas) - len(result)
        if skipped:
            logger.info(
                "Schema filter: kept %d of %d schemas (%d excluded)",
                len(result), len(schemas), skipped,
            )
        return result

    def filter_tables(
        self,
        tables: list[str],
        *,
        schema_extractor=None,
    ) -> list[str]:
        """Return only tables that pass both schema and table filters.

        Parameters
        ----------
        tables:
            List of table name strings (may be ``schema.table`` or bare names).
        schema_extractor:
            Optional callable ``(table_name) -> schema_name``.  When provided
            the schema filter is applied via this function.  When absent the
            filter tries to split on ``"."`` automatically.
        """
        result = []
        for table in tables:
            schema = None
            bare = table
            if schema_extractor is not None:
                schema = schema_extractor(table)
            elif "." in table:
                parts = table.split(".", 1)
                schema = parts[0]
                bare = parts[1]
            if schema is not None and not self.accepts_schema(schema):
                continue
            if not self.accepts_table(bare, schema):
                continue
            result.append(table)
        skipped = len(tables) - len(result)
        if skipped:
            logger.info(
                "Table filter: kept %d of %d tables (%d excluded)",
                len(result), len(tables), skipped,
            )
        return result

    def describe(self) -> dict:
        """Return a JSON-safe summary of the active filter rules."""
        desc: dict = {}
        if self.include_schemas:
            desc["include_schemas"] = list(self.include_schemas)
        if self.exclude_schemas:
            desc["exclude_schemas"] = list(self.exclude_schemas)
        if self.include_tables:
            desc["include_tables"] = list(self.include_tables)
        if self.exclude_tables:
            desc["exclude_tables"] = list(self.exclude_tables)
        return desc

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _norm(self, value: str) -> str:
        return value if self.case_sensitive else value.lower()

    def _norm_list(self, values: list[str]) -> list[str]:
        return [self._norm(v) for v in values]

    def _schema_only_patterns(self, values: list[str]) -> list[str]:
        return [pattern for pattern in self._norm_list(values) if "." not in pattern]

    def _qualified_patterns(self, values: list[str]) -> list[str]:
        return [pattern for pattern in self._norm_list(values) if "." in pattern]


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _as_list(value) -> list[str]:
    """Coerce *value* into a list of strings."""
    if value is None:
        return []
    if isinstance(value, str):
        return _split_csv(value)
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value]
    return []


def _split_csv(value: str | None) -> list[str]:
    """Split a comma-separated string into a cleaned list."""
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]
