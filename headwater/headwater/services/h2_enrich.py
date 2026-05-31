"""LLM-assisted enrichment for H2: goal suggestion and column descriptions.

Both honor invariant I-3 — only column/table names, dtypes, and statistical
summaries are sent to the model, never raw rows.  Both degrade gracefully when
no local model is available (Ollama down / NoLLMProvider): goal suggestion falls
back to a generic, non-domain-specific statement; description generation reports
that it produced nothing.

Per the No-Domain-Hardcoding rule, no domain values or meanings are embedded
here — the model infers from the data's own names and stats.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from headwater.analyzer.llm import LLMProvider, NoLLMProvider, get_provider
from headwater.core.config import HeadwaterSettings, get_settings
from headwater.core.store import HeadwaterStore


def _invoke(provider: LLMProvider, prompt: str, system: str) -> dict[str, Any]:
    """Call the async provider from sync code, even when a loop already runs."""
    try:
        return asyncio.run(provider.analyze(prompt, system))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(provider.analyze(prompt, system))
        finally:
            loop.close()
    except Exception:
        return {}


def _provider(
    settings: HeadwaterSettings | None, provider: LLMProvider | None
) -> tuple[LLMProvider, HeadwaterSettings]:
    settings = settings or get_settings()
    if provider is None:
        provider = get_provider(settings, store=None)  # H2 store lacks audit methods
    return provider, settings


def _schema_brief(store: HeadwaterStore, source_name: str) -> list[dict[str, Any]]:
    brief = []
    for t in store.get_tables(source_name):
        cols = store.get_columns(source_name, t["name"])
        brief.append(
            {
                "table": t["name"],
                "row_count": t.get("row_count"),
                "columns": [
                    {"name": c["name"], "dtype": c.get("dtype")} for c in cols
                ],
            }
        )
    return brief


# ── Goal suggestion ───────────────────────────────────────────────────────────


def suggest_goal(
    store: HeadwaterStore,
    source_name: str,
    *,
    settings: HeadwaterSettings | None = None,
    provider: LLMProvider | None = None,
) -> dict[str, Any]:
    """Propose an analysis goal inferred from the source's schema."""
    if store.get_source(source_name) is None:
        raise ValueError(f"Source '{source_name}' not found.")
    brief = _schema_brief(store, source_name)
    table_names = [t["table"] for t in brief]

    prov, _ = _provider(settings, provider)
    if isinstance(prov, NoLLMProvider) or not brief:
        return {
            "goal": _fallback_goal(table_names),
            "rationale": "Generated without a model — start Ollama for a data-aware suggestion.",
            "available": False,
        }

    system = (
        "You are a data analyst. Given only table and column names with their "
        "types, propose ONE concrete analytical goal a business user could pursue "
        "with this data. Be specific to what the names imply; do not invent "
        "columns. Respond as JSON only."
    )
    prompt = (
        "SCHEMA (names and types only):\n"
        f"{json.dumps(brief, indent=2, default=str)}\n\n"
        "Return JSON: {\"goal\": \"<one concrete goal sentence>\", "
        "\"rationale\": \"<one short sentence on why this data supports it>\"}"
    )
    raw = _invoke(prov, prompt, system)
    goal = str(raw.get("goal") or "").strip()
    if not goal:
        return {
            "goal": _fallback_goal(table_names),
            "rationale": "Model returned no suggestion; using a generic goal.",
            "available": False,
        }
    return {
        "goal": goal,
        "rationale": str(raw.get("rationale") or "").strip(),
        "available": True,
    }


def _fallback_goal(table_names: list[str]) -> str:
    if not table_names:
        return "Understand the key patterns and relationships in this data."
    shown = ", ".join(table_names[:4])
    return f"Understand the key patterns, trends, and relationships across {shown}."


# ── Column descriptions ─────────────────────────────────────────────────────────


def generate_descriptions(
    store: HeadwaterStore,
    source_name: str,
    *,
    overwrite: bool = False,
    settings: HeadwaterSettings | None = None,
    provider: LLMProvider | None = None,
) -> dict[str, Any]:
    """LLM-generate column descriptions from names/types, one call per table.

    Skips locked columns and (unless overwrite) columns that already have a
    description.  Returns counts.  Never overwrites raw data; metadata only.
    """
    from headwater.services.h2_catalog import update_column

    if store.get_source(source_name) is None:
        raise ValueError(f"Source '{source_name}' not found.")

    prov, _ = _provider(settings, provider)
    if isinstance(prov, NoLLMProvider):
        return {"updated": 0, "available": False, "note": "No model available."}

    system = (
        "You write concise, plain-English data dictionary entries. Given a table "
        "name and its columns (name + type), return a one-sentence description for "
        "each column INFERRED FROM ITS NAME. Only include columns you are "
        "reasonably confident about; omit the rest. Respond as JSON only."
    )

    updated = 0
    for t in store.get_tables(source_name):
        cols = store.get_columns(source_name, t["name"])
        targets = [
            c
            for c in cols
            if not c.get("locked")
            and (overwrite or not (c.get("description") or "").strip())
        ]
        if not targets:
            continue
        prompt = (
            f"TABLE: {t['name']}\n"
            "COLUMNS (name: type):\n"
            + "\n".join(f"- {c['name']}: {c.get('dtype')}" for c in targets)
            + "\n\nReturn JSON mapping column name -> one-sentence description, "
            'e.g. {"created_at": "Timestamp when the record was created."}'
        )
        raw = _invoke(prov, prompt, system)
        if not isinstance(raw, dict):
            continue
        valid = {c["name"] for c in targets}
        for name, desc in raw.items():
            if name in valid and isinstance(desc, str) and desc.strip():
                update_column(
                    store, source_name, t["name"], name, description=desc.strip()
                )
                updated += 1

    return {"updated": updated, "available": True}


# ── Resolve-card suggestion (Ask AI) ────────────────────────────────────────────


def suggest_resolution(
    store: HeadwaterStore,
    project_id: str,
    card_id: str,
    *,
    settings: HeadwaterSettings | None = None,
    provider: LLMProvider | None = None,
) -> dict[str, Any]:
    """Draft a resolution for a Resolve card with the local LLM, for human review.

    I-3-safe: only the column name/table and the already-surfaced known codes
    (top-N distinct values from the card payload) are sent — never raw rows.  The
    model proposes a markdown table the analyst edits and saves; it is never
    auto-applied.  Degrades gracefully when no model is running.
    """
    card = next(
        (r for r in store.list_resolve_items(project_id) if r["id"] == card_id), None
    )
    if card is None:
        raise ValueError(f"Resolve card '{card_id}' not found.")

    payload = card.get("payload") or {}
    issue_kind = card.get("issue_kind", "")
    table = payload.get("table")
    column = payload.get("column")
    top_values = payload.get("top_values") or []
    codes = [
        str(v[0]) if isinstance(v, (list, tuple)) else str(v)
        for v in top_values
        if v not in (None, "")
    ][:12]

    prov, _ = _provider(settings, provider)
    if isinstance(prov, NoLLMProvider):
        return {
            "available": False,
            "markdown": "",
            "note": "No model available — start Ollama to draft a suggestion.",
        }

    system = (
        "You help a data analyst document data. Propose a concise DRAFT the analyst "
        "will review and edit. Infer ONLY from the names and the listed codes; do not "
        "invent columns or codes. Respond as JSON only."
    )
    if issue_kind == "enum_mapping_needed" and column and codes:
        prompt = (
            f"TABLE: {table}\nCOLUMN: {column}\n"
            f"KNOWN CODES: {', '.join(codes)}\n\n"
            "Propose a plausible business meaning for EACH known code. "
            'Return JSON: {"markdown": "| code | meaning |\\n| --- | --- |\\n'
            '| <code> | <meaning> |"} covering exactly the known codes.'
        )
    else:
        col_line = f"COLUMN: {table}.{column}\n" if column else ""
        prompt = (
            f"GAP: {card.get('title', '')}\n{card.get('body', '')}\n{col_line}\n"
            "Propose the markdown the analyst should provide to resolve this. "
            'Return JSON: {"markdown": "| column | meaning |\\n| --- | --- |\\n'
            '| <column> | <meaning> |"}.'
        )

    raw = _invoke(prov, prompt, system)
    markdown = str(raw.get("markdown") or "").strip()
    if not markdown:
        return {
            "available": False,
            "markdown": "",
            "note": "The model returned no suggestion. Try again or write it yourself.",
        }
    return {
        "available": True,
        "markdown": markdown,
        "note": "Draft from a local model — review and edit before saving.",
    }
