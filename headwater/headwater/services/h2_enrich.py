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

# Columns per LLM call. A 14B local model generates ~15 tok/s; one sentence per
# column is ~25-30 tokens, so 16 columns ≈ 30s — comfortably inside the timeout.
# A 66-column table in a single call (~2000 tokens) blows the 120s budget.
_DESC_CHUNK_SIZE = 16


def _profile_hint(prof: dict[str, Any]) -> str:
    """One short, I-3-safe stats hint for a column prompt line (aggregates only)."""
    if not prof:
        return ""
    bits: list[str] = []
    null_rate = prof.get("null_rate")
    if null_rate:
        bits.append(f"{int(float(null_rate) * 100)}% null")
    distinct = prof.get("distinct_count")
    if distinct:
        bits.append(f"{int(distinct)} distinct")
    lo, hi = prof.get("min_value"), prof.get("max_value")
    if lo is None and hi is None:
        lo, hi = prof.get("min_date"), prof.get("max_date")
    if lo is not None and hi is not None:
        bits.append(f"range {lo}..{hi}")
    top = prof.get("top_values") or []
    values = [str(v[0]) if isinstance(v, (list, tuple)) else str(v) for v in top[:3]]
    if not values:
        values = [str(v) for v in (prof.get("sample_values") or [])[:3]]
    if values:
        bits.append(f"e.g. {', '.join(values)}")
    return f"  ({'; '.join(bits)})" if bits else ""


def _chunks(items: list, size: int) -> list[list]:
    return [items[i : i + size] for i in range(0, len(items), size)]


# Keys a model commonly nests the mapping under instead of returning it flat.
_DESC_WRAPPER_KEYS = ("descriptions", "columns", "result", "mapping", "fields")


def _match_descriptions(
    raw: Any, valid: set[str]
) -> dict[str, str]:
    """Pull {column_name: description} out of a model response, robustly.

    Local models don't reliably return the exact JSON shape asked for. This
    tolerates the common deviations so a good answer isn't silently dropped:
      * the mapping nested under a wrapper key ({"descriptions": {...}})
      * a list of {name/column, description} objects
      * qualified keys ("schema.table.col") — matched by last component
      * case differences
    Only columns in ``valid`` are returned; the match is exact first, then
    case-insensitive, then by the final dotted component.
    """
    if not isinstance(raw, dict):
        # A bare list of {name, description} objects.
        if isinstance(raw, list):
            raw = {"columns": raw}
        else:
            return {}

    # Unwrap a single common wrapper key if the top level isn't the mapping.
    payload: Any = raw
    if not any(k in valid for k in raw) and not _looks_flat_mapping(raw, valid):
        for key in _DESC_WRAPPER_KEYS:
            if key in raw:
                payload = raw[key]
                break

    pairs: list[tuple[str, str]] = []
    if isinstance(payload, dict):
        pairs = [(str(k), v) for k, v in payload.items()]
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                name = item.get("name") or item.get("column") or item.get("col")
                desc = item.get("description") or item.get("desc")
                if name is not None:
                    pairs.append((str(name), desc))

    # Resolution maps for case-insensitive and last-component matching.
    by_lower = {v.lower(): v for v in valid}
    by_tail = {v.rsplit(".", 1)[-1].lower(): v for v in valid}

    out: dict[str, str] = {}
    for raw_name, desc in pairs:
        if not isinstance(desc, str) or not desc.strip():
            continue
        key = raw_name.strip()
        target = (
            key
            if key in valid
            else by_lower.get(key.lower())
            or by_tail.get(key.rsplit(".", 1)[-1].lower())
        )
        if target and target not in out:
            out[target] = desc.strip()
    return out


def _looks_flat_mapping(raw: dict, valid: set[str]) -> bool:
    """True when the dict's values are description strings (a flat mapping)."""
    str_vals = sum(1 for v in raw.values() if isinstance(v, str))
    return str_vals >= max(1, len(raw) // 2)


def generate_descriptions(
    store: HeadwaterStore,
    source_name: str,
    *,
    overwrite: bool = False,
    settings: HeadwaterSettings | None = None,
    provider: LLMProvider | None = None,
) -> dict[str, Any]:
    """LLM-generate table + column descriptions, chunked to fit the model budget.

    Columns go to the model in batches of ``_DESC_CHUNK_SIZE`` so a wide table
    cannot exceed the Ollama timeout in a single call.  Prompts carry the column's
    statistical summary when a profile exists (I-3-safe: aggregates only, never
    rows) so descriptions reflect the actual data.  Tables without a description
    get one too.  Skips locked columns and (unless overwrite) columns that already
    have a description.  Returns counts; partial failure is reported, not hidden.
    """
    from headwater.analyzer.llm import check_llm_available
    from headwater.services.h2_catalog import update_column

    if store.get_source(source_name) is None:
        raise ValueError(f"Source '{source_name}' not found.")

    settings = settings or get_settings()
    # Use the capable reasoning model (qwen) rather than a possibly-weak default.
    model = getattr(settings, "reasoning_model", "") or settings.llm_model
    if provider is None:
        ok, why = check_llm_available(settings, model=model)
        if not ok:
            return {"updated": 0, "available": False, "note": why}
        provider = get_provider(settings.model_copy(update={"llm_model": model}))
    prov = provider
    if isinstance(prov, NoLLMProvider):
        return {"updated": 0, "available": False, "note": "AI is off — no model configured."}

    profiles = {
        f"{p['table_name']}.{p['column_name']}": p["profile"]
        for p in store.get_profiles(source_name)
    }

    system = (
        "You write concise, plain-English data dictionary entries. Given a table "
        "name and some of its columns (name, type, and data statistics when "
        "known), return a one-sentence description for EACH listed column, "
        "grounded in the name, type, and statistics. Respond as JSON only."
    )

    updated = 0
    tables_updated = 0
    attempted = 0
    failed_calls = 0
    unparsed_calls = 0
    for t in store.get_tables(source_name):
        cols = store.get_columns(source_name, t["name"])
        targets = [
            c
            for c in cols
            if not c.get("locked")
            and (overwrite or not (c.get("description") or "").strip())
        ]
        for chunk in _chunks(targets, _DESC_CHUNK_SIZE):
            attempted += 1
            lines = []
            for c in chunk:
                hint = _profile_hint(profiles.get(f"{t['name']}.{c['name']}", {}))
                lines.append(f"- {c['name']}: {c.get('dtype')}{hint}")
            prompt = (
                f"TABLE: {t['name']}\n"
                "COLUMNS (name: type, then statistics if known):\n"
                + "\n".join(lines)
                + "\n\nReturn a JSON object mapping each column name (exactly as "
                "listed, unqualified) to a one-sentence description, e.g. "
                '{"created_at": "Timestamp when the record was created."}'
            )
            raw = _invoke(prov, prompt, system)
            if not raw:
                # Empty result from a reachable model almost always means the call
                # failed (timeout / dropped connection) — track it, keep going.
                failed_calls += 1
                continue
            valid = {c["name"] for c in chunk}
            matched = _match_descriptions(raw, valid)
            if not matched:
                # The model answered but nothing matched these columns (a shape
                # we couldn't parse). That is NOT "nothing to do" — surface it
                # so we never silently report success while columns stay blank.
                unparsed_calls += 1
                continue
            for name, desc in matched.items():
                update_column(store, source_name, t["name"], name, description=desc)
                updated += 1

        # Table description: one cheap call (short output) when missing.
        if overwrite or not (t.get("description") or "").strip():
            attempted += 1
            col_names = ", ".join(c["name"] for c in cols[:40])
            tprompt = (
                f"TABLE: {t['name']} ({t.get('row_count') or 'unknown'} rows)\n"
                f"COLUMNS: {col_names}\n\n"
                "Describe in ONE sentence what each row of this table most likely "
                'represents. Return JSON: {"description": "<sentence>"}'
            )
            traw = _invoke(prov, tprompt, system)
            tdesc = ""
            if isinstance(traw, dict):
                tdesc = str(
                    traw.get("description") or traw.get("summary") or ""
                ).strip()
            if tdesc:
                store.upsert_table(
                    source_name,
                    t["name"],
                    schema_name=t.get("schema_name"),
                    row_count=int(t.get("row_count") or 0),
                    description=tdesc,
                    domain=t.get("domain"),
                    selected=bool(t.get("selected")),
                )
                tables_updated += 1
            else:
                failed_calls += 1

    if attempted == 0:
        # Genuinely nothing to do: every column already has a description (or is
        # locked) and every table is described. A true no-op, reported as such.
        return {"updated": 0, "tables_updated": 0, "available": True, "note": ""}

    if updated == 0 and tables_updated == 0:
        # We asked the model and got nothing usable — a timeout, or a response
        # shape we couldn't parse. Surface the real reason; never pretend success.
        # Prefer the unparsed diagnosis when present: it is the more actionable
        # one (the model IS responding, just not in the asked-for format).
        if unparsed_calls:
            note = (
                f"The model ({model}) replied but not in the expected format, so "
                "no descriptions could be applied. Try again, or set a stronger "
                "reasoning model."
            )
        else:
            note = (
                f"The model ({model}) returned no descriptions — it may have timed "
                "out or be too small. Try again, or set a stronger reasoning model."
            )
        return {"updated": 0, "available": False, "note": note}

    note_parts: list[str] = []
    if failed_calls:
        note_parts.append(f"{failed_calls} call(s) timed out")
    if unparsed_calls:
        note_parts.append(f"{unparsed_calls} reply(ies) couldn't be parsed")
    note = ""
    if note_parts:
        note = f"{'; '.join(note_parts)} — re-run to fill the rest."
    return {
        "updated": updated,
        "tables_updated": tables_updated,
        "available": True,
        "note": note,
    }


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
    # The enum card stores its codes under "values" (shown as chips); accept the
    # older "top_values" too. Without this the codes were lost and the model fell
    # back to a generic template instead of mapping the actual codes.
    raw_codes = payload.get("values") or payload.get("top_values") or []
    codes = [
        str(v[0]) if isinstance(v, (list, tuple)) else str(v)
        for v in raw_codes
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


# ── Relationship + key inference (Move D — advisory, human-verified) ───────────


def _valid_columns(store: HeadwaterStore, source_name: str) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for t in store.get_tables(source_name):
        out[t["name"]] = {c["name"] for c in store.get_columns(source_name, t["name"])}
    return out


def suggest_relationships(
    store: HeadwaterStore,
    source_name: str,
    *,
    settings: HeadwaterSettings | None = None,
    provider: LLMProvider | None = None,
) -> dict[str, Any]:
    """Propose foreign-key-style relationships between tables (advisory).

    I-3-safe: only table/column names + dtypes are sent.  Heuristic detection
    stays the base layer; this augments it with a rationale for human verify/lock.
    Degrades to ``available=False`` with no model.
    """
    if store.get_source(source_name) is None:
        raise ValueError(f"Source '{source_name}' not found.")
    brief = _schema_brief(store, source_name)
    prov, _ = _provider(settings, provider)
    if isinstance(prov, NoLLMProvider) or len(brief) < 2:
        return {
            "available": False,
            "relationships": [],
            "note": "No model available — start Ollama.",
        }

    system = (
        "You are a data modeler. Given only table and column names with types, "
        "infer likely foreign-key relationships between tables (a column in one "
        "table referencing a key in another). Only use columns that exist. "
        "Respond as JSON only."
    )
    prompt = (
        "SCHEMA (names and types only):\n"
        f"{json.dumps(brief, indent=2, default=str)}\n\n"
        'Return JSON: {"relationships": [{"from_table": "...", "from_column": "...", '
        '"to_table": "...", "to_column": "...", "rationale": "<short why>", '
        '"confidence": <0..1>}]}'
    )
    raw = _invoke(prov, prompt, system)
    valid = _valid_columns(store, source_name)
    out = []
    for r in raw.get("relationships") or []:
        ft, fc = str(r.get("from_table") or ""), str(r.get("from_column") or "")
        tt, tc = str(r.get("to_table") or ""), str(r.get("to_column") or "")
        if fc in valid.get(ft, set()) and tc in valid.get(tt, set()) and ft != tt:
            out.append(
                {
                    "from_table": ft,
                    "from_column": fc,
                    "to_table": tt,
                    "to_column": tc,
                    "rationale": str(r.get("rationale") or "").strip(),
                    "confidence": round(float(r.get("confidence") or 0.0), 2),
                }
            )
    return {"available": True, "relationships": out}


def suggest_keys(
    store: HeadwaterStore,
    source_name: str,
    *,
    settings: HeadwaterSettings | None = None,
    provider: LLMProvider | None = None,
) -> dict[str, Any]:
    """Propose the business / composite key column(s) per table (advisory).

    Adds per-column uniqueness (an I-3-safe stat) so the model can reason about
    which columns identify a row.  Degrades when no model is available.
    """
    if store.get_source(source_name) is None:
        raise ValueError(f"Source '{source_name}' not found.")
    profiles = {
        f"{p['table_name']}.{p['column_name']}": p["profile"]
        for p in store.get_profiles(source_name)
    }
    brief = []
    for t in store.get_tables(source_name):
        cols = []
        for c in store.get_columns(source_name, t["name"]):
            prof = profiles.get(f"{t['name']}.{c['name']}", {})
            cols.append(
                {
                    "name": c["name"],
                    "dtype": c.get("dtype"),
                    "uniqueness": prof.get("uniqueness_ratio"),
                }
            )
        brief.append({"table": t["name"], "columns": cols})

    prov, _ = _provider(settings, provider)
    if isinstance(prov, NoLLMProvider) or not brief:
        return {"available": False, "keys": [], "note": "No model available — start Ollama."}

    system = (
        "You are a data modeler. For each table, identify the column(s) that form "
        "its primary / business key (uniquely identify a row). Prefer columns with "
        "uniqueness near 1.0. Only use columns that exist. Respond as JSON only."
    )
    prompt = (
        "SCHEMA (names, types, uniqueness 0..1):\n"
        f"{json.dumps(brief, indent=2, default=str)}\n\n"
        'Return JSON: {"keys": [{"table": "...", "columns": ["..."], '
        '"rationale": "<short why>", "confidence": <0..1>}]}'
    )
    raw = _invoke(prov, prompt, system)
    valid = _valid_columns(store, source_name)
    out = []
    for k in raw.get("keys") or []:
        table = str(k.get("table") or "")
        cols = [str(c) for c in (k.get("columns") or []) if str(c) in valid.get(table, set())]
        if table and cols:
            out.append(
                {
                    "table": table,
                    "columns": cols,
                    "rationale": str(k.get("rationale") or "").strip(),
                    "confidence": round(float(k.get("confidence") or 0.0), 2),
                }
            )
    return {"available": True, "keys": out}
