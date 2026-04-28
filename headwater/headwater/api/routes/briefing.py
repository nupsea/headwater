"""Briefing API -- narrative priorities + wins for the homepage.

Aggregates signals already computed elsewhere (review queue, drift events,
failed syncs, quality contract status) into a small payload the UI renders
as a continuous briefing: "you have N things worth your attention and M
that can wait."
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, Request

logger = logging.getLogger(__name__)
router = APIRouter()


Urgency = Literal["high", "medium", "low"]


def _priority(
    urgency: Urgency,
    headline: str,
    detail: str,
    action: str,
    route: str,
    *,
    deeplink: str | None = None,
) -> dict:
    return {
        "urgency": urgency,
        "headline": headline,
        "detail": detail,
        "action": action,
        "route": route,
        "deeplink": deeplink,
    }


@router.get("/briefing/today")
async def briefing_today(request: Request):
    """Return today's briefing -- priorities and wins -- aggregated from system state."""
    pipeline = request.app.state.pipeline
    store = request.app.state.metadata_store

    discovery = pipeline.get("discovery")
    mart_models = pipeline.get("mart_models") or []
    contracts = pipeline.get("contracts") or []
    exec_results = pipeline.get("execution_results") or []
    latest_quality = store.get_latest_quality_report()

    priorities: list[dict] = []
    wins: list[str] = []

    # 1. Mart models awaiting approval
    pending_marts = [m for m in mart_models if getattr(m, "status", "proposed") == "proposed"]
    if pending_marts:
        priorities.append(
            _priority(
                "high" if len(pending_marts) >= 3 else "medium",
                f"{len(pending_marts)} mart model"
                f"{'s' if len(pending_marts) != 1 else ''} awaiting approval",
                "Generated SQL is ready. Each model materializes a key analytical view; "
                "review before they run on schedule.",
                "Open review queue",
                "/models",
            )
        )
    elif mart_models:
        wins.append(f"All {len(mart_models)} mart models approved")

    # 2. Failed source syncs
    sources = store.list_sources()
    failed_sources = [s for s in sources if s.get("status") == "error"]
    drifting_sources = [
        s for s in sources if s.get("status") == "warning" or (s.get("drift_count") or 0) > 0
    ]
    healthy_sources = [s for s in sources if s.get("status") == "healthy"]

    for s in failed_sources:
        priorities.append(
            _priority(
                "high",
                f"{s.get('display_name') or s['name']} connection failed",
                f"Last successful sync was at {s.get('last_sync_at') or 'unknown time'}. "
                "Likely an expired credential or network hiccup -- quick fix.",
                "Reconnect source",
                "/sources",
                deeplink=f"/sources?focus={s['name']}",
            )
        )

    if drifting_sources and not failed_sources:
        for s in drifting_sources[:2]:
            priorities.append(
                _priority(
                    "medium",
                    f"Drift detected on {s.get('display_name') or s['name']}",
                    f"{s.get('drift_count') or 0} drift event(s) since the last review. "
                    "Could be real change or a unit mismatch -- worth a glance.",
                    "Open drift report",
                    "/quality",
                    deeplink=f"/sources?focus={s['name']}",
                )
            )

    # 3. Low-confidence column descriptions (from discovery)
    if discovery:
        low_conf_cols = []
        for t in discovery.tables:
            for c in t.columns:
                conf = getattr(c, "confidence", None)
                if conf is not None and conf < 0.5 and not getattr(c, "is_primary_key", False):
                    low_conf_cols.append(f"{t.name}.{c.name}")
        if low_conf_cols:
            sample = ", ".join(low_conf_cols[:3])
            priorities.append(
                _priority(
                    "medium" if len(low_conf_cols) >= 5 else "low",
                    f"{len(low_conf_cols)} column description"
                    f"{'s' if len(low_conf_cols) != 1 else ''} need confirmation",
                    f"Auto-generated descriptions are below the confidence threshold "
                    f"(e.g. {sample}). A quick confirm-or-edit locks them in.",
                    "Review descriptions",
                    "/dictionary",
                )
            )

    # 4. Failed quality contracts
    if latest_quality and latest_quality.get("failed", 0) > 0:
        priorities.append(
            _priority(
                "high",
                f"{latest_quality['failed']} quality contract"
                f"{'s' if latest_quality['failed'] != 1 else ''} failing",
                "Validation rules broke since the last run. Investigate before downstream "
                "models pick up bad data.",
                "View quality report",
                "/quality",
            )
        )
    elif latest_quality and latest_quality.get("total_contracts", 0) > 0:
        wins.append(
            f"{latest_quality['passed']} of {latest_quality['total_contracts']} "
            "quality contracts passing"
        )
    else:
        failed_contracts = [c for c in contracts if getattr(c, "status", "") == "failed"]
        if failed_contracts:
            priorities.append(
                _priority(
                    "high",
                    f"{len(failed_contracts)} quality contract"
                    f"{'s' if len(failed_contracts) != 1 else ''} failing",
                    "Validation rules broke since the last run. Investigate before downstream "
                    "models pick up bad data.",
                    "View quality report",
                    "/quality",
                )
            )
        elif contracts:
            passing = sum(
                1 for c in contracts if getattr(c, "status", "") in ("passing", "observing")
            )
            if passing:
                wins.append(f"{passing} quality contracts passing -- no regressions")

    # 5. Wins worth surfacing
    if discovery and discovery.relationships:
        high_integrity = [r for r in discovery.relationships if r.referential_integrity >= 0.99]
        if high_integrity:
            wins.append(
                f"{len(high_integrity)} of {len(discovery.relationships)} foreign keys "
                "resolve at 100% integrity"
            )
    if healthy_sources:
        wins.append(f"{len(healthy_sources)} source(s) healthy and syncing on schedule")
    if exec_results:
        successful = sum(1 for r in exec_results if r.success)
        if successful:
            wins.append(f"{successful} models materialized successfully")

    # 6. Sort priorities -- high first, low last
    order = {"high": 0, "medium": 1, "low": 2}
    priorities.sort(key=lambda p: order.get(p["urgency"], 99))

    high_count = sum(1 for p in priorities if p["urgency"] == "high")

    # 7. Summary stats for the briefing footer
    stats = {
        "sources": len(sources),
        "tables": len(discovery.tables) if discovery else 0,
        "quality_checks": latest_quality["total_contracts"] if latest_quality else len(contracts),
        "health_pct": _overall_health(sources, contracts, latest_quality),
    }

    no_data = not sources and not discovery
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "summary": {
            "attention_count": high_count,
            "wait_count": len(priorities) - high_count,
            # all_clear means "we are watching things and nothing is wrong";
            # not the same as "you haven't set anything up yet."
            "all_clear": len(priorities) == 0 and not no_data,
            "no_data": no_data,
        },
        "priorities": priorities,
        "wins": wins,
        "stats": stats,
    }


def _overall_health(
    sources: list[dict],
    contracts: list,
    latest_quality: dict | None = None,
) -> int:
    """Cheap overall-health metric for the briefing footer."""
    if not sources:
        return 0
    src_health = [s.get("health") for s in sources if s.get("health") is not None]
    avg_src = sum(src_health) / len(src_health) if src_health else 100
    if latest_quality:
        contract_pct = float(latest_quality.get("score") or 0)
    elif contracts:
        passing = sum(1 for c in contracts if getattr(c, "status", "") in ("passing", "observing"))
        contract_pct = (passing / len(contracts)) * 100
    else:
        contract_pct = 100
    return int(round((avg_src + contract_pct) / 2))
