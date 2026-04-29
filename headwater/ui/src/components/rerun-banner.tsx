"use client";

import { useEffect, useState } from "react";
import { api, type RerunPlan } from "@/lib/api";
import { useToast } from "@/components/toast";

const STORAGE_KEY = "hw-needs-rerun";

/**
 * Surface a banner whenever the user has made edits (column descriptions,
 * model approvals, etc.) that haven't been re-applied via the pipeline yet.
 *
 * Pages signal this by dispatching a window event:
 *   window.dispatchEvent(new CustomEvent("hw-needs-rerun"))
 * and clear it via:
 *   window.dispatchEvent(new CustomEvent("hw-rerun-cleared"))
 */
export function RerunBanner() {
  const { toast } = useToast();
  const [needs, setNeeds] = useState(
    () =>
      typeof window !== "undefined" &&
      localStorage.getItem(STORAGE_KEY) === "1"
  );
  const [running, setRunning] = useState(false);
  const [done, setDone] = useState(false);
  const [sourceUri, setSourceUri] = useState<string | null>(null);
  const [sourceName, setSourceName] = useState<string | null>(null);
  const [plan, setPlan] = useState<RerunPlan | null>(null);

  useEffect(() => {
    const onSet = () => {
      setNeeds(true);
      setDone(false);
      try {
        localStorage.setItem(STORAGE_KEY, "1");
      } catch {}
    };
    const onClear = () => {
      setNeeds(false);
      try {
        localStorage.removeItem(STORAGE_KEY);
      } catch {}
    };
    window.addEventListener("hw-needs-rerun", onSet);
    window.addEventListener("hw-rerun-cleared", onClear);

    fetch("/api/sources")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => {
        if (d?.sources?.length) {
          const s = d.sources[d.sources.length - 1];
          setSourceUri(s.uri || s.path || s.name);
          setSourceName(s.name);
        }
      })
      .catch(() => {});

    return () => {
      window.removeEventListener("hw-needs-rerun", onSet);
      window.removeEventListener("hw-rerun-cleared", onClear);
    };
  }, []);

  useEffect(() => {
    api
      .rerunPlan(sourceName || undefined)
      .then((nextPlan) => {
        setPlan(nextPlan);
        if (!nextPlan.no_action_needed) setNeeds(true);
      })
      .catch(() => {});
  }, [sourceName]);

  if (!needs) return null;

  const dismiss = () => {
    setNeeds(false);
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch {}
  };

  const rerun = async () => {
    if (!sourceUri) {
      toast("No source registered. Add one on the Sources page.", "error");
      return;
    }
    setRunning(true);
    try {
      await api.pipelineRun(sourceUri);
      setDone(true);
      toast("Pipeline complete -- analysis updated", "success");
      setTimeout(dismiss, 1800);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      toast(`Pipeline failed: ${msg}`, "error");
    }
    setRunning(false);
  };

  return (
    <div
      className={`mb-5 px-4 py-2.5 border rounded-lg flex items-center justify-between gap-3 ${
        done
          ? "bg-green-50 border-green-200 dark:bg-green-950/30 dark:border-green-800"
          : "bg-amber-50 border-amber-200 dark:bg-amber-950/30 dark:border-amber-800"
      }`}
    >
      <div className="flex items-center gap-2.5">
        {running && (
          <div className="w-3.5 h-3.5 border-2 border-amber-600 border-t-transparent rounded-full animate-spin" />
        )}
        {done && <span className="text-green-600">✓</span>}
        {!running && !done && <span className="text-amber-600 text-base">↻</span>}
        <div>
          <div
            className={`text-[13px] font-semibold ${
              done
                ? "text-green-700 dark:text-green-400"
                : "text-amber-800 dark:text-amber-300"
            }`}
          >
            {done
              ? "Pipeline complete -- analysis updated"
              : running
              ? "Running pipeline…"
              : plan?.human_review_required
              ? "Review required after drift"
              : "Rerun recommended"}
          </div>
          {!done && (
            <div className="text-[11px] text-amber-700 dark:text-amber-400/80 mt-0.5">
              {plan?.summary ||
                "Re-run Headwater to apply edits and refine analysis across all pages."}
            </div>
          )}
        </div>
      </div>
      {!done && (
        <div className="flex gap-2 shrink-0">
          {plan && !plan.no_action_needed && (
            <a
              href="/models"
              className="px-3 py-1 border border-border rounded-md text-[11px] text-muted hover:bg-background"
            >
              View plan
            </a>
          )}
          <button
            onClick={rerun}
            disabled={running}
            className="px-3 py-1 bg-amber-600 hover:bg-amber-700 text-white rounded-md text-[11px] font-medium disabled:opacity-60"
          >
            {running ? "Running…" : "Re-run pipeline"}
          </button>
          <button
            onClick={dismiss}
            className="px-3 py-1 border border-border rounded-md text-[11px] text-muted hover:bg-background"
          >
            Dismiss
          </button>
        </div>
      )}
    </div>
  );
}
