"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api, type Project } from "@/lib/api";
import { useToast } from "@/components/toast";

export function AppTopbar() {
  const { toast } = useToast();
  const [project, setProject] = useState<Project | null>(null);
  const [lastRun, setLastRun] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [sourceUri, setSourceUri] = useState<string | null>(null);

  useEffect(() => {
    api
      .projects()
      .then((res) => {
        const p = res.projects?.[0] ?? null;
        setProject(p);
      })
      .catch(() => {});
    // Try to get source URI for re-run
    fetch("/api/sources")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => {
        if (d?.sources?.length) {
          const s = d.sources[d.sources.length - 1];
          setLastRun(s.last_sync_at);
          // Fall back through uri -> path -> host -> name
          setSourceUri(s.uri || s.path || s.host || s.name);
        }
      })
      .catch(() => {});
    // Also check status for discovered state
    api.status().then((st) => {
      if (st.discovered && !lastRun) {
        setLastRun("discovered");
      }
    }).catch(() => {});
  }, []);

  const rerun = async () => {
    if (!sourceUri) {
      toast(
        "Set a data source on the Sources page before re-running.",
        "error"
      );
      return;
    }
    setRunning(true);
    try {
      await api.syncSource(sourceUri);
      toast("Pipeline complete", "success");
      setLastRun(new Date().toISOString());
    } catch (e) {
      // Fall back to pipelineRun if syncSource fails
      try {
        await api.pipelineRun(sourceUri);
        toast("Pipeline complete", "success");
        setLastRun(new Date().toISOString());
      } catch (e2) {
        const msg = e2 instanceof Error ? e2.message : String(e2);
        toast(`Pipeline failed: ${msg}`, "error");
      }
    }
    setRunning(false);
  };

  return (
    <header className="h-12 px-5 flex items-center justify-between border-b border-border bg-card shrink-0">
      <div className="flex items-center gap-3">
        <Link href="/" className="flex items-center gap-2">
          <div className="w-5 h-5 rounded-full bg-accent/15 flex items-center justify-center">
            <div className="w-2 h-2 rounded-full bg-accent" />
          </div>
          <span className="font-bold text-[15px] tracking-tight text-foreground">
            Headwater
          </span>
        </Link>
        {project && (
          <>
            <span className="text-border">·</span>
            <span className="text-[13px] text-muted truncate max-w-xs">
              {project.display_name}
            </span>
          </>
        )}
      </div>
      <div className="flex items-center gap-3">
        <span className="text-[12px] text-muted">
          {lastRun === "discovered"
            ? "Data loaded"
            : lastRun
              ? `Last run ${formatRelative(lastRun)}`
              : "No runs yet"}
        </span>
        <button
          onClick={rerun}
          disabled={running}
          className="px-4 py-1 bg-accent text-white rounded-md text-[12px] font-medium hover:opacity-90 disabled:opacity-50"
        >
          {running ? "Running…" : "Re-run pipeline"}
        </button>
      </div>
    </header>
  );
}

function formatRelative(iso: string): string {
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return iso;
  const diff = Date.now() - t;
  const m = Math.floor(diff / 60000);
  if (m < 1) return "just now";
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  const d = Math.floor(h / 24);
  return `${d}d ago`;
}
