"use client";

import { useEffect, useMemo, useState } from "react";
import {
  api,
  type ConnectorType,
  type SourceSummary,
  type SyncEvent,
} from "@/lib/api";
import { useToast } from "@/components/toast";
import { ConnectorWizard } from "@/components/connector-wizard";

const STATUS_STYLES: Record<
  string,
  { label: string; chip: string; bar: string }
> = {
  syncing: {
    label: "Syncing",
    chip: "bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-950/40 dark:text-blue-400 dark:border-blue-900",
    bar: "bg-blue-500",
  },
  healthy: {
    label: "Healthy",
    chip: "bg-green-50 text-green-700 border-green-200 dark:bg-green-950/40 dark:text-green-400 dark:border-green-900",
    bar: "bg-green-500",
  },
  warning: {
    label: "Attention",
    chip: "bg-amber-50 text-amber-700 border-amber-200 dark:bg-amber-950/40 dark:text-amber-400 dark:border-amber-900",
    bar: "bg-amber-500",
  },
  error: {
    label: "Failed",
    chip: "bg-red-50 text-red-700 border-red-200 dark:bg-red-950/40 dark:text-red-400 dark:border-red-900",
    bar: "bg-red-500",
  },
  idle: {
    label: "Idle",
    chip: "bg-background text-muted border-border",
    bar: "bg-muted",
  },
};

const FILTER_ORDER: ("all" | "syncing" | "healthy" | "warning" | "error" | "idle")[] = [
  "all",
  "syncing",
  "healthy",
  "warning",
  "error",
  "idle",
];

const FILTER_LABEL: Record<string, string> = {
  all: "All",
  syncing: "Syncing",
  healthy: "Healthy",
  warning: "Attention",
  error: "Errors",
  idle: "Idle",
};

export default function SourcesPage() {
  const { toast } = useToast();
  const [sources, setSources] = useState<SourceSummary[]>([]);
  const [events, setEvents] = useState<SyncEvent[]>([]);
  const [connectors, setConnectors] = useState<ConnectorType[]>([]);
  const [filter, setFilter] = useState<(typeof FILTER_ORDER)[number]>("all");
  const [selected, setSelected] = useState<string | null>(null);
  const [showAdd, setShowAdd] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [syncing, setSyncing] = useState<Set<string>>(new Set());

  const refresh = () => {
    api
      .sources()
      .then((r) => setSources(r.sources))
      .catch((e: Error) => setError(e.message));
    api
      .syncEvents(20)
      .then((r) => setEvents(r.events))
      .catch(() => {});
  };

  useEffect(() => {
    refresh();
    api
      .connectorCatalog()
      .then((r) => setConnectors(r.connectors))
      .catch(() => {});
  }, []);

  const counts = useMemo(() => {
    const c: Record<string, number> = { all: sources.length };
    for (const s of sources) c[s.status] = (c[s.status] ?? 0) + 1;
    return c;
  }, [sources]);

  const visible = filter === "all" ? sources : sources.filter((s) => s.status === filter);
  const totalTables = sources.reduce((a, s) => a + s.tables, 0);
  const connectorById = (id: string) => connectors.find((c) => c.id === id);

  const sync = async (name: string) => {
    setSyncing((current) => new Set(current).add(name));
    setSources((current) =>
      current.map((source) =>
        source.name === name ? { ...source, status: "syncing" } : source
      )
    );
    try {
      const result = await api.syncSource(name);
      toast(
        result.quality_failed
          ? `Synced ${name}; ${result.quality_failed} quality issue(s) need review`
          : `Synced ${name}`,
        result.quality_failed ? "info" : "success"
      );
      refresh();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      toast(`Sync failed: ${msg}`, "error");
      refresh();
    } finally {
      setSyncing((current) => {
        const next = new Set(current);
        next.delete(name);
        return next;
      });
    }
  };

  const remove = async (name: string) => {
    if (!confirm(`Disconnect source "${name}"? This removes the source registration but does NOT touch the source database.`)) return;
    try {
      await api.deleteSource(name);
      toast(`Disconnected ${name}`, "success");
      refresh();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      toast(`Failed: ${msg}`, "error");
    }
  };

  return (
    <div className="max-w-5xl">
      <div className="flex items-start justify-between mb-6 gap-4">
        <div>
          <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-1">
            Catalog
          </div>
          <h1 className="text-2xl font-bold tracking-tight">
            Connected data sources
          </h1>
          <p className="text-sm text-muted mt-1">
            {sources.length} source{sources.length !== 1 ? "s" : ""} ·{" "}
            {totalTables} tables · syncing continuously
          </p>
        </div>
        <button
          onClick={() => setShowAdd(true)}
          className="px-4 py-2 bg-accent text-white rounded-md text-sm font-medium hover:opacity-90"
        >
          + Connect source
        </button>
      </div>

      <div className="flex gap-2 mb-5 px-3 py-2.5 bg-card border border-border rounded-lg items-center">
        {FILTER_ORDER.map((k) => {
          const active = filter === k;
          return (
            <button
              key={k}
              onClick={() => setFilter(k)}
              className={`flex items-center gap-1.5 px-3 py-1 rounded-md text-xs transition-colors ${
                active
                  ? "bg-accent/10 text-accent font-semibold"
                  : "text-muted hover:bg-background"
              }`}
            >
              {FILTER_LABEL[k]}
              <span
                className={`text-[10px] font-bold px-1.5 py-0.5 rounded-full ${
                  active
                    ? "bg-accent text-white"
                    : "bg-background text-muted"
                }`}
              >
                {counts[k] ?? 0}
              </span>
            </button>
          );
        })}
        <span className="ml-auto text-[11px] text-muted">
          Auto-sync where enabled
        </span>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900 rounded-lg text-sm text-red-700 dark:text-red-400">
          Failed to load sources: {error}
        </div>
      )}

      {sources.length === 0 && !error ? (
        <div className="bg-card border border-border rounded-xl p-10 text-center">
          <h2 className="text-lg font-semibold mb-1">No sources yet</h2>
          <p className="text-sm text-muted mb-4">
            Connect a database, warehouse, or file directory to start the
            advisory pipeline.
          </p>
          <button
            onClick={() => setShowAdd(true)}
            className="px-4 py-2 bg-accent text-white rounded-md text-sm font-medium"
          >
            + Connect your first source
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-6">
          {visible.map((src) => {
            const isSel = selected === src.name;
            const isSyncing = syncing.has(src.name) || src.status === "syncing";
            const status = STATUS_STYLES[isSyncing ? "syncing" : src.status] ?? STATUS_STYLES.idle;
            const conn = connectorById(src.type);
            return (
              <div
                key={src.name}
                onClick={() => setSelected(isSel ? null : src.name)}
                className={`p-4 bg-card border rounded-xl cursor-pointer transition-all ${
                  isSel
                    ? "border-accent ring-2 ring-accent/15"
                    : "border-border hover:border-accent/40"
                }`}
              >
                <div className="flex items-start justify-between mb-3">
                  <div className="flex gap-3 items-center min-w-0">
                    {conn && (
                      <div
                        className="w-10 h-10 rounded-lg flex items-center justify-center font-mono font-bold shrink-0"
                        style={{
                          background: conn.color,
                          color: conn.lightGlyph ? "#0f172a" : "#ffffff",
                          fontSize: conn.glyph.length > 1 ? 12 : 15,
                        }}
                      >
                        {conn.glyph}
                      </div>
                    )}
                    <div className="min-w-0">
                      <div className="text-sm font-mono font-semibold truncate">
                        {src.display_name || src.name}
                      </div>
                      <div className="text-[11px] text-muted truncate">
                        {conn?.name ?? src.type}
                        {src.host ? ` · ${src.host}` : ""}
                      </div>
                    </div>
                  </div>
                  <span
                    className={`text-[10px] font-bold px-2 py-1 rounded-md uppercase tracking-wider border shrink-0 ${status.chip}`}
                  >
                    {status.label}
                  </span>
                </div>

                <div className="grid grid-cols-4 gap-2 mb-3">
                  <Cell label="Schemas" value={src.schemas} />
                  <Cell label="Tables" value={src.tables} />
                  <Cell
                    label="Rows"
                    value={
                      src.rows >= 1000
                        ? `${(src.rows / 1000).toFixed(1)}K`
                        : src.rows
                    }
                  />
                  <Cell
                    label="Last sync"
                    value={src.last_sync_at ? formatRel(src.last_sync_at) : "—"}
                  />
                </div>

                <div className="grid grid-cols-3 gap-2 mb-3">
                  <Cell
                    label="Quality"
                    value={
                      src.quality_score === null
                        ? "—"
                        : `${Math.round(src.quality_score)}%`
                    }
                  />
                  <Cell label="Issues" value={src.quality_failed} />
                  <Cell
                    label="Duration"
                    value={
                      src.latest_run_duration_ms === null
                        ? "—"
                        : formatDuration(src.latest_run_duration_ms)
                    }
                  />
                </div>

                <div className="flex items-center gap-2.5">
                  <div className="flex-1 h-1 bg-border rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full ${status.bar}`}
                      style={{ width: `${src.health ?? 0}%` }}
                    />
                  </div>
                  <span className="text-[10px] font-mono text-muted">
                    {src.health ?? 0}% health
                  </span>
                  {src.drift_count > 0 && (
                    <span className="text-[10px] font-semibold text-amber-700 dark:text-amber-400 bg-amber-50 dark:bg-amber-950/40 px-1.5 py-0.5 rounded">
                      ↗ {src.drift_count} drift
                    </span>
                  )}
                  {src.quality_failed > 0 && (
                    <span className="text-[10px] font-semibold text-red-700 dark:text-red-400 bg-red-50 dark:bg-red-950/40 px-1.5 py-0.5 rounded">
                      ! {src.quality_failed} quality
                    </span>
                  )}
                </div>

                {isSel && (
                  <div className="mt-3 pt-3 border-t border-border">
                    {src.latest_error && (
                      <div className="mb-2 text-[11px] text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900 rounded px-2 py-1">
                        {src.latest_error}
                      </div>
                    )}
                    <div className="flex flex-wrap gap-2">
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        sync(src.name);
                      }}
                      disabled={isSyncing}
                      className="px-2.5 py-1 bg-accent text-white rounded text-[11px] font-medium disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      {isSyncing ? "Syncing…" : "Sync now"}
                    </button>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        remove(src.name);
                      }}
                      className="px-2.5 py-1 bg-card border border-red-200 dark:border-red-900 text-red-600 dark:text-red-400 rounded text-[11px] font-medium"
                    >
                      Disconnect
                    </button>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {events.length > 0 && (
        <div className="bg-card border border-border rounded-xl p-5">
          <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-3">
            Connection activity
          </div>
          <div className="divide-y divide-border">
            {events.map((e, i) => {
              const tone =
                e.severity === "error"
                  ? "bg-red-500"
                  : e.severity === "warning"
                  ? "bg-amber-500"
                  : "bg-accent";
              return (
                <div
                  key={i}
                  className="flex gap-3 py-2.5 first:pt-0 last:pb-0 items-start"
                >
                  <div
                    className={`w-1.5 h-1.5 rounded-full mt-1.5 shrink-0 ${tone}`}
                  />
                  <div className="flex-1 min-w-0">
                    <div className="text-[12px] truncate">{e.detail}</div>
                    <div className="text-[10px] text-muted font-mono mt-0.5">
                      {e.source_name}
                    </div>
                  </div>
                  <div className="text-[11px] text-muted shrink-0">
                    {formatRel(e.created_at)}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      <ConnectorWizard
        open={showAdd}
        onClose={() => setShowAdd(false)}
        onCreated={refresh}
      />
    </div>
  );
}

function Cell({ label, value }: { label: string; value: number | string }) {
  return (
    <div>
      <div className="text-[9px] font-bold text-muted uppercase tracking-wider">
        {label}
      </div>
      <div className="text-[12px] font-medium font-mono mt-0.5">{value}</div>
    </div>
  );
}

function formatRel(iso: string): string {
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

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}
