"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api, type BriefingResponse } from "@/lib/api";

const URGENCY_STYLES: Record<string, { dot: string; chip: string; ring: string }> = {
  high: {
    dot: "text-red-600 dark:text-red-400",
    chip: "bg-red-50 text-red-600 dark:bg-red-950/40 dark:text-red-400",
    ring: "border-red-200 dark:border-red-900",
  },
  medium: {
    dot: "text-amber-600 dark:text-amber-400",
    chip: "bg-amber-50 text-amber-600 dark:bg-amber-950/40 dark:text-amber-400",
    ring: "border-amber-200 dark:border-amber-900",
  },
  low: {
    dot: "text-muted",
    chip: "bg-background text-muted border border-border",
    ring: "border-border",
  },
};

export default function BriefingPage() {
  const [briefing, setBriefing] = useState<BriefingResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api
      .briefingToday()
      .then(setBriefing)
      .catch((e: Error) => setError(e.message));
  }, []);

  const today = new Date().toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
  });

  if (error) {
    return (
      <div className="max-w-2xl">
        <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-2">
          {today} · Continuous briefing
        </div>
        <h1 className="text-2xl font-bold mb-3">No briefing yet</h1>
        <p className="text-sm text-muted mb-4">
          Add a source on the{" "}
          <Link href="/sources" className="text-accent hover:underline">
            Sources page
          </Link>{" "}
          and run the pipeline. Once Headwater has data to watch, your
          continuous briefing will appear here.
        </p>
        <p className="text-xs text-muted">Detail: {error}</p>
      </div>
    );
  }

  if (!briefing) {
    return (
      <div className="max-w-2xl">
        <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-2">
          {today} · Continuous briefing
        </div>
        <div className="h-8 w-2/3 bg-card border border-border rounded animate-pulse mb-3" />
        <div className="h-5 w-1/2 bg-card border border-border rounded animate-pulse" />
      </div>
    );
  }

  const high = briefing.summary.attention_count;
  const wait = briefing.summary.wait_count;
  const noData = briefing.summary.no_data;

  if (noData) {
    return (
      <div className="max-w-2xl">
        <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-2">
          {today} · Continuous briefing
        </div>
        <h1 className="text-2xl font-bold mb-3">
          Welcome to Headwater
        </h1>
        <p className="text-sm text-muted leading-relaxed mb-5">
          Connect your first data source to start the advisory pipeline.
          Once Headwater has data to watch, this page becomes your daily
          briefing — priorities worth your attention and wins worth
          celebrating.
        </p>
        <Link
          href="/sources"
          className="inline-block px-4 py-2 bg-accent text-white rounded-md text-sm font-medium"
        >
          + Connect a source →
        </Link>
      </div>
    );
  }

  return (
    <div className="max-w-3xl">
      <header className="mb-8 pb-6 border-b border-border">
        <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-2">
          {today} · Continuous briefing
        </div>
        <h1 className="text-3xl font-bold tracking-tight leading-tight mb-3">
          {briefing.summary.all_clear ? (
            <>
              All clear. Your data is healthy
              <br />
              and improving steadily.
            </>
          ) : (
            <>
              You have{" "}
              <span className="text-red-600 dark:text-red-400">
                {high} thing{high !== 1 ? "s" : ""}
              </span>{" "}
              worth your attention
              <br />
              and{" "}
              <span className="text-muted">{wait} that can wait</span>.
            </>
          )}
        </h1>
        <p className="text-sm text-muted leading-relaxed max-w-xl">
          Headwater watched{" "}
          <strong className="text-foreground font-semibold">
            {briefing.stats.sources} source
            {briefing.stats.sources !== 1 ? "s" : ""}
          </strong>
          , tracked{" "}
          <strong className="text-foreground font-semibold">
            {briefing.stats.tables} tables
          </strong>
          , and ran{" "}
          <strong className="text-foreground font-semibold">
            {briefing.stats.quality_checks} quality checks
          </strong>
          . Everything below — health, models, insights — reflects the latest
          state.
        </p>
      </header>

      <section className="mb-10">
        {briefing.priorities.length === 0 ? (
          <p className="text-sm text-muted py-8 text-center">
            No priorities right now. Keep shipping.
          </p>
        ) : (
          briefing.priorities.map((p, i) => {
            const styles = URGENCY_STYLES[p.urgency] ?? URGENCY_STYLES.low;
            const target = p.deeplink ?? p.route;
            return (
              <div
                key={i}
                className={`flex gap-5 py-5 ${
                  i < briefing.priorities.length - 1
                    ? "border-b border-border"
                    : ""
                }`}
              >
                <div className="w-12 shrink-0">
                  <div
                    className={`w-9 h-9 rounded-full font-mono font-bold text-sm flex items-center justify-center ${styles.chip}`}
                  >
                    {i + 1}
                  </div>
                  <div
                    className={`text-[9px] font-bold uppercase tracking-wider mt-1.5 text-center ${styles.dot}`}
                  >
                    {p.urgency}
                  </div>
                </div>
                <div className="flex-1 min-w-0">
                  <h3 className="text-base font-semibold mb-1.5 leading-snug">
                    {p.headline}
                  </h3>
                  <p className="text-[13px] text-muted leading-relaxed mb-3">
                    {p.detail}
                  </p>
                  <Link
                    href={target}
                    className="inline-block px-3 py-1 bg-card border border-border rounded-md text-xs font-medium hover:bg-background"
                  >
                    {p.action} →
                  </Link>
                </div>
              </div>
            );
          })
        )}
      </section>

      {briefing.wins.length > 0 && (
        <section className="bg-green-50 dark:bg-green-950/30 border border-green-200 dark:border-green-900 rounded-xl px-5 py-4 mb-6">
          <div className="flex items-center gap-2 mb-3">
            <span className="w-5 h-5 rounded-full bg-green-600 text-white flex items-center justify-center text-[11px] font-bold">
              ✓
            </span>
            <span className="text-[13px] font-bold text-green-800 dark:text-green-300">
              What&apos;s going well
            </span>
          </div>
          <ul className="pl-7 space-y-2">
            {briefing.wins.map((w, i) => (
              <li
                key={i}
                className="text-[13px] text-green-800 dark:text-green-300 leading-relaxed"
              >
                · {w}
              </li>
            ))}
          </ul>
        </section>
      )}

      <footer className="grid grid-cols-4 border-t border-border pt-4">
        <Stat value={String(briefing.stats.sources)} label="sources" />
        <Stat value={String(briefing.stats.tables)} label="tables" />
        <Stat
          value={String(briefing.stats.quality_checks)}
          label="quality checks"
        />
        <Stat value={`${briefing.stats.health_pct}%`} label="health" />
      </footer>
    </div>
  );
}

function Stat({ value, label }: { value: string; label: string }) {
  return (
    <div className="px-4 border-r border-border last:border-r-0">
      <div className="text-2xl font-bold tracking-tight">{value}</div>
      <div className="text-[11px] text-muted mt-0.5">{label}</div>
    </div>
  );
}
