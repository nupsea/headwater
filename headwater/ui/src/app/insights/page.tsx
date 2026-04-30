"use client";

import { useEffect, useState } from "react";
import {
  api,
  type DataInsight,
  type InsightsResponse,
} from "@/lib/api";

const SEVERITY_STYLE: Record<
  DataInsight["severity"],
  { bg: string; fg: string; border: string; accent: string }
> = {
  critical: {
    bg: "bg-card",
    fg: "text-foreground",
    border: "border-border",
    accent: "#D55E00",
  },
  warning: {
    bg: "bg-card",
    fg: "text-foreground",
    border: "border-border",
    accent: "#E69F00",
  },
  info: {
    bg: "bg-card",
    fg: "text-foreground",
    border: "border-border",
    accent: "#0072B2",
  },
};

const CHART_COLORS = ["#0072B2", "#E69F00", "#009E73", "#D55E00", "#CC79A7", "#56B4E9"];

function formatPoint(value: number) {
  return Number.isInteger(value) ? value.toLocaleString() : value.toFixed(1);
}

function LineChart({ insight }: { insight: DataInsight }) {
  const values = insight.chart.map((d) => d.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(max - min, 1);
  const points = insight.chart.map((point, index) => {
    const x = insight.chart.length === 1 ? 50 : (index / (insight.chart.length - 1)) * 100;
    const y = 86 - ((point.value - min) / span) * 70;
    return { ...point, x, y };
  });
  const polyline = points.map((p) => `${p.x},${p.y}`).join(" ");
  const peak = points.reduce((best, point) => (point.value > best.value ? point : best), points[0]);
  const style = SEVERITY_STYLE[insight.severity];

  return (
    <div>
      <svg viewBox="0 0 100 100" className="h-44 w-full overflow-visible">
        <line
          x1="0"
          y1="86"
          x2="100"
          y2="86"
          stroke="currentColor"
          strokeWidth="1"
          className="text-border"
        />
        <polyline
          points={polyline}
          fill="none"
          stroke={style.accent}
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="3"
        />
        {points.map((point) => (
          <circle
            key={point.label}
            cx={point.x}
            cy={point.y}
            r={point.label === peak.label ? 3.4 : 2.2}
            fill={point.label === peak.label ? style.accent : "var(--card)"}
            stroke={style.accent}
            strokeWidth="1.8"
          />
        ))}
      </svg>
      <div className="mt-1 flex justify-between gap-2 text-[10px] text-muted">
        <span className="truncate">{insight.chart[0]?.label}</span>
        <span className="font-mono">{peak.label}: {formatPoint(peak.value)}</span>
        <span className="truncate text-right">{insight.chart.at(-1)?.label}</span>
      </div>
    </div>
  );
}

function PieChart({ insight }: { insight: DataInsight }) {
  const total = insight.chart.reduce((sum, point) => sum + point.value, 0);
  const slices = insight.chart.slice(0, 6).reduce(
    (acc, point, index) => {
      const start = acc.offset;
      const pct = total > 0 ? (point.value / total) * 100 : 0;
      const end = start + pct;
      return {
        offset: end,
        stops: [...acc.stops, `${CHART_COLORS[index]} ${start}% ${end}%`],
      };
    },
    { offset: 0, stops: [] as string[] }
  );

  return (
    <div className="grid grid-cols-[120px_1fr] items-center gap-5">
      <div
        className="h-[120px] w-[120px] rounded-full border border-border"
        style={{ background: `conic-gradient(${slices.stops.join(", ")})` }}
      />
      <div className="space-y-2">
        {insight.chart.slice(0, 6).map((point, index) => (
          <div key={point.label} className="flex items-center justify-between gap-3 text-xs">
            <span className="flex min-w-0 items-center gap-2">
              <span
                className="h-2.5 w-2.5 shrink-0 rounded-sm"
                style={{ backgroundColor: CHART_COLORS[index] }}
              />
              <span className="truncate">{point.label}</span>
            </span>
            <span className="font-mono text-muted">
              {total > 0 ? `${((point.value / total) * 100).toFixed(0)}%` : "0%"}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function BarChart({ insight }: { insight: DataInsight }) {
  const max = Math.max(...insight.chart.map((d) => d.value), 1);

  return (
    <div className="space-y-2">
      {insight.chart.slice(0, 6).map((point) => {
        const width = Math.max(4, (point.value / max) * 100);
        return (
          <div
            key={point.label}
            className="grid grid-cols-[150px_1fr_64px] items-center gap-3"
          >
            <div className="truncate text-xs font-mono text-muted" title={point.label}>
              {point.label}
            </div>
            <div className="h-5 overflow-hidden rounded bg-border">
              <div
                className="h-full rounded"
                style={{ width: `${width}%`, backgroundColor: CHART_COLORS[0] }}
              />
            </div>
            <div className="text-right text-xs font-mono text-muted">
              {formatPoint(point.value)}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function InsightChart({ insight }: { insight: DataInsight }) {
  if (insight.chart_type === "line") {
    return <LineChart insight={insight} />;
  }
  if (insight.chart_type === "pie") {
    return <PieChart insight={insight} />;
  }

  return <BarChart insight={insight} />;
}

function InsightCard({ insight, rank }: { insight: DataInsight; rank: number }) {
  const style = SEVERITY_STYLE[insight.severity];

  return (
    <article className={`rounded-lg border ${style.border} ${style.bg} p-4 shadow-sm`}>
      <div className="mb-3 flex items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="mb-1 flex items-center gap-2">
            <span className="text-[10px] font-bold uppercase tracking-wide text-muted">
              Insight {rank}
            </span>
            <span className="rounded border border-border bg-card px-1.5 py-0.5 text-[10px] uppercase text-muted">
              {insight.category}
            </span>
          </div>
          <h2 className="text-lg font-semibold leading-snug text-foreground">
            {insight.title}
          </h2>
        </div>
        <div className="shrink-0 rounded-md border border-border bg-background px-2.5 py-1 text-right">
          <div className="text-lg font-bold leading-none text-foreground">
            {insight.value.toLocaleString()}
            <span className="text-xs">{insight.unit}</span>
          </div>
          <div className="mt-0.5 text-[10px] uppercase text-muted">
            {insight.metric.replaceAll("_", " ")}
          </div>
        </div>
      </div>

      <p className="mb-4 text-sm leading-6 text-muted">{insight.detail}</p>
      <InsightChart insight={insight} />
      <div className="mt-4 flex flex-wrap items-center gap-2 text-xs text-muted">
        <span className="font-mono">{insight.table}</span>
        {insight.column && <span className="font-mono">{insight.column}</span>}
      </div>
    </article>
  );
}

function SummaryTile({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="text-[10px] font-semibold uppercase tracking-wide text-muted">
        {label}
      </div>
      <div className="mt-2 text-2xl font-bold text-foreground">{value}</div>
      <div className="mt-1 text-xs text-muted">{detail}</div>
    </div>
  );
}

export default function InsightsPage() {
  const [insights, setInsights] = useState<InsightsResponse | null>(null);
  const [error, setError] = useState("");
  const [moreGenerated, setMoreGenerated] = useState(false);

  useEffect(() => {
    api
      .insights()
      .then(setInsights)
      .catch(() => setError("Run the pipeline from the Dashboard first."));
  }, []);

  if (error) {
    return (
      <div>
        <h1 className="mb-4 text-2xl font-bold">Insights</h1>
        <div className="mx-auto max-w-xl rounded-lg border border-border bg-card p-8 text-center">
          <h2 className="mb-2 text-lg font-semibold">No Data Yet</h2>
          <p className="text-sm text-muted">
            Run the pipeline to generate statistical insights from profiles,
            relationships, and quality signals.
          </p>
        </div>
      </div>
    );
  }

  if (!insights) {
    return (
      <div>
        <h1 className="mb-4 text-2xl font-bold">Insights</h1>
        <div className="space-y-4">
          <div className="h-44 animate-pulse rounded-lg border border-border bg-card" />
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            <div className="h-64 animate-pulse rounded-lg border border-border bg-card" />
            <div className="h-64 animate-pulse rounded-lg border border-border bg-card" />
          </div>
        </div>
      </div>
    );
  }

  const overview = insights.overview;
  const profile = insights.data_profile;
  const topInsights = insights.top_insights || [];
  const visibleInsights = moreGenerated ? topInsights.slice(0, 10) : topInsights.slice(0, 5);
  const strongest = visibleInsights[0];
  const secondaryInsights = visibleInsights.slice(1);
  const canGenerateMore = !moreGenerated && topInsights.length > 5;
  const relationshipIntegrity = profile.fk_integrity.avg_integrity_pct;
  const pkPct = Math.round(
    (profile.pk_coverage.tables_with_pk /
      Math.max(1, profile.pk_coverage.total_tables)) *
      100
  );

  return (
    <div className="max-w-[1500px]">
      <header className="mb-6">
        <div className="mb-1 text-[10px] font-bold uppercase tracking-wide text-muted">
          Did You Know
        </div>
        <h1 className="text-2xl font-bold text-foreground">
          Business signals worth a second look
        </h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-muted">
          Outcome-oriented observations from actual values across{" "}
          {overview.total_rows.toLocaleString()} rows: temporal peaks, segment concentration,
          and measurable drivers.
        </p>
      </header>

      {strongest && (
        <section className="mb-5 rounded-lg border border-border bg-card p-5">
          <div className="grid gap-5 lg:grid-cols-[minmax(0,1.15fr)_minmax(420px,0.85fr)]">
            <div>
              <div className="mb-2 text-[10px] font-bold uppercase tracking-wide text-muted">
                Strongest Signal
              </div>
              <h2 className="text-xl font-semibold leading-snug text-foreground">
                {strongest.title}
              </h2>
              <p className="mt-2 text-sm leading-6 text-muted">{strongest.detail}</p>
              <div className="mt-4 grid grid-cols-2 gap-3 md:grid-cols-4">
                <SummaryTile
                  label="Tables"
                  value={overview.total_tables.toLocaleString()}
                  detail={`${overview.total_columns} profiled columns`}
                />
                <SummaryTile
                  label="Completeness"
                  value={`${profile.completeness_pct}%`}
                  detail={`${profile.high_null_columns} high-null columns`}
                />
                <SummaryTile
                  label="PK Coverage"
                  value={`${pkPct}%`}
                  detail={`${profile.pk_coverage.tables_with_pk}/${profile.pk_coverage.total_tables} tables`}
                />
                <SummaryTile
                  label="FK Integrity"
                  value={relationshipIntegrity === null ? "n/a" : `${relationshipIntegrity}%`}
                  detail={`${profile.fk_integrity.total_relationships} relationships`}
                />
              </div>
            </div>
            <div className="rounded-lg border border-border bg-background p-4">
              <InsightChart insight={strongest} />
            </div>
          </div>
        </section>
      )}

      <section className="mb-6 grid grid-cols-1 gap-4 xl:grid-cols-2">
        {secondaryInsights.map((insight, index) => (
          <InsightCard key={insight.id} insight={insight} rank={index + 2} />
        ))}
      </section>

      {canGenerateMore && (
        <div className="mb-6 flex justify-center">
          <button
            type="button"
            onClick={() => setMoreGenerated(true)}
            className="rounded-md border border-border bg-card px-4 py-2 text-sm font-medium text-foreground hover:bg-background"
          >
            Generate more insights
          </button>
        </div>
      )}

      {topInsights.length === 0 && (
        <div className="rounded-lg border border-border bg-card p-8 text-sm text-muted">
          No statistical insights were generated yet. Run profiling against a source with
          row counts and column profiles.
        </div>
      )}

      <section className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <SummaryTile
          label="Quality Contracts"
          value={
            profile.quality.pass_rate_pct === null
              ? "n/a"
              : `${profile.quality.pass_rate_pct}%`
          }
          detail={`${profile.quality.passed}/${profile.quality.total} passing`}
        />
        <SummaryTile
          label="Patterns Found"
          value={insights.patterns_found.length.toLocaleString()}
          detail="Detected formats such as dates, ids, email, or URLs"
        />
        <SummaryTile
          label="Column Issues"
          value={insights.column_issues.length.toLocaleString()}
          detail="Null, constant, uniqueness, and empty-column signals"
        />
      </section>
    </div>
  );
}
