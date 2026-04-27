"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  api,
  type StatusResponse,
  type InsightsResponse,
  type PipelineRunResponse,
  type DriftReport,
  type Project,
  type ProjectProgress,
  type ActivityEntry,
  type ConnectionTestResult,
} from "@/lib/api";
import { useToast } from "@/components/toast";
import { PipelineStepper } from "@/components/pipeline-stepper";
import { ReviewQueue } from "@/components/review-queue";
import { ActivityFeed } from "@/components/activity-feed";
import { ProjectSummary } from "@/components/project-summary";
import { AdvisoryActions } from "@/components/advisory-actions";
import { DomainMap } from "@/components/domain-map";
import { RelationshipDiagram } from "@/components/relationship-diagram";
import { DriftBanner } from "@/components/drift-banner";
import { ConfidenceDot } from "@/components/confidence-dot";

export default function HealthPage() {
  const { toast } = useToast();
  const [, setStatus] = useState<StatusResponse | null>(null);
  const [insights, setInsights] = useState<InsightsResponse | null>(null);
  const [driftReport, setDriftReport] = useState<DriftReport | null>(null);
  const [project, setProject] = useState<Project | null>(null);
  const [progress, setProgress] = useState<ProjectProgress | null>(null);
  const [activities, setActivities] = useState<ActivityEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [phase, setPhase] = useState("");
  const [sourcePath, setSourcePath] = useState(
    "postgresql://headwater:headwater@localhost:5434/headwater_dev"
  );
  const [error, setError] = useState("");
  const [connStatus, setConnStatus] = useState<ConnectionTestResult | null>(null);
  const [connTesting, setConnTesting] = useState(false);

  const isDbUri = (s: string) =>
    ["postgresql://", "postgres://", "mysql://", "sqlite://"].some((p) =>
      s.startsWith(p)
    );

  const testConnection = async (source?: string) => {
    const uri = source ?? sourcePath;
    if (!isDbUri(uri)) {
      setConnStatus(null);
      return;
    }
    setConnTesting(true);
    try {
      const result = await api.testConnection(uri);
      setConnStatus(result);
    } catch {
      setConnStatus({
        status: "error",
        source_type: "unknown",
        tables: 0,
        table_names: [],
        detail: "Backend unreachable. Is the Headwater server running?",
      });
    }
    setConnTesting(false);
  };

  const refresh = async () => {
    try {
      const s = await api.status();
      setStatus(s);
      if (s.discovered) {
        const ins = await api.insights();
        setInsights(ins);
        // Fetch project info
        try {
          const projRes = await api.projects();
          if (projRes.projects && projRes.projects.length > 0) {
            const proj = projRes.projects[0];
            setProject(proj);
            try {
              const progRes = await api.projectProgress(proj.id);
              setProgress(progRes.progress);
            } catch {
              /* progress endpoint may not return data yet */
            }
          }
        } catch {
          /* projects endpoint may not be available */
        }
        // Fetch activity feed
        try {
          const actRes = await api.activity(10);
          setActivities(actRes.activities || []);
        } catch {
          /* activity endpoint may not be available */
        }
        // Fetch latest drift report
        try {
          const dr = await api.driftLatest();
          if ("id" in dr && !dr.acknowledged) {
            setDriftReport(dr as DriftReport);
          } else {
            setDriftReport(null);
          }
        } catch {
          /* drift endpoint may not be available yet */
        }
      }
    } catch {
      /* server not ready */
    }
  };

  useEffect(() => {
    refresh();
    testConnection();
  }, []);

  const runFullPipeline = async () => {
    setLoading(true);
    setError("");
    setPhase("Discovering, profiling, modeling, and validating...");
    try {
      const result: PipelineRunResponse = await api.pipelineRun(sourcePath);
      const summary =
        `Done: ${result.tables_discovered} tables, ` +
        `${result.profiles} profiles, ${result.relationships} relationships, ` +
        `${result.quality_passed}/${result.quality_total} quality checks passed.`;
      setPhase(summary);
      toast("Pipeline complete", "success");
      await refresh();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
      toast(`Pipeline failed: ${msg}`, "error");
      setPhase("");
    }
    setLoading(false);
  };

  // Derive attention-needed items from insights
  const attentionItems: {
    type: string;
    label: string;
    detail: string;
    link: string;
    severity: "critical" | "warning" | "info";
  }[] = [];

  if (insights) {
    // Proposed mart models
    const proposedMarts =
      insights.model_suggestions?.length ?? 0;
    // Check from workflow if models phase has pending items
    const modelsPhase = insights.workflow?.phases?.find(
      (p) => p.key === "models" || p.key === "model"
    );
    if (modelsPhase && modelsPhase.status === "active") {
      attentionItems.push({
        type: "models",
        label: "Mart models awaiting review",
        detail: modelsPhase.detail || "Review proposed analytical models before execution",
        link: "/models",
        severity: "critical",
      });
    }

    // Low-confidence columns
    const lowConfCols = insights.column_issues?.filter(
      (ci) =>
        ci.issues.some(
          (iss) => iss.severity === "warning" || iss.severity === "error"
        )
    );
    if (lowConfCols && lowConfCols.length > 0) {
      attentionItems.push({
        type: "dictionary",
        label: `${lowConfCols.length} column${lowConfCols.length > 1 ? "s" : ""} need review`,
        detail: `${lowConfCols
          .slice(0, 3)
          .map((c) => `${c.table}.${c.column}`)
          .join(", ")}${lowConfCols.length > 3 ? "..." : ""}`,
        link: "/dictionary",
        severity: "warning",
      });
    }

    // Weak relationships
    const weakRels = insights.relationship_map?.filter(
      (r) => r.integrity < 0.5
    );
    if (weakRels && weakRels.length > 0) {
      attentionItems.push({
        type: "relationships",
        label: `${weakRels.length} weak relationship${weakRels.length > 1 ? "s" : ""}`,
        detail: weakRels
          .slice(0, 2)
          .map(
            (r) =>
              `${r.from_table}.${r.from_column} (${(r.integrity * 100).toFixed(0)}% integrity)`
          )
          .join(", "),
        link: "/models",
        severity: "warning",
      });
    }

    // High-null columns
    const highNulls = insights.null_analysis?.filter(
      (n) => n.null_rate > 0.3
    );
    if (highNulls && highNulls.length > 0) {
      attentionItems.push({
        type: "quality",
        label: `${highNulls.length} column${highNulls.length > 1 ? "s" : ""} with >30% nulls`,
        detail: highNulls
          .slice(0, 3)
          .map(
            (n) =>
              `${n.table}.${n.column} (${(n.null_rate * 100).toFixed(0)}%)`
          )
          .join(", "),
        link: "/quality",
        severity: "info",
      });
    }
  }

  const SEVERITY_STYLES = {
    critical:
      "border-l-[var(--danger)] bg-[color-mix(in_srgb,var(--danger)_10%,var(--card))]",
    warning:
      "border-l-[var(--warning)] bg-[color-mix(in_srgb,var(--warning)_10%,var(--card))]",
    info: "border-l-[var(--accent)] bg-[color-mix(in_srgb,var(--accent)_10%,var(--card))]",
  };

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <div className="text-[10px] font-bold uppercase tracking-wider text-muted">
            Today
          </div>
          <h1 className="text-2xl font-bold">Project Health</h1>
          <p className="text-sm text-muted mt-0.5">
            Live state of your discovery, modeling, and quality pipeline.
          </p>
        </div>
        {insights && (
          <div className="flex items-center gap-3">
            {/* Inline connection indicator */}
            {connStatus && isDbUri(sourcePath) && (
              <div
                className={`w-2 h-2 rounded-full shrink-0 ${
                  connStatus.status === "ok"
                    ? "bg-green-500"
                    : "bg-red-500 animate-pulse"
                }`}
                title={connStatus.detail}
              />
            )}
            <input
              type="text"
              value={sourcePath}
              onChange={(e) => setSourcePath(e.target.value)}
              onBlur={() => testConnection()}
              className="w-56 px-3 py-1.5 border border-border rounded bg-background text-xs font-mono"
            />
            <button
              onClick={runFullPipeline}
              disabled={loading}
              className="px-4 py-1.5 bg-accent text-white rounded text-xs font-medium hover:opacity-90 disabled:opacity-50 whitespace-nowrap"
            >
              {loading ? "Running..." : "Re-run"}
            </button>
          </div>
        )}
      </div>

      {/* Connection status banner */}
      {connStatus && connStatus.status === "error" && (
        <div className="mb-4 p-4 bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-800 rounded-lg">
          <div className="flex items-start gap-3">
            <div className="shrink-0 w-5 h-5 mt-0.5 text-red-600 dark:text-red-400">
              <svg viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.28 7.22a.75.75 0 00-1.06 1.06L8.94 10l-1.72 1.72a.75.75 0 101.06 1.06L10 11.06l1.72 1.72a.75.75 0 101.06-1.06L11.06 10l1.72-1.72a.75.75 0 00-1.06-1.06L10 8.94 8.28 7.22z" clipRule="evenodd" />
              </svg>
            </div>
            <div className="flex-1 min-w-0">
              <h3 className="text-sm font-semibold text-red-800 dark:text-red-300">
                Connection failed
              </h3>
              <p className="text-sm text-red-700 dark:text-red-400 mt-0.5">
                {connStatus.detail}
              </p>
              {isDbUri(sourcePath) && (
                <div className="mt-2 text-xs text-red-600 dark:text-red-400 font-mono bg-red-100 dark:bg-red-900/40 rounded px-2 py-1 inline-block break-all">
                  {sourcePath.replace(/\/\/([^:]+):([^@]+)@/, "//$1:***@")}
                </div>
              )}
            </div>
            <button
              onClick={() => testConnection()}
              disabled={connTesting}
              className="shrink-0 px-3 py-1.5 text-xs font-medium text-red-700 dark:text-red-300 bg-red-100 dark:bg-red-900/40 rounded hover:bg-red-200 dark:hover:bg-red-900/60 disabled:opacity-50"
            >
              {connTesting ? "Testing..." : "Retry"}
            </button>
          </div>
        </div>
      )}

      {connStatus && connStatus.status === "ok" && isDbUri(sourcePath) && !insights && (
        <div className="mb-4 p-3 bg-green-50 dark:bg-green-950/30 border border-green-200 dark:border-green-800 rounded-lg">
          <div className="flex items-center gap-3">
            <div className="shrink-0 w-5 h-5 text-green-600 dark:text-green-400">
              <svg viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.857-9.809a.75.75 0 00-1.214-.882l-3.483 4.79-1.88-1.88a.75.75 0 10-1.06 1.061l2.5 2.5a.75.75 0 001.137-.089l4-5.5z" clipRule="evenodd" />
              </svg>
            </div>
            <div className="text-sm text-green-800 dark:text-green-300">
              <span className="font-medium">Connected</span>
              {connStatus.tables > 0 && (
                <span className="text-green-600 dark:text-green-400">
                  {" "}&middot; {connStatus.tables} table{connStatus.tables !== 1 ? "s" : ""} found
                </span>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Pipeline launcher (first run) */}
      {!insights && (
        <div className="bg-card border border-border rounded-lg p-8 mb-6 text-center max-w-xl mx-auto">
          <h2 className="text-lg font-semibold mb-2">Discover Your Data</h2>
          <p className="text-sm text-muted mb-6">
            Point Headwater at a dataset. In seconds, get a profiled,
            documented, relationship-mapped, quality-baselined analytical
            warehouse -- ready for review.
          </p>
          <div className="flex items-end gap-3 justify-center">
            <div className="text-left">
              <label className="text-xs text-muted block mb-1">
                Data source (path or DSN)
              </label>
              <input
                type="text"
                value={sourcePath}
                onChange={(e) => setSourcePath(e.target.value)}
                onBlur={() => testConnection()}
                className="w-72 px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
            </div>
            <button
              onClick={() => testConnection()}
              disabled={connTesting}
              className="px-3 py-2 border border-border rounded text-sm font-medium hover:bg-muted/20 disabled:opacity-50"
            >
              {connTesting ? "Testing..." : "Test"}
            </button>
            <button
              onClick={runFullPipeline}
              disabled={loading}
              className="px-6 py-2 bg-accent text-white rounded text-sm font-medium hover:opacity-90 disabled:opacity-50"
            >
              {loading ? "Running..." : "Run Full Pipeline"}
            </button>
          </div>

          {/* Connection detail split view for DB sources */}
          {connStatus && isDbUri(sourcePath) && (
            <div className="mt-4 text-left max-w-md mx-auto">
              <div className="text-xs font-semibold text-muted uppercase tracking-wider mb-2">
                Connection Details
              </div>
              <div className="bg-background border border-border rounded-lg divide-y divide-border text-sm">
                {(() => {
                  try {
                    const url = new URL(sourcePath);
                    return (
                      <>
                        <div className="flex justify-between px-3 py-2">
                          <span className="text-muted">Host</span>
                          <span className="font-mono text-xs">{url.hostname || "localhost"}</span>
                        </div>
                        <div className="flex justify-between px-3 py-2">
                          <span className="text-muted">Port</span>
                          <span className="font-mono text-xs">{url.port || "5432"}</span>
                        </div>
                        <div className="flex justify-between px-3 py-2">
                          <span className="text-muted">Database</span>
                          <span className="font-mono text-xs">{url.pathname.replace("/", "") || "-"}</span>
                        </div>
                        <div className="flex justify-between px-3 py-2">
                          <span className="text-muted">User</span>
                          <span className="font-mono text-xs">{url.username || "-"}</span>
                        </div>
                        <div className="flex justify-between px-3 py-2">
                          <span className="text-muted">Status</span>
                          <span className={`font-medium text-xs ${
                            connStatus.status === "ok"
                              ? "text-green-600 dark:text-green-400"
                              : "text-red-600 dark:text-red-400"
                          }`}>
                            {connStatus.status === "ok" ? "Connected" : "Failed"}
                          </span>
                        </div>
                        {connStatus.status === "ok" && connStatus.tables > 0 && (
                          <div className="flex justify-between px-3 py-2">
                            <span className="text-muted">Tables</span>
                            <span className="font-mono text-xs">{connStatus.tables}</span>
                          </div>
                        )}
                      </>
                    );
                  } catch {
                    return (
                      <div className="px-3 py-2 text-muted text-xs">
                        Could not parse connection URI
                      </div>
                    );
                  }
                })()}
              </div>
            </div>
          )}

          {phase && (
            <div className="text-sm mt-4 p-3 bg-background rounded border border-border">
              {phase}
            </div>
          )}
          {error && (
            <div className="text-sm mt-4 p-3 bg-danger/10 rounded border border-danger/30 text-danger">
              {error}
            </div>
          )}
        </div>
      )}

      {/* Full dashboard */}
      {insights && (
        <div className="space-y-5">
          {/* Drift banner (US-403) */}
          {driftReport && (
            <DriftBanner
              report={driftReport}
              onDismiss={() => setDriftReport(null)}
            />
          )}

          {/* 1. Project Summary -- the command center */}
          {project && (
            <ProjectSummary project={project} progress={progress ?? undefined} />
          )}

          {/* 2. Pipeline stepper (v3) */}
          <PipelineStepper phases={insights.workflow.phases} />

          {/* 2b. Review queue (v3) */}
          <ReviewQueue
            dictPending={
              insights.table_health?.filter((t) => !t.pk_columns?.length).length ?? 0
            }
            modelsPending={
              insights.model_suggestions?.length ?? 0
            }
            contractsObserving={
              insights.quality_summary
                ? insights.quality_summary.total - insights.quality_summary.passed
                : 0
            }
          />

          {/* 3. Attention Needed */}
          {attentionItems.length > 0 && (
            <div className="bg-card border border-border rounded-lg p-5">
              <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-3">
                Attention Needed
              </h3>
              <div className="space-y-2">
                {attentionItems.map((item, i) => (
                  <Link
                    key={i}
                    href={item.link}
                    className={`block border-l-4 rounded-r-lg p-3 hover:opacity-80 transition-opacity ${
                      SEVERITY_STYLES[item.severity]
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <div className="text-sm font-medium text-foreground">{item.label}</div>
                      <span className="text-xs text-muted shrink-0 ml-2">
                        Review &rarr;
                      </span>
                    </div>
                    <div className="text-xs text-muted mt-0.5 truncate">
                      {item.detail}
                    </div>
                  </Link>
                ))}
              </div>
            </div>
          )}

          {/* 4. Health Scorecard -- 3 cards */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            {/* Completeness */}
            <div className="bg-card border border-border rounded-lg p-4">
              <div className="flex items-center justify-between mb-2">
                <h4 className="text-xs font-semibold text-muted uppercase tracking-wider">
                  Completeness
                </h4>
                <ConfidenceDot
                  value={insights.data_profile.completeness_pct / 100}
                />
              </div>
              <div className="text-2xl font-bold mb-1">
                {insights.data_profile.completeness_pct.toFixed(1)}%
              </div>
              <div className="h-1.5 bg-border rounded-full overflow-hidden mb-2">
                <div
                  className="h-full bg-green-500 rounded-full"
                  style={{
                    width: `${insights.data_profile.completeness_pct}%`,
                  }}
                />
              </div>
              <div className="text-[10px] text-muted space-y-0.5">
                <div>
                  {insights.data_profile.total_columns_profiled} columns
                  profiled
                </div>
                {insights.data_profile.high_null_columns > 0 && (
                  <div className="text-amber-600">
                    {insights.data_profile.high_null_columns} high-null columns
                  </div>
                )}
              </div>
            </div>

            {/* Data Quality */}
            <div className="bg-card border border-border rounded-lg p-4">
              <div className="flex items-center justify-between mb-2">
                <h4 className="text-xs font-semibold text-muted uppercase tracking-wider">
                  Data Quality
                </h4>
                {insights.quality_summary && (
                  <ConfidenceDot
                    value={insights.quality_summary.pass_rate / 100}
                  />
                )}
              </div>
              {insights.quality_summary ? (
                <>
                  <div className="text-2xl font-bold mb-1">
                    {insights.quality_summary.pass_rate.toFixed(0)}%
                  </div>
                  <div className="h-1.5 bg-border rounded-full overflow-hidden mb-2">
                    <div
                      className={`h-full rounded-full ${
                        insights.quality_summary.pass_rate >= 90
                          ? "bg-green-500"
                          : insights.quality_summary.pass_rate >= 70
                          ? "bg-yellow-500"
                          : "bg-red-500"
                      }`}
                      style={{
                        width: `${insights.quality_summary.pass_rate}%`,
                      }}
                    />
                  </div>
                  <div className="text-[10px] text-muted">
                    {insights.quality_summary.passed}/
                    {insights.quality_summary.total} checks passed
                    {insights.quality_summary.failed > 0 && (
                      <span className="text-amber-600">
                        {" "}
                        ({insights.quality_summary.failed} failed)
                      </span>
                    )}
                  </div>
                </>
              ) : (
                <div className="text-sm text-muted">No quality data yet</div>
              )}
            </div>

            {/* Catalog Health */}
            <div className="bg-card border border-border rounded-lg p-4">
              <div className="flex items-center justify-between mb-2">
                <h4 className="text-xs font-semibold text-muted uppercase tracking-wider">
                  Catalog Health
                </h4>
                {insights.catalog_health && (
                  <ConfidenceDot
                    value={insights.catalog_health.catalog_confidence}
                  />
                )}
              </div>
              {insights.catalog_health ? (
                <>
                  <div className="text-2xl font-bold mb-1">
                    {Math.round(
                      insights.catalog_health.catalog_confidence * 100
                    )}
                    %
                  </div>
                  <div className="h-1.5 bg-border rounded-full overflow-hidden mb-2">
                    <div
                      className="h-full bg-accent rounded-full"
                      style={{
                        width: `${
                          insights.catalog_health.catalog_coverage * 100
                        }%`,
                      }}
                    />
                  </div>
                  <div className="text-[10px] text-muted space-y-0.5">
                    <div>
                      {insights.catalog_health.metrics_confirmed}/
                      {insights.catalog_health.metrics_total} metrics |{" "}
                      {insights.catalog_health.dimensions_confirmed}/
                      {insights.catalog_health.dimensions_total} dimensions
                    </div>
                    <div>
                      {Math.round(
                        insights.catalog_health.catalog_coverage * 100
                      )}
                      % coverage
                    </div>
                  </div>
                </>
              ) : (
                <div className="text-sm text-muted">No catalog yet</div>
              )}
            </div>
          </div>

          {/* 5. Domain Map + Relationships side by side */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
            <DomainMap domains={insights.domains} />
            <RelationshipDiagram
              tables={insights.table_health}
              relationships={insights.relationship_map}
            />
          </div>

          {/* 6. Advisory Actions */}
          <AdvisoryActions actions={insights.advisory_actions} />

          {/* 7. Activity Feed (v3) */}
          <ActivityFeed activities={activities} />
        </div>
      )}
    </div>
  );
}
