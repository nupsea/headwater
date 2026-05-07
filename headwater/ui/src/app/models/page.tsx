"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import {
  api,
  type ModelSummary,
  type ModelDetail,
  type ModelImpactResponse,
  type InsightsResponse,
  type GraphData,
  type GraphPatterns,
} from "@/lib/api";
import { useProjects } from "@/lib/project-context";
import { useToast } from "@/components/toast";
import { StatusBadge } from "@/components/status-badge";
import { SqlViewer } from "@/components/sql-viewer";
import { StatCard } from "@/components/stat-card";
import { SuggestionsList } from "@/components/suggestions-list";
import { ModelERD } from "@/components/model-erd";
import { QuestionResolver } from "@/components/question-resolver";

export default function ModelsPage() {
  const { toast } = useToast();
  const { activeProjectId } = useProjects();
  const activeProjectIdRef = useRef<string | null>(activeProjectId);
  const [models, setModels] = useState<ModelSummary[]>([]);
  const [insights, setInsights] = useState<InsightsResponse | null>(null);
  const [impact, setImpact] = useState<ModelImpactResponse | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<ModelDetail | null>(null);
  const [message, setMessage] = useState("");
  const [showSection, setShowSection] = useState<string>("graph");
  // Map of model name -> ModelDetail for the Review Queue tab
  const [reviewDetails, setReviewDetails] = useState<Record<string, ModelDetail>>({});
  const [reviewDetailErrors, setReviewDetailErrors] = useState<Record<string, string>>({});
  // Graph data for Relationships tab
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [graphPatterns, setGraphPatterns] = useState<GraphPatterns | null>(null);
  const [graphProjectId, setGraphProjectId] = useState<string | null>(null);

  useEffect(() => {
    activeProjectIdRef.current = activeProjectId;
  }, [activeProjectId]);

  const refresh = useCallback(() => {
    if (!activeProjectId) return Promise.resolve();
    const projectId = activeProjectId;
    setMessage("");
    return api
      .models(projectId)
      .then((nextModels) => {
        if (activeProjectIdRef.current !== projectId) return;
        setModels(nextModels);
        setSelected((current) =>
          current && nextModels.some((model) => model.name === current)
            ? current
            : nextModels[0]?.name ?? null
        );
        return api.modelImpact(projectId).then((nextImpact) => {
          if (activeProjectIdRef.current === projectId) setImpact(nextImpact);
        });
      })
      .catch(() => {
        if (activeProjectIdRef.current === projectId) {
          setMessage("Generate models from the Dashboard first.");
        }
      });
  }, [activeProjectId]);

  useEffect(() => {
    setModels([]);
    setInsights(null);
    setImpact(null);
    setSelected(null);
    setDetail(null);
    setMessage("");
    setReviewDetails({});
    setReviewDetailErrors({});
    setGraphData(null);
    setGraphPatterns(null);
    setGraphProjectId(null);
    if (!activeProjectId) return;
    refresh();
    api
      .insights(activeProjectId)
      .then(setInsights)
      .catch(() => {});
    api
      .modelImpact(activeProjectId)
      .then(setImpact)
      .catch(() => {});
  }, [activeProjectId, refresh]);

  useEffect(() => {
    setDetail(null);
    if (!selected || !activeProjectId) return;
    const projectId = activeProjectId;
    api.model(selected, projectId).then((nextDetail) => {
      if (activeProjectIdRef.current === projectId) setDetail(nextDetail);
    });
  }, [selected, activeProjectId]);

  // Load graph data when Relationships tab is shown
  useEffect(() => {
    if (showSection !== "graph" || !activeProjectId) return;
    if (graphData && graphProjectId === activeProjectId) return;
    const projectId = activeProjectId;
    api.graphData(projectId).then((data) => {
      if (activeProjectIdRef.current !== projectId) return;
      setGraphData(data);
      setGraphProjectId(projectId);
    }).catch(() => {});
    api.graphPatterns(projectId).then((patterns) => {
      if (activeProjectIdRef.current === projectId) setGraphPatterns(patterns);
    }).catch(() => {});
  }, [showSection, activeProjectId, graphData, graphProjectId]);

  // Fetch full model detail (including SQL) for each proposed model when Review Queue opens
  useEffect(() => {
    if (showSection !== "review" || !activeProjectId) return;
    const proposed = models.filter((m) => m.status === "proposed");
    proposed.forEach((m) => {
      if (reviewDetails[m.name] || reviewDetailErrors[m.name]) return;
      api
        .model(m.name, activeProjectId)
        .then((d) => setReviewDetails((prev) => ({ ...prev, [m.name]: d })))
        .catch((e) =>
          setReviewDetailErrors((prev) => ({
            ...prev,
            [m.name]: e instanceof Error ? e.message : String(e),
          }))
        );
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showSection, models, activeProjectId]);

  const handleApprove = async (name: string) => {
    try {
      await api.approveModel(name, activeProjectId);
      setMessage(`Approved: ${name}`);
      toast(`Approved: ${name}`, "success");
      refresh();
      if (selected === name) api.model(name, activeProjectId).then(setDetail);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setMessage(`Error: ${msg}`);
      toast(`Approve failed: ${msg}`, "error");
    }
  };

  const handleReject = async (name: string) => {
    try {
      await api.rejectModel(name, activeProjectId);
      setMessage(`Rejected: ${name}`);
      toast(`Rejected: ${name}`, "info");
      refresh();
      if (selected === name) api.model(name, activeProjectId).then(setDetail);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setMessage(`Error: ${msg}`);
      toast(`Reject failed: ${msg}`, "error");
    }
  };

  if (models.length === 0 && !message) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-4">Models & Lineage</h1>
        <div className="bg-card border border-border rounded-lg p-8 max-w-xl mx-auto text-center">
          <h2 className="text-lg font-semibold mb-2">No Models Generated Yet</h2>
          <p className="text-sm text-muted mb-4">
            Headwater generates SQL models from your data: staging models
            (mechanical transforms, auto-approved) and mart models (analytical,
            require your review). Each mart comes with assumptions and clarifying
            questions so you stay in control.
          </p>
          <p className="text-sm text-muted mb-4">
            Run the full pipeline from the Dashboard to generate models, or use the CLI:
          </p>
          <div className="bg-background border border-border rounded p-4 text-left text-sm font-mono text-muted">
            <p className="mb-1">headwater demo</p>
            <p>headwater discover --source /path/to/data</p>
          </div>
        </div>
      </div>
    );
  }

  const staging = models.filter((m) => m.model_type === "staging");
  const marts = models.filter((m) => m.model_type === "mart");
  const approved = models.filter((m) => m.status === "approved");
  const proposed = models.filter((m) => m.status === "proposed");
  const executed = models.filter((m) => m.status === "executed");
  const rejected = models.filter((m) => m.status === "rejected");

  // Build lineage: source tables -> staging -> marts
  const sourceToStaging: Record<string, string[]> = {};
  staging.forEach((s) => {
    s.source_tables.forEach((src) => {
      if (!sourceToStaging[src]) sourceToStaging[src] = [];
      sourceToStaging[src].push(s.name);
    });
  });

  // Coverage: which source tables have staging
  const sourceTables = insights?.table_health.map((t) => t.name) || [];
  const coveredSources = new Set(
    staging.flatMap((s) => s.source_tables)
  );

  const sections = [
    { id: "graph", label: "Overview" },
    { id: "lineage", label: "Lineage" },
    { id: "review", label: `Review Queue (${proposed.length})` },
    { id: "browse", label: "Browse All" },
  ];

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Models & Lineage</h1>

      {message && (
        <div className="text-sm mb-4 p-3 bg-card border border-border rounded">
          {message}
          <button
            onClick={() => setMessage("")}
            className="ml-2 text-muted hover:text-foreground"
          >
            dismiss
          </button>
        </div>
      )}

      {/* Section tabs */}
      <div className="flex gap-1 mb-6 border-b border-border">
        {sections.map((s) => (
          <button
            key={s.id}
            onClick={() => setShowSection(s.id)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              showSection === s.id
                ? "border-accent text-accent"
                : "border-transparent text-muted hover:text-foreground"
            }`}
          >
            {s.label}
          </button>
        ))}
      </div>

      {/* Overview */}
      {showSection === "overview" && (
        <div className="space-y-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard
              label="Total Models"
              value={models.length}
              sub={`${staging.length} staging, ${marts.length} marts`}
            />
            <StatCard
              label="Approved"
              value={approved.length}
              sub={`of ${models.length} total`}
            />
            <StatCard
              label="Pending Review"
              value={proposed.length}
              sub={proposed.length > 0 ? "mart models need decisions" : "all reviewed"}
            />
            <StatCard
              label="Executed"
              value={executed.length}
              sub={`${rejected.length} rejected`}
            />
          </div>

          {/* Approval status breakdown */}
          <div className="bg-card border border-border rounded-lg p-5">
            <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-3">
              Approval Pipeline
            </h3>
            <div className="flex items-center gap-4">
              <div className="flex-1">
                <div className="h-6 bg-border rounded-full overflow-hidden flex">
                  {executed.length > 0 && (
                    <div
                      className="h-full bg-accent flex items-center justify-center text-xs text-white font-medium"
                      style={{
                        width: `${(executed.length / models.length) * 100}%`,
                      }}
                    >
                      {executed.length > 1 && `${executed.length} executed`}
                    </div>
                  )}
                  {approved.length > 0 && (
                    <div
                      className="h-full bg-success flex items-center justify-center text-xs text-white font-medium"
                      style={{
                        width: `${(approved.length / models.length) * 100}%`,
                      }}
                    >
                      {approved.length > 1 && `${approved.length} approved`}
                    </div>
                  )}
                  {proposed.length > 0 && (
                    <div
                      className="h-full bg-warning flex items-center justify-center text-xs text-white font-medium"
                      style={{
                        width: `${(proposed.length / models.length) * 100}%`,
                      }}
                    >
                      {proposed.length > 1 && `${proposed.length} proposed`}
                    </div>
                  )}
                  {rejected.length > 0 && (
                    <div
                      className="h-full bg-danger flex items-center justify-center text-xs text-white font-medium"
                      style={{
                        width: `${(rejected.length / models.length) * 100}%`,
                      }}
                    >
                      {rejected.length > 0 && `${rejected.length} rejected`}
                    </div>
                  )}
                </div>
                <div className="flex justify-between text-xs text-muted mt-1">
                  <div className="flex gap-4">
                    <span className="flex items-center gap-1">
                      <span className="w-2 h-2 rounded-full bg-accent inline-block" />
                      Executed ({executed.length})
                    </span>
                    <span className="flex items-center gap-1">
                      <span className="w-2 h-2 rounded-full bg-success inline-block" />
                      Approved ({approved.length})
                    </span>
                    <span className="flex items-center gap-1">
                      <span className="w-2 h-2 rounded-full bg-warning inline-block" />
                      Proposed ({proposed.length})
                    </span>
                    <span className="flex items-center gap-1">
                      <span className="w-2 h-2 rounded-full bg-danger inline-block" />
                      Rejected ({rejected.length})
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Staging vs Mart summary */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-card border border-border rounded-lg p-5">
              <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-3">
                Staging Models ({staging.length})
              </h3>
              <p className="text-xs text-muted mb-3">
                Mechanical transforms: rename, cast, deduplicate. Auto-approved -- no business logic.
              </p>
              <div className="space-y-1">
                {staging.map((m) => (
                  <div
                    key={m.name}
                    className="flex items-center justify-between py-1.5 border-b border-border/50 last:border-0"
                  >
                    <span className="font-mono text-sm">{m.name}</span>
                    <StatusBadge status={m.status} />
                  </div>
                ))}
              </div>
            </div>
            <div className="bg-card border border-border rounded-lg p-5">
              <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-3">
                Mart Models ({marts.length})
              </h3>
              <p className="text-xs text-muted mb-3">
                Analytical models with business logic. Each requires individual human review before approval.
              </p>
              <div className="space-y-1">
                {marts.map((m) => (
                  <div
                    key={m.name}
                    className="flex items-center justify-between py-1.5 border-b border-border/50 last:border-0"
                  >
                    <div className="min-w-0 flex-1">
                      <span className="font-mono text-sm">{m.name}</span>
                      {m.questions.length > 0 && (
                        <span className="ml-2 text-xs text-warning">
                          {m.questions.length} question{m.questions.length > 1 ? "s" : ""}
                        </span>
                      )}
                    </div>
                    <StatusBadge status={m.status} />
                  </div>
                ))}
                {marts.length === 0 && (
                  <p className="text-sm text-muted">No mart models generated yet.</p>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Lineage & Coverage */}
      {showSection === "lineage" && (
        <LineageCoverage
          sourceTables={sourceTables}
          coveredSources={coveredSources}
          sourceToStaging={sourceToStaging}
          staging={staging}
          marts={marts}
          insights={insights}
          graphData={graphData}
          onSelectModel={(name) => {
            setSelected(name);
            setShowSection("browse");
          }}
          onLoadGraph={() => {
            if (!graphData && activeProjectId) {
              api.graphData(activeProjectId).then(setGraphData).catch(() => {});
            }
          }}
        />
      )}

      {/* Overview (Relationship Graph + Summary) */}
      {showSection === "graph" && (
        <div className="space-y-6">
          {/* Compact model summary strip */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <StatCard
              label="Total Models"
              value={models.length}
              sub={`${staging.length} staging, ${marts.length} marts`}
            />
            <StatCard
              label="Approved"
              value={approved.length}
              sub={`of ${models.length} total`}
            />
            <StatCard
              label="Pending Review"
              value={proposed.length}
              sub={proposed.length > 0 ? "mart models need decisions" : "all reviewed"}
            />
            <StatCard
              label="Executed"
              value={executed.length}
              sub={`${rejected.length} rejected`}
            />
          </div>

          {impact && (
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              <div className="bg-card border border-border rounded-lg p-5">
                <div className="text-xs text-muted uppercase tracking-wide mb-1">
                  Model Maturity
                </div>
                <div className="text-3xl font-bold">
                  {Math.round(impact.summary.maturity_score)}%
                </div>
                <div className="text-xs text-muted mt-2">
                  {impact.summary.reviewed_marts}/{impact.summary.mart_models} marts reviewed ·{" "}
                  {impact.summary.materialized_models} materialized ·{" "}
                  {impact.summary.monitored_models} monitored
                </div>
              </div>
              <div className="bg-card border border-border rounded-lg p-5">
                <div className="text-xs text-muted uppercase tracking-wide mb-1">
                  Impacted Models
                </div>
                <div className="text-3xl font-bold">
                  {impact.summary.impacted_models}
                </div>
                <div className="text-xs text-muted mt-2">
                  {impact.summary.invalidated_models} invalidated by drift or quality failures
                </div>
              </div>
              <div className="bg-card border border-border rounded-lg p-5">
                <div className="text-xs text-muted uppercase tracking-wide mb-3">
                  Top Blockers
                </div>
                {impact.summary.top_blockers.length > 0 ? (
                  <div className="space-y-2">
                    {impact.summary.top_blockers.map((blocker) => (
                      <div key={blocker.title} className="flex justify-between text-sm">
                        <span>{blocker.title}</span>
                        <span className="text-muted">{blocker.count}</span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-sm text-muted">No model maturity blockers.</div>
                )}
              </div>
            </div>
          )}

          {impact && impact.models.some((m) => m.blockers.length > 0) && (
            <div className="bg-card border border-border rounded-lg overflow-hidden">
              <div className="px-5 py-3 border-b border-border">
                <h3 className="text-sm font-semibold">Model Impact Queue</h3>
                <p className="text-xs text-muted">
                  Models blocked by review, materialization, drift, or quality failures.
                </p>
              </div>
              <div className="divide-y divide-border">
                {impact.models
                  .filter((m) => m.blockers.length > 0)
                  .slice(0, 8)
                  .map((model) => (
                    <button
                      key={model.name}
                      onClick={() => {
                        setSelected(model.name);
                        setShowSection("browse");
                      }}
                      className="w-full px-5 py-3 text-left hover:bg-background transition-colors"
                    >
                      <div className="flex items-center justify-between gap-3">
                        <div>
                          <div className="font-mono text-sm">{model.name}</div>
                          <div className="text-xs text-muted">
                            {model.maturity_state} · downstream{" "}
                            {model.downstream_models.length} · contracts {model.contracts}
                          </div>
                        </div>
                        <div className="text-xs text-warning text-right">
                          {model.blockers.join(", ")}
                        </div>
                      </div>
                    </button>
                  ))}
              </div>
            </div>
          )}

          {!graphData ? (
            <div className="bg-card border border-border rounded-lg p-8 text-center">
              <p className="text-sm text-muted">
                Loading relationship graph data... If no data appears, run the
                pipeline to discover tables and relationships first.
              </p>
            </div>
          ) : (
            <>
              {/* Summary stats */}
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
                <div className="bg-card border border-border rounded-lg p-3 text-center">
                  <div className="text-lg font-bold">{graphData.nodes.length}</div>
                  <div className="text-xs text-muted">Tables</div>
                </div>
                <div className="bg-card border border-border rounded-lg p-3 text-center">
                  <div className="text-lg font-bold">{graphData.edges.length}</div>
                  <div className="text-xs text-muted">Relationships</div>
                </div>
                {graphPatterns && (
                  <>
                    <div className="bg-card border border-border rounded-lg p-3 text-center">
                      <div className="text-lg font-bold">{graphPatterns.conformed_dimensions.length}</div>
                      <div className="text-xs text-muted">Conformed Dims</div>
                    </div>
                    <div className="bg-card border border-border rounded-lg p-3 text-center">
                      <div className="text-lg font-bold">{graphPatterns.nullable_warnings.length}</div>
                      <div className="text-xs text-muted">Nullable Warnings</div>
                    </div>
                  </>
                )}
              </div>

              {/* Adaptive ERD: clustered table cards, pannable + zoomable, drill-down to columns */}
              <ModelERD
                graphData={graphData}
                tableHealth={insights?.table_health || []}
                height={620}
              />

              {/* Edges table beneath -- tabular reference for scanning all FKs */}
              {graphData.edges.length > 0 && (
                <div className="bg-card border border-border rounded-lg overflow-hidden">
                  <div className="px-5 py-3 border-b border-border flex items-center justify-between">
                    <h3 className="text-sm font-semibold">Foreign Key Relationships</h3>
                    <span className="text-xs text-muted">{graphData.edges.length} total</span>
                  </div>
                  <div className="max-h-72 overflow-y-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-background/50 sticky top-0">
                        <tr className="border-b border-border">
                          {["From", "Via FK", "To", "Integrity", "Type"].map((h) => (
                            <th
                              key={h}
                              className="px-4 py-2 text-left text-[10px] font-semibold text-muted uppercase tracking-wider"
                            >
                              {h}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {graphData.edges.map((e, i) => {
                          const integrity = Math.round(e.ref_integrity * 100);
                          const strong = e.ref_integrity >= 0.95 && !e.nullable;
                          return (
                            <tr
                              key={i}
                              className="border-b border-border/40 last:border-0 hover:bg-background/40"
                            >
                              <td className="px-4 py-2 font-mono text-xs">
                                {e.source}
                                <span className="text-muted">.{e.from_column}</span>
                              </td>
                              <td className="px-4 py-2 font-mono text-xs text-muted">
                                {e.from_column} → {e.to_column}
                              </td>
                              <td className="px-4 py-2 font-mono text-xs">
                                {e.target}
                                <span className="text-muted">.{e.to_column}</span>
                              </td>
                              <td className="px-4 py-2">
                                <div className="flex items-center gap-2">
                                  <div className="w-16 h-1.5 bg-border rounded-full overflow-hidden">
                                    <div
                                      className="h-full rounded-full"
                                      style={{
                                        width: `${integrity}%`,
                                        background: strong ? "var(--success)" : "var(--warning)",
                                      }}
                                    />
                                  </div>
                                  <span
                                    className="text-xs font-mono"
                                    style={{ color: strong ? "var(--success)" : "var(--warning)" }}
                                  >
                                    {integrity}%
                                  </span>
                                </div>
                              </td>
                              <td className="px-4 py-2 text-xs text-muted">
                                {e.rel_type}
                                {e.nullable && (
                                  <span className="ml-1 text-amber-600">·nullable</span>
                                )}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Patterns */}
              {graphPatterns && (
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  {/* Conformed Dimensions */}
                  <div className="bg-card border border-border rounded-lg p-5">
                    <h3 className="text-sm font-semibold mb-1">Conformed Dimensions</h3>
                    <p className="text-xs text-muted mb-3">
                      Tables referenced by multiple fact tables -- shared lookup dimensions.
                    </p>
                    {graphPatterns.conformed_dimensions.length === 0 ? (
                      <p className="text-xs text-muted">None detected.</p>
                    ) : (
                      <div className="space-y-2">
                        {graphPatterns.conformed_dimensions.map((cd) => (
                          <div
                            key={cd.name}
                            className="flex items-center justify-between p-2 border border-border rounded"
                          >
                            <span className="font-mono text-sm">{cd.name}</span>
                            <span className="text-xs text-muted">
                              referenced by {cd.connection_count} table{cd.connection_count > 1 ? "s" : ""}
                            </span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>

                  {/* Nullable FK Warnings */}
                  <div className="bg-card border border-border rounded-lg p-5">
                    <h3 className="text-sm font-semibold mb-1">Nullable FK Warnings</h3>
                    <p className="text-xs text-muted mb-3">
                      Foreign keys with low referential integrity -- JOINs may lose rows.
                    </p>
                    {graphPatterns.nullable_warnings.length === 0 ? (
                      <p className="text-xs text-muted">No warnings.</p>
                    ) : (
                      <div className="space-y-2">
                        {graphPatterns.nullable_warnings.map((w, i) => (
                          <div
                            key={i}
                            className="p-2 border border-amber-200 bg-amber-50/50 rounded text-xs"
                          >
                            <span className="font-mono">
                              {w.from_table}.{w.from_column}
                            </span>
                            <span className="text-muted"> -&gt; </span>
                            <span className="font-mono">
                              {w.to_table}.{w.to_column}
                            </span>
                            <span className="ml-2 text-amber-700">
                              {(w.ref_integrity * 100).toFixed(0)}% integrity
                            </span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>

                  {/* Star schemas */}
                  {graphPatterns.star_schemas.length > 0 && (
                    <div className="bg-card border border-border rounded-lg p-5">
                      <h3 className="text-sm font-semibold mb-1">Star Schema Patterns</h3>
                      <p className="text-xs text-muted mb-3">
                        Hub tables with multiple satellite/fact connections.
                      </p>
                      <div className="space-y-2">
                        {graphPatterns.star_schemas.map((s) => (
                          <div
                            key={s.hub}
                            className="flex items-center justify-between p-2 border border-border rounded"
                          >
                            <span className="font-mono text-sm">{s.hub}</span>
                            <span className="text-xs text-muted">
                              {s.spoke_count} connected table{s.spoke_count > 1 ? "s" : ""}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Chains */}
                  {graphPatterns.chains.length > 0 && (
                    <div className="bg-card border border-border rounded-lg p-5">
                      <h3 className="text-sm font-semibold mb-1">Relationship Chains</h3>
                      <p className="text-xs text-muted mb-3">
                        Multi-hop join paths through the data model.
                      </p>
                      <div className="space-y-2">
                        {graphPatterns.chains.map((c, i) => (
                          <div
                            key={i}
                            className="p-2 border border-border rounded text-xs font-mono"
                          >
                            {c.path.join(" -> ")}
                            <span className="text-muted ml-2">({c.hop_count} hops)</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </>
          )}
        </div>
      )}

      {/* Review Queue */}
      {showSection === "review" && (
        <div className="space-y-4">
          {proposed.length === 0 ? (
            <div className="bg-card border border-border rounded-lg p-8 text-center">
              <div className="text-2xl font-bold text-success mb-2">
                All Clear
              </div>
              <p className="text-sm text-muted">
                No models pending review. All mart models have been approved or rejected.
              </p>
            </div>
          ) : (
            <>
              <p className="text-sm text-muted">
                {proposed.length} mart model{proposed.length > 1 ? "s" : ""} pending
                review. Each encodes business logic and assumptions that need human
                validation before execution.
              </p>
              {proposed.map((m) => (
                <div
                  key={m.name}
                  className="bg-card border border-warning/30 rounded-lg p-5"
                >
                  <div className="flex items-start justify-between mb-3">
                    <div>
                      <h3 className="text-lg font-semibold font-mono">
                        {m.name}
                      </h3>
                      <p className="text-sm text-muted mt-1">{m.description}</p>
                      <div className="flex gap-2 mt-2 text-xs text-muted">
                        <span>
                          Sources: {m.source_tables.join(", ")}
                        </span>
                      </div>
                    </div>
                    <div className="flex gap-2 shrink-0">
                      <button
                        onClick={() => handleApprove(m.name)}
                        className="px-4 py-2 bg-green-600 text-white rounded text-sm font-medium hover:bg-green-700 transition-colors"
                      >
                        Confirm
                      </button>
                      <button
                        onClick={() => handleReject(m.name)}
                        className="px-4 py-2 border border-border rounded text-sm text-muted hover:text-foreground transition-colors"
                      >
                        Reject
                      </button>
                    </div>
                  </div>

                  {/* Questions -- resolved via QuestionResolver (v3) */}
                  {m.questions.length > 0 && (
                    <div className="mb-3">
                      <QuestionResolver
                        questions={m.questions}
                        onAnswer={async (answers) => {
                          await api.submitModelAnswers(m.name, answers, activeProjectId);
                        }}
                      />
                    </div>
                  )}

                  {/* Assumptions */}
                  {m.assumptions.length > 0 && (
                    <div className="bg-accent/5 border border-accent/20 rounded-lg p-3 mb-3">
                      <h4 className="text-xs font-semibold text-accent uppercase tracking-wide mb-2">
                        Assumptions Made ({m.assumptions.length})
                      </h4>
                      <ul className="space-y-1.5">
                        {m.assumptions.map((a, i) => (
                          <li
                            key={i}
                            className="flex items-start gap-2 text-sm"
                          >
                            <span className="text-accent font-bold mt-0.5">!</span>
                            <span>{a}</span>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {/* Expandable SQL preview */}
                  <details className="group">
                    <summary className="text-xs font-semibold text-muted uppercase tracking-wide cursor-pointer hover:text-foreground">
                      View SQL
                    </summary>
                    <div className="mt-2">
                      {reviewDetailErrors[m.name] ? (
                        <p className="text-xs text-danger">
                          Failed to load SQL: {reviewDetailErrors[m.name]}
                        </p>
                      ) : reviewDetails[m.name] ? (
                        <SqlViewer sql={reviewDetails[m.name].sql} />
                      ) : (
                        <p className="text-xs text-muted">Loading SQL...</p>
                      )}
                      <button
                        onClick={() => {
                          setSelected(m.name);
                          setShowSection("browse");
                        }}
                        className="text-xs text-accent hover:underline mt-1"
                      >
                        Open full detail view
                      </button>
                    </div>
                  </details>
                </div>
              ))}
            </>
          )}
        </div>
      )}

      {/* Browse All -- the original detail view, enhanced */}
      {showSection === "browse" && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Model list */}
          <div className="lg:col-span-1 space-y-4">
            {staging.length > 0 && (
              <div>
                <h2 className="text-xs font-semibold text-muted uppercase tracking-wide mb-2">
                  Staging ({staging.length})
                </h2>
                <div className="space-y-1">
                  {staging.map((m) => (
                    <button
                      key={m.name}
                      onClick={() => setSelected(m.name)}
                      className={`w-full text-left px-3 py-2.5 rounded text-sm transition-colors ${
                        selected === m.name
                          ? "bg-accent/10 border border-accent/30"
                          : "hover:bg-card border border-transparent"
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-mono font-medium">{m.name}</span>
                        <StatusBadge status={m.status} />
                      </div>
                      <div className="text-xs text-muted mt-0.5">
                        {m.source_tables.join(", ")}
                      </div>
                    </button>
                  ))}
                </div>
              </div>
            )}

            {marts.length > 0 && (
              <div>
                <h2 className="text-xs font-semibold text-muted uppercase tracking-wide mb-2">
                  Marts ({marts.length})
                </h2>
                <div className="space-y-1">
                  {marts.map((m) => (
                    <button
                      key={m.name}
                      onClick={() => setSelected(m.name)}
                      className={`w-full text-left px-3 py-2.5 rounded text-sm transition-colors ${
                        selected === m.name
                          ? "bg-accent/10 border border-accent/30"
                          : "hover:bg-card border border-transparent"
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-mono font-medium">{m.name}</span>
                        <StatusBadge status={m.status} />
                      </div>
                      <div className="text-xs text-muted mt-0.5">
                        {m.source_tables.join(", ")}
                      </div>
                      {m.questions.length > 0 && (
                        <div className="text-xs text-warning mt-0.5">
                          {m.questions.length} question{m.questions.length > 1 ? "s" : ""}
                        </div>
                      )}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Detail panel */}
          <div className="lg:col-span-2">
            {selected && detail ? (
              <div className="space-y-4">
                {/* Header */}
                <div className="bg-card border border-border rounded-lg p-5">
                  <div className="flex items-start justify-between">
                    <div>
                      <h2 className="text-xl font-semibold font-mono">
                        {detail.name}
                      </h2>
                      <p className="text-sm text-muted mt-1">
                        {detail.description}
                      </p>
                      <div className="flex flex-wrap gap-2 mt-3">
                        <StatusBadge status={detail.status} />
                        <span className="px-2 py-0.5 bg-background border border-border rounded text-xs">
                          {detail.model_type}
                        </span>
                      </div>
                    </div>
                    {detail.status === "proposed" && (
                      <div className="flex gap-2 shrink-0">
                        <button
                          onClick={() => handleApprove(detail.name)}
                          className="px-4 py-2 bg-green-600 text-white rounded text-sm font-medium hover:bg-green-700 transition-colors"
                        >
                          Confirm
                        </button>
                        <button
                          onClick={() => handleReject(detail.name)}
                          className="px-4 py-2 border border-border rounded text-sm text-muted hover:text-foreground transition-colors"
                        >
                          Reject
                        </button>
                      </div>
                    )}
                  </div>
                </div>

                {/* Dependencies / Lineage */}
                <div className="bg-card border border-border rounded-lg p-5">
                  <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-3">
                    Dependencies
                  </h3>
                  <div className="flex flex-wrap gap-2">
                    {detail.source_tables.map((dep) => (
                      <span
                        key={dep}
                        className="px-2 py-1 bg-background border border-border rounded text-xs font-mono"
                      >
                        {dep}
                      </span>
                    ))}
                    {detail.depends_on
                      .filter((d) => !detail.source_tables.includes(d))
                      .map((dep) => (
                        <span
                          key={dep}
                          className="px-2 py-1 bg-accent/5 border border-accent/20 rounded text-xs font-mono"
                        >
                          {dep}
                        </span>
                      ))}
                  </div>
                  {detail.depends_on.length === 0 &&
                    detail.source_tables.length === 0 && (
                      <p className="text-sm text-muted">No dependencies.</p>
                    )}
                </div>

                {/* Questions -- QuestionResolver (v3) */}
                {detail.questions.length > 0 && (
                  <QuestionResolver
                    questions={detail.questions}
                    onAnswer={async (answers) => {
                      await api.submitModelAnswers(detail.name, answers, activeProjectId);
                    }}
                  />
                )}

                {/* Assumptions */}
                {detail.assumptions.length > 0 && (
                  <div className="bg-accent/5 border border-accent/20 rounded-lg p-5">
                    <h3 className="text-sm font-semibold text-accent uppercase tracking-wide mb-3">
                      Assumptions ({detail.assumptions.length})
                    </h3>
                    <ul className="space-y-2">
                      {detail.assumptions.map((a, i) => (
                        <li
                          key={i}
                          className="flex items-start gap-2 text-sm"
                        >
                          <span className="text-accent font-bold mt-0.5">!</span>
                          <span>{a}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                {/* SQL */}
                <div className="bg-card border border-border rounded-lg p-5">
                  <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-3">
                    SQL Definition
                  </h3>
                  <SqlViewer sql={detail.sql} />
                </div>
              </div>
            ) : (
              <div className="text-muted text-sm">
                Select a model to view details.
              </div>
            )}
          </div>
        </div>
      )}

      {/* Suggestions */}
      {showSection === "suggestions" && insights && (
        <div className="space-y-6">
          {insights.model_suggestions.length > 0 ? (
            <SuggestionsList suggestions={insights.model_suggestions} />
          ) : (
            <div className="bg-card border border-border rounded-lg p-8 text-center">
              <p className="text-sm text-muted">
                No model improvement suggestions at this time. The generated models
                cover the discovered data well.
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Lineage & Coverage sub-component                                    */
/* ------------------------------------------------------------------ */

function LineageCoverage({
  sourceTables,
  coveredSources,
  sourceToStaging,
  staging,
  marts,
  insights,
  graphData,
  onSelectModel,
  onLoadGraph,
}: {
  sourceTables: string[];
  coveredSources: Set<string>;
  sourceToStaging: Record<string, string[]>;
  staging: ModelSummary[];
  marts: ModelSummary[];
  insights: InsightsResponse | null;
  graphData: GraphData | null;
  onSelectModel: (name: string) => void;
  onLoadGraph: () => void;
}) {
  // Load graph data for FK connections if not already loaded
  useEffect(() => {
    onLoadGraph();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Build FK connections between source tables from graph edges
  const fkConnections: { from: string; to: string; col: string; toCol: string; integrity: number }[] = [];
  if (graphData) {
    for (const edge of graphData.edges) {
      if (sourceTables.includes(edge.source) && sourceTables.includes(edge.target)) {
        fkConnections.push({
          from: edge.source,
          to: edge.target,
          col: edge.from_column,
          toCol: edge.to_column,
          integrity: edge.ref_integrity,
        });
      }
    }
  }

  // Build which marts connect to which source tables (through staging or direct)
  const martToSources: Record<string, string[]> = {};
  for (const m of marts) {
    const sources: string[] = [];
    for (const dep of m.source_tables) {
      // dep could be a staging model name or a source table
      const stgModel = staging.find((s) => s.name === dep);
      if (stgModel) {
        sources.push(...stgModel.source_tables);
      } else if (sourceTables.includes(dep)) {
        sources.push(dep);
      }
    }
    martToSources[m.name] = [...new Set(sources)];
  }

  return (
    <div className="space-y-6">
      {/* Source table coverage */}
      <div className="bg-card border border-border rounded-lg p-5">
        <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-1">
          Source Table Coverage
        </h3>
        <p className="text-xs text-muted mb-4">
          Shows which source tables have staging models. Full coverage means every
          discovered table has a clean, typed staging layer.
        </p>
        {sourceTables.length > 0 ? (
          <>
            <div className="flex items-center gap-3 mb-4">
              <div className="flex-1 h-4 bg-border rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full ${
                    coveredSources.size === sourceTables.length
                      ? "bg-success"
                      : coveredSources.size >= sourceTables.length * 0.8
                        ? "bg-warning"
                        : "bg-danger"
                  }`}
                  style={{
                    width: `${(coveredSources.size / sourceTables.length) * 100}%`,
                  }}
                />
              </div>
              <span className="text-sm font-mono">
                {coveredSources.size}/{sourceTables.length}
              </span>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
              {sourceTables.map((t) => (
                <div
                  key={t}
                  className={`flex items-center gap-2 px-3 py-2 border rounded text-sm ${
                    coveredSources.has(t)
                      ? "border-success/30 bg-success/5"
                      : "border-danger/30 bg-danger/5"
                  }`}
                >
                  <span
                    className={`w-2 h-2 rounded-full ${
                      coveredSources.has(t) ? "bg-success" : "bg-danger"
                    }`}
                  />
                  <span className="font-mono">{t}</span>
                </div>
              ))}
            </div>
          </>
        ) : (
          <p className="text-sm text-muted">
            Run the pipeline to see source table coverage.
          </p>
        )}
      </div>

      {/* FK Connections between source tables */}
      {fkConnections.length > 0 && (
        <div className="bg-card border border-border rounded-lg p-5">
          <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-1">
            Table Relationships (FK Connections)
          </h3>
          <p className="text-xs text-muted mb-4">
            Foreign key relationships between source tables. These connections
            determine how mart models join data across tables.
          </p>
          <div className="space-y-2">
            {fkConnections.map((fk, i) => (
              <div
                key={i}
                className="flex items-center gap-3 px-3 py-2 border border-border/50 rounded"
              >
                <span className="font-mono text-sm font-medium">{fk.from}</span>
                <span className="text-xs text-muted">.{fk.col}</span>
                <svg width="40" height="12" className="shrink-0">
                  <line
                    x1="0" y1="6" x2="32" y2="6"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeDasharray="4 3"
                    className="text-accent"
                  />
                  <polygon points="32,2 40,6 32,10" fill="currentColor" className="text-accent" />
                </svg>
                <span className="font-mono text-sm font-medium">{fk.to}</span>
                <span className="text-xs text-muted">.{fk.toCol}</span>
                <span className={`ml-auto text-xs px-2 py-0.5 rounded ${
                  fk.integrity >= 0.9
                    ? "bg-success/10 text-success"
                    : fk.integrity >= 0.5
                      ? "bg-warning/10 text-warning"
                      : "bg-danger/10 text-danger"
                }`}>
                  {(fk.integrity * 100).toFixed(0)}% integrity
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Lineage diagram: source -> staging -> mart with FK connections */}
      <div className="bg-card border border-border rounded-lg p-5">
        <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-1">
          Data Lineage
        </h3>
        <p className="text-xs text-muted mb-4">
          Source tables feed into staging models, which feed into analytical marts.
          Dotted lines show FK relationships between source tables.
          Click any model to view its SQL and details.
        </p>
        <div className="space-y-4">
          {/* Column headers */}
          <div className="grid grid-cols-3 gap-4 text-xs font-semibold text-muted uppercase tracking-wide">
            <div>Source Tables</div>
            <div>Staging Layer</div>
            <div>Mart Layer</div>
          </div>

          {/* For each source table, show the lineage chain */}
          {sourceTables.map((src) => {
            const stgModels = sourceToStaging[src] || [];
            const dependentMarts = marts.filter((m) =>
              m.source_tables.some(
                (dep) => stgModels.includes(dep) || dep === src
              )
            );
            // FK connections FROM this source table
            const outboundFKs = fkConnections.filter((fk) => fk.from === src);
            const inboundFKs = fkConnections.filter((fk) => fk.to === src);

            return (
              <div
                key={src}
                className="grid grid-cols-3 gap-4 items-start py-2 border-b border-border/50 last:border-0"
              >
                {/* Source */}
                <div>
                  <div className="flex items-center gap-2">
                    <span className="px-2 py-1 bg-background border border-border rounded text-xs font-mono">
                      {src}
                    </span>
                    <span className="text-muted text-xs">
                      {insights?.table_health.find((t) => t.name === src)
                        ?.row_count.toLocaleString() || "?"}{" "}
                      rows
                    </span>
                  </div>
                  {/* Show FK connections as dotted arrows */}
                  {(outboundFKs.length > 0 || inboundFKs.length > 0) && (
                    <div className="mt-1 space-y-0.5">
                      {outboundFKs.map((fk, i) => (
                        <div key={`out-${i}`} className="flex items-center gap-1 text-[10px] text-accent pl-2">
                          <svg width="16" height="8" className="shrink-0">
                            <line x1="0" y1="4" x2="12" y2="4" stroke="currentColor" strokeWidth="1" strokeDasharray="2 2" />
                            <polygon points="12,1 16,4 12,7" fill="currentColor" />
                          </svg>
                          <span className="font-mono">{fk.to}</span>
                          <span className="text-muted">via {fk.col}</span>
                        </div>
                      ))}
                      {inboundFKs.map((fk, i) => (
                        <div key={`in-${i}`} className="flex items-center gap-1 text-[10px] text-muted pl-2">
                          <svg width="16" height="8" className="shrink-0">
                            <line x1="4" y1="4" x2="16" y2="4" stroke="currentColor" strokeWidth="1" strokeDasharray="2 2" />
                            <polygon points="4,1 0,4 4,7" fill="currentColor" />
                          </svg>
                          <span className="font-mono">{fk.from}</span>
                          <span className="text-muted">via {fk.col}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                {/* Staging */}
                <div className="space-y-1">
                  {stgModels.length > 0 ? (
                    stgModels.map((stg) => {
                      const m = staging.find((s) => s.name === stg);
                      return (
                        <button
                          key={stg}
                          onClick={() => onSelectModel(stg)}
                          className="flex items-center gap-2 px-2 py-1 bg-success/5 border border-success/20 rounded text-xs font-mono hover:bg-success/10 transition-colors"
                        >
                          <span className="w-1.5 h-1.5 rounded-full bg-success" />
                          {stg}
                          {m && <StatusBadge status={m.status} />}
                        </button>
                      );
                    })
                  ) : (
                    <span className="text-xs text-danger">No staging model</span>
                  )}
                </div>

                {/* Marts */}
                <div className="space-y-1">
                  {dependentMarts.length > 0 ? (
                    dependentMarts.map((m) => (
                      <button
                        key={m.name}
                        onClick={() => onSelectModel(m.name)}
                        className="flex items-center gap-2 px-2 py-1 bg-accent/5 border border-accent/20 rounded text-xs font-mono hover:bg-accent/10 transition-colors"
                      >
                        <span className="w-1.5 h-1.5 rounded-full bg-accent" />
                        {m.name}
                        <StatusBadge status={m.status} />
                      </button>
                    ))
                  ) : (
                    <span className="text-xs text-muted">--</span>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Mart Model Connections -- which marts share source tables */}
      {marts.length > 1 && (
        <div className="bg-card border border-border rounded-lg p-5">
          <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-1">
            Mart Model Connections
          </h3>
          <p className="text-xs text-muted mb-4">
            Mart models that share source tables or are connected through FK
            relationships. Shared connections indicate potential join paths.
          </p>
          <div className="space-y-2">
            {marts.map((m1, i) =>
              marts.slice(i + 1).map((m2) => {
                const m1Sources = martToSources[m1.name] || [];
                const m2Sources = martToSources[m2.name] || [];
                const shared = m1Sources.filter((s) => m2Sources.includes(s));
                // Also check for FK connections between their source tables
                const fkBridge = fkConnections.filter(
                  (fk) =>
                    (m1Sources.includes(fk.from) && m2Sources.includes(fk.to)) ||
                    (m2Sources.includes(fk.from) && m1Sources.includes(fk.to))
                );
                if (shared.length === 0 && fkBridge.length === 0) return null;
                return (
                  <div
                    key={`${m1.name}-${m2.name}`}
                    className="flex items-center gap-3 px-3 py-2 border border-border/50 rounded"
                  >
                    <button
                      onClick={() => onSelectModel(m1.name)}
                      className="font-mono text-sm font-medium text-accent hover:underline"
                    >
                      {m1.name}
                    </button>
                    <svg width="40" height="12" className="shrink-0">
                      <line
                        x1="0" y1="6" x2="32" y2="6"
                        stroke="currentColor"
                        strokeWidth="1.5"
                        strokeDasharray="4 3"
                        className="text-slate-400"
                      />
                      <circle cx="20" cy="6" r="2" fill="currentColor" className="text-slate-400" />
                    </svg>
                    <button
                      onClick={() => onSelectModel(m2.name)}
                      className="font-mono text-sm font-medium text-accent hover:underline"
                    >
                      {m2.name}
                    </button>
                    <div className="ml-auto flex gap-2 text-xs text-muted">
                      {shared.length > 0 && (
                        <span className="px-2 py-0.5 bg-background border border-border rounded">
                          {shared.length} shared table{shared.length > 1 ? "s" : ""}:
                          {" "}{shared.join(", ")}
                        </span>
                      )}
                      {fkBridge.length > 0 && (
                        <span className="px-2 py-0.5 bg-accent/10 border border-accent/20 rounded">
                          {fkBridge.length} FK bridge{fkBridge.length > 1 ? "s" : ""}
                        </span>
                      )}
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </div>
      )}
    </div>
  );
}
