"use client";

import { useEffect, useState } from "react";
import {
  api,
  type ModelSummary,
  type ModelDetail,
  type InsightsResponse,
} from "@/lib/api";
import { StatusBadge } from "@/components/status-badge";
import { SqlViewer } from "@/components/sql-viewer";
import { StatCard } from "@/components/stat-card";
import { SuggestionsList } from "@/components/suggestions-list";

export default function ModelsPage() {
  const [models, setModels] = useState<ModelSummary[]>([]);
  const [insights, setInsights] = useState<InsightsResponse | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<ModelDetail | null>(null);
  const [message, setMessage] = useState("");
  const [showSection, setShowSection] = useState<string>("overview");

  const refresh = () =>
    api
      .models()
      .then(setModels)
      .catch(() => setMessage("Generate models from the Dashboard first."));

  useEffect(() => {
    refresh();
    api
      .insights()
      .then(setInsights)
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (!selected) return;
    api.model(selected).then(setDetail);
  }, [selected]);

  const handleApprove = async (name: string) => {
    try {
      await api.approveModel(name);
      setMessage(`Approved: ${name}`);
      refresh();
      if (selected === name) api.model(name).then(setDetail);
    } catch (e) {
      setMessage(`Error: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const handleReject = async (name: string) => {
    try {
      await api.rejectModel(name);
      setMessage(`Rejected: ${name}`);
      refresh();
      if (selected === name) api.model(name).then(setDetail);
    } catch (e) {
      setMessage(`Error: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  if (models.length === 0 && !message) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-4">Models</h1>
        <p className="text-muted">
          Run the pipeline from the Dashboard first.
        </p>
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
    { id: "overview", label: "Overview" },
    { id: "lineage", label: "Lineage & Coverage" },
    { id: "review", label: `Review Queue (${proposed.length})` },
    { id: "browse", label: "Browse All" },
    { id: "suggestions", label: "Suggestions" },
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

          {/* Lineage diagram: source -> staging -> mart */}
          <div className="bg-card border border-border rounded-lg p-5">
            <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-1">
              Data Lineage
            </h3>
            <p className="text-xs text-muted mb-4">
              Source tables feed into staging models, which feed into analytical marts.
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
                // Find marts that depend on these staging models
                const dependentMarts = marts.filter((m) =>
                  m.source_tables.some(
                    (dep) =>
                      stgModels.includes(dep) ||
                      dep === src
                  )
                );

                return (
                  <div
                    key={src}
                    className="grid grid-cols-3 gap-4 items-start py-2 border-b border-border/50 last:border-0"
                  >
                    {/* Source */}
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

                    {/* Staging */}
                    <div className="space-y-1">
                      {stgModels.length > 0 ? (
                        stgModels.map((stg) => {
                          const m = staging.find((s) => s.name === stg);
                          return (
                            <button
                              key={stg}
                              onClick={() => {
                                setSelected(stg);
                                setShowSection("browse");
                              }}
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
                            onClick={() => {
                              setSelected(m.name);
                              setShowSection("browse");
                            }}
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
                        className="px-4 py-2 bg-success text-white rounded text-sm font-medium hover:opacity-90"
                      >
                        Approve
                      </button>
                      <button
                        onClick={() => handleReject(m.name)}
                        className="px-4 py-2 bg-danger text-white rounded text-sm font-medium hover:opacity-90"
                      >
                        Reject
                      </button>
                    </div>
                  </div>

                  {/* Questions -- the key advisory feature */}
                  {m.questions.length > 0 && (
                    <div className="bg-warning/5 border border-warning/20 rounded-lg p-3 mb-3">
                      <h4 className="text-xs font-semibold text-warning uppercase tracking-wide mb-2">
                        Questions for Review ({m.questions.length})
                      </h4>
                      <ul className="space-y-1.5">
                        {m.questions.map((q, i) => (
                          <li
                            key={i}
                            className="flex items-start gap-2 text-sm"
                          >
                            <span className="text-warning font-bold mt-0.5">?</span>
                            <span>{q}</span>
                          </li>
                        ))}
                      </ul>
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
                      <SqlViewer sql={m.assumptions.length > 0 ? "-- Click 'Browse All' tab and select this model to view full SQL" : ""} />
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
                          className="px-4 py-2 bg-success text-white rounded text-sm font-medium hover:opacity-90"
                        >
                          Approve
                        </button>
                        <button
                          onClick={() => handleReject(detail.name)}
                          className="px-4 py-2 bg-danger text-white rounded text-sm font-medium hover:opacity-90"
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

                {/* Questions */}
                {detail.questions.length > 0 && (
                  <div className="bg-warning/5 border border-warning/30 rounded-lg p-5">
                    <h3 className="text-sm font-semibold text-warning uppercase tracking-wide mb-3">
                      Questions for Review ({detail.questions.length})
                    </h3>
                    <ul className="space-y-2">
                      {detail.questions.map((q, i) => (
                        <li
                          key={i}
                          className="flex items-start gap-2 text-sm"
                        >
                          <span className="text-warning font-bold mt-0.5">?</span>
                          <span>{q}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
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
