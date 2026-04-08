"use client";

import { useEffect, useState } from "react";
import { api, type InsightsResponse, type ContractSummary } from "@/lib/api";
import { StatCard } from "@/components/stat-card";
import { NullHeatmap } from "@/components/null-heatmap";
import { SuggestionsList } from "@/components/suggestions-list";
import { StatusBadge } from "@/components/status-badge";

export default function QualityPage() {
  const [insights, setInsights] = useState<InsightsResponse | null>(null);
  const [contracts, setContracts] = useState<ContractSummary[]>([]);
  const [filterType, setFilterType] = useState("all");
  const [filterSeverity, setFilterSeverity] = useState("all");
  const [showSection, setShowSection] = useState<string>("overview");
  const [error, setError] = useState("");

  useEffect(() => {
    api
      .insights()
      .then(setInsights)
      .catch(() => setError("Run the pipeline from the Dashboard first."));
    api
      .contracts()
      .then(setContracts)
      .catch(() => {});
  }, []);

  if (error) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-4">Data Quality</h1>
        <p className="text-muted">{error}</p>
      </div>
    );
  }

  if (!insights) return <div className="text-muted">Loading...</div>;

  const ruleTypes = [
    "all",
    ...Array.from(new Set(contracts.map((c) => c.rule_type))),
  ];
  const severities = [
    "all",
    ...Array.from(new Set(contracts.map((c) => c.severity))),
  ];
  const filtered = contracts.filter((c) => {
    if (filterType !== "all" && c.rule_type !== filterType) return false;
    if (filterSeverity !== "all" && c.severity !== filterSeverity) return false;
    return true;
  });

  const sections = [
    { id: "overview", label: "Overview" },
    { id: "nulls", label: "Null Analysis" },
    { id: "uniqueness", label: "Uniqueness" },
    { id: "patterns", label: "Patterns" },
    { id: "contracts", label: "Contracts" },
    { id: "suggestions", label: "Suggestions" },
  ];

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Data Quality & Metrics</h1>

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
              label="Overall Completeness"
              value={`${insights.overview.completeness_pct}%`}
              sub={`${insights.overview.total_cells.toLocaleString()} total cells`}
            />
            <StatCard
              label="Columns with Nulls"
              value={insights.null_analysis.length}
              sub={`of ${insights.overview.total_profiles} profiled`}
            />
            <StatCard
              label="PK Candidates"
              value={
                insights.uniqueness_analysis.filter((u) => u.is_pk_candidate)
                  .length
              }
              sub="100% unique columns"
            />
            <StatCard
              label="Patterns Detected"
              value={insights.patterns_found.length}
              sub="email, uuid, phone, etc."
            />
          </div>

          {/* Quality pass rate */}
          {insights.quality_summary && (
            <div className="bg-card border border-border rounded-lg p-5">
              <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-3">
                Quality Contract Results
              </h3>
              <div className="flex items-center gap-6">
                <div className="text-center">
                  <div
                    className={`text-4xl font-bold ${
                      insights.quality_summary.pass_rate >= 95
                        ? "text-success"
                        : insights.quality_summary.pass_rate >= 80
                          ? "text-warning"
                          : "text-danger"
                    }`}
                  >
                    {insights.quality_summary.pass_rate}%
                  </div>
                  <div className="text-xs text-muted">pass rate</div>
                </div>
                <div className="flex-1">
                  <div className="h-4 bg-border rounded-full overflow-hidden flex">
                    <div
                      className="h-full bg-success"
                      style={{
                        width: `${
                          (insights.quality_summary.passed /
                            insights.quality_summary.total) *
                          100
                        }%`,
                      }}
                    />
                    <div
                      className="h-full bg-danger"
                      style={{
                        width: `${
                          (insights.quality_summary.failed /
                            insights.quality_summary.total) *
                          100
                        }%`,
                      }}
                    />
                  </div>
                  <div className="flex justify-between text-xs text-muted mt-1">
                    <span className="text-success">
                      {insights.quality_summary.passed} passed
                    </span>
                    <span className="text-danger">
                      {insights.quality_summary.failed} failed
                    </span>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Column issues */}
          {insights.column_issues.length > 0 && (
            <div className="bg-card border border-border rounded-lg p-5">
              <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-3">
                Column-Level Issues ({insights.column_issues.length})
              </h3>
              <div className="space-y-2">
                {insights.column_issues.map((ci, idx) => (
                  <div
                    key={idx}
                    className="flex items-start gap-3 py-2 border-b border-border/50 last:border-0"
                  >
                    <div className="min-w-0 flex-1">
                      <div className="text-sm font-mono">
                        {ci.table}.{ci.column}
                        <span className="text-muted ml-2 font-sans text-xs">
                          {ci.dtype}
                        </span>
                      </div>
                      {ci.issues.map((iss, j) => (
                        <div key={j} className="text-xs mt-0.5 flex gap-2">
                          <span
                            className={`font-semibold ${
                              iss.severity === "error"
                                ? "text-danger"
                                : iss.severity === "warning"
                                  ? "text-warning"
                                  : "text-accent"
                            }`}
                          >
                            {iss.severity.toUpperCase()}
                          </span>
                          <span>{iss.message}</span>
                          <span className="text-muted">{iss.detail}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Table completeness ranking */}
          <div className="bg-card border border-border rounded-lg p-5">
            <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-3">
              Table Completeness Ranking
            </h3>
            <div className="space-y-2">
              {[...insights.table_health]
                .sort((a, b) => a.completeness - b.completeness)
                .map((t) => (
                  <div key={t.name} className="flex items-center gap-3 text-sm">
                    <span className="w-28 font-mono">{t.name}</span>
                    <div className="flex-1 h-3 bg-border rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full ${
                          t.completeness >= 99
                            ? "bg-success"
                            : t.completeness >= 90
                              ? "bg-warning"
                              : "bg-danger"
                        }`}
                        style={{ width: `${t.completeness}%` }}
                      />
                    </div>
                    <span className="w-16 text-right font-mono text-xs">
                      {t.completeness}%
                    </span>
                    <span className="w-20 text-right text-xs text-muted">
                      {t.avg_null_rate}% nulls
                    </span>
                  </div>
                ))}
            </div>
          </div>
        </div>
      )}

      {/* Null analysis */}
      {showSection === "nulls" && (
        <NullHeatmap entries={insights.null_analysis} />
      )}

      {/* Uniqueness */}
      {showSection === "uniqueness" && (
        <div className="bg-card border border-border rounded-lg p-5">
          <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-1">
            Uniqueness Analysis
          </h3>
          <p className="text-xs text-muted mb-4">
            Columns with 100% uniqueness are primary key candidates. Low
            uniqueness on _id columns may indicate duplicates.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-muted">
                  <th className="py-2 pr-4">Table</th>
                  <th className="py-2 pr-4">Column</th>
                  <th className="py-2 pr-4 text-right">Distinct</th>
                  <th className="py-2 pr-4 text-right">Uniqueness</th>
                  <th className="py-2 pr-4">Assessment</th>
                </tr>
              </thead>
              <tbody>
                {insights.uniqueness_analysis
                  .filter(
                    (u) =>
                      u.is_pk_candidate ||
                      (u.column.endsWith("_id") && u.uniqueness_ratio < 100)
                  )
                  .map((u, i) => (
                    <tr key={i} className="border-b border-border/50">
                      <td className="py-2 pr-4 font-mono text-xs">
                        {u.table}
                      </td>
                      <td className="py-2 pr-4 font-mono">{u.column}</td>
                      <td className="py-2 pr-4 text-right">
                        {u.distinct_count.toLocaleString()}
                      </td>
                      <td className="py-2 pr-4 text-right">
                        <span
                          className={
                            u.uniqueness_ratio === 100
                              ? "text-success"
                              : u.uniqueness_ratio > 90
                                ? "text-warning"
                                : "text-danger"
                          }
                        >
                          {u.uniqueness_ratio}%
                        </span>
                      </td>
                      <td className="py-2 pr-4 text-xs">
                        {u.is_pk_candidate ? (
                          <span className="text-success font-medium">
                            Primary key candidate
                          </span>
                        ) : u.column.endsWith("_id") ? (
                          <span className="text-warning">
                            FK -- not expected to be unique
                          </span>
                        ) : (
                          <span className="text-muted">Regular column</span>
                        )}
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Patterns */}
      {showSection === "patterns" && (
        <div className="bg-card border border-border rounded-lg p-5">
          <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-1">
            Detected Patterns
          </h3>
          <p className="text-xs text-muted mb-4">
            Columns where data matches known patterns (email, UUID, phone,
            dates, URLs).
          </p>
          {insights.patterns_found.length === 0 ? (
            <p className="text-sm text-muted">No patterns detected.</p>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
              {insights.patterns_found.map((p, i) => (
                <div
                  key={i}
                  className="flex items-center gap-2 px-3 py-2 border border-border rounded"
                >
                  <span className="px-2 py-0.5 bg-accent/15 text-accent rounded text-xs font-semibold">
                    {p.pattern}
                  </span>
                  <span className="font-mono text-sm">
                    {p.table}.{p.column}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Contracts */}
      {showSection === "contracts" && (
        <div className="space-y-4">
          <div className="flex flex-wrap items-center gap-4">
            <select
              value={filterType}
              onChange={(e) => setFilterType(e.target.value)}
              className="px-3 py-2 border border-border rounded bg-background text-sm"
            >
              {ruleTypes.map((t) => (
                <option key={t} value={t}>
                  {t === "all" ? "All types" : t}
                </option>
              ))}
            </select>
            <select
              value={filterSeverity}
              onChange={(e) => setFilterSeverity(e.target.value)}
              className="px-3 py-2 border border-border rounded bg-background text-sm"
            >
              {severities.map((s) => (
                <option key={s} value={s}>
                  {s === "all" ? "All severities" : s}
                </option>
              ))}
            </select>
            <span className="text-sm text-muted">
              {filtered.length} of {contracts.length} contracts
            </span>
          </div>

          <div className="bg-card border border-border rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-background text-left text-muted">
                  <th className="px-4 py-2">Model</th>
                  <th className="px-4 py-2">Column</th>
                  <th className="px-4 py-2">Rule</th>
                  <th className="px-4 py-2">Severity</th>
                  <th className="px-4 py-2 text-right">Confidence</th>
                  <th className="px-4 py-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {filtered.slice(0, 100).map((c) => (
                  <tr key={c.id} className="border-b border-border/50">
                    <td className="px-4 py-2 font-mono text-xs">
                      {c.model_name}
                    </td>
                    <td className="px-4 py-2 font-mono text-xs">
                      {c.column_name || "(table)"}
                    </td>
                    <td className="px-4 py-2">{c.rule_type}</td>
                    <td className="px-4 py-2">
                      <span
                        className={
                          c.severity === "error"
                            ? "text-danger"
                            : c.severity === "warning"
                              ? "text-warning"
                              : "text-muted"
                        }
                      >
                        {c.severity}
                      </span>
                    </td>
                    <td className="px-4 py-2 text-right">
                      {(c.confidence * 100).toFixed(0)}%
                    </td>
                    <td className="px-4 py-2">
                      <StatusBadge status={c.status} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {filtered.length > 100 && (
              <div className="px-4 py-2 text-xs text-muted border-t border-border">
                Showing first 100 of {filtered.length} contracts
              </div>
            )}
          </div>
        </div>
      )}

      {/* Suggestions */}
      {showSection === "suggestions" && (
        <SuggestionsList suggestions={insights.model_suggestions} />
      )}
    </div>
  );
}
