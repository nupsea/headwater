"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { api, type InsightsResponse, type ContractSummary } from "@/lib/api";
import { NullHeatmap } from "@/components/null-heatmap";
import { SuggestionsList } from "@/components/suggestions-list";
import { ProfileTable } from "@/components/profile-table";
import { ConfidenceDot } from "@/components/confidence-dot";

export default function QualityPage() {
  const [insights, setInsights] = useState<InsightsResponse | null>(null);
  const [contracts, setContracts] = useState<ContractSummary[]>([]);
  const [showDetails, setShowDetails] = useState(false);
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
        <div className="bg-card border border-border rounded-lg p-8 max-w-xl mx-auto text-center">
          <h2 className="text-lg font-semibold mb-2">No Quality Data Yet</h2>
          <p className="text-sm text-muted mb-4">
            Headwater automatically generates quality contracts from your data
            profiles: not-null checks, uniqueness constraints, range
            validations, and more. Contracts start in observation mode.
          </p>
          <p className="text-sm text-muted mb-4">
            Run the full pipeline from the Dashboard first.
          </p>
        </div>
      </div>
    );
  }

  if (!insights) return <div className="text-muted">Loading...</div>;

  // Derive data
  const highNulls = [...insights.null_analysis]
    .sort((a, b) => b.null_rate - a.null_rate)
    .slice(0, 8);
  const weakRels = insights.relationship_map.filter(
    (r) => r.integrity < 0.5
  );
  const cleanTables = insights.table_health.filter(
    (t) => t.completeness >= 99 && t.avg_null_rate === 0
  );
  const criticalIssues = insights.column_issues.filter((ci) =>
    ci.issues.some((i) => i.severity === "error")
  );
  const warningIssues = insights.column_issues.filter((ci) =>
    ci.issues.some((i) => i.severity === "warning") &&
    !ci.issues.some((i) => i.severity === "error")
  );

  // Contract grouping by status
  const contractsByStatus: Record<string, ContractSummary[]> = {};
  contracts.forEach((c) => {
    const s = c.status || "proposed";
    if (!contractsByStatus[s]) contractsByStatus[s] = [];
    contractsByStatus[s].push(c);
  });
  const enforcing = contractsByStatus["enforcing"] || [];
  const observing = contractsByStatus["observing"] || contractsByStatus["active"] || [];
  const proposed = contractsByStatus["proposed"] || [];

  return (
    <div>
      <h1 className="text-2xl font-bold mb-2">Data Quality</h1>
      <p className="text-sm text-muted mb-6">
        Quality scorecard, contract monitoring, and data health tracking.
        Contracts start in observation mode before enforcement.
      </p>

      {/* 3 Scorecard Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6">
        {/* Completeness */}
        <div className="bg-card border border-border rounded-lg p-5">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-xs font-semibold text-muted uppercase tracking-wider">
              Completeness
            </h3>
            <ConfidenceDot
              value={insights.data_profile.completeness_pct / 100}
            />
          </div>
          <div className="text-3xl font-bold mb-2">
            {insights.data_profile.completeness_pct.toFixed(1)}%
          </div>
          <div className="h-2 bg-border rounded-full overflow-hidden mb-3">
            <div
              className={`h-full rounded-full ${
                insights.data_profile.completeness_pct >= 95
                  ? "bg-green-500"
                  : insights.data_profile.completeness_pct >= 80
                  ? "bg-yellow-500"
                  : "bg-red-500"
              }`}
              style={{
                width: `${insights.data_profile.completeness_pct}%`,
              }}
            />
          </div>
          <div className="text-xs text-muted space-y-1">
            <div>
              {insights.data_profile.total_columns_profiled} columns profiled
            </div>
            <div>
              {insights.null_analysis.length} columns have nulls
            </div>
            {insights.data_profile.high_null_columns > 0 && (
              <div className="text-amber-600">
                {insights.data_profile.high_null_columns} high-null columns
                (&gt;30%)
              </div>
            )}
            {insights.data_profile.constant_columns > 0 && (
              <div className="text-amber-600">
                {insights.data_profile.constant_columns} constant columns
              </div>
            )}
          </div>
        </div>

        {/* Data Quality */}
        <div className="bg-card border border-border rounded-lg p-5">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-xs font-semibold text-muted uppercase tracking-wider">
              Data Quality
            </h3>
            {insights.quality_summary && (
              <ConfidenceDot
                value={insights.quality_summary.pass_rate / 100}
              />
            )}
          </div>
          {insights.quality_summary ? (
            <>
              <div className="text-3xl font-bold mb-2">
                {insights.quality_summary.pass_rate.toFixed(0)}%
              </div>
              <div className="h-2 bg-border rounded-full overflow-hidden mb-3">
                <div
                  className={`h-full rounded-full ${
                    insights.quality_summary.pass_rate >= 95
                      ? "bg-green-500"
                      : insights.quality_summary.pass_rate >= 80
                      ? "bg-yellow-500"
                      : "bg-red-500"
                  }`}
                  style={{
                    width: `${insights.quality_summary.pass_rate}%`,
                  }}
                />
              </div>
              <div className="text-xs text-muted space-y-1">
                <div>
                  {insights.quality_summary.passed}/
                  {insights.quality_summary.total} checks passed
                </div>
                {criticalIssues.length > 0 && (
                  <div className="text-red-600">
                    {criticalIssues.length} critical issues
                  </div>
                )}
                {warningIssues.length > 0 && (
                  <div className="text-amber-600">
                    {warningIssues.length} warnings
                  </div>
                )}
              </div>
            </>
          ) : (
            <div className="text-sm text-muted">No quality checks yet</div>
          )}
        </div>

        {/* Catalog Health */}
        <div className="bg-card border border-border rounded-lg p-5">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-xs font-semibold text-muted uppercase tracking-wider">
              Catalog Health
            </h3>
            {insights.catalog_health && (
              <ConfidenceDot
                value={insights.catalog_health.catalog_confidence}
              />
            )}
          </div>
          {insights.catalog_health ? (
            <>
              <div className="text-3xl font-bold mb-2">
                {Math.round(
                  insights.catalog_health.catalog_confidence * 100
                )}
                %
              </div>
              <div className="h-2 bg-border rounded-full overflow-hidden mb-3">
                <div
                  className="h-full bg-accent rounded-full"
                  style={{
                    width: `${
                      insights.catalog_health.catalog_coverage * 100
                    }%`,
                  }}
                />
              </div>
              <div className="text-xs text-muted space-y-1">
                <div>
                  {Math.round(insights.catalog_health.catalog_coverage * 100)}%
                  coverage
                </div>
                <div>
                  {insights.catalog_health.metrics_confirmed}/
                  {insights.catalog_health.metrics_total} metrics confirmed
                </div>
                <div>
                  {insights.catalog_health.dimensions_confirmed}/
                  {insights.catalog_health.dimensions_total} dimensions
                  confirmed
                </div>
              </div>
            </>
          ) : (
            <div className="text-sm text-muted">No catalog yet</div>
          )}
        </div>
      </div>

      {/* Needs Attention */}
      {(highNulls.length > 0 || weakRels.length > 0 || criticalIssues.length > 0) && (
        <div className="bg-card border border-border rounded-lg p-5 mb-6">
          <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-3">
            Needs Attention
          </h3>
          <div className="space-y-2">
            {criticalIssues.map((ci, i) => (
              <Link
                key={`issue-${i}`}
                href="/dictionary"
                className="block border-l-4 border-l-red-500 bg-red-50 rounded-r-lg p-3 hover:opacity-80 transition-opacity"
              >
                <div className="flex items-center justify-between">
                  <div className="text-sm font-medium">
                    {ci.table}.{ci.column}
                  </div>
                  <span className="text-xs text-muted">Review &rarr;</span>
                </div>
                <div className="text-xs text-red-700 mt-0.5">
                  {ci.issues
                    .filter((i) => i.severity === "error")
                    .map((i) => i.message)
                    .join("; ")}
                </div>
              </Link>
            ))}
            {highNulls.map((n) => (
              <div
                key={`null-${n.table}-${n.column}`}
                className="border-l-4 border-l-amber-500 bg-amber-50 rounded-r-lg p-3"
              >
                <div className="flex items-center justify-between">
                  <div className="text-sm">
                    <span className="font-mono font-medium">
                      {n.table}.{n.column}
                    </span>
                    <span className="text-muted ml-2">
                      {(n.null_rate * 100).toFixed(0)}% null (
                      {n.null_count.toLocaleString()}/
                      {n.total_rows.toLocaleString()} rows)
                    </span>
                  </div>
                </div>
              </div>
            ))}
            {weakRels.map((r, i) => (
              <Link
                key={`rel-${i}`}
                href="/models"
                className="block border-l-4 border-l-amber-500 bg-amber-50 rounded-r-lg p-3 hover:opacity-80 transition-opacity"
              >
                <div className="flex items-center justify-between">
                  <div className="text-sm font-mono">
                    {r.from_table}.{r.from_column} &rarr; {r.to_table}.
                    {r.to_column}
                  </div>
                  <span className="text-xs text-muted">Investigate &rarr;</span>
                </div>
                <div className="text-xs text-amber-700 mt-0.5">
                  {(r.integrity * 100).toFixed(0)}% integrity -- JOINs will
                  lose {((1 - r.integrity) * 100).toFixed(0)}% of data
                </div>
              </Link>
            ))}
          </div>
        </div>
      )}

      {/* Clean & Reliable */}
      {cleanTables.length > 0 && (
        <div className="bg-card border border-border rounded-lg p-5 mb-6">
          <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-3">
            Clean & Reliable
          </h3>
          <div className="flex flex-wrap gap-2">
            {cleanTables.map((t) => (
              <div
                key={t.name}
                className="flex items-center gap-2 px-3 py-2 border border-green-200 bg-green-50/50 rounded-lg"
              >
                <span className="w-2 h-2 rounded-full bg-green-500" />
                <span className="font-mono text-sm">{t.name}</span>
                <span className="text-[10px] text-muted">
                  {t.row_count.toLocaleString()} rows, 0% nulls
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Contracts */}
      <div className="bg-card border border-border rounded-lg p-5 mb-6">
        <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-3">
          Quality Contracts
        </h3>
        <div className="flex gap-3 mb-4">
          <div className="flex items-center gap-2 px-3 py-2 border border-green-200 bg-green-50/50 rounded-lg">
            <span className="w-2 h-2 rounded-full bg-green-500" />
            <span className="text-sm font-medium">
              {enforcing.length} Enforcing
            </span>
          </div>
          <div className="flex items-center gap-2 px-3 py-2 border border-blue-200 bg-blue-50/50 rounded-lg">
            <span className="w-2 h-2 rounded-full bg-blue-500" />
            <span className="text-sm font-medium">
              {observing.length || contracts.length - enforcing.length - proposed.length} Observing
            </span>
          </div>
          <div className="flex items-center gap-2 px-3 py-2 border border-gray-200 bg-gray-50/50 rounded-lg">
            <span className="w-2 h-2 rounded-full bg-gray-400" />
            <span className="text-sm font-medium">
              {proposed.length} Proposed
            </span>
          </div>
        </div>

        {contracts.length > 0 && (
          <div className="overflow-auto max-h-64">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-background text-left">
                  <th className="px-3 py-2 font-medium text-muted">Model</th>
                  <th className="px-3 py-2 font-medium text-muted">Rule</th>
                  <th className="px-3 py-2 font-medium text-muted">
                    Severity
                  </th>
                  <th className="px-3 py-2 font-medium text-muted">Status</th>
                  <th className="px-3 py-2 font-medium text-muted">Conf.</th>
                </tr>
              </thead>
              <tbody>
                {contracts.slice(0, 20).map((c) => (
                  <tr
                    key={c.id}
                    className="border-b border-border last:border-0"
                  >
                    <td className="px-3 py-2 font-mono text-xs">
                      {c.model_name}
                    </td>
                    <td className="px-3 py-2 text-xs">
                      {c.description || c.rule_type}
                    </td>
                    <td className="px-3 py-2">
                      <span
                        className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${
                          c.severity === "error"
                            ? "bg-red-100 text-red-800"
                            : c.severity === "warning"
                            ? "bg-amber-100 text-amber-800"
                            : "bg-blue-100 text-blue-800"
                        }`}
                      >
                        {c.severity}
                      </span>
                    </td>
                    <td className="px-3 py-2">
                      <span
                        className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${
                          c.status === "enforcing"
                            ? "bg-green-100 text-green-800"
                            : c.status === "active" || c.status === "observing"
                            ? "bg-blue-100 text-blue-800"
                            : "bg-gray-100 text-gray-600"
                        }`}
                      >
                        {c.status}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-center">
                      <ConfidenceDot value={c.confidence} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {contracts.length > 20 && (
              <div className="text-xs text-muted text-center py-2">
                Showing 20 of {contracts.length} contracts
              </div>
            )}
          </div>
        )}
      </div>

      {/* Detailed Analysis (collapsible) */}
      <div className="border border-border rounded-lg overflow-hidden">
        <button
          onClick={() => setShowDetails(!showDetails)}
          className="w-full flex items-center justify-between px-5 py-3 bg-card hover:bg-background transition-colors"
        >
          <h3 className="text-xs font-semibold text-muted uppercase tracking-wider">
            Detailed Analysis
          </h3>
          <span className="text-muted text-sm">
            {showDetails ? "Collapse" : "Expand"}
          </span>
        </button>

        {showDetails && (
          <div className="border-t border-border p-5 space-y-6">
            {/* Null Analysis */}
            {insights.null_analysis.length > 0 && (
              <div>
                <h4 className="text-sm font-semibold mb-3">Null Analysis</h4>
                <NullHeatmap entries={insights.null_analysis} />
              </div>
            )}

            {/* Uniqueness */}
            <div>
              <h4 className="text-sm font-semibold mb-3">
                Uniqueness ({insights.uniqueness_analysis.length})
              </h4>
              <div className="overflow-auto max-h-64">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left">
                      <th className="py-2 pr-4 font-medium text-muted">
                        Table
                      </th>
                      <th className="py-2 pr-4 font-medium text-muted">
                        Column
                      </th>
                      <th className="py-2 pr-4 font-medium text-muted">
                        Uniqueness
                      </th>
                      <th className="py-2 pr-4 font-medium text-muted">
                        Distinct
                      </th>
                      <th className="py-2 font-medium text-muted">
                        PK Candidate
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {insights.uniqueness_analysis
                      .sort((a, b) => b.uniqueness_ratio - a.uniqueness_ratio)
                      .slice(0, 20)
                      .map((u, i) => (
                        <tr
                          key={i}
                          className="border-b border-border/50 last:border-0"
                        >
                          <td className="py-2 pr-4 font-mono text-xs">
                            {u.table}
                          </td>
                          <td className="py-2 pr-4 font-mono text-xs">
                            {u.column}
                          </td>
                          <td className="py-2 pr-4 text-xs">
                            {(u.uniqueness_ratio * 100).toFixed(1)}%
                          </td>
                          <td className="py-2 pr-4 text-xs">
                            {u.distinct_count.toLocaleString()}
                          </td>
                          <td className="py-2 text-xs">
                            {u.is_pk_candidate && (
                              <span className="text-green-600 font-medium">
                                Yes
                              </span>
                            )}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Patterns */}
            {insights.patterns_found.length > 0 && (
              <div>
                <h4 className="text-sm font-semibold mb-3">
                  Patterns ({insights.patterns_found.length})
                </h4>
                <div className="flex flex-wrap gap-2">
                  {insights.patterns_found.map((p, i) => (
                    <div
                      key={i}
                      className="px-3 py-2 border border-border rounded text-xs"
                    >
                      <span className="font-mono font-medium">
                        {p.table}.{p.column}
                      </span>
                      <span className="ml-2 text-accent">{p.pattern}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Suggestions */}
            {insights.model_suggestions.length > 0 && (
              <div>
                <h4 className="text-sm font-semibold mb-3">Suggestions</h4>
                <SuggestionsList suggestions={insights.model_suggestions} />
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
