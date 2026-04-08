"use client";

import { useEffect, useState } from "react";
import {
  api,
  type InsightsResponse,
  type TableDetail,
  type ColumnProfile,
} from "@/lib/api";
import { ProfileTable } from "@/components/profile-table";

export default function DiscoveryPage() {
  const [insights, setInsights] = useState<InsightsResponse | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<TableDetail | null>(null);
  const [profiles, setProfiles] = useState<ColumnProfile[]>([]);
  const [error, setError] = useState("");

  useEffect(() => {
    api
      .insights()
      .then((ins) => {
        setInsights(ins);
        if (ins.table_health.length > 0) setSelected(ins.table_health[0].name);
      })
      .catch(() => setError("Run the pipeline from the Dashboard first."));
  }, []);

  useEffect(() => {
    if (!selected) return;
    api.table(selected).then(setDetail);
    api.tableProfile(selected).then(setProfiles).catch(() => setProfiles([]));
  }, [selected]);

  if (error) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-4">Discovery</h1>
        <p className="text-muted">{error}</p>
      </div>
    );
  }

  if (!insights) return <div className="text-muted">Loading...</div>;

  const selectedHealth = insights.table_health.find(
    (t) => t.name === selected
  );
  const selectedRels = insights.relationship_map.filter(
    (r) => r.from_table === selected || r.to_table === selected
  );
  const selectedIssues = insights.column_issues.filter(
    (i) => i.table === selected
  );

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Discovery Explorer</h1>

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Table list sidebar */}
        <div className="lg:col-span-1">
          <div className="text-xs font-semibold text-muted uppercase tracking-wide mb-2">
            Tables ({insights.table_health.length})
          </div>
          <div className="space-y-1">
            {insights.table_health.map((t) => (
              <button
                key={t.name}
                onClick={() => setSelected(t.name)}
                className={`w-full text-left px-3 py-2.5 rounded text-sm transition-colors ${
                  selected === t.name
                    ? "bg-accent/10 border border-accent/30"
                    : "hover:bg-card border border-transparent"
                }`}
              >
                <div className="flex items-center justify-between">
                  <span className="font-mono font-medium">{t.name}</span>
                  <span
                    className={`text-xs font-mono ${
                      t.completeness >= 99
                        ? "text-success"
                        : t.completeness >= 90
                          ? "text-warning"
                          : "text-danger"
                    }`}
                  >
                    {t.completeness}%
                  </span>
                </div>
                <div className="text-xs text-muted mt-0.5 flex gap-2">
                  <span>{t.row_count.toLocaleString()} rows</span>
                  <span>{t.column_count} cols</span>
                  {t.domain && (
                    <span className="text-accent">{t.domain}</span>
                  )}
                </div>
                {/* Mini completeness bar */}
                <div className="mt-1 h-1 bg-border rounded-full overflow-hidden">
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
              </button>
            ))}
          </div>
        </div>

        {/* Detail panel */}
        <div className="lg:col-span-3 space-y-5">
          {selected && detail && selectedHealth ? (
            <>
              {/* Table header */}
              <div className="bg-card border border-border rounded-lg p-5">
                <div className="flex items-start justify-between">
                  <div>
                    <h2 className="text-xl font-semibold font-mono">
                      {detail.name}
                    </h2>
                    <p className="text-sm text-muted mt-1">
                      {detail.description}
                    </p>
                  </div>
                  <div className="text-right">
                    <div
                      className={`text-2xl font-bold ${
                        selectedHealth.completeness >= 99
                          ? "text-success"
                          : selectedHealth.completeness >= 90
                            ? "text-warning"
                            : "text-danger"
                      }`}
                    >
                      {selectedHealth.completeness}%
                    </div>
                    <div className="text-xs text-muted">completeness</div>
                  </div>
                </div>

                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
                  <div>
                    <div className="text-xs text-muted">Rows</div>
                    <div className="font-semibold">
                      {detail.row_count.toLocaleString()}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-muted">Columns</div>
                    <div className="font-semibold">
                      {detail.columns.length}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-muted">Domain</div>
                    <div className="font-semibold">
                      {detail.domain || "Unclassified"}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-muted">Avg Null Rate</div>
                    <div className="font-semibold">
                      {selectedHealth.avg_null_rate}%
                    </div>
                  </div>
                </div>
              </div>

              {/* Keys & Relationships */}
              <div className="bg-card border border-border rounded-lg p-5">
                <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-3">
                  Keys & Relationships
                </h3>
                <div className="space-y-2">
                  {selectedHealth.pk_columns.length > 0 ? (
                    <div className="flex items-center gap-2 text-sm">
                      <span className="px-2 py-0.5 bg-warning/15 text-warning rounded text-xs font-semibold">
                        PK
                      </span>
                      <span className="font-mono">
                        {selectedHealth.pk_columns.join(", ")}
                      </span>
                    </div>
                  ) : (
                    <div className="text-sm text-warning">
                      No primary key detected -- consider adding one
                    </div>
                  )}

                  {selectedHealth.fk_columns.map((fk, i) => (
                    <div key={i} className="flex items-center gap-2 text-sm">
                      <span className="px-2 py-0.5 bg-accent/15 text-accent rounded text-xs font-semibold">
                        FK
                      </span>
                      <span className="font-mono">{fk.column}</span>
                      <span className="text-muted">references</span>
                      <span className="font-mono text-accent">
                        {fk.references}
                      </span>
                    </div>
                  ))}

                  {selectedRels
                    .filter((r) => r.to_table === selected)
                    .map((r, i) => (
                      <div key={i} className="flex items-center gap-2 text-sm">
                        <span className="px-2 py-0.5 bg-success/15 text-success rounded text-xs font-semibold">
                          REF
                        </span>
                        <span className="font-mono text-muted">
                          {r.from_table}.{r.from_column}
                        </span>
                        <span className="text-muted">references this table</span>
                        <span className="text-xs text-muted">
                          ({r.confidence}% confidence, {r.integrity}% integrity)
                        </span>
                      </div>
                    ))}

                  {selectedHealth.fk_columns.length === 0 &&
                    selectedRels.length === 0 && (
                      <div className="text-sm text-muted">
                        No foreign key relationships detected for this table.
                      </div>
                    )}
                </div>
              </div>

              {/* Column issues */}
              {selectedIssues.length > 0 && (
                <div className="bg-warning/5 border border-warning/30 rounded-lg p-5">
                  <h3 className="text-sm font-semibold text-warning uppercase tracking-wide mb-3">
                    Data Concerns ({selectedIssues.length})
                  </h3>
                  <div className="space-y-2">
                    {selectedIssues.map((issue, i) => (
                      <div key={i}>
                        {issue.issues.map((iss, j) => (
                          <div
                            key={j}
                            className="flex items-center gap-2 text-sm"
                          >
                            <span
                              className={`text-xs font-semibold ${
                                iss.severity === "error"
                                  ? "text-danger"
                                  : iss.severity === "warning"
                                    ? "text-warning"
                                    : "text-accent"
                              }`}
                            >
                              {iss.severity.toUpperCase()}
                            </span>
                            <span className="font-mono">{issue.column}</span>
                            <span className="text-muted">--</span>
                            <span>{iss.message}</span>
                            <span className="text-xs text-muted">
                              ({iss.detail})
                            </span>
                          </div>
                        ))}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Column schema */}
              <div className="bg-card border border-border rounded-lg p-5">
                <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-3">
                  Column Schema
                </h3>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-left text-muted">
                        <th className="py-2 pr-4">Name</th>
                        <th className="py-2 pr-4">Type</th>
                        <th className="py-2 pr-4">Key</th>
                        <th className="py-2 pr-4">Nullable</th>
                        <th className="py-2 pr-4">Semantic Type</th>
                        <th className="py-2">Description</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.columns.map((c) => (
                        <tr
                          key={c.name}
                          className="border-b border-border/50"
                        >
                          <td className="py-2 pr-4 font-mono font-medium">
                            {c.name}
                          </td>
                          <td className="py-2 pr-4 text-muted">{c.dtype}</td>
                          <td className="py-2 pr-4">
                            {c.is_primary_key && (
                              <span className="text-xs text-warning font-semibold">
                                PK
                              </span>
                            )}
                            {selectedHealth.fk_columns.some(
                              (fk) => fk.column === c.name
                            ) && (
                              <span className="text-xs text-accent font-semibold">
                                FK
                              </span>
                            )}
                          </td>
                          <td className="py-2 pr-4 text-muted text-xs">
                            {c.nullable ? "yes" : "no"}
                          </td>
                          <td className="py-2 pr-4">
                            {c.semantic_type && (
                              <span className="px-1.5 py-0.5 bg-accent/10 text-accent rounded text-xs">
                                {c.semantic_type}
                              </span>
                            )}
                          </td>
                          <td className="py-2 text-xs text-muted">
                            {c.description || "-"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Statistical profiles */}
              {profiles.length > 0 && (
                <div className="bg-card border border-border rounded-lg p-5">
                  <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-3">
                    Statistical Profiles
                  </h3>
                  <ProfileTable profiles={profiles} />
                </div>
              )}
            </>
          ) : (
            <div className="text-muted text-sm">
              Select a table to explore.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
