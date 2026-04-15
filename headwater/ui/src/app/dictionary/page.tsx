"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  api,
  type DictTable,
  type DictColumn,
  type DictReviewSummary,
  type ColumnReviewPayload,
  type CatalogReviewResponse,
  type ColumnProfile,
} from "@/lib/api";
import { KeyColumnsView } from "@/components/key-columns-view";
import { ConfidenceDot } from "@/components/confidence-dot";

const ROLE_OPTIONS = [
  "metric",
  "dimension",
  "temporal",
  "identifier",
  "geographic",
  "text",
];

const STATUS_BADGE: Record<string, string> = {
  reviewed: "bg-green-100 text-green-800 border-green-200",
  pending: "bg-yellow-100 text-yellow-800 border-yellow-200",
  in_review: "bg-blue-100 text-blue-800 border-blue-200",
  skipped: "bg-gray-100 text-gray-600 border-gray-200",
};

const CONFIDENCE_BG: Record<string, string> = {
  high: "",
  medium: "bg-yellow-50",
  low: "bg-orange-50",
};

function confidenceLevel(c: number): "high" | "medium" | "low" {
  if (c >= 0.7) return "high";
  if (c >= 0.5) return "medium";
  return "low";
}



export default function DictionaryPage() {
  const [tables, setTables] = useState<DictTable[]>([]);
  const [summary, setSummary] = useState<DictReviewSummary | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [editedCols, setEditedCols] = useState<Record<string, Partial<DictColumn>>>({});
  const [activeTab, setActiveTab] = useState<"tables" | "catalog">("tables");
  const [columnsSubTab, setColumnsSubTab] = useState<"key" | "all">("key");
  const [catalog, setCatalog] = useState<CatalogReviewResponse | null>(null);
  const [profiles, setProfiles] = useState<ColumnProfile[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [addingSynonym, setAddingSynonym] = useState<string | null>(null);
  const [newSynonym, setNewSynonym] = useState("");

  const loadCatalog = () => {
    api.catalogReview().then(setCatalog).catch(() => setCatalog(null));
  };

  useEffect(() => {
    Promise.all([api.dictionary(), api.dictionarySummary()])
      .then(([dict, sum]) => {
        setTables(dict.tables);
        setSummary(sum);
        if (dict.tables.length > 0 && !selected) {
          setSelected(dict.tables[0].name);
        }
      })
      .catch(() => setError("Run the pipeline from the Dashboard first."))
      .finally(() => setLoading(false));
    loadCatalog();
  }, []);

  // Fetch profile data when a table is selected
  useEffect(() => {
    if (!selected) return;
    api.tableProfile(selected).then(setProfiles).catch(() => setProfiles([]));
  }, [selected]);

  const selectedTable = tables.find((t) => t.name === selected);

  const handleColEdit = (colName: string, field: string, value: unknown) => {
    setEditedCols((prev) => ({
      ...prev,
      [colName]: { ...prev[colName], [field]: value },
    }));
  };

  const getColValue = (col: DictColumn, field: keyof DictColumn) => {
    const edit = editedCols[col.name];
    if (edit && field in edit) return edit[field as keyof typeof edit];
    return col[field];
  };

  const handleConfirm = async (tableName: string) => {
    setSaving(true);
    setMessage("");
    try {
      const columns: ColumnReviewPayload[] = Object.entries(editedCols).map(
        ([name, edits]) => ({ name, ...edits })
      );
      await api.reviewTable(tableName, { columns, confirm: true });
      const [dict, sum] = await Promise.all([
        api.dictionary(),
        api.dictionarySummary(),
      ]);
      setTables(dict.tables);
      setSummary(sum);
      setEditedCols({});
      setMessage(`Table "${tableName}" reviewed and locked.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setSaving(false);
  };

  const handleSkip = async (tableName: string) => {
    setSaving(true);
    try {
      await api.skipTable(tableName);
      const [dict, sum] = await Promise.all([
        api.dictionary(),
        api.dictionarySummary(),
      ]);
      setTables(dict.tables);
      setSummary(sum);
      setMessage(`Table "${tableName}" skipped.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setSaving(false);
  };

  const handleConfirmAll = async () => {
    setSaving(true);
    try {
      const res = await api.confirmAllTables();
      const [dict, sum] = await Promise.all([
        api.dictionary(),
        api.dictionarySummary(),
      ]);
      setTables(dict.tables);
      setSummary(sum);
      setEditedCols({});
      setMessage(`${res.confirmed} table(s) confirmed.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setSaving(false);
  };

  const handleAddSynonym = async (dimName: string, existingSynonyms: string[]) => {
    if (!newSynonym.trim()) return;
    try {
      await api.reviewDimension(dimName, "confirmed", [
        ...existingSynonyms,
        newSynonym.trim(),
      ]);
      setNewSynonym("");
      setAddingSynonym(null);
      loadCatalog();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  if (loading) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-4">Data Dictionary</h1>
        <p className="text-muted text-sm">Loading...</p>
      </div>
    );
  }

  if (error && !tables.length) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-4">Data Dictionary</h1>
        <div className="bg-card border border-border rounded-lg p-8 max-w-xl mx-auto text-center">
          <h2 className="text-lg font-semibold mb-2">No Data Discovered Yet</h2>
          <p className="text-sm text-muted mb-4">
            The data dictionary requires a completed discovery run. Run the
            pipeline from the Dashboard first.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <h1 className="text-2xl font-bold">Data Dictionary</h1>
        {summary && summary.reviewed === summary.total && summary.total > 0 && (
          <Link
            href="/explore"
            className="px-4 py-2 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700 transition-colors"
          >
            All reviewed -- Go to Explore
          </Link>
        )}
      </div>
      <p className="text-muted text-sm mb-4">
        Review table metadata and semantic catalog. Reviewing improves accuracy
        but does not block exploration.
      </p>

      {/* Tab bar */}
      <div className="flex gap-4 border-b border-border mb-4">
        <button
          onClick={() => setActiveTab("tables")}
          className={`pb-2 text-sm font-medium border-b-2 transition-colors ${
            activeTab === "tables"
              ? "border-foreground text-foreground"
              : "border-transparent text-muted hover:text-foreground"
          }`}
        >
          Tables ({tables.length})
        </button>
        <button
          onClick={() => setActiveTab("catalog")}
          className={`pb-2 text-sm font-medium border-b-2 transition-colors ${
            activeTab === "catalog"
              ? "border-foreground text-foreground"
              : "border-transparent text-muted hover:text-foreground"
          }`}
        >
          Catalog{" "}
          {catalog
            ? `(${catalog.summary.metrics_total + catalog.summary.dimensions_total})`
            : ""}
        </button>
      </div>

      {/* Tables tab */}
      {activeTab === "tables" && (
        <>
          {/* Progress bar */}
          {summary && (
            <div className="mb-6">
              <div className="flex items-center justify-between text-xs text-muted mb-1">
                <span>
                  {summary.reviewed} of {summary.total} tables reviewed
                  ({summary.pct_complete}%)
                </span>
                <button
                  onClick={handleConfirmAll}
                  disabled={saving || summary.pending === 0}
                  className="text-xs underline hover:text-foreground disabled:opacity-50"
                >
                  Confirm all as correct
                </button>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div
                  className="bg-green-500 h-2 rounded-full transition-all"
                  style={{ width: `${summary.pct_complete}%` }}
                />
              </div>
            </div>
          )}

          {message && (
            <div className="mb-4 p-3 bg-green-50 border border-green-200 rounded text-sm text-green-800">
              {message}
            </div>
          )}

          <div className="flex gap-6">
            {/* Table list sidebar */}
            <div className="w-64 shrink-0">
              <div className="border border-border rounded-lg bg-card overflow-hidden">
                {tables.map((t) => (
                  <button
                    key={t.name}
                    onClick={() => {
                      setSelected(t.name);
                      setEditedCols({});
                      setMessage("");
                    }}
                    className={`w-full text-left px-3 py-2 text-sm border-b border-border last:border-0 transition-colors ${
                      selected === t.name
                        ? "bg-blue-50 font-medium"
                        : "hover:bg-background"
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className="truncate">{t.name}</span>
                      <span
                        className={`ml-2 px-1.5 py-0.5 rounded text-[10px] border shrink-0 ${
                          STATUS_BADGE[t.review_status]
                        }`}
                      >
                        {t.review_status}
                      </span>
                    </div>
                    <div className="text-[10px] text-muted mt-0.5">
                      {t.row_count.toLocaleString()} rows
                      {t.domain ? ` -- ${t.domain}` : ""}
                    </div>
                  </button>
                ))}
              </div>
            </div>

            {/* Main panel */}
            {selectedTable && (
              <div className="flex-1 min-w-0">
                {/* Table header */}
                <div className="border border-border rounded-lg bg-card p-4 mb-4">
                  <div className="flex items-center justify-between mb-2">
                    <h2 className="text-lg font-semibold">{selectedTable.name}</h2>
                    <span
                      className={`px-2 py-1 rounded text-xs border ${
                        STATUS_BADGE[selectedTable.review_status]
                      }`}
                    >
                      {selectedTable.review_status}
                    </span>
                  </div>
                  <div className="text-sm text-muted mb-2">
                    {selectedTable.description}
                  </div>
                  <div className="flex gap-4 text-xs text-muted">
                    <span>{selectedTable.row_count.toLocaleString()} rows</span>
                    <span>{selectedTable.columns.length} columns</span>
                    {selectedTable.domain && (
                      <span>Domain: {selectedTable.domain}</span>
                    )}
                    {selectedTable.relationships.length > 0 && (
                      <span>
                        {selectedTable.relationships.length} relationship(s)
                      </span>
                    )}
                  </div>
                </div>

                {/* Clarifying questions */}
                {selectedTable.questions.length > 0 && (
                  <div className="border border-amber-200 rounded-lg bg-amber-50 p-4 mb-4">
                    <h3 className="text-xs font-semibold text-amber-800 uppercase tracking-wider mb-2">
                      Needs Clarification
                    </h3>
                    <ul className="space-y-1">
                      {selectedTable.questions.map((q, i) => (
                        <li key={i} className="text-sm text-amber-900">
                          {q}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                {/* Sub-tabs: Key Columns vs All Columns */}
                <div className="flex gap-3 mb-3">
                  <button
                    onClick={() => setColumnsSubTab("key")}
                    className={`px-3 py-1 rounded-full text-xs border transition-colors ${
                      columnsSubTab === "key"
                        ? "bg-foreground text-background border-foreground"
                        : "bg-background text-muted border-border hover:border-foreground"
                    }`}
                  >
                    Key Columns
                  </button>
                  <button
                    onClick={() => setColumnsSubTab("all")}
                    className={`px-3 py-1 rounded-full text-xs border transition-colors ${
                      columnsSubTab === "all"
                        ? "bg-foreground text-background border-foreground"
                        : "bg-background text-muted border-border hover:border-foreground"
                    }`}
                  >
                    All Columns ({selectedTable.columns.length})
                  </button>
                </div>

                {/* Key Columns view */}
                {columnsSubTab === "key" && (
                  <div className="mb-4">
                    <KeyColumnsView
                      columns={selectedTable.columns}
                      profiles={profiles}
                      editable={selectedTable.review_status !== "reviewed"}
                      onEdit={handleColEdit}
                    />
                  </div>
                )}

                {/* All Columns grid */}
                {columnsSubTab === "all" && (
                  <div className="border border-border rounded-lg bg-card overflow-auto mb-4">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-border bg-background text-left">
                          <th className="px-3 py-2 font-medium text-muted">Column</th>
                          <th className="px-3 py-2 font-medium text-muted">Type</th>
                          <th className="px-3 py-2 font-medium text-muted">Role</th>
                          <th className="px-3 py-2 font-medium text-muted">Semantic Type</th>
                          <th className="px-3 py-2 font-medium text-muted">Description</th>
                          <th className="px-3 py-2 font-medium text-muted w-12">PK</th>
                          <th className="px-3 py-2 font-medium text-muted w-12">FK</th>
                          <th className="px-3 py-2 font-medium text-muted w-16">Conf.</th>
                        </tr>
                      </thead>
                      <tbody>
                        {selectedTable.columns.map((col) => {
                          const conf = confidenceLevel(
                            (getColValue(col, "confidence") as number) ?? col.confidence
                          );
                          const isLocked = col.locked;
                          return (
                            <tr
                              key={col.name}
                              className={`border-b border-border last:border-0 ${
                                CONFIDENCE_BG[conf]
                              } ${col.needs_review ? "border-l-2 border-l-amber-400" : ""}`}
                            >
                              <td className="px-3 py-2 font-mono text-xs">{col.name}</td>
                              <td className="px-3 py-2 text-xs text-muted">{col.dtype}</td>
                              <td className="px-3 py-2">
                                {isLocked ? (
                                  <span className="text-xs">{col.role || "-"}</span>
                                ) : (
                                  <select
                                    value={
                                      (getColValue(col, "role") as string) ?? col.role ?? ""
                                    }
                                    onChange={(e) =>
                                      handleColEdit(col.name, "role", e.target.value || null)
                                    }
                                    className="text-xs border border-border rounded px-1 py-0.5 bg-background"
                                  >
                                    <option value="">--</option>
                                    {ROLE_OPTIONS.map((r) => (
                                      <option key={r} value={r}>{r}</option>
                                    ))}
                                  </select>
                                )}
                              </td>
                              <td className="px-3 py-2 text-xs text-muted">
                                {col.semantic_type || "-"}
                              </td>
                              <td className="px-3 py-2">
                                {isLocked ? (
                                  <span className="text-xs">{col.description || "-"}</span>
                                ) : (
                                  <input
                                    type="text"
                                    value={
                                      (getColValue(col, "description") as string) ??
                                      col.description ?? ""
                                    }
                                    onChange={(e) =>
                                      handleColEdit(col.name, "description", e.target.value || null)
                                    }
                                    className="text-xs border border-border rounded px-1 py-0.5 bg-background w-full"
                                  />
                                )}
                              </td>
                              <td className="px-3 py-2 text-center">
                                {isLocked ? (
                                  col.is_primary_key ? (
                                    <span className="text-green-600 font-bold">PK</span>
                                  ) : null
                                ) : (
                                  <input
                                    type="checkbox"
                                    checked={
                                      (getColValue(col, "is_primary_key") as boolean) ??
                                      col.is_primary_key
                                    }
                                    onChange={(e) =>
                                      handleColEdit(col.name, "is_primary_key", e.target.checked)
                                    }
                                  />
                                )}
                              </td>
                              <td className="px-3 py-2 text-center text-xs">
                                {col.is_foreign_key && (
                                  <span
                                    className="text-purple-600 cursor-help"
                                    title={col.fk_references || ""}
                                  >
                                    FK
                                  </span>
                                )}
                              </td>
                              <td className="px-3 py-2 text-center">
                                <ConfidenceDot value={col.confidence} />
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}

                {/* Relationships with integrity warnings */}
                {selectedTable.relationships.length > 0 && (
                  <div className="border border-border rounded-lg bg-card p-4 mb-4">
                    <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-2">
                      Relationships
                    </h3>
                    <div className="space-y-1.5">
                      {selectedTable.relationships.map((r, i) => (
                        <div
                          key={i}
                          className={`text-xs p-2 rounded ${
                            r.integrity < 0.5
                              ? "bg-amber-50 border border-amber-200"
                              : ""
                          }`}
                        >
                          <div className="flex items-center gap-2">
                            <span className="font-mono">
                              {r.from_table}.{r.from_column}
                            </span>
                            <span className="text-muted">&rarr;</span>
                            <span className="font-mono">
                              {r.to_table}.{r.to_column}
                            </span>
                            <span className="text-[10px] text-muted">
                              ({r.type}, {(r.confidence * 100).toFixed(0)}% conf,{" "}
                              {(r.integrity * 100).toFixed(0)}% integrity)
                            </span>
                            <ConfidenceDot value={r.integrity} />
                          </div>
                          {r.integrity < 0.5 && (
                            <div className="text-[10px] text-amber-700 mt-1">
                              {(r.integrity * 100).toFixed(0)}% of rows have
                              matching records -- JOINs will lose{" "}
                              {((1 - r.integrity) * 100).toFixed(0)}% of data
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Action buttons */}
                {selectedTable.review_status !== "reviewed" && (
                  <div className="flex gap-3">
                    <button
                      onClick={() => handleConfirm(selectedTable.name)}
                      disabled={saving}
                      className="px-4 py-2 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700 disabled:opacity-50 transition-colors"
                    >
                      {saving
                        ? "Saving..."
                        : Object.keys(editedCols).length > 0
                        ? "Save & Confirm"
                        : "Confirm as Correct"}
                    </button>
                    <button
                      onClick={() => handleSkip(selectedTable.name)}
                      disabled={saving}
                      className="px-4 py-2 border border-border rounded-lg text-sm text-muted hover:text-foreground disabled:opacity-50 transition-colors"
                    >
                      Skip
                    </button>
                  </div>
                )}

                {selectedTable.review_status === "reviewed" && (
                  <div className="text-sm text-green-700 bg-green-50 border border-green-200 rounded-lg p-3">
                    This table has been reviewed and locked. Column
                    classifications are confirmed and will persist across
                    re-runs.
                  </div>
                )}
              </div>
            )}
          </div>
        </>
      )}

      {/* Catalog tab */}
      {activeTab === "catalog" && (
        <div>
          {!catalog ? (
            <div className="bg-card border border-border rounded-lg p-8 text-center max-w-xl mx-auto">
              <p className="text-sm text-muted">
                No semantic catalog available yet. Run the pipeline to generate
                metrics, dimensions, and entities.
              </p>
            </div>
          ) : (
            <div className="space-y-6">
              {/* Summary bar */}
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
                <div className="bg-card border border-border rounded-lg p-3 text-center">
                  <div className="text-lg font-bold">
                    {catalog.summary.metrics_confirmed}/{catalog.summary.metrics_total}
                  </div>
                  <div className="text-xs text-muted">Metrics confirmed</div>
                </div>
                <div className="bg-card border border-border rounded-lg p-3 text-center">
                  <div className="text-lg font-bold">
                    {catalog.summary.dimensions_confirmed}/{catalog.summary.dimensions_total}
                  </div>
                  <div className="text-xs text-muted">Dimensions confirmed</div>
                </div>
                <div className="bg-card border border-border rounded-lg p-3 text-center">
                  <div className="text-lg font-bold">{catalog.entities.length}</div>
                  <div className="text-xs text-muted">Entities</div>
                </div>
                <div className="bg-card border border-border rounded-lg p-3 text-center">
                  <div className="text-lg font-bold">
                    {catalog.summary.metrics_rejected + catalog.summary.dimensions_rejected}
                  </div>
                  <div className="text-xs text-muted">Rejected</div>
                </div>
              </div>

              {/* Metrics section */}
              <div>
                <h3 className="text-sm font-semibold mb-3">
                  Metrics ({catalog.metrics.length})
                </h3>
                {catalog.metrics.length === 0 ? (
                  <p className="text-xs text-muted">No metrics generated yet.</p>
                ) : (
                  <div className="space-y-2">
                    {catalog.metrics.map((m) => (
                      <div
                        key={m.name}
                        className={`border rounded-lg p-3 ${
                          m.status === "confirmed"
                            ? "border-green-200 bg-green-50/50"
                            : m.status === "rejected"
                            ? "border-red-200 bg-red-50/50 opacity-60"
                            : "border-border bg-card"
                        }`}
                      >
                        <div className="flex items-start justify-between gap-4">
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-1">
                              <span className="font-medium text-sm">{m.display_name}</span>
                              <span className="text-[10px] px-1.5 py-0.5 rounded bg-background border border-border font-mono">
                                {m.agg_type}
                              </span>
                              <ConfidenceDot value={m.confidence} />
                              {m.status !== "proposed" && (
                                <span
                                  className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${
                                    m.status === "confirmed"
                                      ? "bg-green-100 text-green-800"
                                      : "bg-red-100 text-red-800"
                                  }`}
                                >
                                  {m.status}
                                </span>
                              )}
                            </div>
                            <div className="text-xs text-muted mb-1">{m.description}</div>
                            <div className="flex gap-3 text-[10px] text-muted font-mono">
                              <span>{m.expression}</span>
                              <span>on {m.table_name}</span>
                              {m.source !== "human" && (
                                <span className="italic">via {m.source}</span>
                              )}
                            </div>
                          </div>
                          {m.status === "proposed" && (
                            <div className="flex gap-1.5 shrink-0">
                              <button
                                onClick={async () => {
                                  await api.reviewMetric(m.name, "confirmed");
                                  loadCatalog();
                                }}
                                className="px-2.5 py-1 bg-green-600 text-white rounded text-xs font-medium hover:bg-green-700 transition-colors"
                              >
                                Confirm
                              </button>
                              <button
                                onClick={async () => {
                                  await api.reviewMetric(m.name, "rejected");
                                  loadCatalog();
                                }}
                                className="px-2.5 py-1 border border-border rounded text-xs text-muted hover:text-foreground transition-colors"
                              >
                                Reject
                              </button>
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Dimensions section */}
              <div>
                <h3 className="text-sm font-semibold mb-3">
                  Dimensions ({catalog.dimensions.length})
                </h3>
                {catalog.dimensions.length === 0 ? (
                  <p className="text-xs text-muted">No dimensions generated yet.</p>
                ) : (
                  <div className="space-y-2">
                    {catalog.dimensions.map((d) => (
                      <div
                        key={d.name}
                        className={`border rounded-lg p-3 ${
                          d.status === "confirmed"
                            ? "border-green-200 bg-green-50/50"
                            : d.status === "rejected"
                            ? "border-red-200 bg-red-50/50 opacity-60"
                            : "border-border bg-card"
                        }`}
                      >
                        <div className="flex items-start justify-between gap-4">
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-1">
                              <span className="font-medium text-sm">{d.display_name}</span>
                              <span className="text-[10px] px-1.5 py-0.5 rounded bg-background border border-border font-mono">
                                {d.dtype}
                              </span>
                              <ConfidenceDot value={d.confidence} />
                              {d.status !== "proposed" && (
                                <span
                                  className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${
                                    d.status === "confirmed"
                                      ? "bg-green-100 text-green-800"
                                      : "bg-red-100 text-red-800"
                                  }`}
                                >
                                  {d.status}
                                </span>
                              )}
                            </div>
                            <div className="text-xs text-muted mb-1">{d.description}</div>
                            <div className="flex flex-wrap gap-3 text-[10px] text-muted">
                              <span className="font-mono">
                                {d.table_name}.{d.column_name}
                              </span>
                              {d.cardinality > 0 && (
                                <span>{d.cardinality} distinct values</span>
                              )}
                              {d.join_path && (
                                <span className="font-mono">join: {d.join_path}</span>
                              )}
                              {d.join_nullable && (
                                <span className="text-amber-600">nullable FK</span>
                              )}
                            </div>
                            {/* Synonyms with add button */}
                            <div className="flex flex-wrap items-center gap-1 mt-1.5">
                              {d.synonyms.map((s) => (
                                <span
                                  key={s}
                                  className="text-[10px] px-1.5 py-0.5 rounded-full bg-blue-50 text-blue-700 border border-blue-200"
                                >
                                  {s}
                                </span>
                              ))}
                              {addingSynonym === d.name ? (
                                <div className="flex items-center gap-1">
                                  <input
                                    type="text"
                                    value={newSynonym}
                                    onChange={(e) => setNewSynonym(e.target.value)}
                                    onKeyDown={(e) => {
                                      if (e.key === "Enter")
                                        handleAddSynonym(d.name, d.synonyms);
                                      if (e.key === "Escape") {
                                        setAddingSynonym(null);
                                        setNewSynonym("");
                                      }
                                    }}
                                    placeholder="synonym..."
                                    className="text-[10px] px-1.5 py-0.5 border border-blue-300 rounded w-20 bg-white"
                                    autoFocus
                                  />
                                  <button
                                    onClick={() =>
                                      handleAddSynonym(d.name, d.synonyms)
                                    }
                                    className="text-[10px] px-1 text-blue-600 hover:text-blue-800"
                                  >
                                    add
                                  </button>
                                  <button
                                    onClick={() => {
                                      setAddingSynonym(null);
                                      setNewSynonym("");
                                    }}
                                    className="text-[10px] px-1 text-muted hover:text-foreground"
                                  >
                                    cancel
                                  </button>
                                </div>
                              ) : (
                                <button
                                  onClick={() => setAddingSynonym(d.name)}
                                  className="text-[10px] px-1.5 py-0.5 rounded-full border border-dashed border-blue-300 text-blue-500 hover:border-blue-500 hover:text-blue-700 transition-colors"
                                >
                                  + synonym
                                </button>
                              )}
                            </div>
                            {d.sample_values.length > 0 && (
                              <div className="text-[10px] text-muted mt-1 font-mono truncate">
                                values: {d.sample_values.slice(0, 6).join(", ")}
                                {d.sample_values.length > 6 ? ", ..." : ""}
                              </div>
                            )}
                          </div>
                          {d.status === "proposed" && (
                            <div className="flex gap-1.5 shrink-0">
                              <button
                                onClick={async () => {
                                  await api.reviewDimension(d.name, "confirmed");
                                  loadCatalog();
                                }}
                                className="px-2.5 py-1 bg-green-600 text-white rounded text-xs font-medium hover:bg-green-700 transition-colors"
                              >
                                Confirm
                              </button>
                              <button
                                onClick={async () => {
                                  await api.reviewDimension(d.name, "rejected");
                                  loadCatalog();
                                }}
                                className="px-2.5 py-1 border border-border rounded text-xs text-muted hover:text-foreground transition-colors"
                              >
                                Reject
                              </button>
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Entities section */}
              <div>
                <h3 className="text-sm font-semibold mb-3">
                  Entities ({catalog.entities.length})
                </h3>
                {catalog.entities.length === 0 ? (
                  <p className="text-xs text-muted">No entities generated yet.</p>
                ) : (
                  <div className="space-y-2">
                    {catalog.entities.map((e) => (
                      <div
                        key={e.name}
                        className="border border-border rounded-lg bg-card p-3"
                      >
                        <div className="flex items-center gap-2 mb-1">
                          <span className="font-medium text-sm">{e.display_name}</span>
                          <span className="text-[10px] px-1.5 py-0.5 rounded bg-background border border-border font-mono">
                            {e.table_name}
                          </span>
                          {e.temporal_grain && (
                            <span className="text-[10px] text-muted">
                              grain: {e.temporal_grain}
                            </span>
                          )}
                        </div>
                        <div className="text-xs text-muted mb-1.5">{e.description}</div>
                        <div className="text-xs text-muted mb-0.5">{e.row_semantics}</div>
                        <div className="flex flex-wrap gap-3 text-[10px] mt-1.5">
                          {e.metrics.length > 0 && (
                            <span>
                              <span className="text-muted">Metrics:</span>{" "}
                              {e.metrics.join(", ")}
                            </span>
                          )}
                          {e.dimensions.length > 0 && (
                            <span>
                              <span className="text-muted">Dimensions:</span>{" "}
                              {e.dimensions.join(", ")}
                            </span>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
