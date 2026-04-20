"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  api,
  type DictTable,
  type DictColumn,
  type DictReviewSummary,
  type ColumnReviewPayload,
  type CatalogItemSummary,
  type ColumnProfile,
} from "@/lib/api";
import { ConfidenceDot } from "@/components/confidence-dot";
import { PKFKManager } from "@/components/pk-fk-manager";

const ROLE_OPTIONS = [
  "metric",
  "dimension",
  "temporal",
  "identifier",
  "geographic",
  "text",
];

const STATUS_BADGE: Record<string, string> = {
  reviewed:
    "bg-green-100 text-green-800 border-green-200 dark:bg-green-900/30 dark:text-green-300 dark:border-green-700",
  pending:
    "bg-yellow-100 text-yellow-800 border-yellow-200 dark:bg-yellow-900/30 dark:text-yellow-300 dark:border-yellow-700",
  in_review:
    "bg-blue-100 text-blue-800 border-blue-200 dark:bg-blue-900/30 dark:text-blue-300 dark:border-blue-700",
  skipped:
    "bg-gray-100 text-gray-600 border-gray-200 dark:bg-gray-800/30 dark:text-gray-400 dark:border-gray-600",
};

const SIGNAL_BORDER: Record<string, string> = {
  auto_confirmed: "border-l-green-400",
  needs_review: "border-l-amber-400",
  conflict: "border-l-red-400",
};

const SIGNAL_BG: Record<string, string> = {
  auto_confirmed: "",
  needs_review: "bg-amber-50/50 dark:bg-amber-900/10",
  conflict: "bg-red-50/50 dark:bg-red-900/10",
};

export default function DictionaryPage() {
  const [tables, setTables] = useState<DictTable[]>([]);
  const [summary, setSummary] = useState<DictReviewSummary | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [editedCols, setEditedCols] = useState<
    Record<string, Partial<DictColumn>>
  >({});
  const [profiles, setProfiles] = useState<ColumnProfile[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [showAutoConfirmed, setShowAutoConfirmed] = useState(false);
  const [showAutoConfirmedCatalog, setShowAutoConfirmedCatalog] =
    useState(false);

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
  }, []);

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

  const handleCatalogAction = async (
    item: CatalogItemSummary,
    action: "confirmed" | "rejected"
  ) => {
    try {
      if (item.item_type === "metric") {
        await api.reviewMetric(item.name, action);
      } else {
        await api.reviewDimension(item.name, action);
      }
      // Reload dictionary to get updated catalog items
      const dict = await api.dictionary();
      setTables(dict.tables);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  // Sort tables: those with needs_review_count > 0 first
  const sortedTables = [...tables].sort((a, b) => {
    if (a.needs_review_count > 0 && b.needs_review_count === 0) return -1;
    if (a.needs_review_count === 0 && b.needs_review_count > 0) return 1;
    return 0;
  });

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

  // Split columns by review signal
  const needsReviewCols = selectedTable
    ? selectedTable.columns.filter(
        (c) => c.review_signal === "needs_review" || c.review_signal === "conflict"
      )
    : [];
  const autoConfirmedCols = selectedTable
    ? selectedTable.columns.filter((c) => c.review_signal === "auto_confirmed")
    : [];

  // Split catalog items
  const needsReviewCatalog = selectedTable
    ? selectedTable.catalog_items.filter(
        (ci) =>
          ci.review_signal === "needs_review" || ci.review_signal === "conflict"
      )
    : [];
  const autoConfirmedCatalog = selectedTable
    ? selectedTable.catalog_items.filter(
        (ci) => ci.review_signal === "auto_confirmed"
      )
    : [];

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <h1 className="text-2xl font-bold">Data Dictionary</h1>
        {summary &&
          summary.reviewed === summary.total &&
          summary.total > 0 && (
            <Link
              href="/explore"
              className="px-4 py-2 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700 transition-colors"
            >
              All reviewed -- Go to Explore
            </Link>
          )}
      </div>
      <p className="text-muted text-sm mb-4">
        Review items that need attention. High-confidence classifications are
        auto-confirmed.
      </p>

      {/* Progress bar */}
      {summary && (
        <div className="mb-6">
          <div className="flex items-center justify-between text-xs text-muted mb-1">
            <span>
              {summary.reviewed} of {summary.total} tables reviewed (
              {summary.pct_complete}%)
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
        <div className="mb-4 p-3 bg-[var(--success)]/10 border border-[var(--success)]/20 rounded text-sm text-[var(--success)]">
          {message}
        </div>
      )}

      <div className="flex gap-6">
        {/* Table list sidebar */}
        <div className="w-64 shrink-0">
          <div className="border border-border rounded-lg bg-card overflow-hidden">
            {sortedTables.map((t) => (
              <button
                key={t.name}
                onClick={() => {
                  setSelected(t.name);
                  setEditedCols({});
                  setMessage("");
                  setShowAutoConfirmed(false);
                  setShowAutoConfirmedCatalog(false);
                }}
                className={`w-full text-left px-3 py-2 text-sm border-b border-border last:border-0 transition-colors ${
                  selected === t.name
                    ? "bg-blue-50 font-medium"
                    : "hover:bg-background"
                }`}
              >
                <div className="flex items-center justify-between">
                  <span className="truncate">{t.name}</span>
                  <div className="flex items-center gap-1.5 shrink-0">
                    {t.needs_review_count > 0 ? (
                      <span className="px-1.5 py-0.5 rounded text-[10px] bg-amber-100 text-amber-800 border border-amber-200">
                        {t.needs_review_count}
                      </span>
                    ) : (
                      <span className="text-green-600 text-xs">&#10003;</span>
                    )}
                    <span
                      className={`px-1.5 py-0.5 rounded text-[10px] border ${
                        STATUS_BADGE[t.review_status]
                      }`}
                    >
                      {t.review_status}
                    </span>
                  </div>
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
              {/* Review summary bar */}
              <div className="flex gap-4 mt-3 text-xs">
                <span className="px-2 py-1 rounded bg-green-50 text-green-700 border border-green-200">
                  {selectedTable.auto_confirmed_count} auto-confirmed
                </span>
                {selectedTable.needs_review_count > 0 && (
                  <span className="px-2 py-1 rounded bg-amber-50 text-amber-700 border border-amber-200">
                    {selectedTable.needs_review_count} need review
                  </span>
                )}
              </div>
            </div>

            {/* Needs Review columns */}
            {needsReviewCols.length > 0 && (
              <div className="mb-4">
                <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-2">
                  Columns needing review ({needsReviewCols.length})
                </h3>
                <div className="border border-border rounded-lg bg-card overflow-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border bg-background text-left">
                        <th className="px-3 py-2 font-medium text-muted w-4"></th>
                        <th className="px-3 py-2 font-medium text-muted">
                          Column
                        </th>
                        <th className="px-3 py-2 font-medium text-muted">
                          Type
                        </th>
                        <th className="px-3 py-2 font-medium text-muted">
                          Role
                        </th>
                        <th className="px-3 py-2 font-medium text-muted">
                          Description
                        </th>
                        <th className="px-3 py-2 font-medium text-muted w-12">
                          PK
                        </th>
                        <th className="px-3 py-2 font-medium text-muted w-16">
                          Conf.
                        </th>
                        <th className="px-3 py-2 font-medium text-muted">
                          Reason
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {needsReviewCols.map((col) => {
                        const isLocked = col.locked;
                        return (
                          <tr
                            key={col.name}
                            className={`border-b border-border last:border-0 border-l-2 ${
                              SIGNAL_BORDER[col.review_signal]
                            } ${SIGNAL_BG[col.review_signal]}`}
                          >
                            <td className="px-1 py-2 text-center">
                              {col.review_signal === "conflict" ? (
                                <span className="text-red-500 text-xs" title="Conflict">&#9679;</span>
                              ) : (
                                <span className="text-amber-500 text-xs" title="Needs review">&#9679;</span>
                              )}
                            </td>
                            <td className="px-3 py-2 font-mono text-xs">
                              {col.name}
                              {col.is_foreign_key && (
                                <span
                                  className="ml-1 text-purple-600 text-[10px]"
                                  title={col.fk_references || ""}
                                >
                                  FK
                                </span>
                              )}
                            </td>
                            <td className="px-3 py-2 text-xs text-muted">
                              {col.dtype}
                            </td>
                            <td className="px-3 py-2">
                              {isLocked ? (
                                <span className="text-xs">
                                  {col.role || "-"}
                                </span>
                              ) : (
                                <select
                                  value={
                                    (getColValue(col, "role") as string) ??
                                    col.role ??
                                    ""
                                  }
                                  onChange={(e) =>
                                    handleColEdit(
                                      col.name,
                                      "role",
                                      e.target.value || null
                                    )
                                  }
                                  className="text-xs border border-border rounded px-1 py-0.5 bg-background"
                                >
                                  <option value="">--</option>
                                  {ROLE_OPTIONS.map((r) => (
                                    <option key={r} value={r}>
                                      {r}
                                    </option>
                                  ))}
                                </select>
                              )}
                            </td>
                            <td className="px-3 py-2">
                              {isLocked ? (
                                <span className="text-xs">
                                  {col.description || "-"}
                                </span>
                              ) : (
                                <input
                                  type="text"
                                  value={
                                    (getColValue(col, "description") as string) ??
                                    col.description ??
                                    ""
                                  }
                                  onChange={(e) =>
                                    handleColEdit(
                                      col.name,
                                      "description",
                                      e.target.value || null
                                    )
                                  }
                                  className="text-xs border border-border rounded px-1 py-0.5 bg-background w-full"
                                />
                              )}
                            </td>
                            <td className="px-3 py-2 text-center">
                              {isLocked ? (
                                col.is_primary_key ? (
                                  <span className="text-[var(--success)] font-bold">
                                    PK
                                  </span>
                                ) : null
                              ) : (
                                <input
                                  type="checkbox"
                                  checked={
                                    (getColValue(
                                      col,
                                      "is_primary_key"
                                    ) as boolean) ?? col.is_primary_key
                                  }
                                  onChange={(e) =>
                                    handleColEdit(
                                      col.name,
                                      "is_primary_key",
                                      e.target.checked
                                    )
                                  }
                                />
                              )}
                            </td>
                            <td className="px-3 py-2 text-center">
                              <ConfidenceDot value={col.confidence} />
                            </td>
                            <td className="px-3 py-2 text-[10px] text-muted italic">
                              {col.review_reason}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Auto-confirmed columns (collapsed) */}
            {autoConfirmedCols.length > 0 && (
              <div className="mb-4">
                <button
                  onClick={() => setShowAutoConfirmed(!showAutoConfirmed)}
                  className="flex items-center gap-2 text-xs text-muted hover:text-foreground transition-colors mb-2"
                >
                  <span
                    className={`transition-transform ${
                      showAutoConfirmed ? "rotate-90" : ""
                    }`}
                  >
                    &#9654;
                  </span>
                  <span className="font-semibold uppercase tracking-wider">
                    {autoConfirmedCols.length} columns auto-confirmed
                  </span>
                </button>
                {showAutoConfirmed && (
                  <div className="border border-border rounded-lg bg-card overflow-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-border bg-background text-left">
                          <th className="px-3 py-2 font-medium text-muted">
                            Column
                          </th>
                          <th className="px-3 py-2 font-medium text-muted">
                            Type
                          </th>
                          <th className="px-3 py-2 font-medium text-muted">
                            Role
                          </th>
                          <th className="px-3 py-2 font-medium text-muted">
                            Semantic Type
                          </th>
                          <th className="px-3 py-2 font-medium text-muted">
                            Description
                          </th>
                          <th className="px-3 py-2 font-medium text-muted w-12">
                            PK
                          </th>
                          <th className="px-3 py-2 font-medium text-muted w-12">
                            FK
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {autoConfirmedCols.map((col) => (
                          <tr
                            key={col.name}
                            className="border-b border-border last:border-0 border-l-2 border-l-green-400"
                          >
                            <td className="px-3 py-2 font-mono text-xs">
                              {col.name}
                            </td>
                            <td className="px-3 py-2 text-xs text-muted">
                              {col.dtype}
                            </td>
                            <td className="px-3 py-2 text-xs">
                              {col.role || "-"}
                            </td>
                            <td className="px-3 py-2 text-xs text-muted">
                              {col.semantic_type || "-"}
                            </td>
                            <td className="px-3 py-2 text-xs">
                              {col.description || "-"}
                            </td>
                            <td className="px-3 py-2 text-center">
                              {col.is_primary_key && (
                                <span className="text-[var(--success)] font-bold">
                                  PK
                                </span>
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
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}

            {/* No columns need review message */}
            {needsReviewCols.length === 0 && selectedTable.review_status !== "reviewed" && (
              <div className="mb-4 p-3 bg-green-50 border border-green-200 rounded text-sm text-green-700">
                All columns auto-confirmed with high confidence. You can review
                the details above or confirm to proceed.
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
                          ? "bg-[var(--warning)]/10 border border-[var(--warning)]/20"
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
                        <div className="text-[10px] text-[var(--warning)] mt-1">
                          {(r.integrity * 100).toFixed(0)}% of rows have matching
                          records -- JOINs will lose{" "}
                          {((1 - r.integrity) * 100).toFixed(0)}% of data
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* PK/FK suggestions */}
            <div className="mb-4">
              <PKFKManager tableName={selectedTable.name} />
            </div>

            {/* Catalog items needing review */}
            {needsReviewCatalog.length > 0 && (
              <div className="mb-4">
                <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-2">
                  Catalog items needing review ({needsReviewCatalog.length})
                </h3>
                <div className="space-y-1.5">
                  {needsReviewCatalog.map((ci) => (
                    <div
                      key={ci.name}
                      className={`border rounded-lg p-3 border-l-2 ${
                        SIGNAL_BORDER[ci.review_signal]
                      } ${SIGNAL_BG[ci.review_signal]}`}
                    >
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 mb-1">
                            <span className="font-medium text-sm">
                              {ci.display_name}
                            </span>
                            <span className="text-[10px] px-1.5 py-0.5 rounded bg-background border border-border font-mono">
                              {ci.item_type}
                              {ci.agg_type ? ` / ${ci.agg_type}` : ""}
                            </span>
                            <ConfidenceDot value={ci.confidence} />
                          </div>
                          <div className="text-xs text-muted">
                            {ci.description}
                          </div>
                          {ci.expression && (
                            <div className="text-[10px] text-muted font-mono mt-0.5">
                              {ci.expression}
                            </div>
                          )}
                        </div>
                        {ci.status === "proposed" && (
                          <div className="flex gap-1.5 shrink-0">
                            <button
                              onClick={() =>
                                handleCatalogAction(ci, "confirmed")
                              }
                              className="px-2.5 py-1 bg-green-600 text-white rounded text-xs font-medium hover:bg-green-700 transition-colors"
                            >
                              Confirm
                            </button>
                            <button
                              onClick={() =>
                                handleCatalogAction(ci, "rejected")
                              }
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
              </div>
            )}

            {/* Auto-confirmed catalog items (collapsed) */}
            {autoConfirmedCatalog.length > 0 && (
              <div className="mb-4">
                <button
                  onClick={() =>
                    setShowAutoConfirmedCatalog(!showAutoConfirmedCatalog)
                  }
                  className="flex items-center gap-2 text-xs text-muted hover:text-foreground transition-colors mb-2"
                >
                  <span
                    className={`transition-transform ${
                      showAutoConfirmedCatalog ? "rotate-90" : ""
                    }`}
                  >
                    &#9654;
                  </span>
                  <span className="font-semibold uppercase tracking-wider">
                    {autoConfirmedCatalog.length} catalog items auto-confirmed
                  </span>
                </button>
                {showAutoConfirmedCatalog && (
                  <div className="space-y-1">
                    {autoConfirmedCatalog.map((ci) => (
                      <div
                        key={ci.name}
                        className="flex items-center gap-3 text-xs p-2 border border-border rounded bg-card"
                      >
                        <span className="text-green-600">&#10003;</span>
                        <span className="font-medium">{ci.display_name}</span>
                        <span className="text-[10px] px-1.5 py-0.5 rounded bg-background border border-border font-mono">
                          {ci.item_type}
                          {ci.agg_type ? ` / ${ci.agg_type}` : ""}
                        </span>
                        <span className="text-muted truncate flex-1">
                          {ci.description}
                        </span>
                        {ci.expression && (
                          <span className="text-muted font-mono text-[10px]">
                            {ci.expression}
                          </span>
                        )}
                      </div>
                    ))}
                  </div>
                )}
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
                This table has been reviewed and locked. Column classifications
                are confirmed and will persist across re-runs.
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
