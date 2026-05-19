"use client";

import { useEffect, useState } from "react";
import {
  api,
  type InsightsResponse,
  type TableDetail,
  type ColumnProfile,
  type DictColumn,
  type DictTable,
  type ColumnReviewPayload,
  type DatasetContext,
  type SemanticSchema,
} from "@/lib/api";
import { useProjects } from "@/lib/project-context";
import { ProfileTable } from "@/components/profile-table";
import { KeyColumnsView } from "@/components/key-columns-view";
import { PKFKManager } from "@/components/pk-fk-manager";
import { ProjectContextReview } from "@/components/project-context-review";

const ROLE_OPTIONS = [
  "metric",
  "dimension",
  "temporal",
  "identifier",
  "geographic",
  "text",
];

const SEMANTIC_TYPE_OPTIONS = [
  "id",
  "primary_key",
  "foreign_key",
  "dimension",
  "metric",
  "temporal",
  "geographic",
  "text",
  "pii",
];

function reviewBreakdown(table: DictTable | null) {
  if (!table) return null;
  const columns = table.columns.filter(
    (c) => c.review_signal === "needs_review" || c.review_signal === "conflict"
  ).length;
  const catalog = table.catalog_items.filter(
    (c) => c.review_signal === "needs_review" || c.review_signal === "conflict"
  ).length;
  return { columns, catalog };
}

export default function DiscoveryPage() {
  const { activeProjectId } = useProjects();
  const [insights, setInsights] = useState<InsightsResponse | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<TableDetail | null>(null);
  const [profiles, setProfiles] = useState<ColumnProfile[]>([]);
  const [error, setError] = useState("");
  const [schemaTab, setSchemaTab] = useState<"key" | "full">("key");
  // Dictionary editing state
  const [dictTable, setDictTable] = useState<DictTable | null>(null);
  const [editDescs, setEditDescs] = useState<Record<string, string>>({});
  const [editedCols, setEditedCols] = useState<Record<string, Partial<DictColumn>>>({});
  const [editTableDesc, setEditTableDesc] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveMsg, setSaveMsg] = useState("");
  const [datasetContext, setDatasetContext] = useState<DatasetContext | null>(null);
  const [contextDraft, setContextDraft] = useState<DatasetContext | null>(null);
  const [semanticSchema, setSemanticSchema] = useState<SemanticSchema | null>(null);
  const [contextMsg, setContextMsg] = useState("");

  useEffect(() => {
    let cancelled = false;
    queueMicrotask(() => {
      if (cancelled) return;
      setInsights(null);
      setSelected(null);
      setDetail(null);
      setProfiles([]);
      setDictTable(null);
      setEditTableDesc(null);
      setEditDescs({});
      setEditedCols({});
      setSaveMsg("");
      setDatasetContext(null);
      setContextDraft(null);
      setSemanticSchema(null);
      setContextMsg("");
      setError("");
    });
    if (!activeProjectId) return;
    api
      .insights(activeProjectId)
      .then((ins) => {
        if (cancelled) return;
        setError("");
        setInsights(ins);
        if (ins.table_health.length > 0) setSelected(ins.table_health[0].name);
        else setSelected(null);
      })
      .catch(() => {
        if (!cancelled) setError("Run the pipeline from the Dashboard first.");
      });
    Promise.all([
      api.datasetContext(activeProjectId).catch(() => null),
      api.semanticSchema(activeProjectId).catch(() => null),
    ]).then(([ctx, schema]) => {
      if (cancelled) return;
      if (ctx) {
        setDatasetContext(ctx);
        setContextDraft(ctx);
      }
      if (schema) setSemanticSchema(schema);
    });
    return () => {
      cancelled = true;
    };
  }, [activeProjectId]);

  useEffect(() => {
    let cancelled = false;
    queueMicrotask(() => {
      if (cancelled) return;
      setDetail(null);
      setProfiles([]);
      setDictTable(null);
    });
    if (!selected || !activeProjectId) return;
    api.table(selected, activeProjectId).then((td) => {
      if (!cancelled) setDetail(td);
    });
    api.tableProfile(selected, activeProjectId).then((profileData) => {
      if (!cancelled) setProfiles(profileData);
    }).catch(() => {
      if (!cancelled) setProfiles([]);
    });
    api.dictionaryTable(selected, activeProjectId).then((dt) => {
      if (cancelled) return;
      setDictTable(dt);
      setEditTableDesc(null);
      setEditDescs({});
      setEditedCols({});
      setSaveMsg("");
    }).catch(() => {
      if (!cancelled) setDictTable(null);
    });
    return () => {
      cancelled = true;
    };
  }, [selected, activeProjectId]);

  const refreshSelected = async () => {
    if (!selected) return;
    const [td, dt, profileData] = await Promise.all([
      api.table(selected, activeProjectId),
      api.dictionaryTable(selected, activeProjectId).catch(() => null),
      api.tableProfile(selected, activeProjectId).catch(() => [] as ColumnProfile[]),
    ]);
    setDetail(td);
    setDictTable(dt);
    setProfiles(profileData);
  };

  const refreshAll = async () => {
    const ins = await api.insights(activeProjectId);
    setInsights(ins);
    const schema = await api.semanticSchema(activeProjectId).catch(() => null);
    if (schema) setSemanticSchema(schema);
    await refreshSelected();
  };

  const updateContextDraft = (field: keyof DatasetContext, value: string) => {
    if (!contextDraft) return;
    setContextDraft({
      ...contextDraft,
      [field]:
        field === "external_references"
          ? value.split("\n").map((item) => item.trim()).filter(Boolean)
          : value,
    });
  };

  const handleSaveContext = async () => {
    if (!contextDraft) return;
    setSaving(true);
    setContextMsg("");
    try {
      const saved = await api.saveDatasetContext(contextDraft, activeProjectId);
      const schema = await api.semanticSchema(activeProjectId).catch(() => null);
      setDatasetContext(saved);
      setContextDraft(saved);
      if (schema) setSemanticSchema(schema);
      setContextMsg("Framing saved.");
      setTimeout(() => setContextMsg(""), 3000);
    } catch (e) {
      setContextMsg(e instanceof Error ? e.message : "Save failed");
    }
    setSaving(false);
  };

  const handleConfirmSemanticRoles = async () => {
    setSaving(true);
    setContextMsg("");
    try {
      const res = await api.confirmSemanticSchema(
        { min_confidence: 0.8, table_name: selected },
        activeProjectId
      );
      const schema = await api.semanticSchema(activeProjectId).catch(() => null);
      if (schema) setSemanticSchema(schema);
      await refreshSelected();
      setContextMsg(`${res.columns_confirmed} role(s) confirmed.`);
      setTimeout(() => setContextMsg(""), 3000);
    } catch (e) {
      setContextMsg(e instanceof Error ? e.message : "Confirmation failed");
    }
    setSaving(false);
  };

  const refreshSemanticSchema = async () => {
    if (!activeProjectId) return;
    const schema = await api.semanticSchema(activeProjectId).catch(() => null);
    if (schema) setSemanticSchema(schema);
  };

  const handleColEdit = (colName: string, field: keyof DictColumn, value: unknown) => {
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

  const handleSaveEdits = async () => {
    if (!selected) return;
    setSaving(true);
    try {
      const merged = new Map<string, ColumnReviewPayload>();
      for (const [name, description] of Object.entries(editDescs)) {
        merged.set(name, { name, description });
      }
      for (const [name, edits] of Object.entries(editedCols)) {
        merged.set(name, { ...(merged.get(name) ?? { name }), ...edits });
      }
      const columns = Array.from(merged.values());
      const payload = {
        columns,
        table_description: editTableDesc ?? undefined,
        confirm: false,
      };
      await api.reviewTable(selected, payload, activeProjectId);
      await refreshAll();
      setEditDescs({});
      setEditedCols({});
      setEditTableDesc(null);
      setSaveMsg("Changes saved.");
      setTimeout(() => setSaveMsg(""), 3000);
    } catch (e) {
      setSaveMsg(e instanceof Error ? e.message : "Save failed");
    }
    setSaving(false);
  };

  const handleConfirmReview = async () => {
    if (!selected) return;
    setSaving(true);
    setSaveMsg("");
    try {
      const columns = dictTable
        ? dictTable.columns.map((col) => ({
            name: col.name,
            ...(editedCols[col.name] ?? {}),
            ...(editDescs[col.name] !== undefined
              ? { description: editDescs[col.name] }
              : {}),
          }))
        : [];
      await api.reviewTable(selected, {
        columns,
        table_description: editTableDesc ?? undefined,
        confirm: true,
      }, activeProjectId);
      await refreshAll();
      setEditedCols({});
      setEditDescs({});
      setEditTableDesc(null);
      setSaveMsg("Table reviewed.");
      setTimeout(() => setSaveMsg(""), 3000);
    } catch (e) {
      setSaveMsg(e instanceof Error ? e.message : "Review failed");
    }
    setSaving(false);
  };

  const hasEdits =
    Object.keys(editDescs).length > 0 ||
    Object.keys(editedCols).length > 0 ||
    editTableDesc !== null;

  if (error) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-4">Discovery Explorer</h1>
        <div className="bg-card border border-border rounded-lg p-8 max-w-xl mx-auto text-center">
          <h2 className="text-lg font-semibold mb-2">No Data Discovered Yet</h2>
          <p className="text-sm text-muted mb-4">
            The Discovery Explorer shows table schemas, column profiles, relationships,
            and data quality insights after you run the Headwater pipeline.
          </p>
          <div className="bg-background border border-border rounded p-4 text-left text-sm font-mono text-muted">
            <p className="mb-1"># From the Dashboard, click &quot;Run Full Pipeline&quot;, or:</p>
            <p className="mb-1">headwater discover --source /path/to/data</p>
            <p>headwater discover --source postgres://user:pass@host/db</p>
          </div>
        </div>
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
  const reviewTodo = reviewBreakdown(dictTable);
  const selectedRoles = semanticSchema?.columns.filter((role) => role.table_name === selected) ?? [];
  const highConfidenceRoles = selectedRoles.filter((role) => !role.locked && role.confidence >= 0.8).length;
  const ambiguousRoles = selectedRoles.filter((role) => !role.locked && role.confidence < 0.8).length;

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Discover & Access</h1>

      {contextDraft && (
        <div className="mb-6 bg-card border border-border rounded-lg p-5">
          <div className="flex items-start justify-between gap-4 mb-4">
            <div>
              <h2 className="text-sm font-semibold text-muted uppercase tracking-wide">
                Dataset Framing
              </h2>
              <div className="text-xs text-muted mt-1">
                Optional context for semantic roles and insight families.
                {datasetContext?.updated_at ? ` Last saved ${datasetContext.updated_at}.` : ""}
              </div>
            </div>
            <div className="flex items-center gap-3">
              {contextMsg && <span className="text-xs text-muted">{contextMsg}</span>}
              <button
                onClick={handleSaveContext}
                disabled={saving}
                className="px-3 py-1.5 bg-foreground text-background rounded text-xs font-medium disabled:opacity-50"
              >
                Save Framing
              </button>
            </div>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
            <label className="text-xs text-muted">
              Row represents
              <input
                value={contextDraft.row_represents ?? ""}
                onChange={(e) => updateContextDraft("row_represents", e.target.value)}
                placeholder="trip, transaction, event, snapshot"
                className="mt-1 w-full px-2 py-1.5 border border-border rounded bg-background text-sm text-foreground"
              />
            </label>
            <label className="text-xs text-muted">
              Time grain and period
              <input
                value={contextDraft.time_grain ?? ""}
                onChange={(e) => updateContextDraft("time_grain", e.target.value)}
                placeholder="event-time, daily, monthly"
                className="mt-1 w-full px-2 py-1.5 border border-border rounded bg-background text-sm text-foreground"
              />
            </label>
            <label className="text-xs text-muted">
              Period covered
              <input
                value={contextDraft.period_covered ?? ""}
                onChange={(e) => updateContextDraft("period_covered", e.target.value)}
                placeholder="Jan-Feb 2026"
                className="mt-1 w-full px-2 py-1.5 border border-border rounded bg-background text-sm text-foreground"
              />
            </label>
            <label className="text-xs text-muted">
              Entity lifecycle
              <input
                value={contextDraft.lifecycle ?? ""}
                onChange={(e) => updateContextDraft("lifecycle", e.target.value)}
                placeholder="pickup -> dropoff, order -> deliver"
                className="mt-1 w-full px-2 py-1.5 border border-border rounded bg-background text-sm text-foreground"
              />
            </label>
            <label className="text-xs text-muted">
              Decisions supported
              <input
                value={contextDraft.decisions ?? ""}
                onChange={(e) => updateContextDraft("decisions", e.target.value)}
                placeholder="operations, pricing, compliance"
                className="mt-1 w-full px-2 py-1.5 border border-border rounded bg-background text-sm text-foreground"
              />
            </label>
            <label className="text-xs text-muted">
              Known caveats
              <input
                value={contextDraft.quality_caveats ?? ""}
                onChange={(e) => updateContextDraft("quality_caveats", e.target.value)}
                placeholder="sparse location ids, delayed updates"
                className="mt-1 w-full px-2 py-1.5 border border-border rounded bg-background text-sm text-foreground"
              />
            </label>
          </div>
          <label className="block text-xs text-muted mt-3">
            External references
            <textarea
              value={(contextDraft.external_references ?? []).join("\n")}
              onChange={(e) => updateContextDraft("external_references", e.target.value)}
              placeholder="Data dictionary URL, schema doc URL, glossary notes"
              rows={2}
              className="mt-1 w-full px-2 py-1.5 border border-border rounded bg-background text-sm text-foreground"
            />
          </label>
        </div>
      )}

      <ProjectContextReview
        projectId={activeProjectId}
        selectedTable={selected}
        onChanged={refreshSemanticSchema}
      />

      <div className="grid grid-cols-1 xl:grid-cols-[280px_minmax(0,1fr)] gap-6">
        {/* Table list sidebar */}
        <div>
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
        <div className="min-w-0 space-y-5">
          {selected && detail && selectedHealth ? (
            <>
              {/* Table header */}
              <div className="bg-card border border-border rounded-lg p-5">
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <h2 className="text-xl font-semibold font-mono">
                      {detail.name}
                    </h2>
                    {editTableDesc !== null ? (
                      <input
                        type="text"
                        value={editTableDesc}
                        onChange={(e) => setEditTableDesc(e.target.value)}
                        className="mt-1 w-full text-sm border border-border rounded px-2 py-1 bg-background"
                        placeholder="Table description..."
                      />
                    ) : (
                      <p
                        className="text-sm text-muted mt-1 cursor-pointer hover:text-foreground transition-colors"
                        onClick={() => setEditTableDesc(detail.description || "")}
                        title="Click to edit description"
                      >
                        {detail.description || "No description — click to add"}
                        <span className="text-[10px] ml-1 text-accent">✎</span>
                      </p>
                    )}
                  </div>
                  <div className="text-right">
                    {dictTable && (
                      <div className="mb-2">
                        <span className="inline-flex px-2 py-0.5 rounded border border-border text-[10px] uppercase tracking-wide text-muted">
                          {dictTable.review_status.replace("_", " ")}
                        </span>
                      </div>
                    )}
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

                <div className="mt-4 flex flex-wrap items-center gap-2">
                  <button
                    onClick={handleConfirmReview}
                    disabled={saving}
                    className="px-3 py-1.5 bg-foreground text-background rounded text-xs font-medium hover:opacity-90 disabled:opacity-50"
                  >
                    {saving ? "Saving..." : "Confirm Table Review"}
                  </button>
                  {hasEdits && (
                    <button
                      onClick={handleSaveEdits}
                      disabled={saving}
                      className="px-3 py-1.5 border border-border rounded text-xs font-medium hover:border-foreground disabled:opacity-50"
                    >
                      Save Edits
                    </button>
                  )}
                  {dictTable && (
                    <span className="text-xs text-muted">
                      {dictTable.review_status === "reviewed"
                        ? "Review complete"
                        : `${dictTable.needs_review_count} item(s) need review${
                            reviewTodo
                              ? `: ${reviewTodo.columns} column metadata, ${reviewTodo.catalog} catalog terms`
                              : ""
                          }`}
                    </span>
                  )}
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

              {selectedRoles.length > 0 && (
                <div className="bg-card border border-border rounded-lg p-5">
                  <div className="flex items-start justify-between gap-4 mb-3">
                    <div>
                      <h3 className="text-sm font-semibold text-muted uppercase tracking-wide">
                        Semantic Roles
                      </h3>
                      <div className="text-xs text-muted mt-1">
                        {highConfidenceRoles} high-confidence role(s), {ambiguousRoles} ambiguous.
                      </div>
                    </div>
                    <button
                      onClick={handleConfirmSemanticRoles}
                      disabled={saving || highConfidenceRoles === 0}
                      className="px-3 py-1.5 border border-border rounded text-xs font-medium hover:border-foreground disabled:opacity-50"
                    >
                      Confirm High Confidence
                    </button>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full min-w-[720px] text-sm">
                      <thead>
                        <tr className="border-b border-border text-left text-muted">
                          <th className="py-2 pr-4">Column</th>
                          <th className="py-2 pr-4">Canonical Role</th>
                          <th className="py-2 pr-4">Confidence</th>
                          <th className="py-2 pr-4">Source</th>
                          <th className="py-2">Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {selectedRoles.slice(0, 10).map((role) => (
                          <tr key={`${role.table_name}.${role.column_name}.${role.canonical_role}`} className="border-b border-border/50">
                            <td className="py-2 pr-4 font-mono">{role.column_name}</td>
                            <td className="py-2 pr-4 font-mono text-accent">{role.canonical_role}</td>
                            <td className="py-2 pr-4">{Math.round(role.confidence * 100)}%</td>
                            <td className="py-2 pr-4">{role.source.replace("_", " ")}</td>
                            <td className="py-2">
                              <span className={`px-2 py-0.5 rounded text-xs border ${
                                role.locked
                                  ? "border-success/30 text-success bg-success/10"
                                  : role.confidence >= 0.8
                                    ? "border-accent/30 text-accent bg-accent/10"
                                    : "border-warning/30 text-warning bg-warning/10"
                              }`}>
                                {role.locked ? "locked" : role.confidence >= 0.8 ? "auto-ready" : "review"}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

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

              {/* Column tabs: Key Columns / Full Schema */}
              <div className="bg-card border border-border rounded-lg p-5">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-sm font-semibold text-muted uppercase tracking-wide">
                    Columns
                  </h3>
                  <div className="flex gap-2">
                    <button
                      onClick={() => setSchemaTab("key")}
                      className={`px-3 py-1 rounded-full text-xs border transition-colors ${
                        schemaTab === "key"
                          ? "bg-foreground text-background border-foreground"
                          : "bg-background text-muted border-border hover:border-foreground"
                      }`}
                    >
                      Key Columns
                    </button>
                    <button
                      onClick={() => setSchemaTab("full")}
                      className={`px-3 py-1 rounded-full text-xs border transition-colors ${
                        schemaTab === "full"
                          ? "bg-foreground text-background border-foreground"
                          : "bg-background text-muted border-border hover:border-foreground"
                      }`}
                    >
                      Full Schema ({detail.columns.length})
                    </button>
                  </div>
                </div>

                {schemaTab === "key" && (
                  <KeyColumnsView
                    columns={detail.columns.map(
                      (c): DictColumn => ({
                        name: c.name,
                        dtype: c.dtype,
                        nullable: c.nullable,
                        is_primary_key: c.is_primary_key,
                        is_foreign_key: selectedHealth.fk_columns.some(
                          (fk) => fk.column === c.name
                        ),
                        fk_references:
                          selectedHealth.fk_columns.find(
                            (fk) => fk.column === c.name
                          )?.references || null,
                        semantic_type: c.semantic_type,
                        role: c.semantic_type
                          ? c.semantic_type.includes("date") ||
                            c.semantic_type.includes("time")
                            ? "temporal"
                            : null
                          : null,
                        description: c.description,
                        confidence: 0.7,
                        locked: false,
                        needs_review: false,
                        review_signal: "auto_confirmed",
                        review_reason: null,
                      })
                    )}
                    profiles={profiles}
                  />
                )}

                {schemaTab === "full" && (
                  <div className="overflow-x-auto">
                    <table className="w-full min-w-[1180px] table-fixed text-sm">
                      <colgroup>
                        <col className="w-[180px]" />
                        <col className="w-[95px]" />
                        <col className="w-[95px]" />
                        <col className="w-[130px]" />
                        <col className="w-[80px]" />
                        <col className="w-[145px]" />
                        <col className="w-[135px]" />
                        <col />
                      </colgroup>
                      <thead>
                        <tr className="border-b border-border text-left text-muted">
                          <th className="py-2 pr-4">Name</th>
                          <th className="py-2 pr-4">Type</th>
                          <th className="py-2 pr-4">Key</th>
                          <th className="py-2 pr-4">Role</th>
                          <th className="py-2 pr-4">Nullable</th>
                          <th className="py-2 pr-4">Semantic Type</th>
                          <th className="py-2 pr-4">Profile</th>
                          <th className="py-2">Description</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(dictTable?.columns ??
                          detail.columns.map(
                            (col): DictColumn => ({
                              ...col,
                              is_foreign_key: selectedHealth.fk_columns.some(
                                (fk) => fk.column === col.name
                              ),
                              fk_references:
                                selectedHealth.fk_columns.find(
                                  (fk) => fk.column === col.name
                                )?.references || null,
                              role: null,
                              confidence: 0.7,
                              locked: false,
                              needs_review: false,
                              review_signal: "auto_confirmed",
                              review_reason: null,
                            })
                          )).map((c) => {
                          const prof = profiles.find(
                            (p) => p.column_name === c.name
                          );
                          const isPk = Boolean(
                            getColValue(c, "is_primary_key")
                          );
                          const isFk = Boolean(c.is_foreign_key);
                          const fkRef = c.fk_references;
                          return (
                            <tr
                              key={c.name}
                              className="border-b border-border/50"
                            >
                              <td className="py-2 pr-4 font-mono font-medium break-words">
                                {c.name}
                              </td>
                              <td className="py-2 pr-4 text-muted">
                                {c.dtype}
                              </td>
                              <td className="py-2 pr-4">
                                <label className="inline-flex items-center gap-1 text-xs">
                                  <input
                                    type="checkbox"
                                    checked={isPk}
                                    onChange={(e) =>
                                      handleColEdit(
                                        c.name,
                                        "is_primary_key",
                                        e.target.checked
                                      )
                                    }
                                    className="h-3.5 w-3.5"
                                  />
                                  <span className="text-warning font-semibold">
                                    PK
                                  </span>
                                </label>
                                {isFk && (
                                  <span
                                    className="text-xs text-accent font-semibold ml-2"
                                    title={fkRef || ""}
                                  >
                                    FK
                                  </span>
                                )}
                              </td>
                              <td className="py-2 pr-4">
                                <select
                                  value={
                                    (getColValue(c, "role") as string | null) ||
                                    ""
                                  }
                                  onChange={(e) =>
                                    handleColEdit(
                                      c.name,
                                      "role",
                                      e.target.value || null
                                    )
                                  }
                                  className="text-xs border border-border rounded px-1.5 py-0.5 bg-background"
                                >
                                  <option value="">-</option>
                                  {ROLE_OPTIONS.map((role) => (
                                    <option key={role} value={role}>
                                      {role}
                                    </option>
                                  ))}
                                </select>
                              </td>
                              <td className="py-2 pr-4 text-muted text-xs">
                                {c.nullable ? "yes" : "no"}
                              </td>
                              <td className="py-2 pr-4">
                                <select
                                  value={
                                    (getColValue(
                                      c,
                                      "semantic_type"
                                    ) as string | null) || ""
                                  }
                                  onChange={(e) =>
                                    handleColEdit(
                                      c.name,
                                      "semantic_type",
                                      e.target.value || null
                                    )
                                  }
                                  className="text-xs border border-border rounded px-1.5 py-0.5 bg-background"
                                >
                                  <option value="">-</option>
                                  {SEMANTIC_TYPE_OPTIONS.map((type) => (
                                    <option key={type} value={type}>
                                      {type}
                                    </option>
                                  ))}
                                </select>
                              </td>
                              <td className="py-2 pr-4">
                                {prof ? (
                                  <div className="flex items-center gap-2">
                                    <span
                                      className={`inline-block w-2 h-2 rounded-full shrink-0 ${
                                        prof.null_rate <= 0.01
                                          ? "bg-green-500"
                                          : prof.null_rate <= 0.1
                                            ? "bg-yellow-500"
                                            : "bg-orange-500"
                                      }`}
                                      title={`${(prof.null_rate * 100).toFixed(1)}% null`}
                                    />
                                    <span className="text-[10px] text-muted whitespace-nowrap">
                                      {prof.distinct_count} distinct
                                      {prof.null_rate > 0.01 &&
                                        ` | ${(prof.null_rate * 100).toFixed(0)}% null`}
                                    </span>
                                  </div>
                                ) : (
                                  <span className="text-[10px] text-muted">
                                    --
                                  </span>
                                )}
                              </td>
                              <td className="py-2">
                                <input
                                  type="text"
                                  value={
                                    editDescs[c.name] ??
                                    c.description ??
                                    ""
                                  }
                                  onChange={(e) =>
                                    setEditDescs((prev) => ({
                                      ...prev,
                                      [c.name]: e.target.value,
                                    }))
                                  }
                                  className="min-h-8 text-xs border border-border rounded px-2 py-1 bg-background w-full"
                                  placeholder="Add description..."
                                />
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              {/* PK/FK Manager */}
              <div className="bg-card border border-border rounded-lg p-5">
                <PKFKManager tableName={selected} projectId={activeProjectId} onChanged={refreshAll} />
              </div>

              {/* Save bar */}
              {hasEdits && (
                <div className="flex items-center gap-3 p-3 bg-accent/5 border border-accent/20 rounded-lg">
                  <button
                    onClick={handleSaveEdits}
                    disabled={saving}
                    className="px-4 py-1.5 bg-accent text-white rounded text-sm font-medium hover:bg-accent/90 disabled:opacity-50"
                  >
                    {saving ? "Saving..." : "Save Changes"}
                  </button>
                  <span className="text-xs text-muted">
                    {Object.keys(editDescs).length} column(s) edited
                    {editTableDesc !== null ? " + table description" : ""}
                  </span>
                </div>
              )}
              {saveMsg && (
                <div className="text-sm text-success">{saveMsg}</div>
              )}

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
