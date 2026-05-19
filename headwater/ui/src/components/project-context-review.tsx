"use client";

import { useEffect, useMemo, useState } from "react";

import {
  api,
  type ProjectContextItem,
  type ProjectContextResponse,
} from "@/lib/api";

const RESOURCE_TYPES = ["url", "markdown", "pdf", "csv", "yaml", "note"];
const FILE_ORDER = [
  "context.yaml",
  "semantic_types.yaml",
  "semantic_schema.yaml",
  "lookups.yaml",
  "glossary.yaml",
  "resources.yaml",
  "REVIEW.md",
] as const;

type ContextAction = "approve" | "reject" | "lock";

function statusTone(status: string) {
  if (status === "locked") return "border-success/30 bg-success/10 text-foreground";
  if (status === "approved") return "border-accent/30 bg-accent/10 text-foreground";
  if (status === "rejected") return "border-danger/30 bg-danger/10 text-foreground";
  if (status === "needs_review") return "border-warning/30 bg-warning/10 text-foreground";
  return "border-border bg-background text-muted";
}

function itemSummary(item: ProjectContextItem) {
  const value = item.value || {};
  if (item.item_type === "open_question") {
    return String(value.question || item.title || item.name);
  }
  if (typeof value.description === "string" && value.description.trim()) {
    return value.description;
  }
  if (typeof value.definition === "string" && value.definition.trim()) {
    return value.definition;
  }
  if (typeof value.role === "string" && typeof value.semantic_type === "string") {
    return `${value.semantic_type} / ${value.role}`;
  }
  if (typeof value.semantic_type === "string") {
    return value.semantic_type;
  }
  if (typeof value.label_column === "string" && typeof value.key_column === "string") {
    return `${value.key_column} -> ${value.label_column}`;
  }
  if (typeof value.relationship_type === "string") {
    return value.relationship_type;
  }
  return item.title || item.name;
}

function itemLocation(item: ProjectContextItem) {
  if (item.table_name && item.column_name) return `${item.table_name}.${item.column_name}`;
  if (item.table_name) return item.table_name;
  return item.scope;
}

function itemPriority(item: ProjectContextItem, selectedTable: string | null) {
  const selectedBoost = item.table_name && item.table_name === selectedTable ? -4 : 0;
  const statusRank =
    item.status === "needs_review"
      ? 0
      : item.status === "proposed"
        ? 1
        : item.status === "approved"
          ? 2
          : item.status === "locked"
            ? 3
            : 4;
  const questionBoost = item.item_type === "open_question" ? -2 : 0;
  return selectedBoost + questionBoost + statusRank + (item.confidence >= 0.9 ? 0 : 1);
}

export function ProjectContextReview({
  projectId,
  selectedTable,
  onChanged,
}: {
  projectId: string | null;
  selectedTable: string | null;
  onChanged?: () => Promise<void> | void;
}) {
  const [context, setContext] = useState<ProjectContextResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");
  const [busyItemId, setBusyItemId] = useState<string | null>(null);
  const [selectedTableOnly, setSelectedTableOnly] = useState(true);
  const [resourceType, setResourceType] = useState("url");
  const [resourceTitle, setResourceTitle] = useState("");
  const [resourceLocation, setResourceLocation] = useState("");
  const [resourceUseFor, setResourceUseFor] = useState("");
  const [exportFiles, setExportFiles] = useState<Record<string, string> | null>(null);
  const [selectedFile, setSelectedFile] = useState<string>("context.yaml");
  const [fileEditor, setFileEditor] = useState("");
  const [loadingFiles, setLoadingFiles] = useState(false);

  useEffect(() => {
    let cancelled = false;
    if (!projectId) {
      setContext(null);
      setExportFiles(null);
      setFileEditor("");
      return () => {
        cancelled = true;
      };
    }
    setLoading(true);
    api.projectContext(projectId)
      .then((payload) => {
        if (cancelled) return;
        setContext(payload);
      })
      .catch((error) => {
        if (!cancelled) {
          setMessage(error instanceof Error ? error.message : "Failed to load project context.");
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [projectId]);

  useEffect(() => {
    if (!exportFiles) return;
    const current = exportFiles[selectedFile];
    if (typeof current === "string") setFileEditor(current);
  }, [exportFiles, selectedFile]);

  useEffect(() => {
    if (!selectedTable) setSelectedTableOnly(false);
  }, [selectedTable]);

  const visibleItems = useMemo(() => {
    if (!context) return [];
    const base = context.items.filter((item) => {
      if (!selectedTableOnly || !selectedTable) return true;
      return item.table_name === selectedTable;
    });
    return [...base].sort(
      (left, right) => itemPriority(left, selectedTable) - itemPriority(right, selectedTable)
    );
  }, [context, selectedTable, selectedTableOnly]);

  const openQuestions = visibleItems.filter((item) => item.item_type === "open_question");
  const attentionItems = visibleItems.filter(
    (item) => item.status === "proposed" || item.status === "needs_review"
  );
  const previewItems = visibleItems.slice(0, 12);

  async function refreshContext() {
    if (!projectId) return;
    const payload = await api.projectContext(projectId);
    setContext(payload);
  }

  async function notifyChanged(nextMessage: string) {
    await refreshContext();
    setMessage(nextMessage);
    if (onChanged) await onChanged();
    setTimeout(() => setMessage(""), 3000);
  }

  async function handleDecision(item: ProjectContextItem, action: ContextAction) {
    if (!projectId) return;
    setBusyItemId(item.id);
    try {
      await api.decideProjectContextItem(projectId, item.id, action, {
        reason: `${action}d from the discovery review panel`,
      });
      await notifyChanged(`Context item ${action}d.`);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : `Failed to ${action} item.`);
    }
    setBusyItemId(null);
  }

  async function handleAddResource() {
    if (!projectId || !resourceTitle.trim()) return;
    setLoading(true);
    try {
      await api.addProjectContextResource(projectId, {
        resource_type: resourceType,
        title: resourceTitle.trim(),
        location: resourceLocation.trim() || null,
        metadata: {
          use_for: resourceUseFor
            .split(",")
            .map((value) => value.trim())
            .filter(Boolean),
        },
      });
      setResourceTitle("");
      setResourceLocation("");
      setResourceUseFor("");
      await notifyChanged("Context resource added.");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Failed to add resource.");
    }
    setLoading(false);
  }

  async function handleLoadFiles() {
    if (!projectId) return;
    setLoadingFiles(true);
    try {
      const exported = await api.exportProjectContext(projectId, true);
      setExportFiles(exported.files);
      const initialFile = FILE_ORDER.find((name) => exported.files[name]) || Object.keys(exported.files)[0];
      if (initialFile) {
        setSelectedFile(initialFile);
        setFileEditor(exported.files[initialFile]);
      }
      setMessage("Context files loaded.");
      setTimeout(() => setMessage(""), 3000);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Failed to load context files.");
    }
    setLoadingFiles(false);
  }

  async function handleImportFiles() {
    if (!projectId || !exportFiles) return;
    setLoadingFiles(true);
    try {
      const nextFiles = {
        ...exportFiles,
        [selectedFile]: fileEditor,
      };
      const imported = await api.importProjectContext(projectId, {
        files: nextFiles,
      });
      setExportFiles(nextFiles);
      setContext(imported.context);
      if (onChanged) await onChanged();
      setMessage("Context files imported.");
      setTimeout(() => setMessage(""), 3000);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Failed to import context files.");
    }
    setLoadingFiles(false);
  }

  if (!projectId) return null;

  return (
    <div className="mb-6 bg-card border border-border rounded-lg p-5">
      <div className="flex items-start justify-between gap-4 mb-4">
        <div>
          <h2 className="text-sm font-semibold text-muted uppercase tracking-wide">
            Project Context
          </h2>
          <div className="text-xs text-muted mt-1">
            Review generated context, attach resources, and round-trip machine-readable files.
          </div>
        </div>
        <div className="flex items-center gap-3">
          {message && <span className="text-xs text-muted">{message}</span>}
          {selectedTable && (
            <button
              onClick={() => setSelectedTableOnly((value) => !value)}
              className={`px-3 py-1.5 border rounded text-xs font-medium ${
                selectedTableOnly ? "border-accent/40 bg-accent/10" : "border-border"
              }`}
            >
              {selectedTableOnly ? `Showing ${selectedTable} only` : "Show all context"}
            </button>
          )}
          <button
            onClick={() => refreshContext().catch(() => undefined)}
            disabled={loading}
            className="px-3 py-1.5 border border-border rounded text-xs font-medium hover:border-foreground disabled:opacity-50"
          >
            Refresh
          </button>
        </div>
      </div>

      {loading && !context ? (
        <div className="text-sm text-muted">Loading project context...</div>
      ) : context ? (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
            <div className="rounded border border-border bg-background px-3 py-2">
              <div className="text-[10px] uppercase tracking-wide text-muted">Items</div>
              <div className="text-lg font-semibold">{context.summary.item_count}</div>
            </div>
            <div className="rounded border border-border bg-background px-3 py-2">
              <div className="text-[10px] uppercase tracking-wide text-muted">Attention</div>
              <div className="text-lg font-semibold">{attentionItems.length}</div>
            </div>
            <div className="rounded border border-border bg-background px-3 py-2">
              <div className="text-[10px] uppercase tracking-wide text-muted">Open Questions</div>
              <div className="text-lg font-semibold">{openQuestions.length}</div>
            </div>
            <div className="rounded border border-border bg-background px-3 py-2">
              <div className="text-[10px] uppercase tracking-wide text-muted">Resources</div>
              <div className="text-lg font-semibold">{context.resources.length}</div>
            </div>
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.45fr)_minmax(320px,1fr)] gap-4">
            <div className="space-y-4">
              <div className="rounded-lg border border-border bg-background p-4">
                <div className="flex items-center justify-between mb-3">
                  <div>
                    <h3 className="text-sm font-semibold">Review Queue</h3>
                    <div className="text-xs text-muted">
                      Highest-priority items from canonical project context.
                    </div>
                  </div>
                  <div className="text-xs text-muted">
                    {visibleItems.length} visible
                  </div>
                </div>
                <div className="space-y-3 max-h-[32rem] overflow-y-auto pr-1">
                  {previewItems.map((item) => (
                    <div key={item.id} className="rounded border border-border p-3">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="text-sm font-medium">{item.title || item.name}</div>
                          <div className="text-[11px] text-muted mt-0.5">
                            {item.item_type} · {itemLocation(item)} · confidence{" "}
                            {Math.round(item.confidence * 100)}%
                          </div>
                        </div>
                        <span
                          className={`inline-flex px-2 py-0.5 rounded border text-[10px] uppercase tracking-wide ${statusTone(item.status)}`}
                        >
                          {item.status.replace("_", " ")}
                        </span>
                      </div>
                      <div className="text-sm mt-2">{itemSummary(item)}</div>
                      {item.evidence.length > 0 && (
                        <div className="mt-2 text-[11px] text-muted">
                          Evidence: {item.evidence.slice(0, 2).map((entry) => entry.summary).join(" · ")}
                        </div>
                      )}
                      <div className="flex flex-wrap items-center gap-2 mt-3">
                        <button
                          onClick={() => handleDecision(item, "approve")}
                          disabled={busyItemId === item.id}
                          className="px-2.5 py-1 rounded bg-accent text-white text-[11px] font-medium disabled:opacity-50"
                        >
                          Approve
                        </button>
                        <button
                          onClick={() => handleDecision(item, "lock")}
                          disabled={busyItemId === item.id}
                          className="px-2.5 py-1 rounded border border-success/30 bg-success/10 text-[11px] font-medium disabled:opacity-50"
                        >
                          Lock
                        </button>
                        <button
                          onClick={() => handleDecision(item, "reject")}
                          disabled={busyItemId === item.id}
                          className="px-2.5 py-1 rounded border border-danger/30 bg-danger/10 text-[11px] font-medium disabled:opacity-50"
                        >
                          Reject
                        </button>
                      </div>
                    </div>
                  ))}
                  {previewItems.length === 0 && (
                    <div className="text-sm text-muted">No context items matched the current filter.</div>
                  )}
                </div>
              </div>

              <details className="rounded-lg border border-border bg-background p-4">
                <summary className="cursor-pointer text-sm font-semibold">
                  Context Files
                </summary>
                <div className="mt-4">
                  <div className="flex flex-wrap items-center gap-2 mb-3">
                    <button
                      onClick={handleLoadFiles}
                      disabled={loadingFiles}
                      className="px-3 py-1.5 border border-border rounded text-xs font-medium hover:border-foreground disabled:opacity-50"
                    >
                      Load Export
                    </button>
                    <button
                      onClick={handleImportFiles}
                      disabled={loadingFiles || !exportFiles}
                      className="px-3 py-1.5 bg-foreground text-background rounded text-xs font-medium disabled:opacity-50"
                    >
                      Apply Import
                    </button>
                    {exportFiles && (
                      <select
                        value={selectedFile}
                        onChange={(event) => setSelectedFile(event.target.value)}
                        className="px-2 py-1.5 border border-border rounded bg-card text-xs"
                      >
                        {Object.keys(exportFiles)
                          .sort((left, right) => {
                            const leftIndex = FILE_ORDER.indexOf(left as (typeof FILE_ORDER)[number]);
                            const rightIndex = FILE_ORDER.indexOf(right as (typeof FILE_ORDER)[number]);
                            return (leftIndex === -1 ? 99 : leftIndex) - (rightIndex === -1 ? 99 : rightIndex);
                          })
                          .map((name) => (
                            <option key={name} value={name}>
                              {name}
                            </option>
                          ))}
                      </select>
                    )}
                  </div>
                  <textarea
                    value={fileEditor}
                    onChange={(event) => setFileEditor(event.target.value)}
                    rows={14}
                    placeholder="Load exported context files to review or edit them here."
                    className="w-full px-3 py-2 border border-border rounded bg-card text-xs font-mono resize-y"
                  />
                </div>
              </details>
            </div>

            <div className="space-y-4">
              <div className="rounded-lg border border-border bg-background p-4">
                <h3 className="text-sm font-semibold mb-2">Resource Enrichment</h3>
                <div className="text-xs text-muted mb-3">
                  Attach dictionaries, notes, or URLs so this project context can be grounded in business material.
                </div>
                <div className="grid grid-cols-1 gap-3">
                  <label className="text-xs text-muted">
                    Resource type
                    <select
                      value={resourceType}
                      onChange={(event) => setResourceType(event.target.value)}
                      className="mt-1 w-full px-2 py-1.5 border border-border rounded bg-card text-sm"
                    >
                      {RESOURCE_TYPES.map((option) => (
                        <option key={option} value={option}>
                          {option}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="text-xs text-muted">
                    Title
                    <input
                      value={resourceTitle}
                      onChange={(event) => setResourceTitle(event.target.value)}
                      placeholder="Business glossary, SOP, metric notes"
                      className="mt-1 w-full px-2 py-1.5 border border-border rounded bg-card text-sm"
                    />
                  </label>
                  <label className="text-xs text-muted">
                    Location
                    <input
                      value={resourceLocation}
                      onChange={(event) => setResourceLocation(event.target.value)}
                      placeholder="https://..., docs/glossary.md, /shared/metrics.pdf"
                      className="mt-1 w-full px-2 py-1.5 border border-border rounded bg-card text-sm"
                    />
                  </label>
                  <label className="text-xs text-muted">
                    Use for
                    <input
                      value={resourceUseFor}
                      onChange={(event) => setResourceUseFor(event.target.value)}
                      placeholder="glossary, semantic_roles, lookups"
                      className="mt-1 w-full px-2 py-1.5 border border-border rounded bg-card text-sm"
                    />
                  </label>
                </div>
                <button
                  onClick={handleAddResource}
                  disabled={loading || !resourceTitle.trim()}
                  className="mt-3 px-3 py-1.5 bg-foreground text-background rounded text-xs font-medium disabled:opacity-50"
                >
                  Add Resource
                </button>
              </div>

              <div className="rounded-lg border border-border bg-background p-4">
                <div className="flex items-center justify-between mb-2">
                  <h3 className="text-sm font-semibold">Attached Resources</h3>
                  <span className="text-xs text-muted">{context.resources.length}</span>
                </div>
                <div className="space-y-2 max-h-56 overflow-y-auto pr-1">
                  {context.resources.map((resource) => (
                    <div key={resource.id} className="rounded border border-border px-3 py-2">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="text-sm font-medium">{resource.title}</div>
                          <div className="text-[11px] text-muted">
                            {resource.resource_type}
                            {resource.location ? ` · ${resource.location}` : ""}
                          </div>
                        </div>
                        <span className={`inline-flex px-2 py-0.5 rounded border text-[10px] ${statusTone(resource.status)}`}>
                          {resource.status}
                        </span>
                      </div>
                    </div>
                  ))}
                  {context.resources.length === 0 && (
                    <div className="text-sm text-muted">No context resources have been added yet.</div>
                  )}
                </div>
              </div>

              <div className="rounded-lg border border-border bg-background p-4">
                <h3 className="text-sm font-semibold mb-2">Signals</h3>
                <div className="flex flex-wrap gap-2">
                  {Object.entries(context.summary.item_types)
                    .sort((left, right) => right[1] - left[1])
                    .slice(0, 6)
                    .map(([type, count]) => (
                      <span
                        key={type}
                        className="inline-flex items-center gap-2 rounded-full border border-border px-2.5 py-1 text-[11px]"
                      >
                        <span>{type}</span>
                        <span className="text-muted">{count}</span>
                      </span>
                    ))}
                </div>
                {context.dataset_contexts.length > 0 && (
                  <div className="mt-3 border-t border-border pt-3 text-xs text-muted space-y-1">
                    {context.dataset_contexts.map((datasetContext) => (
                      <div key={datasetContext.source_name}>
                        <span className="font-medium text-foreground">{datasetContext.source_name}</span>
                        {datasetContext.row_represents
                          ? ` · row = ${datasetContext.row_represents}`
                          : ""}
                        {datasetContext.time_grain
                          ? ` · grain = ${datasetContext.time_grain}`
                          : ""}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>
        </>
      ) : (
        <div className="text-sm text-muted">
          Run discovery or pipeline first to bootstrap project context.
        </div>
      )}
    </div>
  );
}
