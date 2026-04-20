"use client";

import { useState } from "react";
import { api, type Project, type ConnectionTestResult } from "@/lib/api";

interface CreateProjectDialogProps {
  open: boolean;
  onClose: () => void;
  onCreated: (project: Project) => void;
}

export function CreateProjectDialog({
  open,
  onClose,
  onCreated,
}: CreateProjectDialogProps) {
  const [displayName, setDisplayName] = useState("");
  const [sourcePath, setSourcePath] = useState("");
  const [description, setDescription] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [connStatus, setConnStatus] = useState<ConnectionTestResult | null>(null);
  const [connTesting, setConnTesting] = useState(false);

  const isDbUri = (s: string) =>
    ["postgresql://", "postgres://", "mysql://", "sqlite://"].some((p) =>
      s.startsWith(p)
    );

  const testConn = async () => {
    if (!sourcePath.trim() || !isDbUri(sourcePath.trim())) {
      setConnStatus(null);
      return;
    }
    setConnTesting(true);
    try {
      const result = await api.testConnection(sourcePath.trim());
      setConnStatus(result);
    } catch {
      setConnStatus({
        status: "error",
        source_type: "unknown",
        tables: 0,
        table_names: [],
        detail: "Backend unreachable.",
      });
    }
    setConnTesting(false);
  };

  if (!open) return null;

  const handleCreate = async () => {
    if (!displayName.trim()) {
      setError("Project name is required.");
      return;
    }
    setSaving(true);
    setError("");
    try {
      const project = await api.createProject({
        display_name: displayName.trim(),
        source_path: sourcePath.trim() || undefined,
        description: description.trim() || undefined,
      });
      setDisplayName("");
      setSourcePath("");
      setDescription("");
      onCreated(project);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setSaving(false);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-card border border-border rounded-lg shadow-lg w-full max-w-md p-6">
        <h2 className="text-lg font-semibold mb-4">New Project</h2>

        {error && (
          <div className="mb-3 p-2 bg-red-50 border border-red-200 rounded text-sm text-red-700">
            {error}
          </div>
        )}

        <div className="space-y-3">
          <div>
            <label className="block text-xs text-muted mb-1">
              Project Name *
            </label>
            <input
              type="text"
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              placeholder="My Data Project"
              className="w-full px-3 py-2 border border-border rounded bg-background text-sm"
              autoFocus
            />
          </div>
          <div>
            <label className="block text-xs text-muted mb-1">
              Data Source (path or DSN)
            </label>
            <div className="flex gap-2">
              <input
                type="text"
                value={sourcePath}
                onChange={(e) => {
                  setSourcePath(e.target.value);
                  setConnStatus(null);
                }}
                onBlur={testConn}
                placeholder="/path/to/data or postgresql://..."
                className="flex-1 px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
              {isDbUri(sourcePath) && (
                <button
                  type="button"
                  onClick={testConn}
                  disabled={connTesting}
                  className="px-3 py-2 border border-border rounded text-xs font-medium hover:bg-muted/20 disabled:opacity-50 shrink-0"
                >
                  {connTesting ? "..." : "Test"}
                </button>
              )}
            </div>

            {/* Connection split view for DB sources */}
            {connStatus && isDbUri(sourcePath) && (
              <div className="mt-2">
                <div className={`text-xs px-2 py-1.5 rounded border ${
                  connStatus.status === "ok"
                    ? "bg-green-50 dark:bg-green-950/30 border-green-200 dark:border-green-800 text-green-700 dark:text-green-400"
                    : "bg-red-50 dark:bg-red-950/30 border-red-200 dark:border-red-800 text-red-700 dark:text-red-400"
                }`}>
                  <div className="font-medium mb-1">
                    {connStatus.status === "ok" ? "Connected" : "Connection failed"}
                  </div>
                  <div className="text-[11px] opacity-80">{connStatus.detail}</div>
                </div>
                {connStatus.status === "ok" && (() => {
                  try {
                    const url = new URL(sourcePath);
                    return (
                      <div className="mt-1.5 grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] text-muted px-1">
                        <span>Host: <span className="font-mono">{url.hostname}</span></span>
                        <span>Port: <span className="font-mono">{url.port || "5432"}</span></span>
                        <span>Database: <span className="font-mono">{url.pathname.replace("/", "")}</span></span>
                        <span>Tables: <span className="font-mono">{connStatus.tables}</span></span>
                      </div>
                    );
                  } catch {
                    return null;
                  }
                })()}
              </div>
            )}
          </div>
          <div>
            <label className="block text-xs text-muted mb-1">
              Description
            </label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Optional project description"
              rows={2}
              className="w-full px-3 py-2 border border-border rounded bg-background text-sm resize-none"
            />
          </div>
        </div>

        <div className="flex justify-end gap-2 mt-5">
          <button
            onClick={onClose}
            className="px-4 py-2 border border-border rounded text-sm text-muted hover:text-foreground transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleCreate}
            disabled={saving}
            className="px-4 py-2 bg-accent text-white rounded text-sm font-medium hover:opacity-90 disabled:opacity-50 transition-opacity"
          >
            {saving ? "Creating..." : "Create Project"}
          </button>
        </div>
      </div>
    </div>
  );
}
