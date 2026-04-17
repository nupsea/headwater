"use client";

import { useState } from "react";
import { api, type Project } from "@/lib/api";

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
            <input
              type="text"
              value={sourcePath}
              onChange={(e) => setSourcePath(e.target.value)}
              placeholder="/path/to/data or postgresql://..."
              className="w-full px-3 py-2 border border-border rounded bg-background text-sm font-mono"
            />
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
