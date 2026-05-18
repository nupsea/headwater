"use client";

import Link from "next/link";
import { useState } from "react";
import { api, type Project } from "@/lib/api";
import { useProjects } from "@/lib/project-context";
import { CreateProjectDialog } from "@/components/create-project-dialog";
import { EditProjectDialog } from "@/components/edit-project-dialog";
import { useToast } from "@/components/toast";

const MATURITY_LABEL: Record<string, string> = {
  raw: "Raw",
  profiled: "Profiled",
  documented: "Documented",
  modeled: "Modeled",
  production: "Production",
};

export default function ProjectsPage() {
  const { toast } = useToast();
  const {
    projects,
    activeProjectId,
    loading,
    error,
    refreshProjects,
    selectProject,
  } = useProjects();
  const [showCreate, setShowCreate] = useState(false);
  const [editing, setEditing] = useState<Project | null>(null);
  const [deletingId, setDeletingId] = useState<string | null>(null);

  const startEdit = (project: Project) => {
    setEditing(project);
  };

  const removeProject = async (project: Project) => {
    if (
      !confirm(
        `Delete project "${project.display_name}"? This removes Headwater project metadata and catalog entries.`
      )
    ) {
      return;
    }
    setDeletingId(project.id);
    try {
      await api.deleteProject(project.id);
      toast("Project deleted", "success");
      await refreshProjects();
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      toast(`Delete failed: ${message}`, "error");
    } finally {
      setDeletingId(null);
    }
  };

  return (
    <div className="max-w-6xl">
      <div className="flex items-start justify-between gap-4 mb-6">
        <div>
          <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-1">
            Workspace
          </div>
          <h1 className="text-2xl font-bold tracking-tight">Projects</h1>
          <p className="text-sm text-muted mt-1">
            Start a guided project setup, connect a source, and ingest with safe defaults.
          </p>
        </div>
        <button
          onClick={() => setShowCreate(true)}
          className="px-4 py-2 bg-accent text-white rounded-md text-sm font-medium hover:opacity-90"
        >
          Start setup
        </button>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900 rounded-lg text-sm text-red-700 dark:text-red-400">
          Failed to load projects: {error}
        </div>
      )}

      {loading ? (
        <div className="bg-card border border-border rounded-lg p-6 text-sm text-muted">
          Loading projects...
        </div>
      ) : projects.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-8">
          <h2 className="text-lg font-semibold mb-1">No projects yet</h2>
          <p className="text-sm text-muted mb-4">
            Create a project, connect a source, and begin the ingestion workflow.
          </p>
          <button
            onClick={() => setShowCreate(true)}
            className="px-4 py-2 bg-accent text-white rounded-md text-sm font-medium"
          >
            Start setup
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
          {projects.map((project) => {
            const active = project.id === activeProjectId;
            return (
              <div
                key={project.id}
                className={`bg-card border rounded-lg p-4 transition-colors ${
                  active ? "border-accent ring-2 ring-accent/15" : "border-border"
                }`}
              >
                <div className="flex items-start justify-between gap-3 mb-4">
                  <div className="min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <h2 className="text-base font-semibold truncate">
                        {project.display_name}
                      </h2>
                      {active && (
                        <span className="text-[9px] uppercase tracking-wider border border-accent/30 text-accent rounded px-1.5 py-0.5">
                          Active
                        </span>
                      )}
                    </div>
                    <p className="text-xs text-muted line-clamp-2">
                      {project.description || "No description"}
                    </p>
                  </div>
                  <span className="text-[10px] uppercase tracking-wider text-muted bg-background border border-border rounded px-2 py-1 shrink-0">
                    {MATURITY_LABEL[project.maturity] ?? project.maturity}
                  </span>
                </div>

                <div className="grid grid-cols-4 gap-3 mb-4">
                  <Metric
                    label="Maturity"
                    value={`${Math.round(project.maturity_score * 100)}%`}
                  />
                  <Metric
                    label="Catalog"
                    value={`${Math.round(project.catalog_confidence * 100)}%`}
                  />
                  <Metric label="Sources" value={project.sources.length} />
                  <Metric
                    label="Updated"
                    value={new Date(project.updated_at).toLocaleDateString()}
                  />
                </div>

                <div className="mb-4 border-t border-border pt-3">
                  <div className="text-[9px] font-bold uppercase tracking-wider text-muted mb-1.5">
                    Current state
                  </div>
                  <Link
                    href="/health"
                    onClick={() => selectProject(project.id)}
                    className="block rounded border border-transparent px-2 py-1 text-[11px] text-muted hover:border-border hover:bg-background"
                  >
                    <span className="font-medium text-foreground">Health view:</span> open the
                    active project dashboard for live blockers, review counts, and quality details.
                  </Link>
                </div>

                <div className="flex flex-wrap gap-2">
                  <button
                    onClick={() => selectProject(project.id)}
                    disabled={active}
                    className="px-3 py-1.5 bg-accent text-white rounded-md text-xs font-medium hover:opacity-90 disabled:opacity-50"
                  >
                    {active ? "Selected" : "Switch to project"}
                  </button>
                  <button
                    onClick={() => startEdit(project)}
                    className="px-3 py-1.5 border border-border rounded-md text-xs font-medium hover:bg-background"
                  >
                    Edit project
                  </button>
                  <button
                    onClick={() => removeProject(project)}
                    disabled={deletingId === project.id}
                    className="px-3 py-1.5 border border-red-200 dark:border-red-900 text-red-600 dark:text-red-400 rounded-md text-xs font-medium disabled:opacity-50"
                  >
                    {deletingId === project.id ? "Deleting..." : "Delete"}
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}

      <CreateProjectDialog
        open={showCreate}
        onClose={() => setShowCreate(false)}
        onCreated={async (project) => {
          selectProject(project.id);
          setShowCreate(false);
          await refreshProjects();
        }}
      />

      <EditProjectDialog
        open={Boolean(editing)}
        project={editing}
        onClose={() => setEditing(null)}
        onSaved={async (project) => {
          selectProject(project.id);
          setEditing(null);
          await refreshProjects();
        }}
      />
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string | number }) {
  return (
    <div>
      <div className="text-[9px] font-bold text-muted uppercase tracking-wider">
        {label}
      </div>
      <div className="text-[13px] font-mono font-medium mt-0.5">{value}</div>
    </div>
  );
}
