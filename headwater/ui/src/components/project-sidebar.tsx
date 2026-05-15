"use client";

import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import { api, type Project, type ProjectProgress } from "@/lib/api";
import { CreateProjectDialog } from "@/components/create-project-dialog";
import { useProjects } from "@/lib/project-context";

const MATURITY_COLORS: Record<string, string> = {
  raw: "bg-gray-200 text-gray-700 dark:bg-gray-700 dark:text-gray-300",
  profiled: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300",
  documented: "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300",
  modeled: "bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300",
  production: "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300",
};

const MATURITY_WIDTHS: Record<string, string> = {
  raw: "w-1/5",
  profiled: "w-2/5",
  documented: "w-3/5",
  modeled: "w-4/5",
  production: "w-full",
};

export function ProjectSidebar() {
  const [progressMap, setProgressMap] = useState<
    Record<string, ProjectProgress>
  >({});
  const [collapsed, setCollapsed] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const pathname = usePathname();
  const {
    projects,
    activeProjectId,
    refreshProjects,
    selectProject,
  } = useProjects();

  useEffect(() => {
    // Fetch progress for each project
    projects.forEach((p) => {
      api
        .projectProgress(p.id)
        .then((r) =>
          setProgressMap((prev) => ({ ...prev, [p.id]: r.progress }))
        )
        .catch(() => {});
    });
  }, [pathname]); // refresh on navigation

  if (collapsed) {
    return (
      <div className="w-10 border-r border-border bg-card flex flex-col items-center pt-3 shrink-0">
        <button
          onClick={() => setCollapsed(false)}
          className="text-muted hover:text-foreground text-xs p-1"
          title="Expand sidebar"
        >
          &raquo;
        </button>
        {projects.map((p) => (
          <button
            key={p.id}
            onClick={() => selectProject(p.id)}
            className={`w-6 h-6 rounded-full flex items-center justify-center text-[9px] font-bold mt-2 ${
              p.id === activeProjectId
                ? "bg-accent text-white"
                : "bg-accent/20 text-accent"
            }`}
            title={p.display_name}
          >
            {p.display_name.charAt(0).toUpperCase()}
          </button>
        ))}
      </div>
    );
  }

  return (
    <aside className="w-56 border-r border-border bg-card shrink-0 flex flex-col overflow-hidden">
      <div className="px-3 py-3 border-b border-border flex items-center justify-between">
        <span className="text-xs font-semibold text-muted uppercase tracking-wider">
          Projects
        </span>
        <div className="flex items-center gap-1">
          <button
            onClick={() => setShowCreate(true)}
            className="text-accent hover:text-accent/80 text-xs font-medium"
            title="Start setup"
          >
            Start setup
          </button>
          <button
            onClick={() => setCollapsed(true)}
            className="text-muted hover:text-foreground text-xs"
            title="Collapse sidebar"
          >
            &laquo;
          </button>
        </div>
      </div>

      <CreateProjectDialog
        open={showCreate}
        onClose={() => setShowCreate(false)}
        onCreated={async (proj) => {
          selectProject(proj.id);
          await refreshProjects();
          setShowCreate(false);
        }}
      />

      {projects.length === 0 && (
        <div className="px-3 py-6 text-center">
          <p className="text-xs text-muted">No projects yet.</p>
          <p className="text-xs text-muted mt-1">
            Click Start setup or run a pipeline to create one.
          </p>
        </div>
      )}

      <div className="flex-1 overflow-y-auto">
        {projects.map((p) => {
          const prog = progressMap[p.id];
          return (
            <button
              key={p.id}
              onClick={() => selectProject(p.id)}
              className={`w-full px-3 py-3 border-b border-border hover:bg-background transition-colors text-left ${
                p.id === activeProjectId ? "bg-accent/5" : ""
              }`}
            >
              <div className="flex items-center justify-between mb-1">
                <span
                  className={`text-sm font-medium truncate ${
                    p.id === activeProjectId ? "text-accent" : ""
                  }`}
                  title={p.display_name}
                >
                  {p.display_name}
                </span>
                <span
                  className={`text-[9px] px-1.5 py-0.5 rounded-full font-medium ${
                    MATURITY_COLORS[p.maturity] || MATURITY_COLORS.raw
                  }`}
                >
                  {p.maturity}
                </span>
              </div>

              {/* Maturity progress bar */}
              <div className="h-1 bg-border rounded-full overflow-hidden mb-1.5">
                <div
                  className={`h-full bg-accent rounded-full ${
                    MATURITY_WIDTHS[p.maturity] || "w-0"
                  }`}
                />
              </div>

              {/* Progress counters */}
              {prog ? (
                <div className="space-y-0.5">
                  <div className="flex items-center justify-between text-[10px] text-muted">
                    <span>Tables reviewed</span>
                    <span className="font-mono">
                      {prog.tables_reviewed}/{prog.tables_discovered}
                    </span>
                  </div>
                  <div className="flex items-center justify-between text-[10px] text-muted">
                    <span>Catalog</span>
                    <span className="font-mono">
                      {Math.round(p.catalog_confidence * 100)}% conf |{" "}
                      {Math.round(prog.catalog_coverage * 100)}% cov
                    </span>
                  </div>
                  {prog.metrics_defined > 0 && (
                    <div className="flex items-center justify-between text-[10px] text-muted">
                      <span>Metrics</span>
                      <span className="font-mono">
                        {prog.metrics_confirmed ?? 0}/{prog.metrics_defined}
                      </span>
                    </div>
                  )}
                </div>
              ) : (
                <div className="flex items-center justify-between text-[10px] text-muted">
                  <span>Confidence: {Math.round(p.catalog_confidence * 100)}%</span>
                  <span>Score: {Math.round(p.maturity_score * 100)}%</span>
                </div>
              )}
            </button>
          );
        })}
      </div>
    </aside>
  );
}
