"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { api, type Project } from "@/lib/api";

type ProjectContextValue = {
  projects: Project[];
  activeProject: Project | null;
  activeProjectId: string | null;
  loading: boolean;
  error: string | null;
  refreshProjects: () => Promise<void>;
  selectProject: (projectId: string) => void;
};

const STORAGE_KEY = "headwater.activeProjectId";

const ProjectContext = createContext<ProjectContextValue | null>(null);

export function ProjectProvider({ children }: { children: ReactNode }) {
  const [projects, setProjects] = useState<Project[]>([]);
  const [activeProjectId, setActiveProjectId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refreshProjects = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await api.projects();
      const nextProjects = response.projects || [];
      setProjects(nextProjects);
      setActiveProjectId((current) => {
        const stored =
          typeof window === "undefined"
            ? null
            : window.localStorage.getItem(STORAGE_KEY);
        const requested = current || stored;
        const exists = nextProjects.some((project) => project.id === requested);
        const nextId = exists ? requested : nextProjects[0]?.id ?? null;
        if (typeof window !== "undefined") {
          if (nextId) window.localStorage.setItem(STORAGE_KEY, nextId);
          else window.localStorage.removeItem(STORAGE_KEY);
        }
        return nextId;
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refreshProjects();
  }, [refreshProjects]);

  const selectProject = useCallback((projectId: string) => {
    if (!projectId) return;
    setActiveProjectId(projectId);
    window.localStorage.setItem(STORAGE_KEY, projectId);
  }, []);

  const activeProject = useMemo(
    () => projects.find((project) => project.id === activeProjectId) ?? null,
    [activeProjectId, projects]
  );

  const value = useMemo(
    () => ({
      projects,
      activeProject,
      activeProjectId,
      loading,
      error,
      refreshProjects,
      selectProject,
    }),
    [
      activeProject,
      activeProjectId,
      error,
      loading,
      projects,
      refreshProjects,
      selectProject,
    ]
  );

  return (
    <ProjectContext.Provider value={value}>{children}</ProjectContext.Provider>
  );
}

export function useProjects() {
  const context = useContext(ProjectContext);
  if (!context) {
    throw new Error("useProjects must be used within ProjectProvider");
  }
  return context;
}
