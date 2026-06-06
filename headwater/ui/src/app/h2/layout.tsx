"use client";

import {
  createContext,
  useContext,
  useEffect,
  useState,
  useCallback,
} from "react";
import { usePathname, useRouter } from "next/navigation";
import Link from "next/link";
import {
  h2,
  HW2_INPUT_CHANGED,
  HW2_RECOMPUTED,
  notifyRecomputed,
  type H2Project,
  type H2Source,
} from "@/lib/h2api";
import { ReadinessRing, HW2_COLOR } from "@/components/h2/readiness-ring";
import { Stepper, type StageKey } from "@/components/h2/stepper";

// ─── Context ────────────────────────────────────────────────────────────────

interface H2ContextValue {
  projects: H2Project[];
  sources: H2Source[];
  reload: () => void;
  /** Whether a long LLM analysis is currently running (drives the global bar). */
  analysis: { active: boolean; label: string };
  /** Run an async op while showing the global "Analysing with AI" bar. Owned by
   *  the layout, so it survives client-side navigation to other H2 pages. */
  runAnalysis: <T>(label: string, fn: () => Promise<T>) => Promise<T>;
}

const H2Context = createContext<H2ContextValue>({
  projects: [],
  sources: [],
  reload: () => {},
  analysis: { active: false, label: "" },
  runAnalysis: (_label, fn) => fn(),
});

export function useH2Context() {
  return useContext(H2Context);
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function computeProjectReadout(project: H2Project) {
  // Verdict-based counts from the backend are the source of truth; fall back to
  // question.status only if they aren't present.
  const total =
    project.question_count ??
    (project.questions ?? []).filter((q) => q.answerability !== "cannot_answer").length;
  const certifiedCount =
    project.certified_count ??
    (project.questions ?? []).filter((q) => q.status === "certified").length;
  if (total === 0) return { pct: 0, certifiedCount: 0, total: 0 };
  const pct = Math.round((certifiedCount / total) * 100);
  return { pct, certifiedCount, total };
}

function stageFromPath(pathname: string): StageKey | null {
  if (pathname.endsWith("/understand")) return "understand";
  if (pathname.endsWith("/resolve")) return "resolve";
  if (pathname.endsWith("/readiness")) return "readiness";
  if (pathname.endsWith("/answer")) return "answer";
  // project root (the Frame home) or /new
  if (pathname.match(/\/h2\/projects\/[^/]+$/) && !pathname.endsWith("/new"))
    return "frame";
  return null;
}

function projectIdFromPath(pathname: string): string | null {
  const m = pathname.match(/\/h2\/projects\/([^/]+)/);
  if (!m) return null;
  if (m[1] === "new") return null;
  return m[1];
}

// ─── Icons ───────────────────────────────────────────────────────────────────

function TableIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
      <rect
        x="2"
        y="3"
        width="12"
        height="10"
        rx="1.2"
        stroke="currentColor"
        strokeWidth="1.3"
      />
      <line
        x1="2"
        y1="6.5"
        x2="14"
        y2="6.5"
        stroke="currentColor"
        strokeWidth="1.3"
      />
      <line
        x1="6"
        y1="6.5"
        x2="6"
        y2="13"
        stroke="currentColor"
        strokeWidth="1.3"
      />
    </svg>
  );
}

function QueryIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
      <path
        d="M3 5l3 3-3 3"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <line
        x1="8.5"
        y1="11.2"
        x2="13"
        y2="11.2"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
      />
    </svg>
  );
}

// ─── Rail sub-components ─────────────────────────────────────────────────────

function RailItem({
  active,
  onClick,
  icon,
  label,
  hint,
}: {
  active: boolean;
  onClick: () => void;
  icon: React.ReactNode;
  label: string;
  hint?: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      style={{
        appearance: "none",
        cursor: "pointer",
        width: "100%",
        background: active ? "#fff" : "transparent",
        border: active
          ? `1px solid ${HW2_COLOR.rule2}`
          : "1px solid transparent",
        borderRadius: 7,
        padding: "8px 9px",
        textAlign: "left",
        display: "flex",
        alignItems: "center",
        gap: 9,
        fontFamily: "'DM Sans', sans-serif",
        boxShadow: active ? "0 1px 0 rgba(20,20,30,0.02)" : "none",
      }}
    >
      <span
        style={{
          width: 20,
          height: 20,
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          color: active ? HW2_COLOR.ink : HW2_COLOR.muted,
          flexShrink: 0,
        }}
      >
        {icon}
      </span>
      <span style={{ flex: 1, minWidth: 0 }}>
        <div
          style={{
            font: `${active ? 600 : 500} 13px 'DM Sans', sans-serif`,
            color: active ? HW2_COLOR.ink : HW2_COLOR.ink2,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {label}
        </div>
        {hint != null && (
          <div
            style={{
              font: "400 10.5px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              marginTop: 1,
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {hint}
          </div>
        )}
      </span>
    </button>
  );
}

function RailSection({
  label,
  action,
  children,
}: {
  label: string;
  action?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div style={{ padding: "10px 8px 6px" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0 8px",
          marginBottom: 4,
        }}
      >
        <span
          style={{
            font: "600 10px 'DM Sans', sans-serif",
            color: HW2_COLOR.faint,
            letterSpacing: "0.08em",
            textTransform: "uppercase",
          }}
        >
          {label}
        </span>
        {action}
      </div>
      <div style={{ display: "grid", gap: 1 }}>{children}</div>
    </div>
  );
}

function RailDivider() {
  return (
    <div
      style={{
        height: 1,
        background: HW2_COLOR.rule,
        margin: "4px 14px",
      }}
    />
  );
}

// ─── Left Rail ───────────────────────────────────────────────────────────────

function H2Rail({
  sources,
  projects,
  pathname,
  onNavigate,
}: {
  sources: H2Source[];
  projects: H2Project[];
  pathname: string;
  onNavigate: (href: string) => void;
}) {
  const primarySource = sources[0] ?? null;
  const activeProjectId = projectIdFromPath(pathname);
  const { reload } = useH2Context();

  const deleteProject = async (p: H2Project) => {
    if (
      !window.confirm(
        `Delete project "${p.display_name}"?\n\nThis permanently removes its goal, ` +
          "questions, verdicts, and answers. The shared data source is not affected. " +
          "This cannot be undone."
      )
    )
      return;
    try {
      await h2.projects.remove(p.id);
      if (activeProjectId === p.id || activeProjectId === p.slug) onNavigate("/h2");
      reload();
    } catch (e) {
      alert(e instanceof Error ? e.message : "Failed to delete project");
    }
  };

  return (
    <aside
      style={{
        width: 248,
        flexShrink: 0,
        background: "#f3f1ea",
        borderRight: `1px solid ${HW2_COLOR.rule}`,
        display: "flex",
        flexDirection: "column",
        overflowY: "auto",
      }}
    >
      {/* Source switcher */}
      <div style={{ padding: "16px 14px 8px" }}>
        <div
          style={{
            font: "600 10px 'DM Sans', sans-serif",
            color: HW2_COLOR.faint,
            letterSpacing: "0.08em",
            textTransform: "uppercase",
            marginBottom: 6,
            padding: "0 4px",
          }}
        >
          Source
        </div>

        {primarySource ? (
          <button
            onClick={() => onNavigate(`/h2/sources/${encodeURIComponent(primarySource.name)}`)}
            style={{
              appearance: "none",
              cursor: "pointer",
              width: "100%",
              background: "#fff",
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 8,
              padding: "8px 10px",
              textAlign: "left",
              display: "flex",
              alignItems: "center",
              gap: 8,
              fontFamily: "'DM Sans', sans-serif",
            }}
          >
            <span
              style={{
                width: 6,
                height: 6,
                borderRadius: "50%",
                background: primarySource.latest_snapshot_id
                  ? HW2_COLOR.good
                  : HW2_COLOR.faint,
                flexShrink: 0,
              }}
            />
            <span style={{ flex: 1, minWidth: 0 }}>
              <div
                style={{
                  font: "600 13px 'DM Sans', sans-serif",
                  color: HW2_COLOR.ink,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
              >
                {primarySource.name}
              </div>
              <div
                style={{
                  font: "500 10px 'DM Mono', monospace",
                  color: HW2_COLOR.muted,
                  marginTop: 1,
                }}
              >
                {primarySource.type}
                {primarySource.latest_snapshot_id ? " · profiled" : " · not profiled"}
              </div>
            </span>
          </button>
        ) : (
          <div
            style={{
              padding: "8px 10px",
              background: HW2_COLOR.chip,
              border: `1px dashed ${HW2_COLOR.rule2}`,
              borderRadius: 8,
              font: "400 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.faint,
            }}
          >
            No sources yet
          </div>
        )}

        <button
          onClick={() => onNavigate("/h2/sources/new")}
          style={{
            appearance: "none",
            cursor: "pointer",
            width: "100%",
            background: "transparent",
            border: "none",
            padding: "6px 4px",
            textAlign: "left",
            marginTop: 4,
            font: "500 12px 'DM Sans', sans-serif",
            color: HW2_COLOR.muted,
          }}
        >
          + Connect source
          <span
            style={{
              marginLeft: 6,
              font: "400 10px 'DM Mono', monospace",
              color: HW2_COLOR.faint,
            }}
          >
            ({sources.length} connected)
          </span>
        </button>
      </div>

      <RailDivider />

      {/* Workspace tools */}
      <RailSection label="Workspace">
        <RailItem
          active={pathname.startsWith("/h2/sources/")}
          onClick={() =>
            onNavigate(
              primarySource
                ? `/h2/sources/${encodeURIComponent(primarySource.name)}`
                : "/h2/sources/new"
            )
          }
          icon={<TableIcon />}
          label="Catalog"
          hint={primarySource ? `${primarySource.name}` : "connect a source"}
        />
        <RailItem
          active={pathname === "/h2/query" || pathname.startsWith("/h2/query/")}
          onClick={() => onNavigate("/h2/query")}
          icon={<QueryIcon />}
          label="Query"
          hint="SQL console"
        />
      </RailSection>

      <RailDivider />

      {/* Projects */}
      <RailSection
        label="Projects"
        action={
          <button
            onClick={() => onNavigate("/h2/projects/new")}
            title="New project"
            style={{
              appearance: "none",
              background: "transparent",
              border: "none",
              cursor: "pointer",
              padding: "0 2px",
              color: HW2_COLOR.muted,
              font: "600 14px 'DM Sans', sans-serif",
              lineHeight: 1,
            }}
          >
            +
          </button>
        }
      >
        {projects.length === 0 ? (
          <div
            style={{
              padding: "6px 12px",
              font: "400 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.faint,
            }}
          >
            No projects yet.
          </div>
        ) : (
          projects.map((p) => {
            const ro = computeProjectReadout(p);
            const hasDrift = false; // drift info comes from per-project fetch
            const isActive =
              activeProjectId === p.id || activeProjectId === p.slug;

            return (
              <div key={p.id} className="hw2-proj-row" style={{ position: "relative" }}>
                <RailItem
                  active={isActive}
                  onClick={() => onNavigate(`/h2/projects/${p.id}`)}
                  icon={
                    <ReadinessRing
                      value={ro.pct}
                      certified={
                        ro.certifiedCount === ro.total &&
                        ro.total > 0 &&
                        !hasDrift
                      }
                      demoted={hasDrift}
                      size={18}
                      stroke={2.5}
                      showLabel={false}
                      animate={false}
                    />
                  }
                  label={p.display_name}
                  hint={`${p.questions?.length ?? 0} q · ${ro.certifiedCount}/${ro.total} cert.`}
                />
                <button
                  className="hw2-del"
                  title="Delete project"
                  aria-label={`Delete project ${p.display_name}`}
                  onClick={(e) => {
                    e.stopPropagation();
                    deleteProject(p);
                  }}
                  style={{
                    position: "absolute",
                    top: "50%",
                    right: 6,
                    transform: "translateY(-50%)",
                    appearance: "none",
                    cursor: "pointer",
                    background: "#fff",
                    border: `1px solid ${HW2_COLOR.rule2}`,
                    borderRadius: 6,
                    width: 24,
                    height: 24,
                    display: "inline-flex",
                    alignItems: "center",
                    justifyContent: "center",
                    color: HW2_COLOR.muted,
                    padding: 0,
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.color = HW2_COLOR.bad;
                    e.currentTarget.style.borderColor = HW2_COLOR.bad + "66";
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.color = HW2_COLOR.muted;
                    e.currentTarget.style.borderColor = HW2_COLOR.rule2;
                  }}
                >
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" aria-hidden>
                    <path
                      d="M3 6h18M8 6V4h8v2m-9 0v14a1 1 0 001 1h8a1 1 0 001-1V6M10 11v6M14 11v6"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                </button>
              </div>
            );
          })
        )}
        <div
          style={{
            padding: "6px 12px 0",
            font: "400 10px 'DM Mono', monospace",
            color: HW2_COLOR.faint,
          }}
        >
          {projects.length} project{projects.length !== 1 ? "s" : ""}
        </div>
      </RailSection>

      <div style={{ flex: 1 }} />

      {/* Footer hint */}
      <div
        style={{
          padding: "12px 14px",
          borderTop: `1px solid ${HW2_COLOR.rule}`,
        }}
      >
        <div
          style={{
            font: "400 11px 'DM Sans', sans-serif",
            color: HW2_COLOR.faint,
            lineHeight: 1.5,
          }}
        >
          Catalog & Query are power tools. Projects are the guided workflow.
        </div>
      </div>
    </aside>
  );
}

// ─── Project Banner ──────────────────────────────────────────────────────────

function ProjectBanner({
  project,
  stage,
  onJumpStage,
}: {
  project: H2Project;
  stage: StageKey;
  onJumpStage: (s: StageKey) => void;
}) {
  const ro = computeProjectReadout(project);
  const allCertified =
    ro.total > 0 && ro.certifiedCount === ro.total;

  return (
    <section
      style={{
        padding: "20px 32px 14px",
        background: HW2_COLOR.paper,
        borderBottom: `1px solid ${HW2_COLOR.rule}`,
        position: "sticky",
        top: 0,
        zIndex: 5,
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 22,
          marginBottom: 14,
        }}
      >
        <ReadinessRing
          value={ro.pct}
          certified={allCertified}
          demoted={false}
          size={56}
          stroke={5}
          showLabel={false}
        />
        <div style={{ flex: 1, minWidth: 0 }}>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 10,
              marginBottom: 4,
              flexWrap: "wrap",
            }}
          >
            <span
              style={{
                font: "600 10px 'DM Sans', sans-serif",
                color: HW2_COLOR.muted,
                letterSpacing: "0.07em",
                textTransform: "uppercase",
              }}
            >
              Project goal
            </span>
            <span
              style={{
                font: "500 11px 'DM Mono', monospace",
                color: HW2_COLOR.muted,
              }}
            >
              {ro.certifiedCount}/{ro.total} certified
              {ro.total > 0 && (
                <span style={{ color: HW2_COLOR.faint }}>
                  {" "}
                  · {ro.pct}% evidence cleared
                </span>
              )}
            </span>
          </div>
          <h1
            style={{
              font: "600 21px 'DM Sans', sans-serif",
              letterSpacing: "-0.02em",
              color: HW2_COLOR.ink,
              lineHeight: 1.25,
              margin: 0,
            }}
          >
            {project.goal?.statement || project.display_name}
          </h1>
        </div>
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "flex-end",
            gap: 4,
          }}
        >
          <span
            style={{
              font: "500 11px 'DM Mono', monospace",
              color: HW2_COLOR.faint,
            }}
          >
            {(project.sources?.[0]?.selected_tables ?? []).length > 0
              ? `${project.sources![0].selected_tables.length} tables`
              : ""}
          </span>
          <span
            style={{
              font: "400 11px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
            }}
          >
            {new Date(project.updated_at).toLocaleDateString()}
          </span>
        </div>
      </div>

      <Stepper
        current={stage}
        projectId={project.id}
        onJump={onJumpStage}
      />
    </section>
  );
}

// ─── Recompute banner (staged) ───────────────────────────────────────────────

/** Global indeterminate progress strip shown while the LLM is analysing. Rendered
 *  in the persistent layout, so it stays put when the user switches pages. */
function AnalysisBar({ active, label }: { active: boolean; label: string }) {
  if (!active) return null;
  return (
    <div style={{ position: "sticky", top: 0, zIndex: 30 }}>
      <div
        style={{
          position: "relative",
          height: 3,
          background: HW2_COLOR.blue + "22",
          overflow: "hidden",
        }}
      >
        <div
          style={{
            position: "absolute",
            top: 0,
            bottom: 0,
            background: HW2_COLOR.blue,
            borderRadius: 2,
            animation: "hw2-indeterminate 1.15s ease-in-out infinite",
          }}
        />
      </div>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "7px 32px",
          background: HW2_COLOR.blue + "0e",
          borderBottom: `1px solid ${HW2_COLOR.blue}22`,
          font: "600 12px 'DM Sans', sans-serif",
        }}
      >
        <span
          aria-hidden
          style={{ color: HW2_COLOR.blue, animation: "hw2-pulse 1.4s ease-in-out infinite" }}
        >
          ✦
        </span>
        <span style={{ color: HW2_COLOR.blue }}>{label || "Analysing with AI…"}</span>
        <span style={{ color: HW2_COLOR.ink2, fontWeight: 500 }}>
          — you can keep working; this finishes in the background.
        </span>
      </div>
    </div>
  );
}

function RecomputeBanner({ projectId }: { projectId: string }) {
  const [state, setState] = useState<{
    stale: boolean;
    never_computed: boolean;
    impacted_count: number;
  } | null>(null);
  const [busy, setBusy] = useState(false);
  const { runAnalysis } = useH2Context();

  const load = useCallback(() => {
    h2.projects
      .state(projectId)
      .then(setState)
      .catch(() => setState(null));
  }, [projectId]);

  useEffect(() => {
    load();
    // Re-check when an input changes anywhere in the app (edits, context, defer).
    window.addEventListener(HW2_INPUT_CHANGED, load);
    return () => window.removeEventListener(HW2_INPUT_CHANGED, load);
  }, [load]);

  if (!state || !state.stale) return null;

  const recompute = async () => {
    setBusy(true);
    try {
      // Routed through runAnalysis so the global "Analysing" bar shows and persists
      // even if the user navigates to Catalog while this runs.
      await runAnalysis("Re-analysing your goal and data…", () =>
        h2.projects.recompute(projectId)
      );
      // Re-check our own staleness (the banner hides once fresh) and tell every
      // open view to re-fetch — a seamless refresh instead of a full reload.
      load();
      notifyRecomputed();
    } finally {
      setBusy(false);
    }
  };

  const n = state.impacted_count;
  const msg = state.never_computed
    ? `Not computed yet — ${n} question${n === 1 ? "" : "s"} ready to evaluate.`
    : `Inputs changed — ${n} answer${n === 1 ? "" : "s"} will be re-verified.`;

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 16,
        padding: "10px 32px",
        background: HW2_COLOR.warnSoft,
        borderBottom: `1px solid ${HW2_COLOR.warn}44`,
      }}
    >
      <span style={{ font: "500 13px 'DM Sans', sans-serif", color: HW2_COLOR.ink2 }}>
        <strong style={{ color: HW2_COLOR.warn }}>Refresh needed.</strong> {msg}{" "}
        Certification re-runs separately after.
      </span>
      <button
        onClick={recompute}
        disabled={busy}
        style={{
          appearance: "none",
          cursor: busy ? "default" : "pointer",
          background: HW2_COLOR.blue,
          color: "#fff",
          border: "1px solid transparent",
          borderRadius: 8,
          padding: "7px 14px",
          font: "600 12px 'DM Sans', sans-serif",
          fontFamily: "'DM Sans', sans-serif",
          opacity: busy ? 0.6 : 1,
          flexShrink: 0,
        }}
      >
        {busy ? "Recomputing…" : "Recompute now"}
      </button>
    </div>
  );
}

// ─── Layout ──────────────────────────────────────────────────────────────────

export default function H2Layout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();

  const [projects, setProjects] = useState<H2Project[]>([]);
  const [sources, setSources] = useState<H2Source[]>([]);
  const [activeProject, setActiveProject] = useState<H2Project | null>(null);
  const [analysis, setAnalysis] = useState({ active: false, label: "" });

  // Owned by the (persistent) layout, so an in-flight analysis and its bar survive
  // client-side navigation between H2 pages (e.g. switching to Catalog).
  const runAnalysis = useCallback(
    async <T,>(label: string, fn: () => Promise<T>): Promise<T> => {
      setAnalysis({ active: true, label });
      try {
        return await fn();
      } finally {
        setAnalysis({ active: false, label: "" });
      }
    },
    []
  );

  const reload = useCallback(() => {
    Promise.all([h2.projects.list(), h2.sources.list()])
      .then(([ps, ss]) => {
        setProjects(ps);
        setSources(ss);
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    reload();
  }, [reload]);

  // Fetch active project when route changes to a project page
  const projectId = projectIdFromPath(pathname);
  const reloadActiveProject = useCallback(() => {
    if (!projectId) {
      setActiveProject(null);
      return;
    }
    h2.projects
      .get(projectId)
      .then(setActiveProject)
      .catch(() => setActiveProject(null));
  }, [projectId]);

  // Clear the active project synchronously during render when leaving a project
  // route. Doing the clear here (not in an effect) avoids a setState-in-effect.
  const [trackedProjectId, setTrackedProjectId] = useState(projectId);
  if (projectId !== trackedProjectId) {
    setTrackedProjectId(projectId);
    if (!projectId) setActiveProject(null);
  }

  // Fetch (async) when on a project route; the early return keeps the effect
  // free of any synchronous setState.
  useEffect(() => {
    if (!projectId) return;
    let cancelled = false;
    h2.projects
      .get(projectId)
      .then((p) => {
        if (!cancelled) setActiveProject(p);
      })
      .catch(() => {
        if (!cancelled) setActiveProject(null);
      });
    return () => {
      cancelled = true;
    };
  }, [projectId]);

  // After a recompute, refresh the banner (ring/goal/counts) and the rail
  // readouts so they reflect the new derived state — no full page reload.
  useEffect(() => {
    const onRecomputed = () => {
      reloadActiveProject();
      reload();
    };
    window.addEventListener(HW2_RECOMPUTED, onRecomputed);
    return () => window.removeEventListener(HW2_RECOMPUTED, onRecomputed);
  }, [reloadActiveProject, reload]);

  const stage = stageFromPath(pathname) ?? "understand";

  const handleNavigate = (href: string) => {
    router.push(href);
  };

  const handleJumpStage = (key: StageKey) => {
    if (!activeProject) return;
    const stageMap: Record<StageKey, string> = {
      frame:      `/h2/projects/${activeProject.id}`,
      understand: `/h2/projects/${activeProject.id}/understand`,
      resolve:    `/h2/projects/${activeProject.id}/resolve`,
      readiness:  `/h2/projects/${activeProject.id}/readiness`,
      answer:     `/h2/projects/${activeProject.id}/answer`,
    };
    router.push(stageMap[key]);
  };

  return (
    <H2Context.Provider value={{ projects, sources, reload, analysis, runAnalysis }}>
      <div
        style={{
          height: "100vh",
          display: "flex",
          flexDirection: "column",
          background: HW2_COLOR.paper,
          color: HW2_COLOR.ink,
          fontFamily: "'DM Sans', sans-serif",
          overflow: "hidden",
        }}
      >
        {/* Top bar */}
        <header
          style={{
            height: 48,
            flexShrink: 0,
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "0 18px",
            background: "rgba(250, 249, 246, 0.85)",
            backdropFilter: "saturate(140%) blur(8px)",
            borderBottom: `1px solid ${HW2_COLOR.rule}`,
            zIndex: 10,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <Link
              href="/h2"
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                textDecoration: "none",
              }}
            >
              <span
                style={{
                  width: 16,
                  height: 16,
                  borderRadius: 4,
                  background: `linear-gradient(135deg, ${HW2_COLOR.blue}, #4980f0)`,
                  display: "block",
                }}
              />
              <span
                style={{
                  font: "700 14px 'DM Sans', sans-serif",
                  letterSpacing: "-0.02em",
                  color: HW2_COLOR.ink,
                }}
              >
                Headwater
              </span>
            </Link>
            <span
              style={{
                font: "500 11px 'DM Mono', monospace",
                color: HW2_COLOR.faint,
                marginLeft: 2,
              }}
            >
              v2
            </span>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <button
              style={{
                appearance: "none",
                background: "#fff",
                cursor: "pointer",
                border: `1px solid ${HW2_COLOR.rule2}`,
                borderRadius: 999,
                padding: "5px 12px",
                font: "500 12px 'DM Sans', sans-serif",
                color: HW2_COLOR.ink,
                display: "inline-flex",
                alignItems: "center",
                gap: 6,
              }}
            >
              <span
                style={{
                  width: 18,
                  height: 18,
                  borderRadius: "50%",
                  background: HW2_COLOR.chip,
                  display: "grid",
                  placeItems: "center",
                  color: HW2_COLOR.muted,
                  font: "600 10px 'DM Sans', sans-serif",
                }}
              >
                N
              </span>
              Nupul
            </button>
          </div>
        </header>

        {/* Body */}
        <div
          style={{
            flex: 1,
            display: "flex",
            overflow: "hidden",
            minHeight: 0,
          }}
        >
          <H2Rail
            sources={sources}
            projects={projects}
            pathname={pathname}
            onNavigate={handleNavigate}
          />

          <main
            style={{
              flex: 1,
              overflowY: "auto",
              background: HW2_COLOR.paper,
              minWidth: 0,
            }}
          >
            {/* Global: shows on every H2 page (incl. Catalog) and persists while
                the user navigates during a long analysis. */}
            <AnalysisBar active={analysis.active} label={analysis.label} />
            {activeProject ? (
              <>
                <ProjectBanner
                  project={activeProject}
                  stage={stage}
                  onJumpStage={handleJumpStage}
                />
                <RecomputeBanner projectId={activeProject.id} />
                <div>{children}</div>
              </>
            ) : (
              children
            )}
          </main>
        </div>
      </div>
    </H2Context.Provider>
  );
}
