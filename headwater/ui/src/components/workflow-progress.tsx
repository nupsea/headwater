import Link from "next/link";
import type { Workflow } from "@/lib/api";

const PHASE_ROUTES: Record<string, string> = {
  discover: "/discovery",
  discovery: "/discovery",
  profile: "/discovery",
  dictionary: "/dictionary",
  review: "/dictionary",
  models: "/models",
  generate: "/models",
  quality: "/quality",
  contracts: "/quality",
  explore: "/explore",
};

export function WorkflowProgress({ workflow }: { workflow: Workflow }) {
  const { phases } = workflow;
  const completedCount = phases.filter((p) => p.status === "complete").length;

  return (
    <div className="bg-card border border-border rounded-lg px-6 py-5">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-muted uppercase tracking-wide">
          Pipeline Progress
        </h3>
        <span className="text-xs text-muted">
          {completedCount} of {phases.length} phases complete
        </span>
      </div>

      {/* Full-width segmented bar */}
      <div className="flex gap-1.5 mb-4">
        {phases.map((phase) => {
          const route = PHASE_ROUTES[phase.key];
          const bar = (
            <div
              className={`h-2 flex-1 rounded-full transition-colors ${
                route ? "cursor-pointer" : ""
              } ${
                phase.status === "complete"
                  ? "bg-success"
                  : phase.status === "active"
                    ? "bg-accent animate-pulse"
                    : "bg-border"
              }`}
            />
          );
          return route ? (
            <Link key={phase.key} href={route} className="flex-1">
              {bar}
            </Link>
          ) : (
            <div key={phase.key} className="flex-1">
              {bar}
            </div>
          );
        })}
      </div>

      {/* Phase labels */}
      <div className="flex gap-1.5">
        {phases.map((phase) => {
          const route = PHASE_ROUTES[phase.key];
          const content = (
            <>
              <div
                className={`text-xs font-medium ${
                  phase.status === "complete"
                    ? "text-success"
                    : phase.status === "active"
                      ? "text-accent font-semibold"
                      : "text-muted/50"
                }`}
              >
                {phase.status === "complete" && "// "}
                {phase.label}
              </div>
              <div className="text-[11px] text-muted leading-tight mt-0.5 truncate">
                {phase.detail}
              </div>
            </>
          );
          return route ? (
            <Link
              key={phase.key}
              href={route}
              className="flex-1 min-w-0 hover:opacity-80 transition-opacity"
            >
              {content}
            </Link>
          ) : (
            <div key={phase.key} className="flex-1 min-w-0">
              {content}
            </div>
          );
        })}
      </div>
    </div>
  );
}
