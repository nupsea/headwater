import Link from "next/link";
import type { WorkflowPhase } from "@/lib/api";

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

function statusColor(status: WorkflowPhase["status"]) {
  switch (status) {
    case "complete":
      return "bg-success/10 border-success text-success";
    case "active":
      return "bg-accent/10 border-accent text-accent animate-pulse";
    case "pending":
      return "bg-background border-border text-muted";
  }
}

function statusDot(status: WorkflowPhase["status"]) {
  switch (status) {
    case "complete":
      return "bg-success";
    case "active":
      return "bg-accent";
    case "pending":
      return "bg-border";
  }
}

export function PipelineStepper({ phases }: { phases: WorkflowPhase[] }) {
  const completedCount = phases.filter((p) => p.status === "complete").length;

  return (
    <div className="bg-card border border-border rounded-lg px-6 py-5">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-muted uppercase tracking-wide">
          Pipeline
        </h3>
        <span className="text-xs text-muted">
          {completedCount} of {phases.length} phases complete
        </span>
      </div>

      <div className="flex items-start">
        {phases.map((phase, i) => {
          const route = PHASE_ROUTES[phase.key];
          const isLast = i === phases.length - 1;

          const box = (
            <div className="flex items-center">
              {/* Phase box */}
              <div
                className={`rounded-lg border-2 px-4 py-2.5 min-w-[100px] text-center transition-colors ${statusColor(phase.status)} ${route ? "cursor-pointer hover:opacity-80" : ""}`}
              >
                <div className="flex items-center justify-center gap-1.5 mb-0.5">
                  <span
                    className={`inline-block w-2 h-2 rounded-full shrink-0 ${statusDot(phase.status)}`}
                  />
                  <span className="text-xs font-semibold whitespace-nowrap">
                    {phase.label}
                  </span>
                </div>
                <div className="text-[10px] opacity-70 leading-tight truncate">
                  {phase.detail || (phase.status === "pending" ? "not started" : "")}
                </div>
              </div>

              {/* Arrow connector */}
              {!isLast && (
                <div className="flex items-center mx-1 shrink-0">
                  <div className="w-4 h-0.5 bg-border" />
                  <div className="w-0 h-0 border-t-[4px] border-t-transparent border-b-[4px] border-b-transparent border-l-[6px] border-l-border" />
                </div>
              )}
            </div>
          );

          return route ? (
            <Link
              key={phase.key}
              href={route}
              className="flex items-center shrink-0"
            >
              {box}
            </Link>
          ) : (
            <div key={phase.key} className="flex items-center shrink-0">
              {box}
            </div>
          );
        })}
      </div>
    </div>
  );
}
