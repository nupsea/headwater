import Link from "next/link";
import type { AdvisoryAction } from "@/lib/api";

const priorityStyles: Record<
  string,
  { bar: string; badge: string; badgeText: string }
> = {
  blocking: {
    bar: "bg-danger",
    badge: "bg-danger/15 text-danger",
    badgeText: "Needs Action",
  },
  recommended: {
    bar: "bg-warning",
    badge: "bg-warning/15 text-warning",
    badgeText: "Recommended",
  },
  informational: {
    bar: "bg-accent",
    badge: "bg-accent/15 text-accent",
    badgeText: "Info",
  },
  success: {
    bar: "bg-success",
    badge: "bg-success/15 text-success",
    badgeText: "Done",
  },
};

const phaseLabels: Record<string, string> = {
  review: "Schema Review",
  cleanup: "Data Cleanup",
  modeling: "Modeling",
  quality: "Quality",
};

export function AdvisoryActions({ actions }: { actions: AdvisoryAction[] }) {
  if (actions.length === 0) return null;

  const actionable = actions.filter((a) => a.priority !== "success");
  const successes = actions.filter((a) => a.priority === "success");

  return (
    <div className="bg-card border border-border rounded-lg p-5">
      <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-4">
        Next Steps
      </h3>

      {actionable.length > 0 && (
        <div className="space-y-2.5">
          {actionable.map((action, i) => {
            const s = priorityStyles[action.priority] || priorityStyles.informational;
            return (
              <div
                key={i}
                className="flex items-stretch rounded-lg border border-border overflow-hidden"
              >
                {/* Color bar on left edge */}
                <div className={`w-1 shrink-0 ${s.bar}`} />
                <div className="flex-1 flex items-center justify-between px-4 py-3 gap-4">
                  <div className="min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className={`text-[10px] font-semibold px-2 py-0.5 rounded-full ${s.badge}`}>
                        {s.badgeText}
                      </span>
                      <span className="text-[10px] text-muted bg-border/50 px-1.5 py-0.5 rounded-full">
                        {phaseLabels[action.phase] || action.phase}
                      </span>
                    </div>
                    <div className="text-sm font-medium mt-1">{action.title}</div>
                    <div className="text-xs text-muted mt-0.5 leading-relaxed">
                      {action.detail}
                    </div>
                  </div>
                  <Link
                    href={action.link}
                    className="shrink-0 px-4 py-1.5 border border-border rounded text-xs font-medium hover:bg-border/40 transition-colors"
                  >
                    Review &rarr;
                  </Link>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {successes.length > 0 && (
        <div className={actionable.length > 0 ? "mt-4 pt-3 border-t border-border" : ""}>
          <div className="flex flex-wrap gap-2">
            {successes.map((action, i) => (
              <div
                key={i}
                className="inline-flex items-center gap-2 px-3 py-1.5 rounded-full bg-success/10 border border-success/20 text-xs"
              >
                <span className="w-1.5 h-1.5 rounded-full bg-success" />
                <span>{action.title}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
