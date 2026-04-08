import type { ModelSuggestion } from "@/lib/api";

const typeStyles: Record<string, string> = {
  coverage: "border-accent/30 bg-accent/5",
  dedup: "border-warning/30 bg-warning/5",
  integrity: "border-danger/30 bg-danger/5",
  review: "border-purple-500/30 bg-purple-500/5",
};

const typeLabels: Record<string, string> = {
  coverage: "Coverage Gap",
  dedup: "Deduplication",
  integrity: "Referential Integrity",
  review: "Needs Review",
};

export function SuggestionsList({
  suggestions,
}: {
  suggestions: ModelSuggestion[];
}) {
  if (suggestions.length === 0) return null;

  return (
    <div className="bg-card border border-border rounded-lg p-5">
      <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-4">
        Model Improvement Suggestions
      </h3>
      <div className="space-y-3">
        {suggestions.map((s, i) => (
          <div
            key={i}
            className={`border rounded-lg p-3 ${typeStyles[s.type] || "border-border"}`}
          >
            <div className="flex items-center gap-2 mb-1">
              <span className="text-xs font-semibold uppercase tracking-wider text-muted">
                {typeLabels[s.type] || s.type}
              </span>
            </div>
            <div className="text-sm font-medium">{s.title}</div>
            <div className="text-xs text-muted mt-1">{s.detail}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
