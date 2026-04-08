import type { TableHealth } from "@/lib/api";

function barColor(pct: number): string {
  if (pct >= 99) return "bg-success";
  if (pct >= 95) return "bg-success/70";
  if (pct >= 90) return "bg-warning";
  return "bg-danger";
}

function textColor(pct: number): string {
  if (pct >= 99) return "text-success";
  if (pct >= 90) return "text-warning";
  return "text-danger";
}

export function CompletenessChart({ tables }: { tables: TableHealth[] }) {
  const sorted = [...tables].sort((a, b) => a.completeness - b.completeness);

  return (
    <div className="bg-card border border-border rounded-lg p-5">
      <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-4">
        Completeness by Table
      </h3>
      <div className="space-y-2.5">
        {sorted.map((t) => (
          <div key={t.name} className="flex items-center gap-3">
            <div className="w-32 shrink-0 text-right">
              <span className="text-xs font-mono font-medium truncate block">
                {t.name}
              </span>
            </div>
            <div className="flex-1 h-5 bg-border/40 rounded overflow-hidden relative">
              <div
                className={`h-full rounded ${barColor(t.completeness)} transition-all duration-700`}
                style={{ width: `${t.completeness}%` }}
              />
              {/* Row count label inside bar */}
              <span className="absolute inset-y-0 right-2 flex items-center text-[10px] text-muted/80 font-mono">
                {t.row_count.toLocaleString()} rows
              </span>
            </div>
            <div className={`w-12 text-right text-xs font-mono font-bold ${textColor(t.completeness)}`}>
              {t.completeness}%
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
