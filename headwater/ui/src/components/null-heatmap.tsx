import type { NullEntry } from "@/lib/api";

interface Props {
  entries: NullEntry[];
}

function cellColor(rate: number): string {
  if (rate === 0) return "bg-success/20";
  if (rate < 1) return "bg-success/40";
  if (rate < 5) return "bg-warning/30";
  if (rate < 20) return "bg-warning/60";
  return "bg-danger/50";
}

export function NullHeatmap({ entries }: Props) {
  if (entries.length === 0) {
    return (
      <div className="bg-card border border-border rounded-lg p-5">
        <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-2">
          Null Analysis
        </h3>
        <div className="text-sm text-success">
          No null values detected across any column.
        </div>
      </div>
    );
  }

  // Group by table
  const byTable: Record<string, NullEntry[]> = {};
  for (const e of entries) {
    if (!byTable[e.table]) byTable[e.table] = [];
    byTable[e.table].push(e);
  }

  return (
    <div className="bg-card border border-border rounded-lg p-5">
      <h3 className="text-sm font-semibold text-muted uppercase tracking-wide mb-1">
        Null Analysis
      </h3>
      <p className="text-xs text-muted mb-4">
        {entries.length} column{entries.length > 1 ? "s" : ""} with null values across{" "}
        {Object.keys(byTable).length} table{Object.keys(byTable).length > 1 ? "s" : ""}
      </p>

      <div className="space-y-4">
        {Object.entries(byTable).map(([table, cols]) => (
          <div key={table}>
            <div className="text-xs font-semibold font-mono mb-1">{table}</div>
            <div className="space-y-1">
              {cols.map((c) => (
                <div key={c.column} className="flex items-center gap-2 text-xs">
                  <span className="w-40 font-mono truncate">{c.column}</span>
                  <div className="flex-1 h-4 bg-border rounded overflow-hidden relative">
                    <div
                      className={`h-full rounded ${cellColor(c.null_rate)}`}
                      style={{ width: `${Math.max(c.null_rate, 1)}%` }}
                    />
                    <span className="absolute inset-0 flex items-center justify-center text-[10px] font-medium">
                      {c.null_rate.toFixed(1)}%
                    </span>
                  </div>
                  <span className="w-24 text-right text-muted">
                    {c.null_count.toLocaleString()} / {c.total_rows.toLocaleString()}
                  </span>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>

      {/* Legend */}
      <div className="flex items-center gap-3 mt-4 text-[10px] text-muted">
        <span>Null rate:</span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded bg-success/20" /> 0%
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded bg-success/40" /> &lt;1%
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded bg-warning/30" /> &lt;5%
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded bg-warning/60" /> &lt;20%
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded bg-danger/50" /> 20%+
        </span>
      </div>
    </div>
  );
}
