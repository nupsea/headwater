import type { RelationshipEntry, TableHealth } from "@/lib/api";

interface Props {
  tables: TableHealth[];
  relationships: RelationshipEntry[];
}

export function RelationshipDiagram({ tables, relationships }: Props) {
  // Build reference counts
  const refTo: Record<string, number> = {};
  for (const r of relationships) {
    refTo[r.to_table] = (refTo[r.to_table] || 0) + 1;
  }

  // Sort tables: most-referenced first (hubs), then by name
  const sorted = [...tables].sort(
    (a, b) => (refTo[b.name] || 0) - (refTo[a.name] || 0) || a.name.localeCompare(b.name)
  );

  return (
    <div className="bg-card border border-border rounded-lg p-5">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-muted uppercase tracking-wide">
          Relationships
        </h3>
        <span className="text-xs text-muted">
          {relationships.length} foreign keys across {tables.length} tables
        </span>
      </div>

      {/* Compact table nodes */}
      <div className="flex flex-wrap gap-2 mb-4">
        {sorted.map((t) => {
          const refs = refTo[t.name] || 0;
          const hasFks = t.fk_columns.length > 0;
          const isHub = refs >= 2;
          return (
            <div
              key={t.name}
              className={`px-3 py-1.5 rounded border text-xs font-mono ${
                isHub
                  ? "border-accent bg-accent/10 font-semibold"
                  : hasFks || refs > 0
                    ? "border-border bg-background"
                    : "border-border/50 bg-background/50 text-muted"
              }`}
            >
              {t.name}
              {refs > 0 && (
                <span className="ml-1.5 text-accent text-[10px]">
                  {refs} ref{refs > 1 ? "s" : ""}
                </span>
              )}
              {!hasFks && refs === 0 && (
                <span className="ml-1.5 text-muted/50 text-[10px]">isolated</span>
              )}
            </div>
          );
        })}
      </div>

      {/* FK connections as compact list */}
      {relationships.length > 0 && (
        <div className="border-t border-border pt-3">
          <div className="grid grid-cols-1 gap-1.5 max-h-48 overflow-y-auto">
            {relationships.map((r, i) => (
              <div key={i} className="flex items-center gap-2 text-xs">
                <span className="font-mono font-medium">{r.from_table}</span>
                <span className="text-muted">.{r.from_column}</span>
                <svg width="20" height="10" className="shrink-0 text-muted">
                  <line x1="0" y1="5" x2="14" y2="5" stroke="currentColor" strokeWidth="1.5" />
                  <polygon points="14,2 20,5 14,8" fill="currentColor" />
                </svg>
                <span className="font-mono font-medium text-accent">{r.to_table}</span>
                <span className="text-muted">.{r.to_column}</span>
                {r.integrity < 100 && (
                  <span className={`ml-auto text-[10px] font-mono ${
                    r.integrity >= 95 ? "text-warning" : "text-danger"
                  }`}>
                    {r.integrity}%
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
