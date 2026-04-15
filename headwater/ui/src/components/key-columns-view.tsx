"use client";

import { type DictColumn, type ColumnProfile } from "@/lib/api";
import { ConfidenceDot } from "@/components/confidence-dot";

const ROLE_COLORS: Record<string, string> = {
  dimension: "bg-purple-100 text-purple-800 border-purple-200",
  metric: "bg-blue-100 text-blue-800 border-blue-200",
  temporal: "bg-green-100 text-green-800 border-green-200",
  identifier: "bg-gray-100 text-gray-700 border-gray-200",
  geographic: "bg-teal-100 text-teal-800 border-teal-200",
  text: "bg-slate-100 text-slate-700 border-slate-200",
};

const ROLE_PRIORITY: Record<string, number> = {
  dimension: 0,
  metric: 1,
  temporal: 2,
  geographic: 3,
  identifier: 4,
  text: 5,
};

function isKeyColumn(col: DictColumn): boolean {
  if (col.is_foreign_key) return true;
  if (col.is_primary_key) return true;
  if (!col.role) return false;
  return ["dimension", "metric", "temporal", "identifier", "geographic"].includes(
    col.role
  );
}

function sortColumns(cols: DictColumn[]): DictColumn[] {
  return [...cols].sort((a, b) => {
    // FK columns prioritized after role-based columns
    const aRole = a.role || (a.is_foreign_key ? "identifier" : "text");
    const bRole = b.role || (b.is_foreign_key ? "identifier" : "text");
    const aPri = ROLE_PRIORITY[aRole] ?? 99;
    const bPri = ROLE_PRIORITY[bRole] ?? 99;
    if (aPri !== bPri) return aPri - bPri;
    // Then by confidence descending
    return (b.confidence ?? 0) - (a.confidence ?? 0);
  });
}

export function KeyColumnsView({
  columns,
  profiles,
  editable = false,
  onEdit,
}: {
  columns: DictColumn[];
  profiles?: ColumnProfile[];
  editable?: boolean;
  onEdit?: (colName: string, field: string, value: unknown) => void;
}) {
  const keyColumns = sortColumns(columns.filter(isKeyColumn));
  const profileMap = new Map(
    (profiles || []).map((p) => [p.column_name, p])
  );

  if (keyColumns.length === 0) {
    return (
      <div className="text-sm text-muted p-4">
        No key columns detected. All columns will appear in the "All Columns" tab.
      </div>
    );
  }

  return (
    <div className="grid gap-3 md:grid-cols-2">
      {keyColumns.map((col) => {
        const profile = profileMap.get(col.name);
        const role = col.role || (col.is_foreign_key ? "FK" : "");
        const topValues =
          profile?.top_values?.slice(0, 5) || [];

        return (
          <div
            key={col.name}
            className={`border rounded-lg p-3 ${
              col.locked
                ? "border-green-200 bg-green-50/30"
                : col.needs_review
                ? "border-amber-200 bg-amber-50/30"
                : "border-border bg-card"
            }`}
          >
            {/* Header row */}
            <div className="flex items-center gap-2 mb-1.5">
              <span className="font-mono text-sm font-medium">{col.name}</span>
              <span className="text-[10px] px-1.5 py-0.5 rounded bg-background border border-border text-muted">
                {col.dtype}
              </span>
              {role && (
                <span
                  className={`text-[10px] px-1.5 py-0.5 rounded border font-medium ${
                    ROLE_COLORS[col.role || ""] || "bg-amber-100 text-amber-800 border-amber-200"
                  }`}
                >
                  {role}
                </span>
              )}
              <ConfidenceDot value={col.confidence} />
              {col.locked && (
                <span className="text-[10px] text-green-600 font-medium">
                  locked
                </span>
              )}
            </div>

            {/* Description */}
            {editable && !col.locked ? (
              <input
                type="text"
                value={col.description || ""}
                onChange={(e) =>
                  onEdit?.(col.name, "description", e.target.value || null)
                }
                placeholder="Add description..."
                className="text-xs text-muted w-full border border-border rounded px-2 py-1 bg-background mb-2"
              />
            ) : (
              <p className="text-xs text-muted mb-2">
                {col.description || "No description yet"}
              </p>
            )}

            {/* FK reference */}
            {col.is_foreign_key && col.fk_references && (
              <div className="text-[10px] text-purple-700 mb-1.5 font-mono">
                &rarr; {col.fk_references}
              </div>
            )}

            {/* Sample values */}
            {topValues.length > 0 && (
              <div className="flex flex-wrap gap-1 mb-1.5">
                {topValues.map(([val, count]) => (
                  <span
                    key={val}
                    className="text-[10px] px-1.5 py-0.5 rounded bg-background border border-border font-mono"
                    title={`${count} occurrences`}
                  >
                    {val}
                  </span>
                ))}
              </div>
            )}

            {/* Null rate warning */}
            {profile && profile.null_rate > 0.1 && (
              <div className="text-[10px] text-amber-600">
                {(profile.null_rate * 100).toFixed(0)}% null ({profile.null_count.toLocaleString()} rows)
              </div>
            )}

            {/* Stats row */}
            {profile && (
              <div className="flex gap-3 text-[10px] text-muted mt-1">
                <span>{profile.distinct_count} distinct</span>
                {profile.uniqueness_ratio >= 0.99 && (
                  <span className="text-green-600">unique</span>
                )}
                {col.semantic_type && <span>{col.semantic_type}</span>}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
