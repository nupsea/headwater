"use client";

import { useState } from "react";
import type { DriftReport, ColumnChange } from "@/lib/api";
import { api } from "@/lib/api";

interface DriftBannerProps {
  report: DriftReport;
  onDismiss: () => void;
}

function changeColor(changeType: string): string {
  switch (changeType) {
    case "added":
      return "text-[var(--success)] bg-[var(--success)]/10 border-[var(--success)]/20";
    case "removed":
      return "text-[var(--danger)] bg-[var(--danger)]/10 border-[var(--danger)]/20";
    case "type_changed":
      return "text-[var(--warning)] bg-[var(--warning)]/10 border-[var(--warning)]/20";
    case "nullability_changed":
      return "text-[var(--warning)] bg-[var(--warning)]/10 border-[var(--warning)]/20";
    default:
      return "text-muted bg-background border-border";
  }
}

function changeBadge(changeType: string): string {
  switch (changeType) {
    case "added":
      return "Added";
    case "removed":
      return "Removed";
    case "type_changed":
      return "Type changed";
    case "nullability_changed":
      return "Nullability changed";
    default:
      return changeType;
  }
}

function ColumnChangeRow({ change }: { change: ColumnChange }) {
  const color = changeColor(change.change_type);
  return (
    <div className={`flex items-center gap-2 px-2 py-1 rounded border text-xs ${color}`}>
      <span className="font-mono font-medium">{change.column_name}</span>
      <span className="font-medium">{changeBadge(change.change_type)}</span>
      {change.before && change.after && (
        <span className="text-gray-500">
          {change.before} &rarr; {change.after}
        </span>
      )}
    </div>
  );
}

export function DriftBanner({ report, onDismiss }: DriftBannerProps) {
  const [expanded, setExpanded] = useState(false);
  const [dismissing, setDismissing] = useState(false);
  const diff = report.diff;

  if (diff.no_changes || report.acknowledged) {
    return null;
  }

  const totalChanges =
    diff.tables_added.length +
    diff.tables_removed.length +
    diff.tables_changed.length;

  const handleDismiss = async () => {
    setDismissing(true);
    try {
      await api.acknowledgeDrift(report.id);
      onDismiss();
    } catch {
      setDismissing(false);
    }
  };

  return (
    <div className="bg-[color-mix(in_srgb,var(--warning)_10%,var(--card))] border border-[var(--warning)]/20 rounded-lg p-4 mb-5">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className="text-[var(--warning)] font-semibold text-sm">
            Schema Drift Detected
          </span>
          <span className="text-xs text-muted">
            {totalChanges} change{totalChanges !== 1 ? "s" : ""} since run #
            {diff.run_id_from ?? "initial"}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setExpanded(!expanded)}
            className="text-xs text-[var(--warning)] hover:underline"
          >
            {expanded ? "Hide details" : "View details"}
          </button>
          <button
            onClick={handleDismiss}
            disabled={dismissing}
            className="text-xs px-3 py-1 rounded border border-[var(--warning)]/30 text-[var(--warning)] hover:bg-[var(--warning)]/10 disabled:opacity-50"
          >
            {dismissing ? "Dismissing..." : "Dismiss"}
          </button>
        </div>
      </div>

      {expanded && (
        <div className="mt-3 space-y-3">
          {/* Added tables */}
          {diff.tables_added.length > 0 && (
            <div>
              <h4 className="text-xs font-semibold text-[var(--success)] mb-1">
                Tables added
              </h4>
              <div className="flex flex-wrap gap-1">
                {diff.tables_added.map((t) => (
                  <span
                    key={t}
                    className="px-2 py-0.5 rounded border text-xs text-[var(--success)] bg-[var(--success)]/10 border-[var(--success)]/20 font-mono"
                  >
                    {t}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Removed tables */}
          {diff.tables_removed.length > 0 && (
            <div>
              <h4 className="text-xs font-semibold text-[var(--danger)] mb-1">
                Tables removed
              </h4>
              <div className="flex flex-wrap gap-1">
                {diff.tables_removed.map((t) => (
                  <span
                    key={t}
                    className="px-2 py-0.5 rounded border text-xs text-[var(--danger)] bg-[var(--danger)]/10 border-[var(--danger)]/20 font-mono"
                  >
                    {t}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Changed tables (column-level detail) */}
          {diff.tables_changed.length > 0 && (
            <div>
              <h4 className="text-xs font-semibold text-[var(--warning)] mb-1">
                Tables with column changes
              </h4>
              {diff.tables_changed.map((tc) => (
                <div key={tc.table_name} className="mb-2">
                  <span className="text-xs font-mono font-medium text-foreground">
                    {tc.table_name}
                  </span>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {tc.column_changes.map((cc, i) => (
                      <ColumnChangeRow key={`${cc.column_name}-${i}`} change={cc} />
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
