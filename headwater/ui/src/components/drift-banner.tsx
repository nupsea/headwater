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
      return "text-green-600 bg-green-50 border-green-200";
    case "removed":
      return "text-red-600 bg-red-50 border-red-200";
    case "type_changed":
      return "text-amber-600 bg-amber-50 border-amber-200";
    case "nullability_changed":
      return "text-amber-600 bg-amber-50 border-amber-200";
    default:
      return "text-gray-600 bg-gray-50 border-gray-200";
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
    <div className="bg-amber-50 border border-amber-200 rounded-lg p-4 mb-5">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className="text-amber-600 font-semibold text-sm">
            Schema Drift Detected
          </span>
          <span className="text-xs text-amber-500">
            {totalChanges} change{totalChanges !== 1 ? "s" : ""} since run #
            {diff.run_id_from ?? "initial"}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setExpanded(!expanded)}
            className="text-xs text-amber-700 hover:underline"
          >
            {expanded ? "Hide details" : "View details"}
          </button>
          <button
            onClick={handleDismiss}
            disabled={dismissing}
            className="text-xs px-3 py-1 rounded border border-amber-300 text-amber-700 hover:bg-amber-100 disabled:opacity-50"
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
              <h4 className="text-xs font-semibold text-green-700 mb-1">
                Tables added
              </h4>
              <div className="flex flex-wrap gap-1">
                {diff.tables_added.map((t) => (
                  <span
                    key={t}
                    className="px-2 py-0.5 rounded border text-xs text-green-600 bg-green-50 border-green-200 font-mono"
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
              <h4 className="text-xs font-semibold text-red-700 mb-1">
                Tables removed
              </h4>
              <div className="flex flex-wrap gap-1">
                {diff.tables_removed.map((t) => (
                  <span
                    key={t}
                    className="px-2 py-0.5 rounded border text-xs text-red-600 bg-red-50 border-red-200 font-mono"
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
              <h4 className="text-xs font-semibold text-amber-700 mb-1">
                Tables with column changes
              </h4>
              {diff.tables_changed.map((tc) => (
                <div key={tc.table_name} className="mb-2">
                  <span className="text-xs font-mono font-medium text-gray-700">
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
