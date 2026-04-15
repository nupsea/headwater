"use client";

import { type DimensionOption } from "@/lib/api";
import { ConfidenceDot } from "@/components/confidence-dot";

export function DisambiguationUI({
  options,
  onSelect,
  loading,
  question,
}: {
  options: DimensionOption[];
  onSelect: (option: DimensionOption) => void;
  loading: boolean;
  question: string;
}) {
  if (options.length === 0) return null;

  return (
    <div className="px-4 py-4 border-b border-border bg-blue-50/50">
      <div className="text-xs font-semibold text-blue-800 uppercase tracking-wider mb-1">
        Ambiguous dimension
      </div>
      <p className="text-sm text-blue-700 mb-3">
        Multiple dimensions match your query. Select the one you meant:
      </p>
      <div className="grid gap-2 md:grid-cols-2">
        {options.map((opt, i) => (
          <button
            key={i}
            onClick={() => onSelect(opt)}
            disabled={loading}
            className="text-left p-3 border border-blue-200 rounded-lg bg-white hover:border-blue-500 transition-colors disabled:opacity-50"
          >
            <div className="flex items-center gap-2 mb-1">
              <span className="text-sm font-medium text-blue-900">
                {opt.display_name}
              </span>
              <ConfidenceDot value={opt.confidence} />
            </div>
            {opt.description && (
              <p className="text-xs text-blue-700 mb-1.5">{opt.description}</p>
            )}
            {opt.sample_values && opt.sample_values.length > 0 && (
              <div className="flex flex-wrap gap-1">
                {opt.sample_values.slice(0, 5).map((val, j) => (
                  <span
                    key={j}
                    className="text-[10px] px-1.5 py-0.5 rounded bg-blue-50 border border-blue-200 font-mono"
                  >
                    {val}
                  </span>
                ))}
              </div>
            )}
          </button>
        ))}
      </div>
    </div>
  );
}
