"use client";

import { useEffect, useState } from "react";
import { api, type PKFKSuggestions } from "@/lib/api";

interface PKFKManagerProps {
  tableName: string;
}

export function PKFKManager({ tableName }: PKFKManagerProps) {
  const [suggestions, setSuggestions] = useState<PKFKSuggestions | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const load = () => {
    setLoading(true);
    setError("");
    api
      .pkfkSuggestions(tableName)
      .then(setSuggestions)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tableName]);

  const handleConfirmPK = async (column: string) => {
    try {
      await api.persistKeys(tableName, { confirm_pks: [column] });
      setMessage(`PK confirmed: ${column}`);
      load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleRejectPK = async (column: string) => {
    try {
      await api.persistKeys(tableName, { reject_pks: [column] });
      setMessage(`PK rejected: ${column}`);
      load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleConfirmFK = async (fromCol: string, toTable: string, toCol: string) => {
    try {
      await api.persistKeys(tableName, {
        confirm_fks: [{ from_col: fromCol, to_table: toTable, to_col: toCol }],
      });
      setMessage(`FK confirmed: ${fromCol} -> ${toTable}.${toCol}`);
      load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  if (loading) {
    return (
      <div className="border border-border rounded-lg bg-card p-4">
        <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-2">
          PK/FK Suggestions
        </h3>
        <p className="text-xs text-muted">Loading suggestions...</p>
      </div>
    );
  }

  if (error && !suggestions) {
    return (
      <div className="border border-border rounded-lg bg-card p-4">
        <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-2">
          PK/FK Suggestions
        </h3>
        <p className="text-xs text-muted">
          Could not load PK/FK suggestions for this table.
        </p>
      </div>
    );
  }

  if (
    !suggestions ||
    (suggestions.pk_candidates.length === 0 &&
      suggestions.fk_candidates.length === 0)
  ) {
    return null;
  }

  return (
    <div className="border border-border rounded-lg bg-card p-4">
      <h3 className="text-xs font-semibold text-muted uppercase tracking-wider mb-2">
        PK/FK Suggestions
      </h3>

      {message && (
        <div className="mb-3 p-2 bg-green-50 border border-green-200 rounded text-xs text-green-800">
          {message}
          <button
            onClick={() => setMessage("")}
            className="ml-2 text-muted hover:text-foreground"
          >
            dismiss
          </button>
        </div>
      )}

      {error && (
        <div className="mb-3 p-2 bg-red-50 border border-red-200 rounded text-xs text-red-700">
          {error}
        </div>
      )}

      {/* PK candidates */}
      {suggestions.pk_candidates.length > 0 && (
        <div className="mb-4">
          <h4 className="text-[10px] font-semibold text-muted uppercase mb-2">
            Primary Key Candidates
          </h4>
          <div className="space-y-2">
            {suggestions.pk_candidates.map((pk) => (
              <div
                key={pk.column}
                className="flex items-center justify-between p-2 border border-border rounded"
              >
                <div className="min-w-0">
                  <span className="font-mono text-sm">{pk.column}</span>
                  <div className="text-[10px] text-muted mt-0.5">
                    {(pk.uniqueness_ratio * 100).toFixed(0)}% unique,{" "}
                    {(pk.null_rate * 100).toFixed(1)}% nulls,{" "}
                    {(pk.confidence * 100).toFixed(0)}% confidence
                  </div>
                  {pk.reasons.length > 0 && (
                    <div className="text-[10px] text-muted mt-0.5">
                      {pk.reasons.join("; ")}
                    </div>
                  )}
                </div>
                <div className="flex gap-1 shrink-0 ml-2">
                  <button
                    onClick={() => handleConfirmPK(pk.column)}
                    className="px-2 py-1 bg-green-600 text-white rounded text-[10px] font-medium hover:bg-green-700 transition-colors"
                  >
                    Confirm
                  </button>
                  <button
                    onClick={() => handleRejectPK(pk.column)}
                    className="px-2 py-1 border border-border rounded text-[10px] text-muted hover:text-foreground transition-colors"
                  >
                    Reject
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* FK candidates */}
      {suggestions.fk_candidates.length > 0 && (
        <div>
          <h4 className="text-[10px] font-semibold text-muted uppercase mb-2">
            Foreign Key Candidates
          </h4>
          <div className="space-y-2">
            {suggestions.fk_candidates.map((fk, i) => (
              <div
                key={i}
                className="flex items-center justify-between p-2 border border-border rounded"
              >
                <div className="min-w-0">
                  <div className="text-sm">
                    <span className="font-mono">{fk.from_column}</span>
                    <span className="text-muted mx-1">&rarr;</span>
                    <span className="font-mono">
                      {fk.to_table}.{fk.to_column}
                    </span>
                  </div>
                  <div className="text-[10px] text-muted mt-0.5">
                    {(fk.value_overlap * 100).toFixed(0)}% overlap,{" "}
                    {(fk.confidence * 100).toFixed(0)}% confidence
                  </div>
                  {fk.reasons.length > 0 && (
                    <div className="text-[10px] text-muted mt-0.5">
                      {fk.reasons.join("; ")}
                    </div>
                  )}
                </div>
                <div className="flex gap-1 shrink-0 ml-2">
                  <button
                    onClick={() =>
                      handleConfirmFK(fk.from_column, fk.to_table, fk.to_column)
                    }
                    className="px-2 py-1 bg-green-600 text-white rounded text-[10px] font-medium hover:bg-green-700 transition-colors"
                  >
                    Confirm
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
