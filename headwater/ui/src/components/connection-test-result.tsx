"use client";

import type { ConnectionTestResult } from "@/lib/api";

export function ConnectionTestResultPanel({
  result,
}: {
  result: ConnectionTestResult | null;
}) {
  if (!result) return null;

  const ok = result.status === "ok";
  const tableNames = result.table_names ?? [];

  return (
    <div
      className={`rounded-lg border px-3 py-2 ${
        ok
          ? "border-green-200 bg-green-50 text-green-700 dark:border-green-900 dark:bg-green-950/30 dark:text-green-300"
          : "border-red-200 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950/30 dark:text-red-300"
      }`}
    >
      <div className="text-sm font-semibold mb-1">
        {ok ? "Connection ready" : "Connection failed"}
      </div>
      <div className="text-[12px] leading-5">{result.detail}</div>
      {ok && tableNames.length > 0 ? (
        <div className="mt-3">
          <div className="mb-2 text-[11px] font-semibold text-inherit/90">
            Discovered tables ({tableNames.length})
          </div>
          <div className="max-h-64 overflow-y-auto rounded-md border border-current/15 bg-black/5 px-2 py-2 font-mono text-[11px] leading-5 dark:bg-white/5">
            {tableNames.map((tableName) => (
              <div key={tableName} className="break-all">
                {tableName}
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
