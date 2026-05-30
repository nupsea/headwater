"use client";

import { useEffect, useState } from "react";
import { h2, type H2AnswerRow, type H2Source } from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

export default function QueryConsolePage() {
  const [sources, setSources] = useState<H2Source[]>([]);
  const [source, setSource] = useState<string>("");
  const [sql, setSql] = useState<string>("");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<{
    columns: string[];
    rows: H2AnswerRow[];
    row_count: number;
    truncated: boolean;
  } | null>(null);

  useEffect(() => {
    h2.sources
      .list()
      .then((s) => {
        setSources(s);
        if (s[0]) setSource(s[0].name);
      })
      .catch(() => {});
  }, []);

  const run = async () => {
    if (!source || !sql.trim()) return;
    setRunning(true);
    setError(null);
    try {
      const r = await h2.query(source, sql);
      if (r.error) {
        setError(r.error);
        setResult(null);
      } else {
        setResult(r);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Query failed");
      setResult(null);
    } finally {
      setRunning(false);
    }
  };

  const fmt = (v: unknown) =>
    v === null || v === undefined
      ? "—"
      : typeof v === "number" && !Number.isInteger(v)
      ? v.toFixed(2)
      : String(v);

  return (
    <div
      style={{
        maxWidth: 1100,
        margin: "0 auto",
        padding: "28px 32px 80px",
        fontFamily: "'DM Sans', sans-serif",
      }}
    >
      <span
        style={{
          font: "600 11px 'DM Sans', sans-serif",
          color: HW2_COLOR.blue,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
        }}
      >
        Power tool
      </span>
      <h2
        style={{
          font: "600 26px 'DM Sans', sans-serif",
          letterSpacing: "-0.02em",
          color: HW2_COLOR.ink,
          lineHeight: 1.25,
          marginTop: 8,
          marginBottom: 4,
        }}
      >
        SQL console
      </h2>
      <p
        style={{
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          marginBottom: 20,
        }}
      >
        Read-only SQL against a freshly materialized source. Use bare table names
        (e.g. <code style={{ fontFamily: "'DM Mono', monospace" }}>SELECT * FROM exams LIMIT 10</code>).
      </p>

      <div style={{ display: "flex", gap: 8, marginBottom: 10, flexWrap: "wrap" }}>
        {sources.map((s) => (
          <button
            key={s.name}
            type="button"
            onClick={() => setSource(s.name)}
            style={{
              appearance: "none",
              cursor: "pointer",
              padding: "6px 12px",
              borderRadius: 8,
              background: source === s.name ? HW2_COLOR.blueSoft : "#fff",
              border: `1.5px solid ${source === s.name ? HW2_COLOR.blue : HW2_COLOR.rule2}`,
              font: "500 13px 'DM Sans', sans-serif",
              color: source === s.name ? HW2_COLOR.blue : HW2_COLOR.ink2,
            }}
          >
            {s.name}
          </button>
        ))}
      </div>

      <textarea
        value={sql}
        onChange={(e) => setSql(e.target.value)}
        placeholder="SELECT * FROM exams LIMIT 20"
        spellCheck={false}
        style={{
          width: "100%",
          minHeight: 130,
          padding: "12px 16px",
          background: "#fff",
          border: `1px solid ${HW2_COLOR.rule2}`,
          borderRadius: 10,
          font: "500 13px 'DM Mono', monospace",
          color: HW2_COLOR.ink,
          lineHeight: 1.5,
          resize: "vertical",
          outline: "none",
          boxSizing: "border-box",
        }}
      />

      <div style={{ display: "flex", alignItems: "center", gap: 12, marginTop: 10 }}>
        <button
          onClick={run}
          disabled={running || !source || !sql.trim()}
          style={{
            appearance: "none",
            cursor: running ? "default" : "pointer",
            background: HW2_COLOR.blue,
            color: "#fff",
            border: "1px solid transparent",
            borderRadius: 8,
            padding: "9px 18px",
            font: "600 13px 'DM Sans', sans-serif",
            opacity: running || !source || !sql.trim() ? 0.5 : 1,
          }}
        >
          {running ? "Running…" : "▶ Run"}
        </button>
        {result && (
          <span style={{ font: "400 12px 'DM Mono', monospace", color: HW2_COLOR.faint }}>
            {result.row_count.toLocaleString()} row{result.row_count === 1 ? "" : "s"}
            {result.truncated ? " · first 500 shown" : ""}
          </span>
        )}
      </div>

      {error && (
        <div
          style={{
            marginTop: 16,
            padding: "12px 16px",
            background: HW2_COLOR.badSoft,
            border: `1px solid ${HW2_COLOR.bad}44`,
            borderRadius: 10,
            font: "400 12.5px 'DM Mono', monospace",
            color: HW2_COLOR.bad,
            lineHeight: 1.5,
          }}
        >
          {error}
        </div>
      )}

      {result && result.columns.length > 0 && (
        <div
          style={{
            marginTop: 16,
            background: HW2_COLOR.surface,
            border: `1px solid ${HW2_COLOR.rule}`,
            borderRadius: 12,
            overflow: "hidden",
          }}
        >
          <div style={{ overflowX: "auto", maxHeight: 460 }}>
            <table style={{ borderCollapse: "collapse", width: "100%" }}>
              <thead>
                <tr>
                  {result.columns.map((c) => (
                    <th
                      key={c}
                      style={{
                        position: "sticky",
                        top: 0,
                        textAlign: "left",
                        padding: "8px 14px",
                        background: HW2_COLOR.paper,
                        borderBottom: `1px solid ${HW2_COLOR.rule}`,
                        font: "600 11px 'DM Mono', monospace",
                        color: HW2_COLOR.ink2,
                        whiteSpace: "nowrap",
                      }}
                    >
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.rows.slice(0, 100).map((row, ri) => (
                  <tr key={ri}>
                    {result.columns.map((c) => (
                      <td
                        key={c}
                        style={{
                          padding: "7px 14px",
                          borderBottom: `1px solid ${HW2_COLOR.rule}`,
                          font: "400 12px 'DM Mono', monospace",
                          color: HW2_COLOR.ink2,
                          whiteSpace: "nowrap",
                        }}
                      >
                        {fmt(row[c])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
