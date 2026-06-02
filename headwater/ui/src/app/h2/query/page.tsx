"use client";

import { useEffect, useRef, useState } from "react";
import {
  h2,
  type H2AnswerRow,
  type H2CatalogTable,
  type H2Source,
} from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

// ─── Catalog side panel ─────────────────────────────────────────────────────

function sampleHint(profile: Record<string, unknown>): string {
  const vals =
    (profile.top_values as string[] | undefined) ??
    (profile.sample_values as string[] | undefined);
  if (vals && vals.length) return `e.g. ${vals.slice(0, 3).join(", ")}`;
  const mean = profile.mean;
  if (typeof mean === "number") return `mean ${mean}`;
  return "";
}

function CatalogPanel({
  tables,
  loading,
  onInsert,
}: {
  tables: H2CatalogTable[];
  loading: boolean;
  onInsert: (identifier: string) => void;
}) {
  // Tables are expanded by default; track only the ones explicitly collapsed
  // (avoids syncing state from props in an effect).
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});
  const isOpen = (name: string) => !collapsed[name];
  const toggle = (name: string) =>
    setCollapsed((c) => ({ ...c, [name]: !c[name] }));

  return (
    <aside
      style={{
        alignSelf: "flex-start",
        position: "sticky",
        top: 16,
        background: HW2_COLOR.surface,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 12,
        maxHeight: "calc(100vh - 120px)",
        overflowY: "auto",
      }}
    >
      <div
        style={{
          padding: "12px 14px",
          borderBottom: `1px solid ${HW2_COLOR.rule}`,
          font: "600 10px 'DM Sans', sans-serif",
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          color: HW2_COLOR.muted,
          position: "sticky",
          top: 0,
          background: HW2_COLOR.surface,
        }}
      >
        Catalog
      </div>

      {loading ? (
        <div style={{ padding: "14px", font: "400 12px 'DM Sans', sans-serif", color: HW2_COLOR.faint }}>
          Loading…
        </div>
      ) : tables.length === 0 ? (
        <div style={{ padding: "14px", font: "400 12px 'DM Sans', sans-serif", color: HW2_COLOR.faint }}>
          No tables.
        </div>
      ) : (
        <div style={{ padding: "6px 0 10px" }}>
          {tables.map((t) => (
            <div key={t.table_name}>
              <button
                onClick={() => toggle(t.table_name)}
                style={{
                  appearance: "none",
                  cursor: "pointer",
                  width: "100%",
                  background: "transparent",
                  border: "none",
                  padding: "7px 14px",
                  display: "flex",
                  alignItems: "center",
                  gap: 7,
                  textAlign: "left",
                  fontFamily: "'DM Mono', monospace",
                }}
              >
                <span style={{ color: HW2_COLOR.faint, fontSize: 9 }}>
                  {isOpen(t.table_name) ? "▾" : "▸"}
                </span>
                <span
                  onClick={(e) => {
                    e.stopPropagation();
                    onInsert(t.table_name);
                  }}
                  style={{
                    font: "600 12.5px 'DM Mono', monospace",
                    color: HW2_COLOR.ink,
                  }}
                  title="Insert table name"
                >
                  {t.table_name}
                </span>
                <span
                  style={{
                    marginLeft: "auto",
                    font: "400 10px 'DM Mono', monospace",
                    color: HW2_COLOR.faint,
                  }}
                >
                  {t.row_count.toLocaleString()}
                </span>
              </button>

              {isOpen(t.table_name) && (
                <div style={{ paddingBottom: 4 }}>
                  {t.columns.map((c) => (
                    <button
                      key={c.column_name}
                      onClick={() => onInsert(c.column_name)}
                      title={sampleHint(c.profile_summary) || c.semantic_type || ""}
                      style={{
                        appearance: "none",
                        cursor: "pointer",
                        width: "100%",
                        background: "transparent",
                        border: "none",
                        padding: "3px 14px 3px 30px",
                        display: "flex",
                        alignItems: "baseline",
                        gap: 8,
                        textAlign: "left",
                      }}
                    >
                      <span
                        style={{
                          font: "500 12px 'DM Mono', monospace",
                          color: HW2_COLOR.ink2,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {c.column_name}
                      </span>
                      <span
                        style={{
                          marginLeft: "auto",
                          flexShrink: 0,
                          font: "400 10px 'DM Mono', monospace",
                          color: HW2_COLOR.faint,
                        }}
                      >
                        {c.dtype}
                      </span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </aside>
  );
}

// ─── Page ───────────────────────────────────────────────────────────────────

export default function QueryConsolePage() {
  const [sources, setSources] = useState<H2Source[]>([]);
  const [source, setSource] = useState<string>("");
  const [sql, setSql] = useState<string>("");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [catalog, setCatalog] = useState<H2CatalogTable[]>([]);
  const [catalogLoading, setCatalogLoading] = useState(false);
  const taRef = useRef<HTMLTextAreaElement>(null);
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

  // Load the catalog for the selected source so it's at hand while querying.
  useEffect(() => {
    if (!source) {
      setCatalog([]);
      return;
    }
    setCatalogLoading(true);
    h2.sources
      .catalog(source)
      .then(setCatalog)
      .catch(() => setCatalog([]))
      .finally(() => setCatalogLoading(false));
  }, [source]);

  // Insert an identifier at the cursor (or append) — click a table/column.
  const insertIdentifier = (identifier: string) => {
    const ta = taRef.current;
    if (!ta) {
      setSql((s) => (s ? `${s} ${identifier}` : identifier));
      return;
    }
    const start = ta.selectionStart;
    const end = ta.selectionEnd;
    setSql((s) => s.slice(0, start) + identifier + s.slice(end));
    requestAnimationFrame(() => {
      ta.focus();
      const pos = start + identifier.length;
      ta.setSelectionRange(pos, pos);
    });
  };

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
        maxWidth: 1240,
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
        Read-only SQL against a freshly materialized source. Click a table or
        column in the catalog to insert it. Use bare table names (e.g.{" "}
        <code style={{ fontFamily: "'DM Mono', monospace" }}>SELECT * FROM exams LIMIT 10</code>).
      </p>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "260px 1fr",
          gap: 20,
          alignItems: "start",
        }}
      >
        <CatalogPanel
          tables={catalog}
          loading={catalogLoading}
          onInsert={insertIdentifier}
        />

        <div style={{ minWidth: 0 }}>
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
            ref={taRef}
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
      </div>
    </div>
  );
}
