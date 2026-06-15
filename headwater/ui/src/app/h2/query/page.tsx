"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import {
  h2,
  type H2AnswerRow,
  type H2CatalogTable,
  type H2Project,
  type H2Source,
} from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";
import { useH2Context } from "@/app/h2/layout";

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

// Quote an identifier for SQL unless it is already a plain (optionally
// schema-qualified) name. Works for both DuckDB and warehouse dialects.
function sqlIdentifier(name: string): string {
  if (/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$/.test(name)) {
    return name;
  }
  return name
    .split(".")
    .map((part) => `"${part.replace(/"/g, '""')}"`)
    .join(".");
}

function CatalogPanel({
  tables,
  loading,
  onInsert,
  onPreview,
}: {
  tables: H2CatalogTable[];
  loading: boolean;
  onInsert: (identifier: string) => void;
  onPreview: (tableName: string) => void;
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
                    onPreview(t.table_name);
                  }}
                  style={{
                    font: "600 12.5px 'DM Mono', monospace",
                    color: HW2_COLOR.ink,
                  }}
                  title="Preview: SELECT * FROM … LIMIT 100"
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
  const { activeSource, setActiveSource } = useH2Context();
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
  // Promote-to-insight (track a console query as a certifiable answer).
  const [projects, setProjects] = useState<H2Project[]>([]);
  const [promoteOpen, setPromoteOpen] = useState(false);
  const [promoteTitle, setPromoteTitle] = useState("");
  const [promoteProject, setPromoteProject] = useState("");
  const [promoting, setPromoting] = useState(false);
  const [promoted, setPromoted] = useState<{ projectId: string; title: string } | null>(null);

  useEffect(() => {
    h2.sources
      .list()
      .then((s) => {
        setSources(s);
        // Start in the workspace's active source context.
        const preferred =
          (activeSource && s.find((x) => x.name === activeSource)) || s[0];
        if (preferred) setSource((cur) => cur || preferred.name);
      })
      .catch(() => {});
    h2.projects.list().then(setProjects).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeSource]);

  // Switching the console's source switches the workspace context with it.
  const switchSource = (name: string) => {
    setSource(name);
    setActiveSource(name);
  };

  // Only projects framed on the source being explored can track this query.
  const sourceProjects = projects.filter((p) => p.source_name === source);

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

  const run = async (sqlText?: string) => {
    const text = sqlText ?? sql;
    if (!source || !text.trim()) return;
    setRunning(true);
    setError(null);
    setPromoted(null);
    setPromoteOpen(false);
    try {
      const r = await h2.query(source, text);
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

  // Quick preview: clicking a table in the catalog loads and runs a default
  // SELECT * … LIMIT 100. The SQL lands in the editor, ready to refine.
  const previewTable = (tableName: string) => {
    const text = `SELECT * FROM ${sqlIdentifier(tableName)} LIMIT 100`;
    setSql(text);
    void run(text);
  };

  const openPromote = () => {
    setPromoted(null);
    setPromoteProject(sourceProjects[0]?.id ?? "");
    setPromoteOpen((o) => !o);
  };

  const promote = async () => {
    if (!promoteProject || !promoteTitle.trim()) return;
    setPromoting(true);
    setError(null);
    try {
      await h2.projects.promoteQuery(promoteProject, promoteTitle.trim(), sql);
      setPromoted({ projectId: promoteProject, title: promoteTitle.trim() });
      setPromoteOpen(false);
      setPromoteTitle("");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not track this query.");
    } finally {
      setPromoting(false);
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
        Read-only SQL against the active source — file sources are materialized
        locally, warehouse sources run the query on the live connection. Click a
        table in the catalog for a quick preview (SELECT * … LIMIT 100), click a
        column to insert it, and track any result as a certifiable answer.
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
          onPreview={previewTable}
        />

        <div style={{ minWidth: 0 }}>
          <div style={{ display: "flex", gap: 8, marginBottom: 10, flexWrap: "wrap" }}>
            {sources.map((s) => (
              <button
                key={s.name}
                type="button"
                onClick={() => switchSource(s.name)}
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
            placeholder={"SELECT a.category, AVG(b.value) AS avg_value\nFROM table_a a JOIN table_b b ON a.id = b.a_id\nGROUP BY a.category ORDER BY avg_value DESC"}
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
              onClick={() => void run()}
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
            {result && !error && (
              <button
                onClick={openPromote}
                style={{
                  appearance: "none",
                  cursor: "pointer",
                  marginLeft: "auto",
                  background: promoteOpen ? HW2_COLOR.blueSoft : "#fff",
                  color: HW2_COLOR.blue,
                  border: `1.5px solid ${HW2_COLOR.blue}`,
                  borderRadius: 8,
                  padding: "8px 14px",
                  font: "600 12.5px 'DM Sans', sans-serif",
                }}
              >
                + Track as insight
              </button>
            )}
          </div>

          {promoteOpen && result && (
            <div
              style={{
                marginTop: 12,
                padding: "14px 16px",
                background: HW2_COLOR.surface,
                border: `1px solid ${HW2_COLOR.rule}`,
                borderRadius: 10,
              }}
            >
              <div
                style={{
                  font: "600 11px 'DM Sans', sans-serif",
                  letterSpacing: "0.06em",
                  textTransform: "uppercase",
                  color: HW2_COLOR.muted,
                  marginBottom: 4,
                }}
              >
                Track as a certifiable insight
              </div>
              <p
                style={{
                  font: "400 12.5px 'DM Sans', sans-serif",
                  color: HW2_COLOR.muted,
                  margin: "0 0 12px",
                  lineHeight: 1.5,
                }}
              >
                Saves this query as a tracked question. It runs through the same
                draft → finding → two-factor certification as proposed questions.
              </p>
              {sourceProjects.length === 0 ? (
                <p style={{ font: "400 12.5px 'DM Sans', sans-serif", color: HW2_COLOR.faint, margin: 0 }}>
                  No project is framed on <b>{source}</b> yet. Frame a project on
                  this source first, then track queries into it.
                </p>
              ) : (
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                  <input
                    value={promoteTitle}
                    onChange={(e) => setPromoteTitle(e.target.value)}
                    placeholder="Question title (e.g. Exam volume by modality)"
                    style={{
                      flex: "1 1 260px",
                      minWidth: 0,
                      padding: "9px 12px",
                      background: "#fff",
                      border: `1px solid ${HW2_COLOR.rule2}`,
                      borderRadius: 8,
                      font: "500 13px 'DM Sans', sans-serif",
                      color: HW2_COLOR.ink,
                      outline: "none",
                    }}
                  />
                  <select
                    value={promoteProject}
                    onChange={(e) => setPromoteProject(e.target.value)}
                    style={{
                      padding: "9px 12px",
                      background: "#fff",
                      border: `1px solid ${HW2_COLOR.rule2}`,
                      borderRadius: 8,
                      font: "500 13px 'DM Sans', sans-serif",
                      color: HW2_COLOR.ink2,
                    }}
                  >
                    {sourceProjects.map((p) => (
                      <option key={p.id} value={p.id}>
                        {p.display_name}
                      </option>
                    ))}
                  </select>
                  <button
                    onClick={promote}
                    disabled={promoting || !promoteTitle.trim() || !promoteProject}
                    style={{
                      appearance: "none",
                      cursor: promoting ? "default" : "pointer",
                      background: HW2_COLOR.blue,
                      color: "#fff",
                      border: "1px solid transparent",
                      borderRadius: 8,
                      padding: "9px 16px",
                      font: "600 13px 'DM Sans', sans-serif",
                      opacity: promoting || !promoteTitle.trim() || !promoteProject ? 0.5 : 1,
                    }}
                  >
                    {promoting ? "Tracking…" : "Track"}
                  </button>
                </div>
              )}
            </div>
          )}

          {promoted && (
            <div
              style={{
                marginTop: 12,
                padding: "12px 16px",
                background: HW2_COLOR.goodSoft,
                border: `1px solid ${HW2_COLOR.good}44`,
                borderRadius: 10,
                font: "400 13px 'DM Sans', sans-serif",
                color: HW2_COLOR.ink2,
              }}
            >
              Tracked “{promoted.title}”.{" "}
              <Link
                href={`/h2/projects/${promoted.projectId}/answer`}
                style={{ color: HW2_COLOR.blue, fontWeight: 600 }}
              >
                Open in answers →
              </Link>
            </div>
          )}

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
