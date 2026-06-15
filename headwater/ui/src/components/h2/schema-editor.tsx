"use client";

import { useCallback, useEffect, useState } from "react";
import {
  h2,
  notifyInputChanged,
  type H2CatalogColumn,
  type H2CatalogTable,
  type H2Relationship,
} from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

// Generic, domain-agnostic option sets. The current inferred value is always
// pre-selected (and injected if not already present).
const DTYPE_OPTIONS = [
  "varchar",
  "text",
  "integer",
  "bigint",
  "double",
  "decimal",
  "boolean",
  "date",
  "timestamp",
  "time",
  "json",
];
const ROLE_OPTIONS = [
  "identifier",
  "foreign_key",
  "dimension",
  "category",
  "measure",
  "metric",
  "timestamp",
  "flag",
  "geo",
  "text",
];

function withCurrent(options: string[], current: string): string[] {
  const v = (current || "").trim();
  return v && !options.includes(v) ? [v, ...options] : options;
}

const cellInput: React.CSSProperties = {
  background: "#fff",
  border: `1px solid ${HW2_COLOR.rule2}`,
  borderRadius: 6,
  padding: "5px 8px",
  font: "400 12.5px 'DM Sans', sans-serif",
  color: HW2_COLOR.ink,
  outline: "none",
  boxSizing: "border-box",
  width: "100%",
};

function ColumnRow({
  sourceName,
  tableName,
  column,
  isKey,
}: {
  sourceName: string;
  tableName: string;
  column: H2CatalogColumn;
  isKey: boolean;
}) {
  const [desc, setDesc] = useState(column.description ?? "");
  const [dtype, setDtype] = useState(column.dtype ?? "varchar");
  const [role, setRole] = useState(column.semantic_type ?? "");
  const [saving, setSaving] = useState(false);

  const save = async (next: {
    description?: string;
    semantic_type?: string;
    dtype?: string;
  }) => {
    setSaving(true);
    try {
      await h2.sources.updateColumn(sourceName, tableName, column.column_name, next);
      notifyInputChanged();
    } finally {
      setSaving(false);
    }
  };

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "190px 120px 130px 1fr",
        gap: 10,
        alignItems: "center",
        padding: "7px 0",
        borderBottom: `1px solid ${HW2_COLOR.rule}`,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 6, minWidth: 0 }}>
        {isKey && (
          <span title="join key" style={{ color: HW2_COLOR.blue, fontSize: 11 }}>
            ⚷
          </span>
        )}
        <span
          style={{
            font: "500 12.5px 'DM Mono', monospace",
            color: HW2_COLOR.ink2,
            overflow: "hidden",
            textOverflow: "ellipsis",
          }}
        >
          {column.column_name}
        </span>
      </div>

      <select
        value={dtype}
        disabled={column.locked || saving}
        onChange={(e) => {
          setDtype(e.target.value);
          save({ dtype: e.target.value });
        }}
        style={{ ...cellInput, fontFamily: "'DM Mono', monospace", fontSize: 12 }}
      >
        {withCurrent(DTYPE_OPTIONS, dtype).map((o) => (
          <option key={o} value={o}>
            {o}
          </option>
        ))}
      </select>

      <select
        value={role}
        disabled={column.locked || saving}
        onChange={(e) => {
          setRole(e.target.value);
          save({ semantic_type: e.target.value });
        }}
        style={cellInput}
      >
        <option value="">— role —</option>
        {withCurrent(ROLE_OPTIONS, role).map((o) => (
          <option key={o} value={o}>
            {o}
          </option>
        ))}
      </select>

      <input
        value={desc}
        placeholder="describe this column…"
        disabled={column.locked || saving}
        onChange={(e) => setDesc(e.target.value)}
        onBlur={() => desc !== (column.description ?? "") && save({ description: desc })}
        style={cellInput}
      />
    </div>
  );
}

export function SchemaEditor({
  sourceName,
  projectId,
}: {
  sourceName: string;
  projectId: string;
}) {
  const [tables, setTables] = useState<H2CatalogTable[]>([]);
  const [rels, setRels] = useState<H2Relationship[]>([]);
  const [loading, setLoading] = useState(true);
  const [generating, setGenerating] = useState(false);
  const [genNote, setGenNote] = useState("");
  const [genError, setGenError] = useState(false);

  const load = useCallback(() => {
    Promise.all([
      h2.sources.catalog(sourceName, undefined, projectId),
      h2.sources.relationships(sourceName),
    ])
      .then(([cat, r]) => {
        setTables(cat);
        setRels(r);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [sourceName, projectId]);

  useEffect(() => {
    load();
  }, [load]);

  const generate = async () => {
    setGenerating(true);
    setGenNote("");
    setGenError(false);
    try {
      const r = await h2.sources.generateDescriptions(sourceName);
      if (!r.available) {
        // The AI couldn't run — show the concrete reason, never fail silently.
        setGenError(true);
        setGenNote(r.note || "AI is unavailable right now.");
      } else {
        setGenError(false);
        const base =
          r.updated > 0
            ? `Generated ${r.updated} description${r.updated === 1 ? "" : "s"}.`
            : "No new descriptions to add.";
        setGenNote(r.note ? `${base} ${r.note}` : base);
        load();
        notifyInputChanged();
      }
    } catch (e) {
      setGenError(true);
      setGenNote(e instanceof Error ? e.message : "Failed to generate descriptions.");
    } finally {
      setGenerating(false);
    }
  };

  if (loading) return null;

  const keyCols = new Set<string>();
  for (const r of rels) {
    keyCols.add(`${r.from_table}.${r.from_column}`);
    keyCols.add(`${r.to_table}.${r.to_column}`);
  }

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <button
          onClick={generate}
          disabled={generating}
          style={{
            appearance: "none",
            cursor: generating ? "default" : "pointer",
            background: HW2_COLOR.blueSoft,
            border: `1px solid ${HW2_COLOR.blue}44`,
            borderRadius: 8,
            padding: "7px 13px",
            font: "600 12px 'DM Sans', sans-serif",
            color: HW2_COLOR.blue,
            fontFamily: "'DM Sans', sans-serif",
            opacity: generating ? 0.6 : 1,
          }}
        >
          {generating ? "Generating…" : "✦ Generate descriptions with AI"}
        </button>
        {genNote && (
          <span
            style={{
              font: `${genError ? 500 : 400} 12px 'DM Sans', sans-serif`,
              color: genError ? HW2_COLOR.bad : HW2_COLOR.muted,
            }}
          >
            {genError ? "⚠ " : ""}
            {genNote}
          </span>
        )}
      </div>

      {tables.map((t) => (
        <div
          key={t.table_name}
          style={{
            background: HW2_COLOR.surface,
            border: `1px solid ${HW2_COLOR.rule}`,
            borderRadius: 12,
            padding: "16px 20px",
          }}
        >
          <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 10 }}>
            <span style={{ font: "600 14px 'DM Mono', monospace", color: HW2_COLOR.ink }}>
              {t.table_name}
            </span>
            <span style={{ font: "400 11px 'DM Mono', monospace", color: HW2_COLOR.faint }}>
              {t.row_count.toLocaleString()} rows · {t.columns.length} columns
            </span>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "190px 120px 130px 1fr",
              gap: 10,
              padding: "0 0 4px",
              font: "600 10px 'DM Sans', sans-serif",
              color: HW2_COLOR.faint,
              textTransform: "uppercase",
              letterSpacing: "0.05em",
            }}
          >
            <span>Column</span>
            <span>Type</span>
            <span>Role</span>
            <span>Description</span>
          </div>
          {t.columns.map((c) => (
            <ColumnRow
              key={c.column_name}
              sourceName={sourceName}
              tableName={t.table_name}
              column={c}
              isKey={keyCols.has(`${t.table_name}.${c.column_name}`)}
            />
          ))}
        </div>
      ))}

      {rels.length > 0 && (
        <div
          style={{
            background: HW2_COLOR.surface,
            border: `1px solid ${HW2_COLOR.rule}`,
            borderRadius: 12,
            padding: "16px 20px",
          }}
        >
          <div
            style={{
              font: "600 11px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              textTransform: "uppercase",
              letterSpacing: "0.06em",
              marginBottom: 12,
            }}
          >
            Inferred relationships
          </div>
          <div style={{ display: "grid", gap: 8 }}>
            {rels.map((r, i) => (
              <div
                key={i}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 10,
                  font: "500 12px 'DM Mono', monospace",
                  color: HW2_COLOR.ink2,
                }}
              >
                <span>
                  {r.from_table}.<strong>{r.from_column}</strong>
                </span>
                <span style={{ color: HW2_COLOR.faint }}>→</span>
                <span>
                  {r.to_table}.<strong>{r.to_column}</strong>
                </span>
                <span
                  style={{
                    marginLeft: "auto",
                    font: "500 11px 'DM Sans', sans-serif",
                    color: r.confidence >= 0.8 ? HW2_COLOR.good : HW2_COLOR.warn,
                  }}
                >
                  {Math.round(r.confidence * 100)}% confident
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
