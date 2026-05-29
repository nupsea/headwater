"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { h2, type H2CatalogTable, type H2CatalogColumn } from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        font: "600 11px 'DM Sans', sans-serif",
        color: HW2_COLOR.muted,
        textTransform: "uppercase",
        letterSpacing: "0.06em",
        marginBottom: 10,
      }}
    >
      {children}
    </div>
  );
}

function ColumnRow({
  column,
  sourceName,
  tableName,
}: {
  column: H2CatalogColumn;
  sourceName: string;
  tableName: string;
}) {
  const [editing, setEditing] = useState(false);
  const [desc, setDesc] = useState(column.description ?? "");
  const [saving, setSaving] = useState(false);

  const save = async () => {
    setSaving(true);
    try {
      await h2.sources.updateColumn(sourceName, tableName, column.column_name, {
        description: desc || undefined,
      });
      setEditing(false);
    } catch {
      // ignore
    } finally {
      setSaving(false);
    }
  };

  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-start",
        gap: 14,
        padding: "9px 16px",
        borderBottom: `1px solid ${HW2_COLOR.rule}`,
      }}
    >
      <div style={{ minWidth: 180, flexShrink: 0 }}>
        <div
          style={{
            font: "500 12.5px 'DM Mono', monospace",
            color: HW2_COLOR.ink,
          }}
        >
          {column.column_name}
        </div>
        <div
          style={{
            font: "400 10.5px 'DM Mono', monospace",
            color: HW2_COLOR.faint,
            marginTop: 2,
          }}
        >
          {column.dtype}
        </div>
      </div>

      <div style={{ minWidth: 100, flexShrink: 0 }}>
        {column.semantic_type && (
          <span
            style={{
              font: "500 10px 'DM Sans', sans-serif",
              color: HW2_COLOR.blue,
              background: HW2_COLOR.blueSoft,
              padding: "2px 7px",
              borderRadius: 4,
              letterSpacing: "0.01em",
            }}
          >
            {column.semantic_type}
          </span>
        )}
      </div>

      <div style={{ flex: 1, minWidth: 0 }}>
        {editing ? (
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <input
              value={desc}
              onChange={(e) => setDesc(e.target.value)}
              autoFocus
              style={{
                flex: 1,
                padding: "5px 10px",
                background: "#fff",
                border: `1px solid ${HW2_COLOR.blue}`,
                borderRadius: 6,
                font: "400 12px 'DM Sans', sans-serif",
                color: HW2_COLOR.ink,
                fontFamily: "'DM Sans', sans-serif",
                outline: "none",
              }}
            />
            <button
              onClick={save}
              disabled={saving}
              style={{
                appearance: "none",
                cursor: "pointer",
                background: HW2_COLOR.blue,
                color: "#fff",
                border: "none",
                borderRadius: 6,
                padding: "5px 10px",
                font: "500 11px 'DM Sans', sans-serif",
                fontFamily: "'DM Sans', sans-serif",
                opacity: saving ? 0.6 : 1,
              }}
            >
              Save
            </button>
            <button
              onClick={() => { setEditing(false); setDesc(column.description ?? ""); }}
              style={{
                appearance: "none",
                cursor: "pointer",
                background: "transparent",
                border: "none",
                font: "500 11px 'DM Sans', sans-serif",
                color: HW2_COLOR.muted,
                fontFamily: "'DM Sans', sans-serif",
                padding: "5px 8px",
              }}
            >
              Cancel
            </button>
          </div>
        ) : (
          <div
            style={{ display: "flex", alignItems: "center", gap: 8 }}
            onDoubleClick={() => setEditing(true)}
          >
            <span
              style={{
                font: "400 12px 'DM Sans', sans-serif",
                color: desc ? HW2_COLOR.ink2 : HW2_COLOR.faint,
                lineHeight: 1.4,
                flex: 1,
              }}
            >
              {desc || "No description — double-click to edit"}
            </span>
            {column.locked && (
              <span
                style={{
                  font: "500 10px 'DM Sans', sans-serif",
                  color: HW2_COLOR.good,
                  background: HW2_COLOR.goodSoft,
                  padding: "1px 6px",
                  borderRadius: 3,
                  flexShrink: 0,
                }}
              >
                locked
              </span>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function TableCard({
  table,
  sourceName,
  defaultOpen = false,
}: {
  table: H2CatalogTable;
  sourceName: string;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 12,
        overflow: "hidden",
        marginBottom: 10,
      }}
    >
      <button
        onClick={() => setOpen((v) => !v)}
        style={{
          appearance: "none",
          cursor: "pointer",
          width: "100%",
          background: open ? HW2_COLOR.paper : "transparent",
          border: "none",
          padding: "14px 18px",
          textAlign: "left",
          display: "flex",
          alignItems: "center",
          gap: 14,
          fontFamily: "'DM Sans', sans-serif",
          borderBottom: open ? `1px solid ${HW2_COLOR.rule}` : "none",
        }}
      >
        <span
          style={{
            font: "600 13.5px 'DM Mono', monospace",
            color: HW2_COLOR.ink,
            flex: 1,
          }}
        >
          {table.table_name}
        </span>
        <span
          style={{
            font: "500 11px 'DM Mono', monospace",
            color: HW2_COLOR.muted,
          }}
        >
          {table.row_count.toLocaleString()} rows
        </span>
        <span
          style={{
            font: "500 11px 'DM Mono', monospace",
            color: HW2_COLOR.faint,
          }}
        >
          {table.columns.length} cols
        </span>
        {table.description && (
          <span
            style={{
              font: "400 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              maxWidth: 300,
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {table.description}
          </span>
        )}
        <span
          style={{
            font: "500 11px 'DM Mono', monospace",
            color: HW2_COLOR.faint,
          }}
        >
          {open ? "▾" : "▸"}
        </span>
      </button>

      {open && (
        <div>
          {/* Column header */}
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 14,
              padding: "6px 16px",
              background: HW2_COLOR.paper,
              borderBottom: `1px solid ${HW2_COLOR.rule}`,
            }}
          >
            {[
              { label: "Column", width: 180 },
              { label: "Semantic type", width: 100 },
              { label: "Description", flex: 1 },
            ].map((h) => (
              <div
                key={h.label}
                style={{
                  font: "600 10px 'DM Sans', sans-serif",
                  color: HW2_COLOR.faint,
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  minWidth: h.width,
                  flex: h.flex,
                }}
              >
                {h.label}
              </div>
            ))}
          </div>

          {table.columns.map((col) => (
            <ColumnRow
              key={col.column_name}
              column={col}
              sourceName={sourceName}
              tableName={table.table_name}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export default function SourceCatalogPage() {
  const { name } = useParams<{ name: string }>();
  const router = useRouter();
  const sourceName = decodeURIComponent(name);

  const [tables, setTables] = useState<H2CatalogTable[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");

  useEffect(() => {
    h2.sources
      .catalog(sourceName)
      .then(setTables)
      .finally(() => setLoading(false));
  }, [sourceName]);

  const filtered = search
    ? tables.filter(
        (t) =>
          t.table_name.toLowerCase().includes(search.toLowerCase()) ||
          (t.description ?? "")
            .toLowerCase()
            .includes(search.toLowerCase())
      )
    : tables;

  if (loading) {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          height: "40vh",
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
        }}
      >
        Loading…
      </div>
    );
  }

  return (
    <div
      style={{
        maxWidth: 1060,
        margin: "0 auto",
        padding: "32px 32px 80px",
        fontFamily: "'DM Sans', sans-serif",
      }}
    >
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          marginBottom: 28,
        }}
      >
        <div>
          <span
            style={{
              font: "600 11px 'DM Sans', sans-serif",
              color: HW2_COLOR.blue,
              letterSpacing: "0.08em",
              textTransform: "uppercase",
            }}
          >
            Catalog
          </span>
          <h1
            style={{
              font: "600 26px 'DM Sans', sans-serif",
              letterSpacing: "-0.02em",
              color: HW2_COLOR.ink,
              lineHeight: 1.25,
              marginTop: 6,
              marginBottom: 4,
            }}
          >
            {sourceName}
          </h1>
          <p
            style={{
              font: "400 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
            }}
          >
            {tables.length} table{tables.length !== 1 ? "s" : ""} · double-click descriptions to edit
          </p>
        </div>

        <button
          onClick={() => router.push("/h2/projects/new")}
          style={{
            appearance: "none",
            cursor: "pointer",
            background: HW2_COLOR.ink,
            color: "#fff",
            border: "1px solid transparent",
            borderRadius: 10,
            padding: "10px 18px",
            font: "600 13px 'DM Sans', sans-serif",
            fontFamily: "'DM Sans', sans-serif",
            whiteSpace: "nowrap",
          }}
        >
          + New project
        </button>
      </div>

      {/* Search */}
      <div style={{ marginBottom: 20 }}>
        <input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search tables…"
          style={{
            width: "100%",
            maxWidth: 360,
            padding: "9px 14px",
            background: "#fff",
            border: `1px solid ${HW2_COLOR.rule2}`,
            borderRadius: 8,
            font: "400 13px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink,
            fontFamily: "'DM Sans', sans-serif",
            outline: "none",
            boxSizing: "border-box",
          }}
          onFocus={(e) => (e.currentTarget.style.borderColor = HW2_COLOR.blue)}
          onBlur={(e) =>
            (e.currentTarget.style.borderColor = HW2_COLOR.rule2)
          }
        />
      </div>

      {/* Tables */}
      {filtered.length === 0 ? (
        <div
          style={{
            padding: "40px",
            textAlign: "center",
            font: "400 13px 'DM Sans', sans-serif",
            color: HW2_COLOR.faint,
          }}
        >
          {search ? "No tables match." : "No tables found in this source."}
        </div>
      ) : (
        <div>
          <SectionLabel>
            Tables ({filtered.length})
          </SectionLabel>
          {filtered.map((t, i) => (
            <TableCard
              key={t.table_name}
              table={t}
              sourceName={sourceName}
              defaultOpen={i === 0 && filtered.length === 1}
            />
          ))}
        </div>
      )}
    </div>
  );
}
