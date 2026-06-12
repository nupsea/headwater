"use client";

import { useCallback, useEffect, useState } from "react";
import { useParams, useRouter, useSearchParams } from "next/navigation";
import {
  h2,
  type H2CatalogTable,
  type H2CatalogColumn,
  type H2BrowseTable,
} from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";
import { useConfirm } from "@/components/h2/confirm-dialog";
import { useH2Context } from "@/app/h2/layout";

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
  onRemoved,
  confirm,
}: {
  table: H2CatalogTable;
  sourceName: string;
  defaultOpen?: boolean;
  onRemoved: () => void | Promise<void>;
  confirm: (opts: {
    title: string;
    body: string;
    confirmLabel?: string;
    danger?: boolean;
  }) => Promise<boolean>;
}) {
  const [open, setOpen] = useState(defaultOpen);
  const [removing, setRemoving] = useState(false);

  const removeTable = async (e: React.MouseEvent) => {
    e.stopPropagation();
    const ok = await confirm({
      title: `Remove table "${table.table_name}"?`,
      body:
        "Its columns, profiles, and relationships are dropped from this " +
        "source's catalog, and any project using it is updated. The table " +
        "stays in the source system and can be re-added from Browse.",
      confirmLabel: "Remove table",
      danger: true,
    });
    if (!ok) return;
    setRemoving(true);
    try {
      await h2.sources.removeTable(sourceName, table.table_name);
      await onRemoved();
    } catch (err) {
      alert(err instanceof Error ? err.message : "Failed to remove table");
      setRemoving(false);
    }
  };

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
        {table.profiled ? (
          <span
            style={{
              font: "500 11px 'DM Mono', monospace",
              color: HW2_COLOR.muted,
            }}
          >
            {table.row_count.toLocaleString()} rows
          </span>
        ) : (
          <span
            title="Statistics not collected yet — use Refresh stats"
            style={{
              font: "500 11px 'DM Mono', monospace",
              color: HW2_COLOR.faint,
            }}
          >
            rows unknown
          </span>
        )}
        {table.profiled && table.row_count === 0 && (
          <span
            title="Verified empty — no data to answer from; questions will not use this table"
            style={{
              font: "600 10px 'DM Sans', sans-serif",
              color: HW2_COLOR.warn,
              background: HW2_COLOR.warnSoft,
              padding: "2px 7px",
              borderRadius: 4,
              flexShrink: 0,
            }}
          >
            empty — no usable data
          </span>
        )}
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
          role="button"
          tabIndex={0}
          title="Remove this table from the catalog"
          aria-label={`Remove table ${table.table_name}`}
          onClick={removeTable}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === " ") removeTable(e as unknown as React.MouseEvent);
          }}
          style={{
            font: "500 11px 'DM Sans', sans-serif",
            color: removing ? HW2_COLOR.faint : HW2_COLOR.bad,
            border: `1px solid ${HW2_COLOR.bad}44`,
            borderRadius: 6,
            padding: "3px 8px",
            flexShrink: 0,
            cursor: removing ? "default" : "pointer",
            opacity: removing ? 0.6 : 1,
          }}
        >
          {removing ? "Removing…" : "Remove"}
        </span>
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

function BrowseCatalogPanel({
  sourceName,
  onClose,
  onIngested,
}: {
  sourceName: string;
  onClose: () => void;
  onIngested: () => void | Promise<void>;
}) {
  const [rows, setRows] = useState<H2BrowseTable[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [ingesting, setIngesting] = useState(false);

  useEffect(() => {
    let live = true;
    h2.sources
      .browse(sourceName)
      .then((r) => {
        if (!live) return;
        setRows(r);
      })
      .catch((e: unknown) => {
        if (!live) return;
        setError(e instanceof Error ? e.message : "Failed to list catalog");
      })
      .finally(() => live && setLoading(false));
    return () => {
      live = false;
    };
  }, [sourceName]);

  const filtered = search
    ? rows.filter((r) => r.table.toLowerCase().includes(search.toLowerCase()))
    : rows;

  const toggle = (table: string) =>
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(table)) next.delete(table);
      else next.add(table);
      return next;
    });

  const selectableVisible = filtered.filter((r) => !r.ingested);
  const allVisibleSelected =
    selectableVisible.length > 0 &&
    selectableVisible.every((r) => selected.has(r.table));
  const toggleAllVisible = () =>
    setSelected((prev) => {
      const next = new Set(prev);
      if (allVisibleSelected) {
        for (const r of selectableVisible) next.delete(r.table);
      } else {
        for (const r of selectableVisible) next.add(r.table);
      }
      return next;
    });

  const add = async () => {
    if (selected.size === 0) return;
    setIngesting(true);
    setError(null);
    try {
      const r = await h2.sources.ingest(sourceName, [...selected]);
      await onIngested();
      if (r.failed?.length) {
        // Partial success: keep the panel open and say exactly what failed.
        setSelected(new Set(r.failed.map((f) => f.table)));
        setRows((prev) =>
          prev.map((row) =>
            r.ingested.includes(row.table) ? { ...row, ingested: true } : row
          )
        );
        setError(
          `Added ${r.ingested.length} of ${r.ingested.length + r.failed.length}. Failed: ` +
            r.failed.map((f) => `${f.table} — ${f.error}`).join("; ")
        );
        setIngesting(false);
        return;
      }
      onClose();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to add tables");
      setIngesting(false);
    }
  };

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(20,20,25,0.32)",
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "center",
        padding: "60px 20px",
        zIndex: 50,
      }}
      onClick={onClose}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          background: HW2_COLOR.paper,
          border: `1px solid ${HW2_COLOR.rule2}`,
          borderRadius: 14,
          width: "100%",
          maxWidth: 620,
          maxHeight: "80vh",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          fontFamily: "'DM Sans', sans-serif",
        }}
      >
        <div style={{ padding: "20px 22px 14px" }}>
          <div
            style={{
              font: "600 11px 'DM Sans', sans-serif",
              color: HW2_COLOR.blue,
              letterSpacing: "0.08em",
              textTransform: "uppercase",
            }}
          >
            Add tables
          </div>
          <h2
            style={{
              font: "600 19px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink,
              margin: "6px 0 4px",
              letterSpacing: "-0.01em",
            }}
          >
            Browse source catalog
          </h2>
          <p
            style={{
              font: "400 12.5px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              margin: 0,
              lineHeight: 1.5,
            }}
          >
            Listing is metadata only — no rows are read. Selected tables are
            registered into this source&apos;s catalog.
          </p>
        </div>

        <div style={{ padding: "0 22px 12px" }}>
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search tables…"
            style={{
              width: "100%",
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

        <div
          style={{
            flex: 1,
            overflowY: "auto",
            borderTop: `1px solid ${HW2_COLOR.rule}`,
            borderBottom: `1px solid ${HW2_COLOR.rule}`,
          }}
        >
          {loading ? (
            <div
              style={{
                padding: "32px",
                textAlign: "center",
                font: "400 13px 'DM Sans', sans-serif",
                color: HW2_COLOR.muted,
              }}
            >
              Listing catalog…
            </div>
          ) : filtered.length === 0 ? (
            <div
              style={{
                padding: "32px",
                textAlign: "center",
                font: "400 13px 'DM Sans', sans-serif",
                color: HW2_COLOR.faint,
              }}
            >
              {rows.length === 0
                ? "No tables visible to this connection."
                : "No tables match."}
            </div>
          ) : (
            <>
              {selectableVisible.length > 0 && (
                <button
                  onClick={toggleAllVisible}
                  style={{
                    appearance: "none",
                    cursor: "pointer",
                    width: "100%",
                    textAlign: "left",
                    background: "transparent",
                    border: "none",
                    borderBottom: `1px solid ${HW2_COLOR.rule}`,
                    padding: "8px 22px",
                    font: "600 11px 'DM Sans', sans-serif",
                    color: HW2_COLOR.blue,
                    fontFamily: "'DM Sans', sans-serif",
                  }}
                >
                  {allVisibleSelected ? "Clear selection" : "Select all shown"}
                </button>
              )}
              {filtered.map((r) => {
                const checked = selected.has(r.table) || r.ingested;
                return (
                  <label
                    key={r.table}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 12,
                      padding: "9px 22px",
                      borderBottom: `1px solid ${HW2_COLOR.rule}`,
                      cursor: r.ingested ? "default" : "pointer",
                      opacity: r.ingested ? 0.6 : 1,
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      disabled={r.ingested}
                      onChange={() => toggle(r.table)}
                      style={{ cursor: r.ingested ? "default" : "pointer" }}
                    />
                    <span
                      style={{
                        flex: 1,
                        font: "500 12.5px 'DM Mono', monospace",
                        color: HW2_COLOR.ink,
                        minWidth: 0,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {r.table}
                    </span>
                    {r.est_rows != null && (
                      <span
                        style={{
                          font: "500 11px 'DM Mono', monospace",
                          color: HW2_COLOR.faint,
                          flexShrink: 0,
                        }}
                      >
                        ~{r.est_rows.toLocaleString()} rows
                      </span>
                    )}
                    {r.ingested && (
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
                        added
                      </span>
                    )}
                  </label>
                );
              })}
            </>
          )}
        </div>

        {error && (
          <div
            style={{
              padding: "10px 22px",
              font: "500 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.bad,
            }}
          >
            {error}
          </div>
        )}

        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            padding: "14px 22px",
          }}
        >
          <span
            style={{
              flex: 1,
              font: "500 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
            }}
          >
            {selected.size} selected
          </span>
          <button
            onClick={onClose}
            style={{
              appearance: "none",
              cursor: "pointer",
              background: "#fff",
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 8,
              padding: "9px 16px",
              font: "500 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink2,
              fontFamily: "'DM Sans', sans-serif",
            }}
          >
            Cancel
          </button>
          <button
            onClick={add}
            disabled={ingesting || selected.size === 0}
            style={{
              appearance: "none",
              cursor: ingesting || selected.size === 0 ? "default" : "pointer",
              background: HW2_COLOR.ink,
              color: "#fff",
              border: "1px solid transparent",
              borderRadius: 8,
              padding: "9px 18px",
              font: "600 13px 'DM Sans', sans-serif",
              fontFamily: "'DM Sans', sans-serif",
              opacity: ingesting || selected.size === 0 ? 0.5 : 1,
            }}
          >
            {ingesting
              ? "Adding…"
              : `Add ${selected.size || ""} table${selected.size === 1 ? "" : "s"}`}
          </button>
        </div>
      </div>
    </div>
  );
}

export default function SourceCatalogPage() {
  const { name } = useParams<{ name: string }>();
  const router = useRouter();
  const searchParams = useSearchParams();
  const sourceName = decodeURIComponent(name);
  const { activeSource, setActiveSource, reload } = useH2Context();
  const { confirm, confirmDialog } = useConfirm();

  const [tables, setTables] = useState<H2CatalogTable[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [removing, setRemoving] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshNote, setRefreshNote] = useState<string | null>(null);
  // Arriving with ?browse=1 (e.g. right after connecting a warehouse) opens the
  // table picker immediately.
  const [browsing, setBrowsing] = useState(searchParams.get("browse") === "1");

  // Viewing a source makes it the workspace's active source.
  useEffect(() => {
    if (sourceName && sourceName !== activeSource) setActiveSource(sourceName);
  }, [sourceName, activeSource, setActiveSource]);

  const reloadCatalog = useCallback(() => {
    reload(); // keep the rail (snapshot dot, project readouts) in sync
    return h2.sources.catalog(sourceName).then(setTables);
  }, [sourceName, reload]);

  const removeSource = async () => {
    const ok = await confirm({
      title: `Remove source "${sourceName}"?`,
      body:
        "This deletes its catalog and any project that uses only this source. " +
        "This cannot be undone.",
      confirmLabel: "Remove source",
      danger: true,
    });
    if (!ok) return;
    setRemoving(true);
    try {
      await h2.sources.remove(sourceName);
      router.push("/h2");
    } catch (e) {
      alert(e instanceof Error ? e.message : "Failed to remove source");
      setRemoving(false);
    }
  };

  useEffect(() => {
    reloadCatalog().finally(() => setLoading(false));
  }, [reloadCatalog]);

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
      {confirmDialog}
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

        <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
          <button
            onClick={removeSource}
            disabled={removing}
            title="Remove this source and its catalog from Headwater"
            style={{
              appearance: "none",
              cursor: removing ? "default" : "pointer",
              background: "#fff",
              color: HW2_COLOR.bad,
              border: `1px solid ${HW2_COLOR.bad}55`,
              borderRadius: 10,
              padding: "10px 16px",
              font: "500 13px 'DM Sans', sans-serif",
              fontFamily: "'DM Sans', sans-serif",
              whiteSpace: "nowrap",
              opacity: removing ? 0.6 : 1,
            }}
          >
            {removing ? "Removing…" : "Remove source"}
          </button>
          <button
            onClick={async () => {
              setRefreshing(true);
              setRefreshNote(null);
              try {
                const r = await h2.sources.refreshStats(sourceName);
                await reloadCatalog();
                const problems = [
                  ...(r.failed ?? []),
                  ...(r.unprofiled ?? []),
                ];
                setRefreshNote(
                  problems.length
                    ? `Profiled ${r.ingested.length - (r.unprofiled?.length ?? 0)} ` +
                        `of ${r.ingested.length + r.failed.length}. Problems: ` +
                        problems.map((f) => `${f.table} (${f.error})`).join("; ")
                    : `Profiled ${r.ingested.length} table(s).`
                );
              } catch (e) {
                setRefreshNote(
                  e instanceof Error ? e.message : "Failed to refresh statistics"
                );
              } finally {
                setRefreshing(false);
              }
            }}
            disabled={refreshing}
            title="Re-profile all ingested tables (row counts, null rates, distincts, min/max)"
            style={{
              appearance: "none",
              cursor: refreshing ? "default" : "pointer",
              background: "#fff",
              color: HW2_COLOR.ink2,
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 10,
              padding: "10px 16px",
              font: "500 13px 'DM Sans', sans-serif",
              fontFamily: "'DM Sans', sans-serif",
              whiteSpace: "nowrap",
              opacity: refreshing ? 0.6 : 1,
            }}
          >
            {refreshing ? "Profiling…" : "Refresh stats"}
          </button>
          <button
            onClick={() => setBrowsing(true)}
            title="Browse the source catalog and add tables"
            style={{
              appearance: "none",
              cursor: "pointer",
              background: "#fff",
              color: HW2_COLOR.ink2,
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 10,
              padding: "10px 16px",
              font: "500 13px 'DM Sans', sans-serif",
              fontFamily: "'DM Sans', sans-serif",
              whiteSpace: "nowrap",
            }}
          >
            + Add tables
          </button>
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
      </div>

      {refreshNote && (
        <div
          style={{
            marginBottom: 16,
            padding: "10px 14px",
            background: HW2_COLOR.chip,
            borderRadius: 8,
            font: "400 12.5px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink2,
          }}
        >
          {refreshNote}
        </div>
      )}

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
          {search ? (
            "No tables match."
          ) : (
            <div style={{ display: "grid", gap: 14, justifyItems: "center" }}>
              <span>No tables in this source&apos;s catalog yet.</span>
              <button
                onClick={() => setBrowsing(true)}
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
                }}
              >
                Browse catalog &amp; add tables
              </button>
            </div>
          )}
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
              onRemoved={reloadCatalog}
              confirm={confirm}
            />
          ))}
        </div>
      )}

      {browsing && (
        <BrowseCatalogPanel
          sourceName={sourceName}
          onClose={() => setBrowsing(false)}
          onIngested={reloadCatalog}
        />
      )}
    </div>
  );
}
