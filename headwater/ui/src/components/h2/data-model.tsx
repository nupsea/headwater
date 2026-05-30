"use client";

import { useEffect, useState } from "react";
import {
  h2,
  type H2CatalogTable,
  type H2Relationship,
} from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

const NODE_W = 220;
const NODE_H = 150;
const GAP_X = 72;
const GAP_Y = 52;

export function DataModelDiagram({ sourceName }: { sourceName: string }) {
  const [tables, setTables] = useState<H2CatalogTable[]>([]);
  const [rels, setRels] = useState<H2Relationship[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      h2.sources.catalog(sourceName),
      h2.sources.relationships(sourceName),
    ])
      .then(([cat, r]) => {
        setTables(cat);
        setRels(r);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [sourceName]);

  if (loading || tables.length === 0) return null;

  const cols = Math.min(3, Math.ceil(Math.sqrt(tables.length)));
  const pos = new Map<string, { x: number; y: number }>();
  tables.forEach((t, i) => {
    const c = i % cols;
    const r = Math.floor(i / cols);
    pos.set(t.table_name, { x: c * (NODE_W + GAP_X), y: r * (NODE_H + GAP_Y) });
  });
  const rows = Math.ceil(tables.length / cols);
  const width = cols * NODE_W + (cols - 1) * GAP_X;
  const height = rows * NODE_H + (rows - 1) * GAP_Y;

  // Key columns = those participating in a relationship.
  const keyCols = new Map<string, Set<string>>();
  for (const r of rels) {
    for (const [t, c] of [
      [r.from_table, r.from_column],
      [r.to_table, r.to_column],
    ]) {
      if (!keyCols.has(t)) keyCols.set(t, new Set());
      keyCols.get(t)!.add(c);
    }
  }

  const center = (name: string) => {
    const p = pos.get(name);
    if (!p) return null;
    return { x: p.x + NODE_W / 2, y: p.y + NODE_H / 2 };
  };

  return (
    <div style={{ overflowX: "auto", padding: "4px 0 8px" }}>
      <div style={{ position: "relative", width, height, minWidth: width }}>
        <svg
          width={width}
          height={height}
          style={{ position: "absolute", top: 0, left: 0, zIndex: 0, overflow: "visible" }}
        >
          {rels.map((r, i) => {
            const a = center(r.from_table);
            const b = center(r.to_table);
            if (!a || !b) return null;
            const stroke = r.confidence >= 0.8 ? HW2_COLOR.good : HW2_COLOR.warn;
            const mx = (a.x + b.x) / 2;
            const my = (a.y + b.y) / 2;
            return (
              <g key={i}>
                <line
                  x1={a.x}
                  y1={a.y}
                  x2={b.x}
                  y2={b.y}
                  stroke={stroke}
                  strokeWidth={1.5}
                  strokeDasharray={r.confidence >= 0.8 ? undefined : "5 4"}
                  opacity={0.55}
                />
                <text
                  x={mx}
                  y={my - 4}
                  textAnchor="middle"
                  style={{
                    font: "500 10px 'DM Mono', monospace",
                    fill: stroke,
                  }}
                >
                  {r.from_column} → {r.to_column} · {Math.round(r.confidence * 100)}%
                </text>
              </g>
            );
          })}
        </svg>

        {tables.map((t) => {
          const p = pos.get(t.table_name)!;
          const keys = [...(keyCols.get(t.table_name) ?? [])];
          return (
            <div
              key={t.table_name}
              style={{
                position: "absolute",
                left: p.x,
                top: p.y,
                width: NODE_W,
                height: NODE_H,
                zIndex: 1,
                background: "#fff",
                border: `1px solid ${HW2_COLOR.rule2}`,
                borderRadius: 10,
                boxShadow: "0 1px 3px rgba(20,20,30,0.06)",
                overflow: "hidden",
                display: "flex",
                flexDirection: "column",
              }}
            >
              <div
                style={{
                  padding: "8px 12px",
                  borderBottom: `1px solid ${HW2_COLOR.rule}`,
                  background: HW2_COLOR.surface,
                }}
              >
                <div style={{ font: "600 13px 'DM Mono', monospace", color: HW2_COLOR.ink }}>
                  {t.table_name}
                </div>
                <div style={{ font: "400 10px 'DM Mono', monospace", color: HW2_COLOR.faint }}>
                  {t.row_count.toLocaleString()} rows · {t.columns.length} cols
                </div>
              </div>
              <div style={{ padding: "8px 12px", overflow: "hidden", flex: 1 }}>
                {keys.length > 0 ? (
                  keys.slice(0, 5).map((k) => (
                    <div
                      key={k}
                      style={{
                        font: "500 11.5px 'DM Mono', monospace",
                        color: HW2_COLOR.blue,
                        display: "flex",
                        alignItems: "center",
                        gap: 5,
                        padding: "1px 0",
                      }}
                    >
                      <span style={{ fontSize: 9 }}>⚷</span>
                      {k}
                    </div>
                  ))
                ) : (
                  <div style={{ font: "400 11px 'DM Sans', sans-serif", color: HW2_COLOR.faint }}>
                    no detected join keys
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
