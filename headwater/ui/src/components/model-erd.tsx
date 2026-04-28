"use client";

/* Adaptive ERD Canvas
 * - Pan (drag) + zoom (wheel / buttons)
 * - Domain-clustered grid layout, scales to many tables
 * - Level-of-detail rendering: dots when zoomed out, cards mid, columns when expanded
 * - Click a table to drill into columns inline; connected tables/edges highlight
 * - Search jumps to a table; minimap shows where you are
 */

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type WheelEvent as ReactWheelEvent,
  type MouseEvent as ReactMouseEvent,
} from "react";
import { api, type GraphData, type GraphNode, type GraphEdge, type TableHealth, type TableDetail } from "@/lib/api";

// ---------- Constants ----------

const CARD_W = 200;
const CARD_H = 110;
const CARD_GAP = 14;
const CLUSTER_PAD = 22;
const CLUSTER_GAP = 36;
const ROW_WRAP_WIDTH = 1400; // virtual canvas wrap width
const COMPACT_THRESHOLD = 60; // auto-switch to compact dots

const DOMAIN_COLORS = [
  "#6366f1", "#0ea5e9", "#10b981", "#f59e0b", "#ef4444",
  "#8b5cf6", "#ec4899", "#14b8a6", "#f97316", "#84cc16",
  "#0891b2", "#a855f7", "#dc2626", "#ca8a04", "#16a34a",
];

const DEFAULT_DOMAIN = "Other";

function colorFor(domain: string, palette: string[]): string {
  // stable hash so the same domain always gets the same color
  let h = 0;
  for (let i = 0; i < domain.length; i++) h = (h * 31 + domain.charCodeAt(i)) | 0;
  return palette[Math.abs(h) % palette.length];
}

// ---------- Layout ----------

interface LaidOutNode extends GraphNode {
  x: number;
  y: number;
  w: number;
  h: number;
  pk: string[];
  fkCount: number;
}

interface Cluster {
  domain: string;
  x: number;
  y: number;
  w: number;
  h: number;
  count: number;
  color: string;
}

function layoutNodes(
  nodes: GraphNode[],
  edges: GraphEdge[],
  health: Map<string, TableHealth>,
): { nodes: LaidOutNode[]; clusters: Cluster[]; bounds: { w: number; h: number } } {
  // Group by domain
  const byDomain = new Map<string, GraphNode[]>();
  for (const n of nodes) {
    const d = n.domain || DEFAULT_DOMAIN;
    if (!byDomain.has(d)) byDomain.set(d, []);
    byDomain.get(d)!.push(n);
  }

  // Order domains: most-connected first, then greedy nearest-neighbor for adjacency
  const domAdj = new Map<string, Map<string, number>>();
  const idDomain = new Map<string, string>();
  for (const n of nodes) idDomain.set(n.id, n.domain || DEFAULT_DOMAIN);
  for (const e of edges) {
    const a = idDomain.get(e.source);
    const b = idDomain.get(e.target);
    if (!a || !b || a === b) continue;
    if (!domAdj.has(a)) domAdj.set(a, new Map());
    if (!domAdj.has(b)) domAdj.set(b, new Map());
    domAdj.get(a)!.set(b, (domAdj.get(a)!.get(b) || 0) + 1);
    domAdj.get(b)!.set(a, (domAdj.get(b)!.get(a) || 0) + 1);
  }
  const allDomains = [...byDomain.keys()].sort(
    (a, b) => byDomain.get(b)!.length - byDomain.get(a)!.length,
  );
  const ordered: string[] = [];
  const placed = new Set<string>();
  if (allDomains.length > 0) {
    ordered.push(allDomains[0]);
    placed.add(allDomains[0]);
  }
  while (ordered.length < allDomains.length) {
    let bestNext = "";
    let bestScore = -1;
    for (const d of allDomains) {
      if (placed.has(d)) continue;
      let score = 0;
      for (const p of placed) score += domAdj.get(p)?.get(d) || 0;
      if (score > bestScore) {
        bestScore = score;
        bestNext = d;
      }
    }
    if (!bestNext) bestNext = allDomains.find((d) => !placed.has(d))!;
    ordered.push(bestNext);
    placed.add(bestNext);
  }

  // Lay out each cluster as a grid; flow clusters in rows
  const out: LaidOutNode[] = [];
  const clusters: Cluster[] = [];
  let cursorX = 0;
  let cursorY = 0;
  let rowHeight = 0;

  for (const domain of ordered) {
    const tables = byDomain.get(domain)!;
    // Sort tables within cluster: those with most connections first, then alphabetical
    const fkCount = new Map<string, number>();
    for (const t of tables) fkCount.set(t.id, 0);
    for (const e of edges) {
      if (fkCount.has(e.source)) fkCount.set(e.source, fkCount.get(e.source)! + 1);
      if (fkCount.has(e.target)) fkCount.set(e.target, fkCount.get(e.target)! + 1);
    }
    tables.sort((a, b) => (fkCount.get(b.id)! - fkCount.get(a.id)!) || a.id.localeCompare(b.id));

    const cols = Math.max(1, Math.ceil(Math.sqrt(tables.length)));
    const rows = Math.ceil(tables.length / cols);
    const innerW = cols * CARD_W + (cols - 1) * CARD_GAP;
    const innerH = rows * CARD_H + (rows - 1) * CARD_GAP;
    const clusterW = innerW + CLUSTER_PAD * 2;
    const clusterH = innerH + CLUSTER_PAD * 2 + 22; // +22 for header

    if (cursorX > 0 && cursorX + clusterW > ROW_WRAP_WIDTH) {
      cursorX = 0;
      cursorY += rowHeight + CLUSTER_GAP;
      rowHeight = 0;
    }

    const color = colorFor(domain, DOMAIN_COLORS);
    clusters.push({
      domain,
      x: cursorX,
      y: cursorY,
      w: clusterW,
      h: clusterH,
      count: tables.length,
      color,
    });

    tables.forEach((t, i) => {
      const r = Math.floor(i / cols);
      const c = i % cols;
      const h = health.get(t.id);
      out.push({
        ...t,
        x: cursorX + CLUSTER_PAD + c * (CARD_W + CARD_GAP),
        y: cursorY + CLUSTER_PAD + 22 + r * (CARD_H + CARD_GAP),
        w: CARD_W,
        h: CARD_H,
        pk: h?.pk_columns ?? [],
        fkCount: h?.fk_columns?.length ?? 0,
      });
    });

    cursorX += clusterW + CLUSTER_GAP;
    rowHeight = Math.max(rowHeight, clusterH);
  }

  const totalW = Math.max(
    ROW_WRAP_WIDTH,
    ...clusters.map((c) => c.x + c.w),
  );
  const totalH = cursorY + rowHeight;
  return { nodes: out, clusters, bounds: { w: totalW, h: totalH } };
}

// ---------- Edge geometry ----------

interface EdgeRoute {
  edge: GraphEdge;
  x1: number; y1: number;
  x2: number; y2: number;
  midX: number; midY: number;
}

function routeEdge(a: LaidOutNode, b: LaidOutNode, edge: GraphEdge): EdgeRoute {
  const ax = a.x + a.w / 2;
  const ay = a.y + a.h / 2;
  const bx = b.x + b.w / 2;
  const by = b.y + b.h / 2;
  // Pick a face on each card closest to the other
  const dx = bx - ax;
  const dy = by - ay;
  let x1 = ax, y1 = ay, x2 = bx, y2 = by;
  if (Math.abs(dx) > Math.abs(dy)) {
    x1 = dx > 0 ? a.x + a.w : a.x;
    y1 = ay;
    x2 = dx > 0 ? b.x : b.x + b.w;
    y2 = by;
  } else {
    x1 = ax;
    y1 = dy > 0 ? a.y + a.h : a.y;
    x2 = bx;
    y2 = dy > 0 ? b.y : b.y + b.h;
  }
  return { edge, x1, y1, x2, y2, midX: (x1 + x2) / 2, midY: (y1 + y2) / 2 };
}

// ---------- Component ----------

interface Props {
  graphData: GraphData;
  tableHealth: TableHealth[];
  height?: number;
}

export function ModelERD({ graphData, tableHealth, height = 620 }: Props) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const [viewportSize, setViewportSize] = useState({ w: 1000, h: height });

  const [tx, setTx] = useState(0);
  const [ty, setTy] = useState(0);
  const [k, setK] = useState(1);

  const [selected, setSelected] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [expandedDetail, setExpandedDetail] = useState<TableDetail | null>(null);
  const [expandedLoading, setExpandedLoading] = useState(false);
  const [expandedError, setExpandedError] = useState<string | null>(null);

  const [search, setSearch] = useState("");
  const [density, setDensity] = useState<"auto" | "card" | "compact">("auto");
  const [hoveredEdge, setHoveredEdge] = useState<number | null>(null);
  const [showMinimap, setShowMinimap] = useState(true);

  // Build health lookup
  const healthMap = useMemo(() => {
    const m = new Map<string, TableHealth>();
    for (const h of tableHealth) m.set(h.name, h);
    return m;
  }, [tableHealth]);

  // Layout (memoized — recomputes only when input data changes)
  const layout = useMemo(
    () => layoutNodes(graphData.nodes, graphData.edges, healthMap),
    [graphData, healthMap],
  );

  const nodeMap = useMemo(() => {
    const m = new Map<string, LaidOutNode>();
    for (const n of layout.nodes) m.set(n.id, n);
    return m;
  }, [layout]);

  const routes = useMemo<EdgeRoute[]>(() => {
    const r: EdgeRoute[] = [];
    for (const e of graphData.edges) {
      const a = nodeMap.get(e.source);
      const b = nodeMap.get(e.target);
      if (a && b) r.push(routeEdge(a, b, e));
    }
    return r;
  }, [graphData.edges, nodeMap]);

  // Track which tables connect to which
  const adjacency = useMemo(() => {
    const m = new Map<string, Set<string>>();
    for (const n of layout.nodes) m.set(n.id, new Set());
    for (const e of graphData.edges) {
      m.get(e.source)?.add(e.target);
      m.get(e.target)?.add(e.source);
    }
    return m;
  }, [graphData.edges, layout.nodes]);

  // Decide effective rendering density
  const effectiveDensity: "card" | "compact" = useMemo(() => {
    if (density !== "auto") return density;
    if (layout.nodes.length > COMPACT_THRESHOLD || k < 0.45) return "compact";
    return "card";
  }, [density, layout.nodes.length, k]);

  // Resize observer
  useEffect(() => {
    const el = viewportRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        setViewportSize({ w: e.contentRect.width, h: e.contentRect.height });
      }
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Fit to viewport
  const fit = useCallback(() => {
    const { w, h } = layout.bounds;
    if (!w || !h) return;
    const padding = 40;
    const kx = (viewportSize.w - padding * 2) / w;
    const ky = (viewportSize.h - padding * 2) / h;
    const newK = Math.max(0.18, Math.min(1.4, Math.min(kx, ky)));
    setK(newK);
    setTx((viewportSize.w - w * newK) / 2);
    setTy((viewportSize.h - h * newK) / 2);
  }, [layout.bounds, viewportSize]);

  // Initial fit when layout / size becomes known
  useEffect(() => {
    if (viewportSize.w > 100 && layout.bounds.w > 0) fit();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [layout.bounds.w, layout.bounds.h, viewportSize.w]);

  // Pan handling
  const dragRef = useRef<{ x: number; y: number; tx0: number; ty0: number } | null>(null);
  const onMouseDown = (e: ReactMouseEvent) => {
    if ((e.target as HTMLElement).closest("[data-card]")) return;
    dragRef.current = { x: e.clientX, y: e.clientY, tx0: tx, ty0: ty };
    (e.currentTarget as HTMLElement).style.cursor = "grabbing";
  };
  useEffect(() => {
    const onMove = (e: MouseEvent) => {
      const d = dragRef.current;
      if (!d) return;
      setTx(d.tx0 + (e.clientX - d.x));
      setTy(d.ty0 + (e.clientY - d.y));
    };
    const onUp = () => {
      dragRef.current = null;
      const v = viewportRef.current;
      if (v) v.style.cursor = "grab";
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
    return () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
  }, []);

  // Wheel zoom centered on cursor
  const onWheel = (e: ReactWheelEvent) => {
    e.preventDefault();
    const v = viewportRef.current;
    if (!v) return;
    const rect = v.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const factor = e.deltaY < 0 ? 1.12 : 1 / 1.12;
    const newK = Math.max(0.15, Math.min(2.5, k * factor));
    // keep point under cursor stable
    const wx = (mx - tx) / k;
    const wy = (my - ty) / k;
    setK(newK);
    setTx(mx - wx * newK);
    setTy(my - wy * newK);
  };

  const zoomBy = (factor: number) => {
    const newK = Math.max(0.15, Math.min(2.5, k * factor));
    const cx = viewportSize.w / 2;
    const cy = viewportSize.h / 2;
    const wx = (cx - tx) / k;
    const wy = (cy - ty) / k;
    setK(newK);
    setTx(cx - wx * newK);
    setTy(cy - wy * newK);
  };

  // Pan/zoom to a specific node (used by search)
  const focusNode = useCallback(
    (id: string) => {
      const n = nodeMap.get(id);
      if (!n) return;
      const targetK = Math.max(k, 0.9);
      const cx = n.x + n.w / 2;
      const cy = n.y + n.h / 2;
      setK(targetK);
      setTx(viewportSize.w / 2 - cx * targetK);
      setTy(viewportSize.h / 2 - cy * targetK);
      setSelected(id);
    },
    [nodeMap, viewportSize, k],
  );

  // Drill-down: load full table detail when expanded
  useEffect(() => {
    if (!expanded) {
      setExpandedDetail(null);
      setExpandedError(null);
      return;
    }
    if (expandedDetail?.name === expanded) return;
    setExpandedLoading(true);
    setExpandedError(null);
    api
      .table(expanded)
      .then((d) => {
        setExpandedDetail(d);
      })
      .catch((err) => {
        setExpandedError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => setExpandedLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [expanded]);

  const toggleExpand = (id: string) => {
    setSelected(id);
    setExpanded((prev) => (prev === id ? null : id));
  };

  // Highlight: when something is selected, dim non-related
  const isDimmed = (id: string): boolean => {
    if (!selected) return false;
    if (id === selected) return false;
    return !(adjacency.get(selected)?.has(id));
  };
  const edgeIsHighlighted = (e: GraphEdge): boolean => {
    if (!selected) return false;
    return e.source === selected || e.target === selected;
  };

  // Filter: search matches show as full opacity, others fade
  const matchesSearch = (id: string): boolean => {
    if (!search.trim()) return true;
    return id.toLowerCase().includes(search.trim().toLowerCase());
  };

  // ---------- Render ----------
  const canvasStyle: CSSProperties = {
    position: "absolute",
    transformOrigin: "0 0",
    transform: `translate(${tx}px, ${ty}px) scale(${k})`,
    width: layout.bounds.w,
    height: layout.bounds.h,
    willChange: "transform",
  };

  return (
    <div
      className="relative w-full overflow-hidden bg-background border border-border rounded-lg"
      style={{ height }}
    >
      {/* Top toolbar */}
      <div className="absolute top-2 left-2 right-2 z-20 flex items-center gap-2 pointer-events-none">
        <div className="flex items-center gap-1 bg-card border border-border rounded-md p-1 shadow-sm pointer-events-auto">
          <button
            onClick={() => zoomBy(1.2)}
            className="w-7 h-7 grid place-items-center rounded hover:bg-background text-sm"
            title="Zoom in"
          >+</button>
          <button
            onClick={() => zoomBy(1 / 1.2)}
            className="w-7 h-7 grid place-items-center rounded hover:bg-background text-sm"
            title="Zoom out"
          >−</button>
          <button
            onClick={fit}
            className="px-2 h-7 rounded hover:bg-background text-xs"
            title="Fit to view"
          >Fit</button>
          <span className="text-xs text-muted px-1 font-mono w-12 text-center">
            {(k * 100).toFixed(0)}%
          </span>
        </div>

        <div className="flex items-center bg-card border border-border rounded-md shadow-sm pointer-events-auto">
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                const m = layout.nodes.find((n) =>
                  n.id.toLowerCase().includes(search.trim().toLowerCase()),
                );
                if (m) focusNode(m.id);
              }
            }}
            placeholder="Search tables..."
            className="px-2 py-1.5 text-xs bg-transparent outline-none w-44"
          />
          {search && (
            <button
              onClick={() => setSearch("")}
              className="px-2 text-xs text-muted hover:text-foreground"
            >×</button>
          )}
        </div>

        <div className="flex items-center bg-card border border-border rounded-md p-0.5 shadow-sm pointer-events-auto text-xs">
          {(["auto", "card", "compact"] as const).map((d) => (
            <button
              key={d}
              onClick={() => setDensity(d)}
              className={`px-2 py-1 rounded ${
                density === d ? "bg-accent/15 text-accent" : "hover:bg-background text-muted"
              }`}
              title={
                d === "auto"
                  ? "Auto: cards normally, dots when zoomed out or many tables"
                  : d === "card"
                    ? "Always show table cards"
                    : "Compact dots only"
              }
            >
              {d}
            </button>
          ))}
        </div>

        <div className="ml-auto flex items-center gap-1 bg-card border border-border rounded-md px-2 py-1 shadow-sm pointer-events-auto text-xs text-muted">
          <span className="font-mono">{layout.nodes.length}</span> tables
          <span className="text-muted/40">·</span>
          <span className="font-mono">{routes.length}</span> FKs
          <span className="text-muted/40">·</span>
          <span className="font-mono">{layout.clusters.length}</span> domains
          <button
            onClick={() => setShowMinimap((v) => !v)}
            className="ml-2 px-1.5 rounded hover:bg-background"
            title="Toggle minimap"
          >
            {showMinimap ? "hide map" : "show map"}
          </button>
        </div>
      </div>

      {/* Domain legend */}
      <div className="absolute bottom-2 left-2 z-20 bg-card/95 border border-border rounded-md p-2 shadow-sm text-[10px] max-w-[60%]">
        <div className="font-semibold text-muted uppercase tracking-wider mb-1">Domains</div>
        <div className="flex flex-wrap gap-x-3 gap-y-1">
          {layout.clusters.map((c) => (
            <button
              key={c.domain}
              onClick={() => {
                // pan to this cluster
                const cx = c.x + c.w / 2;
                const cy = c.y + c.h / 2;
                setTx(viewportSize.w / 2 - cx * k);
                setTy(viewportSize.h / 2 - cy * k);
              }}
              className="flex items-center gap-1.5 hover:underline"
            >
              <span
                className="w-2.5 h-2.5 rounded-sm inline-block"
                style={{ backgroundColor: c.color }}
              />
              <span>{c.domain}</span>
              <span className="text-muted/70">({c.count})</span>
            </button>
          ))}
        </div>
      </div>

      {/* Viewport */}
      <div
        ref={viewportRef}
        onMouseDown={onMouseDown}
        onWheel={onWheel}
        className="absolute inset-0 overflow-hidden cursor-grab"
        style={{ touchAction: "none" }}
      >
        <div style={canvasStyle}>
          {/* Cluster backgrounds */}
          <svg
            width={layout.bounds.w}
            height={layout.bounds.h}
            style={{ position: "absolute", inset: 0, pointerEvents: "none" }}
          >
            {layout.clusters.map((c) => (
              <g key={c.domain}>
                <rect
                  x={c.x}
                  y={c.y + 18}
                  width={c.w}
                  height={c.h - 18}
                  rx={14}
                  fill={c.color}
                  fillOpacity={0.04}
                  stroke={c.color}
                  strokeOpacity={0.18}
                  strokeWidth={1}
                />
                <text
                  x={c.x + 14}
                  y={c.y + 14}
                  fontSize={11}
                  fontWeight={700}
                  fill={c.color}
                  fontFamily="ui-sans-serif, system-ui, sans-serif"
                  letterSpacing="0.04em"
                  style={{ textTransform: "uppercase" }}
                >
                  {c.domain}
                </text>
                <text
                  x={c.x + c.w - 14}
                  y={c.y + 14}
                  fontSize={10}
                  fill={c.color}
                  fillOpacity={0.7}
                  textAnchor="end"
                  fontFamily="ui-monospace, monospace"
                >
                  {c.count}
                </text>
              </g>
            ))}

            {/* Edges */}
            {routes.map((r, i) => {
              const weak = r.edge.ref_integrity < 0.95 || r.edge.nullable;
              const highlighted = edgeIsHighlighted(r.edge);
              const dimmed = !!selected && !highlighted;
              const stroke = weak ? "#d97706" : "#6366f1";
              return (
                <g
                  key={i}
                  opacity={dimmed ? 0.08 : (highlighted ? 1 : 0.45)}
                  style={{ pointerEvents: "stroke" }}
                  onMouseEnter={() => setHoveredEdge(i)}
                  onMouseLeave={() => setHoveredEdge(null)}
                >
                  <line
                    x1={r.x1} y1={r.y1} x2={r.x2} y2={r.y2}
                    stroke={stroke}
                    strokeWidth={highlighted ? 2.4 : 1.6}
                    strokeDasharray={weak ? "5 4" : undefined}
                  />
                  {(highlighted || hoveredEdge === i) && (
                    <>
                      <rect
                        x={r.midX - 60}
                        y={r.midY - 14}
                        width={120}
                        height={20}
                        rx={4}
                        fill="#0f172a"
                        opacity={0.92}
                      />
                      <text
                        x={r.midX} y={r.midY}
                        textAnchor="middle"
                        fontSize={10}
                        fill="#fff"
                        fontFamily="ui-monospace, monospace"
                      >
                        {r.edge.from_column} → {r.edge.to_column}
                      </text>
                    </>
                  )}
                </g>
              );
            })}
          </svg>

          {/* Cards */}
          {layout.nodes.map((n) => {
            const dimmed = isDimmed(n.id) || !matchesSearch(n.id);
            const isSel = selected === n.id;
            const isExp = expanded === n.id;
            const color = colorFor(n.domain || DEFAULT_DOMAIN, DOMAIN_COLORS);

            if (effectiveDensity === "compact" && !isExp) {
              const rowsLog = Math.max(1, Math.log10(Math.max(1, n.row_count)));
              const r = Math.max(6, Math.min(18, rowsLog * 4));
              return (
                <button
                  key={n.id}
                  data-card
                  onClick={() => toggleExpand(n.id)}
                  title={`${n.id} · ${n.row_count.toLocaleString()} rows`}
                  className="absolute group"
                  style={{
                    left: n.x + n.w / 2 - r,
                    top: n.y + n.h / 2 - r,
                    width: r * 2,
                    height: r * 2,
                    opacity: dimmed ? 0.18 : 1,
                    transition: "opacity 0.12s",
                  }}
                >
                  <span
                    className="block rounded-full border"
                    style={{
                      width: "100%",
                      height: "100%",
                      background: color,
                      borderColor: isSel ? "#0f172a" : "rgba(0,0,0,0.15)",
                      borderWidth: isSel ? 2 : 1,
                    }}
                  />
                  <span
                    className="absolute left-1/2 -translate-x-1/2 mt-0.5 text-[10px] text-foreground bg-card/80 px-1 rounded whitespace-nowrap pointer-events-none"
                    style={{ top: "100%" }}
                  >
                    {n.id}
                  </span>
                </button>
              );
            }

            return (
              <div
                key={n.id}
                data-card
                onClick={(e) => {
                  e.stopPropagation();
                  toggleExpand(n.id);
                }}
                className="absolute bg-card border rounded-lg shadow-sm cursor-pointer overflow-hidden"
                style={{
                  left: n.x,
                  top: n.y,
                  width: n.w,
                  height: isExp ? "auto" : n.h,
                  minHeight: n.h,
                  borderColor: isSel ? color : "var(--border)",
                  borderWidth: isSel ? 2 : 1,
                  opacity: dimmed ? 0.25 : 1,
                  zIndex: isExp ? 5 : isSel ? 3 : 2,
                  transition: "opacity 0.12s, border-color 0.12s",
                  boxShadow: isExp
                    ? "0 8px 24px rgba(15,23,42,0.18)"
                    : "0 1px 2px rgba(15,23,42,0.06)",
                }}
              >
                {/* Domain accent bar */}
                <div style={{ height: 4, background: color }} />
                <div className="px-3 pt-2 pb-2">
                  <div className="flex items-center justify-between gap-2">
                    <span className="font-mono text-[13px] font-semibold text-foreground truncate">
                      {n.id}
                    </span>
                    <span className="text-[10px] text-muted font-mono shrink-0">
                      {n.row_count >= 1000
                        ? `${(n.row_count / 1000).toFixed(n.row_count >= 10000 ? 0 : 1)}K`
                        : n.row_count}
                    </span>
                  </div>
                  {n.pk.length > 0 ? (
                    <div className="flex items-center gap-1 mt-1 text-[10px] font-mono text-amber-700 dark:text-amber-400 truncate">
                      <span className="text-[9px]">PK</span>
                      <span className="truncate">{n.pk.join(", ")}</span>
                    </div>
                  ) : (
                    <div className="mt-1 text-[10px] text-muted italic">no primary key</div>
                  )}
                  <div className="flex items-center justify-between mt-1.5">
                    <span
                      className="text-[9px] px-1.5 py-0.5 rounded"
                      style={{ background: `${color}1a`, color }}
                    >
                      {n.domain || DEFAULT_DOMAIN}
                    </span>
                    {n.fkCount > 0 && (
                      <span className="text-[9px] text-muted font-mono">
                        {n.fkCount} FK{n.fkCount > 1 ? "s" : ""}
                      </span>
                    )}
                  </div>
                </div>

                {/* Drill-down: columns */}
                {isExp && (
                  <div className="border-t border-border bg-background/50 px-3 py-2">
                    {expandedLoading && (
                      <div className="text-[11px] text-muted">Loading columns…</div>
                    )}
                    {expandedError && (
                      <div className="text-[11px] text-danger">
                        Failed to load: {expandedError}
                      </div>
                    )}
                    {expandedDetail && expandedDetail.name === n.id && (
                      <div className="space-y-0.5 max-h-72 overflow-y-auto">
                        {expandedDetail.columns.map((col) => {
                          const isPk = col.is_primary_key;
                          const fkRef = healthMap
                            .get(n.id)
                            ?.fk_columns.find((f) => f.column === col.name);
                          return (
                            <div
                              key={col.name}
                              className="flex items-center justify-between text-[11px] py-0.5 border-b border-border/40 last:border-0"
                            >
                              <span className="flex items-center gap-1.5 min-w-0">
                                <span
                                  className="text-[9px] font-mono w-6 shrink-0"
                                  style={{
                                    color: isPk
                                      ? "#b45309"
                                      : fkRef
                                        ? "#6366f1"
                                        : "transparent",
                                  }}
                                >
                                  {isPk ? "PK" : fkRef ? "FK" : ""}
                                </span>
                                <span className="font-mono truncate">{col.name}</span>
                              </span>
                              <span className="text-[10px] text-muted font-mono shrink-0 ml-2">
                                {col.dtype}
                                {!col.nullable && " ·NN"}
                              </span>
                            </div>
                          );
                        })}
                        {expandedDetail.columns.length === 0 && (
                          <div className="text-[11px] text-muted">No columns.</div>
                        )}
                      </div>
                    )}
                    <div className="mt-2 flex items-center justify-between">
                      <span className="text-[10px] text-muted">
                        {(adjacency.get(n.id)?.size ?? 0)} related table
                        {(adjacency.get(n.id)?.size ?? 0) === 1 ? "" : "s"}
                      </span>
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setExpanded(null);
                        }}
                        className="text-[10px] text-muted hover:text-foreground"
                      >
                        Collapse
                      </button>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* Minimap */}
      {showMinimap && (
        <Minimap
          bounds={layout.bounds}
          clusters={layout.clusters}
          nodes={layout.nodes}
          tx={tx}
          ty={ty}
          k={k}
          viewportSize={viewportSize}
          onPan={(nx, ny) => {
            setTx(nx);
            setTy(ny);
          }}
        />
      )}
    </div>
  );
}

// ---------- Minimap ----------

interface MinimapProps {
  bounds: { w: number; h: number };
  clusters: Cluster[];
  nodes: LaidOutNode[];
  tx: number;
  ty: number;
  k: number;
  viewportSize: { w: number; h: number };
  onPan: (tx: number, ty: number) => void;
}

function Minimap({ bounds, clusters, nodes, tx, ty, k, viewportSize, onPan }: MinimapProps) {
  const W = 180;
  const H = Math.max(80, Math.round(W * (bounds.h / Math.max(1, bounds.w))));
  const scale = W / Math.max(1, bounds.w);

  // Visible window in world coords
  const vx = -tx / k;
  const vy = -ty / k;
  const vw = viewportSize.w / k;
  const vh = viewportSize.h / k;

  const dragRef = useRef<{ x: number; y: number; tx0: number; ty0: number } | null>(null);

  const onDown = (e: ReactMouseEvent) => {
    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
    const mxWorld = (e.clientX - rect.left) / scale;
    const myWorld = (e.clientY - rect.top) / scale;
    // Center viewport on click point
    const newTx = viewportSize.w / 2 - mxWorld * k;
    const newTy = viewportSize.h / 2 - myWorld * k;
    onPan(newTx, newTy);
    dragRef.current = { x: e.clientX, y: e.clientY, tx0: newTx, ty0: newTy };
  };

  useEffect(() => {
    const onMove = (e: MouseEvent) => {
      const d = dragRef.current;
      if (!d) return;
      const dx = (e.clientX - d.x) / scale;
      const dy = (e.clientY - d.y) / scale;
      onPan(d.tx0 - dx * k, d.ty0 - dy * k);
    };
    const onUp = () => {
      dragRef.current = null;
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
    return () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
  }, [scale, k, onPan]);

  return (
    <div
      className="absolute bottom-2 right-2 z-20 bg-card/95 border border-border rounded-md shadow-sm overflow-hidden"
      style={{ width: W + 2, height: H + 2 }}
      onMouseDown={onDown}
    >
      <svg width={W} height={H} style={{ display: "block", cursor: "crosshair" }}>
        {clusters.map((c) => (
          <rect
            key={c.domain}
            x={c.x * scale}
            y={c.y * scale}
            width={c.w * scale}
            height={c.h * scale}
            fill={c.color}
            fillOpacity={0.12}
            stroke={c.color}
            strokeOpacity={0.4}
            strokeWidth={0.5}
          />
        ))}
        {nodes.map((n) => (
          <rect
            key={n.id}
            x={n.x * scale}
            y={n.y * scale}
            width={Math.max(1, n.w * scale)}
            height={Math.max(1, n.h * scale)}
            fill={colorFor(n.domain || DEFAULT_DOMAIN, DOMAIN_COLORS)}
            fillOpacity={0.7}
          />
        ))}
        {/* Viewport rect */}
        <rect
          x={vx * scale}
          y={vy * scale}
          width={vw * scale}
          height={vh * scale}
          fill="none"
          stroke="#0f172a"
          strokeWidth={1.2}
        />
      </svg>
    </div>
  );
}
