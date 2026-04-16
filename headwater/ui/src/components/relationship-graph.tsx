"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import dynamic from "next/dynamic";
import { type GraphData, type GraphEdge, type GraphNode } from "@/lib/api";

// react-force-graph-2d uses canvas/window -- must disable SSR
const ForceGraph2D = dynamic(() => import("react-force-graph-2d"), {
  ssr: false,
});

// Domain color palette -- deterministic assignment
const DOMAIN_COLORS = [
  "#6366f1", // indigo
  "#0ea5e9", // sky
  "#10b981", // emerald
  "#f59e0b", // amber
  "#ef4444", // red
  "#8b5cf6", // violet
  "#ec4899", // pink
  "#14b8a6", // teal
  "#f97316", // orange
  "#84cc16", // lime
];

function domainColor(domain: string, domains: string[]): string {
  const idx = domains.indexOf(domain);
  return DOMAIN_COLORS[idx >= 0 ? idx % DOMAIN_COLORS.length : 0];
}

interface GraphNodeExt extends GraphNode {
  x?: number;
  y?: number;
  vx?: number;
  vy?: number;
}

interface GraphLinkExt {
  source: string | GraphNodeExt;
  target: string | GraphNodeExt;
  edge: GraphEdge;
}

interface TooltipData {
  type: "node" | "edge";
  x: number;
  y: number;
  node?: GraphNode;
  edge?: GraphEdge;
}

export function RelationshipGraph({
  graphData,
}: {
  graphData: GraphData;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<{ zoomToFit: (ms?: number, px?: number) => void } | null>(null);
  const [tooltip, setTooltip] = useState<TooltipData | null>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 500 });

  // Collect unique domains
  const allDomains = Array.from(
    new Set(graphData.nodes.map((n) => n.domain).filter(Boolean))
  );

  // Prepare graph data for react-force-graph-2d
  const fgData = {
    nodes: graphData.nodes.map((n) => ({ ...n })),
    links: graphData.edges.map((e) => ({
      source: e.source,
      target: e.target,
      edge: e,
    })),
  };

  // Auto-fit on mount
  useEffect(() => {
    const timer = setTimeout(() => {
      if (graphRef.current) {
        graphRef.current.zoomToFit(400, 60);
      }
    }, 500);
    return () => clearTimeout(timer);
  }, [graphData]);

  // Resize observer
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setDimensions({
          width: entry.contentRect.width,
          height: Math.max(entry.contentRect.height, 400),
        });
      }
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Node size based on row count
  const nodeSize = useCallback(
    (node: GraphNodeExt) => {
      const rc = node.row_count || 1;
      return Math.max(4, Math.log10(rc) * 4);
    },
    []
  );

  // Draw nodes
  const drawNode = useCallback(
    (node: GraphNodeExt, ctx: CanvasRenderingContext2D) => {
      const size = nodeSize(node);
      const color = domainColor(node.domain || "", allDomains);
      const x = node.x ?? 0;
      const y = node.y ?? 0;

      // Circle
      ctx.beginPath();
      ctx.arc(x, y, size, 0, 2 * Math.PI);
      ctx.fillStyle = color;
      ctx.fill();
      ctx.strokeStyle = "rgba(0,0,0,0.15)";
      ctx.lineWidth = 0.5;
      ctx.stroke();

      // Label
      ctx.font = `${Math.max(3, size * 0.7)}px sans-serif`;
      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      ctx.fillStyle = "#333";
      ctx.fillText(node.id, x, y + size + 1.5);
    },
    [allDomains, nodeSize]
  );

  // Draw links
  const drawLink = useCallback(
    (link: GraphLinkExt, ctx: CanvasRenderingContext2D) => {
      const src = link.source as GraphNodeExt;
      const tgt = link.target as GraphNodeExt;
      if (!src.x || !tgt.x) return;

      const e = link.edge;
      const strong = e.ref_integrity >= 0.8 && !e.nullable;

      ctx.beginPath();
      ctx.moveTo(src.x, src.y ?? 0);

      if (strong) {
        ctx.lineTo(tgt.x, tgt.y ?? 0);
      } else {
        // Dashed line for weak/nullable
        ctx.setLineDash([4, 3]);
        ctx.lineTo(tgt.x, tgt.y ?? 0);
        ctx.setLineDash([]);
      }

      ctx.strokeStyle = strong
        ? "rgba(100, 116, 139, 0.5)"
        : "rgba(245, 158, 11, 0.5)";
      ctx.lineWidth = Math.max(0.5, e.confidence * 2);
      ctx.stroke();
    },
    []
  );

  return (
    <div ref={containerRef} className="relative w-full" style={{ minHeight: 400 }}>
      {/* Legend */}
      <div className="absolute top-2 left-2 z-10 bg-white/90 border border-border rounded-lg p-2 text-[10px] space-y-1">
        <div className="font-semibold text-muted uppercase tracking-wider mb-1">
          Legend
        </div>
        {allDomains.map((d) => (
          <div key={d} className="flex items-center gap-1.5">
            <span
              className="w-2.5 h-2.5 rounded-full inline-block"
              style={{ backgroundColor: domainColor(d, allDomains) }}
            />
            <span>{d}</span>
          </div>
        ))}
        <div className="border-t border-border pt-1 mt-1 space-y-0.5">
          <div className="flex items-center gap-1.5">
            <span className="w-4 h-0 border-t border-slate-400 inline-block" />
            <span>Strong FK</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="w-4 h-0 border-t border-dashed border-amber-500 inline-block" />
            <span>Weak/nullable FK</span>
          </div>
        </div>
      </div>

      {/* Tooltip */}
      {tooltip && (
        <div
          className="absolute z-20 bg-white border border-border rounded-lg shadow-lg p-3 text-xs pointer-events-none"
          style={{
            left: Math.min(tooltip.x + 10, dimensions.width - 200),
            top: Math.min(tooltip.y + 10, dimensions.height - 100),
          }}
        >
          {tooltip.type === "node" && tooltip.node && (
            <div>
              <div className="font-semibold font-mono mb-1">
                {tooltip.node.id}
              </div>
              <div className="text-muted">
                {tooltip.node.row_count.toLocaleString()} rows
              </div>
              {tooltip.node.domain && (
                <div className="text-muted">Domain: {tooltip.node.domain}</div>
              )}
              {tooltip.node.description && (
                <div className="text-muted mt-1 max-w-48">
                  {tooltip.node.description}
                </div>
              )}
            </div>
          )}
          {tooltip.type === "edge" && tooltip.edge && (
            <div>
              <div className="font-mono mb-1">
                {tooltip.edge.source}.{tooltip.edge.from_column} &rarr;{" "}
                {tooltip.edge.target}.{tooltip.edge.to_column}
              </div>
              <div className="text-muted">
                Integrity: {(tooltip.edge.ref_integrity * 100).toFixed(0)}%
              </div>
              <div className="text-muted">Type: {tooltip.edge.rel_type}</div>
              {tooltip.edge.nullable && (
                <div className="text-amber-600">Nullable FK</div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Force graph */}
      <ForceGraph2D
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ref={graphRef as any}
        width={dimensions.width}
        height={dimensions.height}
        graphData={fgData}
        nodeCanvasObject={drawNode as (node: object, ctx: CanvasRenderingContext2D, globalScale: number) => void}
        nodePointerAreaPaint={(n: object, color: string, ctx: CanvasRenderingContext2D) => {
          const node = n as GraphNodeExt;
          const size = nodeSize(node);
          ctx.beginPath();
          ctx.arc(node.x ?? 0, node.y ?? 0, size + 2, 0, 2 * Math.PI);
          ctx.fillStyle = color;
          ctx.fill();
        }}
        linkCanvasObject={drawLink as (link: object, ctx: CanvasRenderingContext2D, globalScale: number) => void}
        onNodeHover={(n: unknown, _prev: unknown) => {
          const node = n as GraphNodeExt | null;
          if (node) {
            setTooltip({
              type: "node",
              x: node.x ?? 0,
              y: node.y ?? 0,
              node: node,
            });
          } else {
            setTooltip(null);
          }
          // Change cursor
          const el = containerRef.current;
          if (el) el.style.cursor = node ? "pointer" : "default";
        }}
        onLinkHover={(l: unknown) => {
          const link = l as GraphLinkExt | null;
          if (link) {
            const src = link.source as GraphNodeExt;
            const tgt = link.target as GraphNodeExt;
            setTooltip({
              type: "edge",
              x: ((src.x ?? 0) + (tgt.x ?? 0)) / 2,
              y: ((src.y ?? 0) + (tgt.y ?? 0)) / 2,
              edge: link.edge,
            });
          } else {
            setTooltip(null);
          }
        }}
        cooldownTicks={100}
        d3AlphaDecay={0.02}
        d3VelocityDecay={0.3}
        backgroundColor="transparent"
      />
    </div>
  );
}
