"use client";

import { useMemo } from "react";
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import type { VisualizationSpec } from "@/lib/api";

// Accessible color palette for multi-series
const COLORS = [
  "#2563eb", // blue-600
  "#dc2626", // red-600
  "#16a34a", // green-600
  "#9333ea", // purple-600
  "#ea580c", // orange-600
  "#0891b2", // cyan-600
  "#c026d3", // fuchsia-600
  "#65a30d", // lime-600
];

interface ResultChartProps {
  spec: VisualizationSpec;
  data: Record<string, unknown>[];
}

export function ResultChart({ spec, data }: ResultChartProps) {
  if (!data.length || spec.chart_type === "table" || spec.chart_type === "kpi") {
    return null;
  }

  switch (spec.chart_type) {
    case "line":
      return <LineChartView spec={spec} data={data} />;
    case "bar":
      return <BarChartView spec={spec} data={data} />;
    case "scatter":
      return <ScatterChartView spec={spec} data={data} />;
    case "heatmap":
      return <HeatmapView spec={spec} data={data} />;
    default:
      return null;
  }
}

// ---------------------------------------------------------------------------
// Line chart
// ---------------------------------------------------------------------------

function LineChartView({ spec, data }: ResultChartProps) {
  const { seriesData, seriesKeys } = useMemo(
    () => buildGroupedSeries(data, spec),
    [data, spec]
  );

  return (
    <div className="p-4">
      <h3 className="text-sm font-medium mb-3">{spec.title}</h3>
      <ResponsiveContainer width="100%" height={360}>
        <LineChart data={seriesData}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border, #e5e7eb)" />
          <XAxis
            dataKey={spec.x_axis ?? "x"}
            tick={{ fontSize: 11 }}
            tickFormatter={formatTick}
          />
          <YAxis tick={{ fontSize: 11 }} />
          <Tooltip
            contentStyle={{ fontSize: 12 }}
            labelFormatter={formatTick}
          />
          {seriesKeys.length > 1 && <Legend wrapperStyle={{ fontSize: 12 }} />}
          {seriesKeys.map((key, i) => (
            <Line
              key={key}
              type="monotone"
              dataKey={key}
              stroke={COLORS[i % COLORS.length]}
              strokeWidth={2}
              dot={seriesData.length <= 60}
              name={key}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Bar chart
// ---------------------------------------------------------------------------

function BarChartView({ spec, data }: ResultChartProps) {
  const { seriesData, seriesKeys } = useMemo(
    () => buildGroupedSeries(data, spec),
    [data, spec]
  );

  return (
    <div className="p-4">
      <h3 className="text-sm font-medium mb-3">{spec.title}</h3>
      <ResponsiveContainer width="100%" height={360}>
        <BarChart data={seriesData}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border, #e5e7eb)" />
          <XAxis
            dataKey={spec.x_axis ?? "x"}
            tick={{ fontSize: 11 }}
            tickFormatter={formatTick}
          />
          <YAxis tick={{ fontSize: 11 }} />
          <Tooltip contentStyle={{ fontSize: 12 }} />
          {seriesKeys.length > 1 && <Legend wrapperStyle={{ fontSize: 12 }} />}
          {seriesKeys.map((key, i) => (
            <Bar
              key={key}
              dataKey={key}
              fill={COLORS[i % COLORS.length]}
              name={key}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Scatter chart
// ---------------------------------------------------------------------------

function ScatterChartView({ spec, data }: ResultChartProps) {
  const xKey = spec.x_axis ?? Object.keys(data[0])[0];
  const yKey = spec.y_axis ?? Object.keys(data[0])[1];

  return (
    <div className="p-4">
      <h3 className="text-sm font-medium mb-3">{spec.title}</h3>
      <ResponsiveContainer width="100%" height={360}>
        <ScatterChart>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border, #e5e7eb)" />
          <XAxis
            type="number"
            dataKey={xKey}
            name={xKey}
            tick={{ fontSize: 11 }}
          />
          <YAxis
            type="number"
            dataKey={yKey}
            name={yKey}
            tick={{ fontSize: 11 }}
          />
          <Tooltip contentStyle={{ fontSize: 12 }} cursor={{ strokeDasharray: "3 3" }} />
          <Scatter
            data={data as Record<string, number>[]}
            fill={COLORS[0]}
          />
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Heatmap (simple grid rendered as a table with color intensity)
// ---------------------------------------------------------------------------

function HeatmapView({ spec, data }: ResultChartProps) {
  const { rows, cols, matrix, min, max } = useMemo(() => {
    const xKey = spec.x_axis ?? Object.keys(data[0])[0];
    const yKey = spec.y_axis ?? Object.keys(data[0])[1];
    // Find the first numeric column that isn't x or y
    const metricKey = Object.keys(data[0]).find(
      (k) => k !== xKey && k !== yKey && typeof data[0][k] === "number"
    ) ?? Object.keys(data[0])[2];

    const rowSet = new Set<string>();
    const colSet = new Set<string>();
    const map = new Map<string, number>();

    for (const row of data) {
      const r = String(row[yKey]);
      const c = String(row[xKey]);
      rowSet.add(r);
      colSet.add(c);
      map.set(`${r}|${c}`, Number(row[metricKey]) || 0);
    }

    const rowArr = Array.from(rowSet);
    const colArr = Array.from(colSet);
    const vals = Array.from(map.values());
    return {
      rows: rowArr,
      cols: colArr,
      matrix: map,
      min: Math.min(...vals),
      max: Math.max(...vals),
    };
  }, [data, spec]);

  const intensity = (val: number) => {
    if (max === min) return 0.5;
    return (val - min) / (max - min);
  };

  return (
    <div className="p-4">
      <h3 className="text-sm font-medium mb-3">{spec.title}</h3>
      <div className="overflow-auto">
        <table className="text-xs border-collapse">
          <thead>
            <tr>
              <th className="p-2 text-left text-muted font-medium" />
              {cols.map((c) => (
                <th key={c} className="p-2 text-center text-muted font-medium">
                  {formatTick(c)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r}>
                <td className="p-2 font-medium text-muted whitespace-nowrap">{r}</td>
                {cols.map((c) => {
                  const val = matrix.get(`${r}|${c}`) ?? 0;
                  const t = intensity(val);
                  // Blue intensity scale
                  const bg = `rgba(37, 99, 235, ${0.1 + t * 0.8})`;
                  const fg = t > 0.5 ? "#fff" : "#1e3a5f";
                  return (
                    <td
                      key={c}
                      className="p-2 text-center font-mono"
                      style={{ backgroundColor: bg, color: fg }}
                    >
                      {typeof val === "number" ? (Number.isInteger(val) ? val : val.toFixed(1)) : val}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Pivot grouped data into a shape recharts can render as multi-series. */
function buildGroupedSeries(
  data: Record<string, unknown>[],
  spec: VisualizationSpec
): { seriesData: Record<string, unknown>[]; seriesKeys: string[] } {
  const xKey = spec.x_axis;
  const yKey = spec.y_axis;
  const groupKey = spec.group_by;

  if (!xKey || !yKey) {
    return { seriesData: data, seriesKeys: [yKey ?? "value"] };
  }

  // No grouping -- single series
  if (!groupKey) {
    return { seriesData: data as Record<string, unknown>[], seriesKeys: [yKey] };
  }

  // Pivot: each group value becomes its own series column
  const groups = new Set<string>();
  const byX = new Map<string, Record<string, unknown>>();

  for (const row of data) {
    const x = String(row[xKey]);
    const g = String(row[groupKey]);
    const v = row[yKey];
    groups.add(g);

    if (!byX.has(x)) {
      byX.set(x, { [xKey]: row[xKey] });
    }
    byX.get(x)![g] = v;
  }

  const seriesKeys = Array.from(groups).sort();
  const seriesData = Array.from(byX.values());

  return { seriesData, seriesKeys };
}

/** Shorten long tick labels (e.g. ISO timestamps). */
function formatTick(value: unknown): string {
  const s = String(value);
  // Shorten ISO dates: "2024-01-15T00:00:00" -> "2024-01-15"
  if (s.length > 10 && s.includes("T")) {
    return s.slice(0, 10);
  }
  // Truncate very long labels
  if (s.length > 16) {
    return s.slice(0, 14) + "..";
  }
  return s;
}
