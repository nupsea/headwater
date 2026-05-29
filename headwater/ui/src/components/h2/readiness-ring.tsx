"use client";

import { useEffect, useState } from "react";

export const HW2_COLOR = {
  paper:    "#faf9f6",
  surface:  "#ffffff",
  ink:      "#15161a",
  ink2:     "#3a3c43",
  muted:    "#74757b",
  faint:    "#9c9da3",
  rule:     "#ebe9e3",
  rule2:    "#dfdcd2",
  chip:     "#f1efe8",
  blue:     "#2b5fd9",
  blueSoft: "#e7eefc",
  good:     "#0e7a55",
  goodSoft: "#e1efe7",
  warn:     "#a26108",
  warnSoft: "#f5ecd9",
  bad:      "#b4351c",
  badSoft:  "#f7e3dc",
} as const;

export function ringTone(value: number, certified: boolean, demoted: boolean) {
  if (certified) return { color: HW2_COLOR.good,  label: "Certified" };
  if (demoted)   return { color: HW2_COLOR.bad,   label: "Re-verify" };
  if (value >= 80) return { color: HW2_COLOR.good, label: "Evidence high" };
  if (value >= 50) return { color: HW2_COLOR.blue, label: "Evidence forming" };
  if (value >= 20) return { color: HW2_COLOR.warn, label: "Evidence low" };
  if (value > 0)   return { color: HW2_COLOR.warn, label: "Starting" };
  return { color: HW2_COLOR.faint, label: "Not started" };
}

interface ReadinessRingProps {
  value: number;
  certified?: boolean;
  demoted?: boolean;
  size?: number;
  stroke?: number;
  showLabel?: boolean;
  animate?: boolean;
}

export function ReadinessRing({
  value,
  certified = false,
  demoted = false,
  size = 56,
  stroke = 4,
  showLabel = true,
  animate = true,
}: ReadinessRingProps) {
  const r = (size - stroke) / 2;
  const circumference = Math.PI * 2 * r;
  const v = Math.max(0, Math.min(100, value || 0));
  const tone = ringTone(v, certified, demoted);

  const [drawn, setDrawn] = useState(animate ? 0 : v);

  useEffect(() => {
    if (!animate) {
      setDrawn(v);
      return;
    }
    let rafId: number;
    let t0: number | null = null;
    const from = drawn;
    const dur = 600;
    const step = (t: number) => {
      if (!t0) t0 = t;
      const p = Math.min(1, (t - t0) / dur);
      const eased = 1 - Math.pow(1 - p, 3);
      setDrawn(from + (v - from) * eased);
      if (p < 1) rafId = requestAnimationFrame(step);
    };
    rafId = requestAnimationFrame(step);
    return () => cancelAnimationFrame(rafId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [v]);

  const fillNow = circumference * (drawn / 100);

  return (
    <div style={{ display: "inline-flex", alignItems: "center", gap: 10 }}>
      <svg
        width={size}
        height={size}
        style={{ display: "block", flexShrink: 0 }}
        aria-label={`Readiness ${Math.round(v)}%${certified ? " — certified" : demoted ? " — needs re-verification" : ""}`}
      >
        <circle
          cx={size / 2}
          cy={size / 2}
          r={r}
          stroke={HW2_COLOR.rule2}
          strokeWidth={stroke}
          fill="none"
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={r}
          stroke={tone.color}
          strokeWidth={stroke}
          fill="none"
          strokeLinecap="round"
          strokeDasharray={`${fillNow} ${circumference}`}
          transform={`rotate(-90 ${size / 2} ${size / 2})`}
        />
        {certified && (
          <text
            x="50%"
            y="52%"
            textAnchor="middle"
            dominantBaseline="central"
            style={{
              font: `700 ${size * 0.32}px 'DM Sans', sans-serif`,
              fill: tone.color,
            }}
          >
            ✓
          </text>
        )}
        {!certified && demoted && (
          <text
            x="50%"
            y="52%"
            textAnchor="middle"
            dominantBaseline="central"
            style={{
              font: `700 ${size * 0.28}px 'DM Sans', sans-serif`,
              fill: tone.color,
            }}
          >
            !
          </text>
        )}
        {!certified && !demoted && (
          <text
            x="50%"
            y="52%"
            textAnchor="middle"
            dominantBaseline="central"
            style={{
              font: `600 ${size * 0.24}px 'DM Sans', sans-serif`,
              fill: HW2_COLOR.ink,
              letterSpacing: "-0.02em",
            }}
          >
            {Math.round(drawn)}
          </text>
        )}
      </svg>

      {showLabel && (
        <div style={{ display: "flex", flexDirection: "column", lineHeight: 1.15 }}>
          <span
            style={{
              font: "600 11px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              letterSpacing: "0.04em",
              textTransform: "uppercase",
            }}
          >
            Readiness
          </span>
          <span
            style={{
              font: "600 14px 'DM Sans', sans-serif",
              color: tone.color,
              letterSpacing: "-0.01em",
            }}
          >
            {tone.label}
          </span>
        </div>
      )}
    </div>
  );
}
