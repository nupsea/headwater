"use client";

import { HW2_COLOR } from "./readiness-ring";

export const HW2_STAGES = [
  { key: "frame",      label: "Frame",      path: (_id: string) => `/h2/projects/new` },
  { key: "understand", label: "Understand", path: (id: string) => `/h2/projects/${id}/understand` },
  { key: "resolve",    label: "Resolve",    path: (id: string) => `/h2/projects/${id}/resolve` },
  { key: "readiness",  label: "Readiness",  path: (id: string) => `/h2/projects/${id}/readiness` },
  { key: "answer",     label: "Answer",     path: (id: string) => `/h2/projects/${id}/answer` },
] as const;

export type StageKey = typeof HW2_STAGES[number]["key"];

interface StepperBadge {
  tone: "bad" | "blue";
  label: string;
}

interface StepperProps {
  current: StageKey | string;
  projectId: string;
  onJump?: (key: StageKey) => void;
  badges?: Partial<Record<StageKey, StepperBadge>>;
}

export function Stepper({ current, projectId, onJump, badges }: StepperProps) {
  const idx = HW2_STAGES.findIndex((s) => s.key === current);

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 0,
        flexWrap: "wrap",
      }}
    >
      {HW2_STAGES.map((s, i) => {
        const active = i === idx;
        const complete = i < idx;
        const badge = badges?.[s.key as StageKey];

        return (
          <span
            key={s.key}
            style={{ display: "inline-flex", alignItems: "center" }}
          >
            <button
              onClick={() => onJump?.(s.key as StageKey)}
              style={{
                appearance: "none",
                background: "none",
                border: "none",
                cursor: "pointer",
                padding: "8px 12px",
                borderRadius: 8,
                display: "inline-flex",
                alignItems: "center",
                gap: 8,
                font: `${active ? 600 : 500} 13px 'DM Sans', sans-serif`,
                color: active
                  ? HW2_COLOR.ink
                  : complete
                  ? HW2_COLOR.ink2
                  : HW2_COLOR.faint,
              }}
            >
              <span
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  justifyContent: "center",
                  width: 20,
                  height: 20,
                  borderRadius: "50%",
                  background: complete
                    ? HW2_COLOR.good
                    : active
                    ? HW2_COLOR.ink
                    : "transparent",
                  border: complete
                    ? "none"
                    : active
                    ? "none"
                    : `1.5px solid ${HW2_COLOR.rule2}`,
                  color: complete || active ? "#fff" : HW2_COLOR.faint,
                  font: "600 10px 'DM Mono', monospace",
                }}
              >
                {complete ? "✓" : i + 1}
              </span>
              <span>{s.label}</span>
              {badge && (
                <span
                  style={{
                    font: "600 10px 'DM Mono', monospace",
                    background:
                      badge.tone === "bad"
                        ? HW2_COLOR.badSoft
                        : HW2_COLOR.blueSoft,
                    color:
                      badge.tone === "bad" ? HW2_COLOR.bad : HW2_COLOR.blue,
                    padding: "2px 6px",
                    borderRadius: 4,
                  }}
                >
                  {badge.label}
                </span>
              )}
            </button>
            {i < HW2_STAGES.length - 1 && (
              <span
                style={{
                  color: HW2_COLOR.rule2,
                  font: "300 14px 'DM Sans', sans-serif",
                  padding: "0 2px",
                }}
              >
                ›
              </span>
            )}
          </span>
        );
      })}
    </div>
  );
}
