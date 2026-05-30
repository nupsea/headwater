"use client";

import { useCallback, useEffect, useState } from "react";
import {
  h2,
  notifyInputChanged,
  type H2Resource,
  type H2ResourceIngest,
} from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

function basename(p: string): string {
  return p.split(/[\\/]/).pop() || p;
}

function whatItTouched(r: {
  claims_created?: number;
  claims_updated?: number;
  conflicts_detected?: number;
}): string {
  const parts: string[] = [];
  if (r.claims_created) parts.push(`${r.claims_created} meaning${r.claims_created === 1 ? "" : "s"} added`);
  if (r.claims_updated) parts.push(`${r.claims_updated} updated`);
  if (r.conflicts_detected)
    parts.push(`${r.conflicts_detected} conflict${r.conflicts_detected === 1 ? "" : "s"}`);
  return parts.length ? parts.join(" · ") : "no new meanings extracted";
}

function ingestSummary(r: H2ResourceIngest): string {
  const base = whatItTouched(r);
  const extra: string[] = [];
  if (r.claims_skipped_locked) extra.push(`${r.claims_skipped_locked} locked-skipped`);
  if (r.sensitivity && r.sensitivity !== "none") extra.push(`sensitivity: ${r.sensitivity}`);
  return [base, ...extra].join(" · ");
}

/**
 * Reusable Inputs surface: shows every resource the project has considered and
 * lets the user add more (paste / .md / .txt) at any time. Every add fires the
 * input-changed signal so the recompute banner offers a complete refresh.
 */
export function InputsPanel({
  projectId,
  onChanged,
}: {
  projectId: string;
  onChanged?: () => void;
}) {
  const [resources, setResources] = useState<H2Resource[]>([]);
  const [text, setText] = useState("");
  const [files, setFiles] = useState<File[]>([]);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState(false);

  const load = useCallback(() => {
    h2.projects.resources
      .list(projectId)
      .then(setResources)
      .catch(() => setResources([]));
  }, [projectId]);

  useEffect(() => {
    load();
  }, [load]);

  const add = async () => {
    if (!text.trim() && files.length === 0) return;
    setBusy(true);
    setError(null);
    setResult(null);
    try {
      const summaries: string[] = [];
      if (text.trim()) {
        const r = await h2.projects.resources.ingest(
          projectId,
          new File([text], "pasted-context.md", { type: "text/markdown" })
        );
        summaries.push(`Pasted note — ${ingestSummary(r)}`);
      }
      for (const f of files) {
        const r = await h2.projects.resources.ingest(projectId, f);
        summaries.push(`${f.name} — ${ingestSummary(r)}`);
      }
      setText("");
      setFiles([]);
      setResult(summaries.join("   •   "));
      load();
      // Any added context is an input change -> the whole loop should refresh.
      notifyInputChanged();
      onChanged?.();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to add input.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 12,
        padding: "18px 20px",
        fontFamily: "'DM Sans', sans-serif",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
          marginBottom: 4,
        }}
      >
        <div
          style={{
            font: "600 11px 'DM Sans', sans-serif",
            color: HW2_COLOR.muted,
            textTransform: "uppercase",
            letterSpacing: "0.06em",
          }}
        >
          Inputs considered
        </div>
        <span style={{ font: "400 11px 'DM Mono', monospace", color: HW2_COLOR.faint }}>
          {resources.length} fed
        </span>
      </div>
      <p
        style={{
          font: "400 12.5px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          lineHeight: 1.5,
          margin: "0 0 14px",
        }}
      >
        Everything Headwater has been given for this project — data dictionaries, column
        meanings, domain notes. Add more any time; every input refreshes the workflow.
      </p>

      {/* Resource list */}
      {resources.length === 0 ? (
        <div
          style={{
            padding: "14px 16px",
            background: HW2_COLOR.paper,
            border: `1px dashed ${HW2_COLOR.rule2}`,
            borderRadius: 8,
            font: "400 13px 'DM Sans', sans-serif",
            color: HW2_COLOR.faint,
          }}
        >
          No inputs yet — Headwater is working from the naked data. Paste a data dictionary
          or column meanings below to ground it.
        </div>
      ) : (
        <div style={{ display: "grid", gap: 8 }}>
          {resources.map((r) => (
            <div
              key={r.path}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 12,
                padding: "10px 14px",
                background: HW2_COLOR.paper,
                border: `1px solid ${HW2_COLOR.rule}`,
                borderRadius: 8,
              }}
            >
              <span
                style={{
                  font: "600 10px 'DM Mono', monospace",
                  color: HW2_COLOR.blue,
                  background: HW2_COLOR.blueSoft,
                  padding: "2px 7px",
                  borderRadius: 4,
                  textTransform: "uppercase",
                  flexShrink: 0,
                }}
              >
                {r.format}
              </span>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div
                  style={{
                    font: "500 13px 'DM Sans', sans-serif",
                    color: HW2_COLOR.ink,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                >
                  {basename(r.path)}
                </div>
                <div
                  style={{
                    font: "400 11.5px 'DM Sans', sans-serif",
                    color: HW2_COLOR.muted,
                    marginTop: 1,
                  }}
                >
                  {whatItTouched(r)}
                  {r.sensitivity && r.sensitivity !== "none" && (
                    <span style={{ color: HW2_COLOR.warn }}> · sensitivity: {r.sensitivity}</span>
                  )}
                </div>
              </div>
              <span
                style={{
                  font: "400 11px 'DM Mono', monospace",
                  color: HW2_COLOR.faint,
                  flexShrink: 0,
                }}
              >
                {r.ingested_at?.replace("T", " ").slice(0, 16)}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* Add input */}
      <button
        onClick={() => setOpen((v) => !v)}
        style={{
          appearance: "none",
          background: "transparent",
          border: "none",
          cursor: "pointer",
          padding: 0,
          marginTop: 14,
          color: HW2_COLOR.blue,
          font: "600 13px 'DM Sans', sans-serif",
          fontFamily: "'DM Sans', sans-serif",
          display: "inline-flex",
          alignItems: "center",
          gap: 6,
        }}
      >
        <span style={{ font: "500 12px 'DM Mono', monospace" }}>{open ? "▾" : "▸"}</span>
        + Add input
      </button>

      {open && (
        <div style={{ marginTop: 12 }}>
          <textarea
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder={
              "Paste a data dictionary or column meanings, e.g.\n| column | meaning |\n| --- | --- |\n| total_wait_time | service_start minus arrival_time |"
            }
            rows={5}
            spellCheck={false}
            style={{
              width: "100%",
              resize: "vertical",
              padding: "12px 14px",
              background: HW2_COLOR.paper,
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 10,
              font: "500 12.5px 'DM Mono', monospace",
              color: HW2_COLOR.ink,
              lineHeight: 1.5,
              outline: "none",
              boxSizing: "border-box",
            }}
          />
          <div
            style={{
              marginTop: 10,
              display: "flex",
              alignItems: "center",
              gap: 12,
              flexWrap: "wrap",
            }}
          >
            <label
              style={{
                cursor: "pointer",
                background: HW2_COLOR.chip,
                border: `1px solid ${HW2_COLOR.rule2}`,
                borderRadius: 8,
                padding: "7px 12px",
                font: "500 12px 'DM Sans', sans-serif",
                color: HW2_COLOR.ink2,
              }}
            >
              Attach .md / .txt
              <input
                type="file"
                accept=".md,.markdown,.txt,.text,.csv"
                multiple
                onChange={(e) => setFiles(Array.from(e.target.files ?? []))}
                style={{ display: "none" }}
              />
            </label>
            {files.length > 0 && (
              <span style={{ font: "500 12px 'DM Mono', monospace", color: HW2_COLOR.blue }}>
                {files.map((f) => f.name).join(", ")}
              </span>
            )}
            <div style={{ flex: 1 }} />
            <button
              onClick={add}
              disabled={busy || (!text.trim() && files.length === 0)}
              style={{
                appearance: "none",
                cursor: busy || (!text.trim() && files.length === 0) ? "default" : "pointer",
                background: HW2_COLOR.blue,
                color: "#fff",
                border: "1px solid transparent",
                borderRadius: 8,
                padding: "8px 16px",
                font: "600 13px 'DM Sans', sans-serif",
                fontFamily: "'DM Sans', sans-serif",
                opacity: busy || (!text.trim() && files.length === 0) ? 0.5 : 1,
              }}
            >
              {busy ? "Adding…" : "Add & refresh"}
            </button>
          </div>
          {result && (
            <div
              style={{
                marginTop: 10,
                padding: "10px 14px",
                background: HW2_COLOR.goodSoft,
                border: `1px solid ${HW2_COLOR.good}33`,
                borderRadius: 8,
                font: "500 12.5px 'DM Sans', sans-serif",
                color: HW2_COLOR.good,
                lineHeight: 1.5,
              }}
            >
              {result} — recompute to propagate it through the workflow.
            </div>
          )}
          {error && (
            <div
              style={{
                marginTop: 10,
                padding: "10px 14px",
                background: HW2_COLOR.badSoft,
                border: `1px solid ${HW2_COLOR.bad}44`,
                borderRadius: 8,
                font: "500 12.5px 'DM Sans', sans-serif",
                color: HW2_COLOR.bad,
              }}
            >
              {error}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
