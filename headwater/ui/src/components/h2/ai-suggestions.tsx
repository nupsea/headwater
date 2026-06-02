"use client";

import { useState } from "react";
import {
  h2,
  notifyInputChanged,
  type H2KeyProposal,
  type H2RelProposal,
} from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

// Move D: the LLM proposes relationships + business keys; the analyst confirms
// (locks) them. Confirming is an input change → flips the fingerprint → the fast
// loop re-runs on verified ground truth. Advisory; degrades when no model.

const ghostBtn: React.CSSProperties = {
  appearance: "none",
  cursor: "pointer",
  background: HW2_COLOR.blueSoft,
  border: `1px solid ${HW2_COLOR.blue}44`,
  borderRadius: 8,
  padding: "7px 13px",
  font: "600 12px 'DM Sans', sans-serif",
  color: HW2_COLOR.blue,
  fontFamily: "'DM Sans', sans-serif",
};

function Confidence({ value }: { value: number }) {
  return (
    <span
      style={{
        font: "500 10px 'DM Mono', monospace",
        color: HW2_COLOR.faint,
        whiteSpace: "nowrap",
      }}
    >
      {Math.round(value * 100)}%
    </span>
  );
}

export function AiSuggestions({ sourceName }: { sourceName: string }) {
  const [rels, setRels] = useState<H2RelProposal[] | null>(null);
  const [keys, setKeys] = useState<H2KeyProposal[] | null>(null);
  const [note, setNote] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmed, setConfirmed] = useState<Set<string>>(new Set());

  if (!sourceName) return null;

  const markConfirmed = (key: string) =>
    setConfirmed((s) => new Set(s).add(key));

  const suggestRels = async () => {
    setBusy(true);
    setNote(null);
    try {
      const r = await h2.sources.suggestRelationships(sourceName);
      setRels(r.relationships);
      if (!r.available) setNote(r.note ?? "No model available.");
    } catch {
      setNote("Could not reach the suggestion service.");
    } finally {
      setBusy(false);
    }
  };

  const suggestKeys = async () => {
    setBusy(true);
    setNote(null);
    try {
      const r = await h2.sources.suggestKeys(sourceName);
      setKeys(r.keys);
      if (!r.available) setNote(r.note ?? "No model available.");
    } catch {
      setNote("Could not reach the suggestion service.");
    } finally {
      setBusy(false);
    }
  };

  const confirmRel = async (rel: H2RelProposal, key: string) => {
    await h2.sources.confirmRelationship(sourceName, rel);
    markConfirmed(key);
    notifyInputChanged();
  };

  const confirmKey = async (k: H2KeyProposal, key: string) => {
    await h2.sources.confirmKey(sourceName, k.table, k.columns);
    markConfirmed(key);
    notifyInputChanged();
  };

  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 12,
        padding: "16px 18px",
      }}
    >
      <div
        style={{
          font: "600 11px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          textTransform: "uppercase",
          letterSpacing: "0.06em",
          marginBottom: 4,
        }}
      >
        ✦ AI structure suggestions
      </div>
      <p
        style={{
          font: "400 12.5px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          lineHeight: 1.5,
          margin: "0 0 12px",
        }}
      >
        Propose relationships and keys from the schema. Review and confirm — a
        confirmed structure locks as ground truth and re-runs the workflow.
      </p>
      <div style={{ display: "flex", gap: 8, marginBottom: 12, flexWrap: "wrap" }}>
        <button onClick={suggestRels} disabled={busy} style={ghostBtn}>
          {busy ? "Thinking…" : "Suggest relationships"}
        </button>
        <button onClick={suggestKeys} disabled={busy} style={ghostBtn}>
          {busy ? "Thinking…" : "Suggest keys"}
        </button>
      </div>

      {note && (
        <p style={{ font: "400 12px 'DM Sans', sans-serif", color: HW2_COLOR.faint, margin: "0 0 8px" }}>
          {note}
        </p>
      )}

      {rels && rels.length > 0 && (
        <div style={{ marginBottom: 10 }}>
          <SubLabel>Relationships</SubLabel>
          <div style={{ display: "grid", gap: 6 }}>
            {rels.map((r) => {
              const key = `rel:${r.from_table}.${r.from_column}->${r.to_table}.${r.to_column}`;
              const done = confirmed.has(key);
              return (
                <Row key={key} done={done} onConfirm={() => confirmRel(r, key)}>
                  <code style={{ fontFamily: "'DM Mono', monospace", fontSize: 12 }}>
                    {r.from_table}.{r.from_column} → {r.to_table}.{r.to_column}
                  </code>
                  {r.rationale && <Why text={r.rationale} />}
                  <Confidence value={r.confidence} />
                </Row>
              );
            })}
          </div>
        </div>
      )}
      {rels && rels.length === 0 && <Empty kind="relationships" />}

      {keys && keys.length > 0 && (
        <div>
          <SubLabel>Keys</SubLabel>
          <div style={{ display: "grid", gap: 6 }}>
            {keys.map((k) => {
              const key = `key:${k.table}:${k.columns.join(",")}`;
              const done = confirmed.has(key);
              return (
                <Row key={key} done={done} onConfirm={() => confirmKey(k, key)} confirmLabel="Lock">
                  <code style={{ fontFamily: "'DM Mono', monospace", fontSize: 12 }}>
                    {k.table} ({k.columns.join(", ")})
                  </code>
                  {k.rationale && <Why text={k.rationale} />}
                  <Confidence value={k.confidence} />
                </Row>
              );
            })}
          </div>
        </div>
      )}
      {keys && keys.length === 0 && <Empty kind="keys" />}
    </div>
  );
}

function SubLabel({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        font: "600 10px 'DM Sans', sans-serif",
        color: HW2_COLOR.faint,
        textTransform: "uppercase",
        letterSpacing: "0.07em",
        margin: "4px 0 6px",
      }}
    >
      {children}
    </div>
  );
}

function Why({ text }: { text: string }) {
  return (
    <span
      style={{
        flex: 1,
        minWidth: 0,
        font: "400 12px 'DM Sans', sans-serif",
        color: HW2_COLOR.muted,
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      }}
      title={text}
    >
      {text}
    </span>
  );
}

function Empty({ kind }: { kind: string }) {
  return (
    <p style={{ font: "400 12px 'DM Sans', sans-serif", color: HW2_COLOR.faint, margin: "4px 0 0" }}>
      No {kind} proposed.
    </p>
  );
}

function Row({
  children,
  done,
  onConfirm,
  confirmLabel = "Confirm",
}: {
  children: React.ReactNode;
  done: boolean;
  onConfirm: () => void;
  confirmLabel?: string;
}) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 10,
        padding: "6px 10px",
        background: HW2_COLOR.paper,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 8,
      }}
    >
      {children}
      {done ? (
        <span
          style={{
            font: "600 11px 'DM Sans', sans-serif",
            color: HW2_COLOR.good,
            whiteSpace: "nowrap",
          }}
        >
          ✓ Locked
        </span>
      ) : (
        <button
          onClick={onConfirm}
          style={{
            appearance: "none",
            cursor: "pointer",
            background: "#fff",
            border: `1px solid ${HW2_COLOR.rule2}`,
            borderRadius: 6,
            padding: "4px 10px",
            font: "500 11.5px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink2,
            whiteSpace: "nowrap",
            fontFamily: "'DM Sans', sans-serif",
          }}
        >
          {confirmLabel}
        </button>
      )}
    </div>
  );
}
