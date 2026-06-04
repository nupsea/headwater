"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import {
  h2,
  notifyInputChanged,
  onHw2Event,
  HW2_RECOMPUTED,
  type H2ResolveCard,
  type H2Definition,
} from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

const secondaryBtn: React.CSSProperties = {
  appearance: "none",
  cursor: "pointer",
  background: "#fff",
  border: `1px solid ${HW2_COLOR.rule2}`,
  borderRadius: 8,
  padding: "7px 13px",
  font: "500 12px 'DM Sans', sans-serif",
  color: HW2_COLOR.ink2,
  fontFamily: "'DM Sans', sans-serif",
};

const primaryBtn: React.CSSProperties = {
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

// Human labels for the raw issue kinds — no internal jargon on screen.
const ISSUE_LABEL: Record<string, string> = {
  answer_gap: "Blocks an answer",
  enum_mapping_needed: "Codes need meaning",
  ambiguous_code: "Ambiguous codes",
  missing_definition: "Missing definition",
  data_quality_risk: "Data quality risk",
  structural_ambiguity: "Structure unclear",
  cannot_answer_gap: "Data limitation",
  insufficient_coverage: "Not enough data",
};

function ImpactPill({ priority }: { priority: "high" | "medium" | "low" }) {
  const tones: Record<string, { bg: string; color: string }> = {
    high:   { bg: HW2_COLOR.badSoft,  color: HW2_COLOR.bad },
    medium: { bg: HW2_COLOR.warnSoft, color: HW2_COLOR.warn },
    low:    { bg: HW2_COLOR.chip,     color: HW2_COLOR.muted },
  };
  const t = tones[priority] ?? tones.low;
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "3px 8px",
        borderRadius: 4,
        background: t.bg,
        color: t.color,
        font: "700 10px 'DM Sans', sans-serif",
        letterSpacing: "0.06em",
        textTransform: "uppercase",
      }}
    >
      {priority.toUpperCase()}
    </span>
  );
}

function SectionLabel({
  dot,
  color,
  children,
}: {
  dot: string;
  color: string;
  children: React.ReactNode;
}) {
  return (
    <div
      style={{
        font: "600 11px 'DM Sans', sans-serif",
        color,
        textTransform: "uppercase",
        letterSpacing: "0.07em",
        marginBottom: 10,
        display: "flex",
        alignItems: "center",
        gap: 8,
      }}
    >
      <span style={{ width: 6, height: 6, borderRadius: "50%", background: dot }} />
      {children}
    </div>
  );
}

function ResolveCardRow({
  card,
  projectId,
  onChanged,
  defaultOpen = true,
}: {
  card: H2ResolveCard;
  projectId: string;
  onChanged: () => void;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen && card.status !== "deferred");
  const [adding, setAdding] = useState(false);
  const [ctx, setCtx] = useState("");
  const [busy, setBusy] = useState(false);
  const [aiBusy, setAiBusy] = useState(false);
  const [aiNote, setAiNote] = useState<string | null>(null);
  // Hydrate from the persisted definition so a saved card shows its value on a
  // return visit instead of an empty box (the claim lives in the store).
  const [saved, setSaved] = useState<string | null>(card.definition ?? null);
  const [showWhy, setShowWhy] = useState(false);
  const deferred = card.status === "deferred";

  const defer = async () => {
    setBusy(true);
    try {
      await h2.projects.resolve.setDisposition(projectId, card.card_id, "deferred");
      notifyInputChanged();
      onChanged();
    } finally {
      setBusy(false);
    }
  };

  const reopen = async () => {
    setBusy(true);
    try {
      await h2.projects.resolve.setDisposition(projectId, card.card_id, "open");
      notifyInputChanged();
      onChanged();
    } finally {
      setBusy(false);
    }
  };

  const addContext = async () => {
    if (!ctx.trim()) return;
    setBusy(true);
    try {
      const file = new File([ctx], "resolve-context.md", { type: "text/markdown" });
      await h2.projects.resources.ingest(projectId, file);
      // S-BIND: bind the text directly to this card's column (when it has one),
      // so the blocking gap clears without the Schema & meaning detour.
      await h2.projects.resolve.define(projectId, card.card_id, ctx.trim());
      setSaved(ctx.trim()); // remember what was saved; show it read-only
      setAdding(false);
      setAiNote(null);
      notifyInputChanged();
      onChanged();
    } finally {
      setBusy(false);
    }
  };

  const askAI = async () => {
    setAdding(true); // open the editor so the draft is visible and editable
    setAiBusy(true);
    setAiNote(null);
    try {
      const r = await h2.projects.resolve.suggest(projectId, card.card_id);
      if (r.available && r.markdown) {
        // Prefill for the user to review/edit — never auto-saved.
        setCtx((prev) => (prev.trim() ? `${prev}\n\n${r.markdown}` : r.markdown));
      }
      setAiNote(r.note);
    } catch {
      setAiNote("Could not reach the suggestion service.");
    } finally {
      setAiBusy(false);
    }
  };

  // Confirm a one-click duration parse (the chosen unit) for a text measure.
  const derive = async (formatId: string) => {
    setBusy(true);
    try {
      await h2.projects.resolve.derive(projectId, card.card_id, formatId);
      notifyInputChanged();
      onChanged();
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${card.priority === "high" ? HW2_COLOR.bad + "44" : HW2_COLOR.rule}`,
        borderRadius: 12,
        overflow: "hidden",
        opacity: deferred ? 0.6 : 1,
      }}
    >
      <button
        onClick={() => setOpen((v) => !v)}
        style={{
          appearance: "none",
          cursor: "pointer",
          width: "100%",
          background: "transparent",
          border: "none",
          padding: "16px 20px",
          textAlign: "left",
          display: "flex",
          alignItems: "flex-start",
          gap: 14,
          fontFamily: "'DM Sans', sans-serif",
        }}
      >
        <ImpactPill priority={card.priority} />
        <div style={{ flex: 1, minWidth: 0 }}>
          <p
            style={{
              font: "600 14px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink,
              marginBottom: 3,
              lineHeight: 1.35,
            }}
          >
            {card.title}
          </p>
          <p
            style={{
              font: "400 11.5px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
            }}
          >
            {ISSUE_LABEL[card.issue_kind] ?? "Needs a decision"}
            {card.affected_questions.length > 0 && (
              <span style={{ color: HW2_COLOR.faint }}>
                {" "}
                · affects {card.affected_questions.length} question
                {card.affected_questions.length !== 1 ? "s" : ""}
              </span>
            )}
            {deferred && (
              <span style={{ color: HW2_COLOR.faint }}> · deferred to next cycle</span>
            )}
          </p>
        </div>
        <span
          style={{
            font: "500 11px 'DM Mono', monospace",
            color: HW2_COLOR.faint,
            flexShrink: 0,
            marginTop: 3,
          }}
        >
          {open ? "▾" : "▸"}
        </span>
      </button>

      {open && (
        <div
          style={{
            borderTop: `1px solid ${HW2_COLOR.rule}`,
            padding: "16px 20px",
            background: HW2_COLOR.paper,
          }}
        >
          <p
            style={{
              font: "400 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink2,
              lineHeight: 1.55,
              marginBottom: 12,
            }}
          >
            {card.body}
          </p>

          {/* The concrete root cause, shown — e.g. the undefined codes A, H, S, D. */}
          {(card.values?.length ?? 0) > 0 && (
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: 6,
                marginBottom: 12,
              }}
            >
              {card.values!.map((v, i) => (
                <span
                  key={i}
                  style={{
                    font: "600 12px 'DM Mono', monospace",
                    color: HW2_COLOR.ink,
                    padding: "3px 9px",
                    background: HW2_COLOR.chip,
                    border: `1px solid ${HW2_COLOR.rule2}`,
                    borderRadius: 5,
                  }}
                >
                  {v}
                </span>
              ))}
            </div>
          )}

          {/* One-click duration parse: a best-guess convert + alternative units.
              Advisory — the user picks the interpretation; it's applied on click. */}
          {card.derivation && (
            <div style={{ marginBottom: 12 }}>
              {card.derivation.samples.length > 0 && (
                <div
                  style={{
                    display: "flex",
                    flexWrap: "wrap",
                    gap: 6,
                    marginBottom: 8,
                  }}
                >
                  {card.derivation.samples.slice(0, 4).map((s, i) => (
                    <span
                      key={i}
                      style={{
                        font: "500 12px 'DM Mono', monospace",
                        color: HW2_COLOR.ink2,
                        padding: "3px 9px",
                        background: HW2_COLOR.chip,
                        borderRadius: 5,
                      }}
                    >
                      {s}
                    </span>
                  ))}
                </div>
              )}
              <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                {card.derivation.options.map((opt, i) => (
                  <button
                    key={opt.id}
                    onClick={() => derive(opt.id)}
                    disabled={busy}
                    title={`Parse ${opt.label} to ${card.derivation!.unit}`}
                    style={{
                      appearance: "none",
                      cursor: busy ? "default" : "pointer",
                      background: i === 0 ? HW2_COLOR.blueSoft : "#fff",
                      border: `1px solid ${i === 0 ? HW2_COLOR.blue + "44" : HW2_COLOR.rule2}`,
                      borderRadius: 7,
                      padding: "7px 13px",
                      font: `${i === 0 ? "600" : "500"} 12px 'DM Sans', sans-serif`,
                      color: i === 0 ? HW2_COLOR.blue : HW2_COLOR.ink2,
                      fontFamily: "'DM Sans', sans-serif",
                      opacity: busy ? 0.6 : 1,
                    }}
                  >
                    {i === 0 ? "Convert" : "It's"} {opt.label} → {card.derivation!.unit}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* The questions this unblocks — concrete, not jargon. */}
          {(card.affected_titles?.length ?? 0) > 0 && (
            <ul
              style={{
                margin: "0 0 4px",
                paddingLeft: 18,
                font: "400 12.5px 'DM Sans', sans-serif",
                color: HW2_COLOR.ink2,
                lineHeight: 1.5,
              }}
            >
              {card.affected_titles!.map((t, i) => (
                <li key={i}>{t}</li>
              ))}
            </ul>
          )}

          {/* Detail (e.g. the judge's findings) stays tucked behind a disclosure
              so the card reads as a simple ask, not a transcript. */}
          {(card.why?.length ?? 0) > 0 && (
            <div style={{ marginTop: 4 }}>
              <button
                onClick={() => setShowWhy((v) => !v)}
                style={{
                  appearance: "none",
                  background: "transparent",
                  border: "none",
                  cursor: "pointer",
                  padding: 0,
                  font: "500 12px 'DM Sans', sans-serif",
                  color: HW2_COLOR.blue,
                  fontFamily: "'DM Sans', sans-serif",
                }}
              >
                {showWhy ? "Hide detail" : "Why?"}
              </button>
              {showWhy && (
                <ul
                  style={{
                    margin: "8px 0 0",
                    paddingLeft: 18,
                    font: "400 12px 'DM Sans', sans-serif",
                    color: HW2_COLOR.muted,
                    lineHeight: 1.55,
                  }}
                >
                  {card.why!.map((r, i) => (
                    <li key={i}>{r}</li>
                  ))}
                </ul>
              )}
            </div>
          )}

          {/* Dispositions — feed the recompute loop */}
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: 8,
              marginTop: 16,
              paddingTop: 14,
              borderTop: `1px solid ${HW2_COLOR.rule}`,
            }}
          >
            {deferred ? (
              <button onClick={reopen} disabled={busy} style={secondaryBtn}>
                Reopen
              </button>
            ) : (
              <button onClick={defer} disabled={busy} style={secondaryBtn}>
                Defer to next cycle
              </button>
            )}
            <button
              onClick={() => {
                if (saved && !adding) {
                  setCtx(saved); // edit again starts from the saved value
                }
                setAdding((v) => !v);
              }}
              disabled={busy}
              style={primaryBtn}
            >
              {adding
                ? "Cancel"
                : saved
                ? "Edit context"
                : "Add context / define a term"}
            </button>
            {!saved && (
              <button
                onClick={askAI}
                disabled={aiBusy || busy}
                title="Let a local model draft a definition from the column name and its known codes — you review and edit before saving."
                style={{
                  appearance: "none",
                  cursor: aiBusy || busy ? "default" : "pointer",
                  background: HW2_COLOR.blueSoft,
                  border: `1px solid ${HW2_COLOR.blue}44`,
                  borderRadius: 7,
                  padding: "7px 13px",
                  font: "600 12px 'DM Sans', sans-serif",
                  color: HW2_COLOR.blue,
                  fontFamily: "'DM Sans', sans-serif",
                  opacity: aiBusy || busy ? 0.6 : 1,
                }}
              >
                {aiBusy ? "Drafting…" : "✦ Ask AI"}
              </button>
            )}
          </div>

          {saved && !adding && (
            <div
              style={{
                marginTop: 12,
                padding: "12px 14px",
                background: HW2_COLOR.goodSoft,
                border: `1px solid ${HW2_COLOR.good}33`,
                borderRadius: 8,
              }}
            >
              <div
                style={{
                  font: "600 12px 'DM Sans', sans-serif",
                  color: HW2_COLOR.good,
                  marginBottom: 6,
                }}
              >
                ✓ Saved — press Recompute (top banner) to apply it across the workflow.
              </div>
              <pre
                style={{
                  margin: 0,
                  font: "500 12px 'DM Mono', monospace",
                  color: HW2_COLOR.ink2,
                  lineHeight: 1.5,
                  whiteSpace: "pre-wrap",
                }}
              >
                {saved}
              </pre>
            </div>
          )}

          {adding && (
            <div style={{ marginTop: 12 }}>
              {aiNote && (
                <div
                  style={{
                    font: "400 11.5px 'DM Sans', sans-serif",
                    color: HW2_COLOR.muted,
                    marginBottom: 8,
                  }}
                >
                  {aiNote}
                </div>
              )}
              <textarea
                value={ctx}
                onChange={(e) => setCtx(e.target.value)}
                placeholder={
                  "Paste a definition or note. A two-column markdown table maps codes to meanings, e.g.\n\n| code | meaning |\n| --- | --- |\n| A | Active |\n| C | Closed |"
                }
                spellCheck={false}
                style={{
                  width: "100%",
                  minHeight: 110,
                  padding: "10px 14px",
                  background: "#fff",
                  border: `1px solid ${HW2_COLOR.rule2}`,
                  borderRadius: 8,
                  font: "500 12.5px 'DM Mono', monospace",
                  color: HW2_COLOR.ink,
                  lineHeight: 1.5,
                  resize: "vertical",
                  outline: "none",
                  boxSizing: "border-box",
                }}
              />
              <button
                onClick={addContext}
                disabled={busy || !ctx.trim()}
                style={{
                  ...primaryBtn,
                  marginTop: 8,
                  opacity: busy || !ctx.trim() ? 0.5 : 1,
                }}
              >
                {busy ? "Saving…" : "Save context — triggers a refresh"}
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/** A saved definition: collapsed by default, expandable to review and edit.
 *  The context is never lost — it lives as a locked claim keyed by table.column. */
function DefinedRow({
  def,
  projectId,
  onChanged,
}: {
  def: H2Definition;
  projectId: string;
  onChanged: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [editing, setEditing] = useState(false);
  const [text, setText] = useState(def.text);
  const [busy, setBusy] = useState(false);

  const save = async () => {
    if (!text.trim()) return;
    setBusy(true);
    try {
      await h2.projects.definitions.save(projectId, def.table, def.column, text.trim());
      setEditing(false);
      notifyInputChanged();
      onChanged();
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      style={{
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 12,
        background: "#fff",
        overflow: "hidden",
      }}
    >
      <button
        onClick={() => setOpen((o) => !o)}
        style={{
          appearance: "none",
          cursor: "pointer",
          width: "100%",
          background: "transparent",
          border: "none",
          padding: "12px 16px",
          display: "flex",
          alignItems: "center",
          gap: 10,
          textAlign: "left",
        }}
      >
        <span style={{ color: HW2_COLOR.good, fontSize: 13 }}>✓</span>
        <span style={{ font: "600 13.5px 'DM Sans', sans-serif", color: HW2_COLOR.ink }}>
          {def.column}
        </span>
        <span style={{ font: "400 11px 'DM Mono', monospace", color: HW2_COLOR.faint }}>
          {def.table}
        </span>
        <span style={{ marginLeft: "auto", color: HW2_COLOR.faint, fontSize: 11 }}>
          {open ? "▾" : "▸"}
        </span>
      </button>

      {open && (
        <div style={{ padding: "0 16px 14px" }}>
          {!editing ? (
            <>
              <pre
                style={{
                  margin: 0,
                  padding: "10px 12px",
                  background: HW2_COLOR.paper,
                  borderRadius: 8,
                  font: "400 12px 'DM Mono', monospace",
                  color: HW2_COLOR.ink2,
                  whiteSpace: "pre-wrap",
                  lineHeight: 1.5,
                }}
              >
                {def.text}
              </pre>
              <button
                onClick={() => {
                  setText(def.text);
                  setEditing(true);
                }}
                style={{ ...secondaryBtn, marginTop: 10 }}
              >
                Edit
              </button>
            </>
          ) : (
            <>
              <textarea
                value={text}
                onChange={(e) => setText(e.target.value)}
                spellCheck={false}
                style={{
                  width: "100%",
                  minHeight: 110,
                  padding: "10px 12px",
                  background: "#fff",
                  border: `1px solid ${HW2_COLOR.rule2}`,
                  borderRadius: 8,
                  font: "400 12.5px 'DM Mono', monospace",
                  color: HW2_COLOR.ink,
                  lineHeight: 1.5,
                  resize: "vertical",
                  outline: "none",
                  boxSizing: "border-box",
                }}
              />
              <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
                <button
                  onClick={save}
                  disabled={busy || !text.trim()}
                  style={{
                    appearance: "none",
                    cursor: busy ? "default" : "pointer",
                    background: HW2_COLOR.blue,
                    color: "#fff",
                    border: "1px solid transparent",
                    borderRadius: 8,
                    padding: "8px 14px",
                    font: "600 12.5px 'DM Sans', sans-serif",
                    opacity: busy || !text.trim() ? 0.5 : 1,
                  }}
                >
                  {busy ? "Saving…" : "Save — triggers a refresh"}
                </button>
                <button onClick={() => setEditing(false)} style={secondaryBtn}>
                  Cancel
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

export default function ResolvePage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();
  const [cards, setCards] = useState<H2ResolveCard[]>([]);
  const [definitions, setDefinitions] = useState<H2Definition[]>([]);
  const [loading, setLoading] = useState(true);
  const [rebuilding, setRebuilding] = useState(false);

  const load = () => {
    // Saved context is loaded alongside the open cards so it persists on the
    // screen (collapsed, editable) instead of vanishing once a gap is cleared.
    h2.projects.definitions.list(id).then(setDefinitions).catch(() => {});
    return h2.projects.resolve
      .list(id)
      .then((c) => setCards(c))
      .finally(() => setLoading(false));
  };

  const rebuild = async () => {
    setRebuilding(true);
    try {
      const result = await h2.projects.resolve.build(id);
      setCards(result);
    } finally {
      setRebuilding(false);
    }
  };

  useEffect(() => {
    load();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

  // A recompute can open/close gap cards; reflect the new set here.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => onHw2Event(HW2_RECOMPUTED, load), [id]);

  // Two buckets: actionable asks the analyst can resolve now vs informational
  // data limitations (too few days, missing source) they can't fix by defining
  // a term. Signal over noise: the headline counts only the asks.
  const RANK: Record<string, number> = { high: 0, medium: 1, low: 2 };
  const inputCards = cards
    .filter((c) => c.category !== "limitation")
    .sort((a, b) => (RANK[a.priority] ?? 3) - (RANK[b.priority] ?? 3));
  const limitationCards = cards.filter((c) => c.category === "limitation");

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
        maxWidth: 860,
        margin: "0 auto",
        padding: "28px 32px 80px",
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
        <span
          style={{
            font: "600 11px 'DM Sans', sans-serif",
            color: HW2_COLOR.blue,
            letterSpacing: "0.08em",
            textTransform: "uppercase",
          }}
        >
          Step 3 of 5 · Resolve
        </span>
        <span
          style={{
            font: "500 12px 'DM Mono', monospace",
            color: HW2_COLOR.muted,
          }}
        >
          {inputCards.length} to address
        </span>
      </div>

      <h2
        style={{
          font: "600 26px 'DM Sans', sans-serif",
          letterSpacing: "-0.02em",
          color: HW2_COLOR.ink,
          lineHeight: 1.25,
          marginTop: 8,
          marginBottom: 4,
        }}
      >
        {inputCards.length === 0
          ? "Nothing needs your input."
          : `${inputCards.length} thing${inputCards.length !== 1 ? "s" : ""} to clear up.`}
      </h2>
      <p
        style={{
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          marginBottom: 24,
          lineHeight: 1.55,
        }}
      >
        These need a human decision — define a term, confirm a meaning, or set it
        aside. If something isn&rsquo;t clear, open the data and develop it.
      </p>

      {cards.length === 0 ? (
        <div
          style={{
            border: `1.5px dashed ${HW2_COLOR.rule2}`,
            borderRadius: 14,
            padding: "56px 40px",
            textAlign: "center",
            marginBottom: 24,
          }}
        >
          <div
            style={{
              font: "500 14px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              marginBottom: 12,
            }}
          >
            No resolve items.
          </div>
          <p
            style={{
              font: "400 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.faint,
              lineHeight: 1.5,
              maxWidth: 380,
              margin: "0 auto 20px",
            }}
          >
            Click &ldquo;Build resolve items&rdquo; to analyse the project for
            gaps and decisions.
          </p>
          <button
            onClick={rebuild}
            disabled={rebuilding}
            style={{
              appearance: "none",
              cursor: rebuilding ? "default" : "pointer",
              background: HW2_COLOR.ink,
              color: "#fff",
              border: "1px solid transparent",
              borderRadius: 8,
              padding: "10px 18px",
              font: "600 13px 'DM Sans', sans-serif",
              opacity: rebuilding ? 0.6 : 1,
              fontFamily: "'DM Sans', sans-serif",
            }}
          >
            {rebuilding ? "Building…" : "Build resolve items"}
          </button>
        </div>
      ) : (
        <>
          <div
            style={{
              display: "flex",
              justifyContent: "flex-end",
              marginBottom: 16,
            }}
          >
            <button
              onClick={rebuild}
              disabled={rebuilding}
              style={{
                appearance: "none",
                cursor: rebuilding ? "default" : "pointer",
                background: "#fff",
                border: `1px solid ${HW2_COLOR.rule2}`,
                borderRadius: 8,
                padding: "6px 12px",
                font: "500 12px 'DM Sans', sans-serif",
                color: HW2_COLOR.ink2,
                fontFamily: "'DM Sans', sans-serif",
                opacity: rebuilding ? 0.5 : 1,
              }}
            >
              {rebuilding ? "Rebuilding…" : "Rebuild"}
            </button>
          </div>

          <div style={{ display: "grid", gap: 28 }}>
            {inputCards.length > 0 && (
              <div>
                <SectionLabel dot={HW2_COLOR.blue} color={HW2_COLOR.ink}>
                  Needs your input · {inputCards.length}
                </SectionLabel>
                <div style={{ display: "grid", gap: 8 }}>
                  {inputCards.map((c) => (
                    <ResolveCardRow key={c.card_id} card={c} projectId={id} onChanged={load} />
                  ))}
                </div>
              </div>
            )}

            {limitationCards.length > 0 && (
              <div>
                <SectionLabel dot={HW2_COLOR.muted} color={HW2_COLOR.muted}>
                  Data limitations · {limitationCards.length}
                </SectionLabel>
                <p
                  style={{
                    font: "400 12.5px 'DM Sans', sans-serif",
                    color: HW2_COLOR.faint,
                    lineHeight: 1.5,
                    margin: "-2px 0 10px",
                  }}
                >
                  These can&rsquo;t be cleared by defining a term — they need more
                  or different data. Listed so you know what the data can&rsquo;t
                  answer yet.
                </p>
                <div style={{ display: "grid", gap: 8 }}>
                  {limitationCards.map((c) => (
                    <ResolveCardRow
                      key={c.card_id}
                      card={c}
                      projectId={id}
                      onChanged={load}
                      defaultOpen={false}
                    />
                  ))}
                </div>
              </div>
            )}
          </div>
        </>
      )}

      {definitions.length > 0 && (
        <div style={{ marginTop: 28 }}>
          <SectionLabel dot={HW2_COLOR.good} color={HW2_COLOR.muted}>
            Defined context · {definitions.length}
          </SectionLabel>
          <p
            style={{
              font: "400 12.5px 'DM Sans', sans-serif",
              color: HW2_COLOR.faint,
              lineHeight: 1.5,
              margin: "-2px 0 10px",
            }}
          >
            Context you&rsquo;ve saved. It stays here so you can review or edit it
            anytime — recompute reads it as ground truth.
          </p>
          <div style={{ display: "grid", gap: 8 }}>
            {definitions.map((d) => (
              <DefinedRow
                key={`${d.table}.${d.column}`}
                def={d}
                projectId={id}
                onChanged={load}
              />
            ))}
          </div>
        </div>
      )}

      {/* CTA */}
      <div
        style={{
          marginTop: 32,
          paddingTop: 20,
          borderTop: `1px solid ${HW2_COLOR.rule}`,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <span
          style={{
            font: "400 12px 'DM Sans', sans-serif",
            color: HW2_COLOR.faint,
          }}
        >
          Skipped items become explicit gaps in the verdict.
        </span>
        <button
          onClick={() => router.push(`/h2/projects/${id}/readiness`)}
          style={{
            appearance: "none",
            cursor: "pointer",
            background: HW2_COLOR.blue,
            color: "#fff",
            border: "1px solid transparent",
            borderRadius: 10,
            padding: "11px 20px",
            font: "600 14px 'DM Sans', sans-serif",
            fontFamily: "'DM Sans', sans-serif",
          }}
        >
          All done → Readiness
        </button>
      </div>
    </div>
  );
}
