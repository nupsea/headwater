"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import {
  h2,
  notifyInputChanged,
  onHw2Event,
  HW2_RECOMPUTED,
  type H2ResolveCard,
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

function ResolveCardRow({
  card,
  projectId,
  onChanged,
}: {
  card: H2ResolveCard;
  projectId: string;
  onChanged: () => void;
}) {
  const [open, setOpen] = useState(card.status !== "deferred");
  const [adding, setAdding] = useState(false);
  const [ctx, setCtx] = useState("");
  const [busy, setBusy] = useState(false);
  const [aiBusy, setAiBusy] = useState(false);
  const [aiNote, setAiNote] = useState<string | null>(null);
  const [saved, setSaved] = useState<string | null>(null);
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
            {card.issue_kind}
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
          {card.contract_impacts.length > 0 && (
            <div>
              <div
                style={{
                  font: "600 11px 'DM Sans', sans-serif",
                  color: HW2_COLOR.muted,
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                  marginBottom: 6,
                }}
              >
                Contract impacts
              </div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                {card.contract_impacts.map((c, i) => (
                  <span
                    key={i}
                    style={{
                      font: "500 11px 'DM Mono', monospace",
                      color: HW2_COLOR.ink2,
                      padding: "2px 8px",
                      background: HW2_COLOR.chip,
                      borderRadius: 4,
                    }}
                  >
                    {c}
                  </span>
                ))}
              </div>
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
                  "Paste a definition or note. Markdown tables map to columns, e.g.\n\n| column | meaning |\n| --- | --- |\n| total_wait_time | service_ts minus arrival_time |"
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

export default function ResolvePage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();
  const [cards, setCards] = useState<H2ResolveCard[]>([]);
  const [loading, setLoading] = useState(true);
  const [rebuilding, setRebuilding] = useState(false);

  const load = () =>
    h2.projects.resolve
      .list(id)
      .then((c) => setCards(c))
      .finally(() => setLoading(false));

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
  useEffect(() => onHw2Event(HW2_RECOMPUTED, load), [id]);

  const high = cards.filter((c) => c.priority === "high");
  const medium = cards.filter((c) => c.priority === "medium");
  const low = cards.filter((c) => c.priority === "low");

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
          {cards.length} item{cards.length !== 1 ? "s" : ""}
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
        {cards.length === 0
          ? "Nothing to resolve."
          : `${cards.length} thing${cards.length !== 1 ? "s" : ""} to address.`}
      </h2>
      <p
        style={{
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          marginBottom: 24,
          lineHeight: 1.55,
        }}
      >
        Ranked by how much each one moves the per-answer verdict. Resolving
        blocking items unlocks certification for the affected questions.
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

          <div style={{ display: "grid", gap: 20 }}>
            {high.length > 0 && (
              <div>
                <div
                  style={{
                    font: "600 11px 'DM Sans', sans-serif",
                    color: HW2_COLOR.bad,
                    textTransform: "uppercase",
                    letterSpacing: "0.07em",
                    marginBottom: 8,
                    display: "flex",
                    alignItems: "center",
                    gap: 8,
                  }}
                >
                  <span
                    style={{
                      width: 6,
                      height: 6,
                      borderRadius: "50%",
                      background: HW2_COLOR.bad,
                    }}
                  />
                  High priority · {high.length} item{high.length !== 1 ? "s" : ""}
                </div>
                <div style={{ display: "grid", gap: 8 }}>
                  {high.map((c) => (
                    <ResolveCardRow key={c.card_id} card={c} projectId={id} onChanged={load} />
                  ))}
                </div>
              </div>
            )}

            {medium.length > 0 && (
              <div>
                <div
                  style={{
                    font: "600 11px 'DM Sans', sans-serif",
                    color: HW2_COLOR.warn,
                    textTransform: "uppercase",
                    letterSpacing: "0.07em",
                    marginBottom: 8,
                    display: "flex",
                    alignItems: "center",
                    gap: 8,
                  }}
                >
                  <span
                    style={{
                      width: 6,
                      height: 6,
                      borderRadius: "50%",
                      background: HW2_COLOR.warn,
                    }}
                  />
                  Medium priority · {medium.length} item{medium.length !== 1 ? "s" : ""}
                </div>
                <div style={{ display: "grid", gap: 8 }}>
                  {medium.map((c) => (
                    <ResolveCardRow key={c.card_id} card={c} projectId={id} onChanged={load} />
                  ))}
                </div>
              </div>
            )}

            {low.length > 0 && (
              <div>
                <div
                  style={{
                    font: "600 11px 'DM Sans', sans-serif",
                    color: HW2_COLOR.muted,
                    textTransform: "uppercase",
                    letterSpacing: "0.07em",
                    marginBottom: 8,
                    display: "flex",
                    alignItems: "center",
                    gap: 8,
                  }}
                >
                  <span
                    style={{
                      width: 6,
                      height: 6,
                      borderRadius: "50%",
                      background: HW2_COLOR.muted,
                    }}
                  />
                  Low priority · {low.length} item{low.length !== 1 ? "s" : ""}
                </div>
                <div style={{ display: "grid", gap: 8 }}>
                  {low.map((c) => (
                    <ResolveCardRow key={c.card_id} card={c} projectId={id} onChanged={load} />
                  ))}
                </div>
              </div>
            )}
          </div>
        </>
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
