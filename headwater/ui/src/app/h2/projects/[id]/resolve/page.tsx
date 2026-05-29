"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { h2, type H2ResolveCard } from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

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

function ResolveCardRow({ card }: { card: H2ResolveCard }) {
  const [open, setOpen] = useState(false);

  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${card.priority === "high" ? HW2_COLOR.bad + "44" : HW2_COLOR.rule}`,
        borderRadius: 12,
        overflow: "hidden",
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
                    <ResolveCardRow key={c.card_id} card={c} />
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
                    <ResolveCardRow key={c.card_id} card={c} />
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
                    <ResolveCardRow key={c.card_id} card={c} />
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
