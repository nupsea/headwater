"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { h2, type H2AnswerDraft } from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

// ─── State pill ───────────────────────────────────────────────────────────────

function QStatePill({ state }: { state: string }) {
  const cfgs: Record<string, { color: string; bg: string; icon: string; label: string }> = {
    certified:    { color: HW2_COLOR.good,  bg: HW2_COLOR.goodSoft, icon: "✓", label: "Certified" },
    draft:        { color: HW2_COLOR.muted, bg: HW2_COLOR.chip,     icon: "○", label: "Draft" },
    demoted:      { color: HW2_COLOR.bad,   bg: HW2_COLOR.badSoft,  icon: "!", label: "Re-verify" },
    cannot_answer:{ color: HW2_COLOR.warn,  bg: HW2_COLOR.warnSoft, icon: "✗", label: "Can't answer" },
  };
  const cfg = cfgs[state] ?? cfgs.draft;

  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 5,
        padding: "3px 10px",
        borderRadius: 4,
        background: cfg.bg,
        color: cfg.color,
        font: "700 10px 'DM Sans', sans-serif",
        letterSpacing: "0.07em",
        textTransform: "uppercase",
        whiteSpace: "nowrap",
      }}
    >
      <span>{cfg.icon}</span>
      {cfg.label}
    </span>
  );
}

// ─── Confidence bar ───────────────────────────────────────────────────────────

function ConfidenceBar({ value }: { value: number }) {
  const pct = Math.round(value * 100);
  const color =
    value >= 0.85
      ? HW2_COLOR.good
      : value >= 0.6
      ? HW2_COLOR.blue
      : HW2_COLOR.warn;

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 12,
        padding: "10px 16px",
        background: HW2_COLOR.paper,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 8,
      }}
    >
      <div style={{ position: "relative", width: 56, height: 28, flexShrink: 0 }}>
        <svg width="56" height="28" viewBox="0 0 56 28">
          <path
            d="M 4 26 A 24 24 0 0 1 52 26"
            stroke={HW2_COLOR.rule2}
            strokeWidth="4"
            fill="none"
            strokeLinecap="round"
          />
          <path
            d="M 4 26 A 24 24 0 0 1 52 26"
            stroke={color}
            strokeWidth="4"
            fill="none"
            strokeLinecap="round"
            strokeDasharray={`${(Math.PI * 24 * value).toFixed(1)} 1000`}
          />
        </svg>
        <span
          style={{
            position: "absolute",
            inset: 0,
            top: 7,
            textAlign: "center",
            font: "600 12px 'DM Mono', monospace",
            color: HW2_COLOR.ink,
          }}
        >
          {pct}
        </span>
      </div>
      <div>
        <div
          style={{
            font: "600 11px 'DM Sans', sans-serif",
            color: HW2_COLOR.muted,
            letterSpacing: "0.06em",
            textTransform: "uppercase",
          }}
        >
          Insight confidence
        </div>
        <div
          style={{
            font: "500 13px 'DM Sans', sans-serif",
            color: color,
          }}
        >
          {value >= 0.85 ? "High" : value >= 0.6 ? "Forming" : "Low"}
        </div>
      </div>
    </div>
  );
}

// ─── Question sidebar item ────────────────────────────────────────────────────

function QuestionItem({
  idx,
  answer,
  active,
  onClick,
}: {
  idx: number;
  answer: H2AnswerDraft;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      style={{
        appearance: "none",
        cursor: "pointer",
        textAlign: "left",
        background: active ? "#fff" : "transparent",
        border: `1px solid ${active ? HW2_COLOR.blue : HW2_COLOR.rule}`,
        borderRadius: 10,
        padding: "11px 12px",
        boxShadow: active ? "0 1px 0 rgba(20,20,30,0.02)" : "none",
        fontFamily: "'DM Sans', sans-serif",
        width: "100%",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          marginBottom: 5,
          flexWrap: "wrap",
        }}
      >
        <span
          style={{
            font: "500 11px 'DM Mono', monospace",
            color: HW2_COLOR.faint,
          }}
        >
          Q{idx + 1}
        </span>
        <QStatePill state={answer.state} />
      </div>
      <div
        style={{
          font: `${active ? 600 : 500} 13px 'DM Sans', sans-serif`,
          color: active ? HW2_COLOR.ink : HW2_COLOR.ink2,
          lineHeight: 1.35,
        }}
      >
        {answer.question_title}
      </div>
      <div
        style={{
          marginTop: 6,
          font: "500 10.5px 'DM Mono', monospace",
          color: HW2_COLOR.faint,
        }}
      >
        {(answer.confidence * 100).toFixed(0)}% confidence
      </div>
    </button>
  );
}

// ─── Cannot-answer card ───────────────────────────────────────────────────────

function CannotAnswerCard({ answer }: { answer: H2AnswerDraft }) {
  return (
    <div
      style={{
        padding: "26px 28px",
        background: "#fff",
        border: `1.5px solid ${HW2_COLOR.warn}`,
        borderRadius: 12,
      }}
    >
      <div
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 6,
          marginBottom: 14,
          padding: "3px 9px",
          borderRadius: 4,
          background: HW2_COLOR.warnSoft,
          color: HW2_COLOR.warn,
          font: "700 10px 'DM Sans', sans-serif",
          letterSpacing: "0.08em",
          textTransform: "uppercase",
        }}
      >
        <span>✗</span> Can&rsquo;t answer with this data
      </div>
      {answer.caveats.length > 0 && (
        <p
          style={{
            font: "500 16px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink,
            lineHeight: 1.5,
            marginBottom: 12,
            letterSpacing: "-0.01em",
          }}
        >
          {answer.caveats[0]}
        </p>
      )}
      <div
        style={{
          marginTop: 16,
          padding: "10px 14px",
          background: HW2_COLOR.warnSoft,
          borderRadius: 8,
          font: "400 12px 'DM Sans', sans-serif",
          color: HW2_COLOR.ink2,
          lineHeight: 1.55,
        }}
      >
        Headwater will watch the source. The moment enough data has accumulated,
        this question becomes answerable and re-enters the workflow
        automatically.
      </div>
    </div>
  );
}

// ─── Draft query card ─────────────────────────────────────────────────────────

function SqlCard({ answer }: { answer: H2AnswerDraft }) {
  const [editing, setEditing] = useState(false);
  const [sql, setSql] = useState(answer.sql_text ?? "");
  const [running, setRunning] = useState(false);

  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 12,
        overflow: "hidden",
        position: "relative",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "10px 14px",
          borderBottom: `1px solid ${HW2_COLOR.rule}`,
        }}
      >
        <span
          style={{
            font: "600 12px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink,
          }}
        >
          Draft query
        </span>
        <div style={{ display: "flex", gap: 6 }}>
          <button
            onClick={() => setEditing((v) => !v)}
            style={{
              appearance: "none",
              cursor: "pointer",
              background: "#fff",
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 6,
              padding: "4px 10px",
              font: "500 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink2,
              fontFamily: "'DM Sans', sans-serif",
            }}
          >
            {editing ? "Done" : "Edit"}
          </button>
          <button
            onClick={() => {
              setRunning(true);
              setTimeout(() => setRunning(false), 800);
            }}
            style={{
              appearance: "none",
              cursor: "pointer",
              background: HW2_COLOR.blue,
              color: "#fff",
              border: "1px solid transparent",
              borderRadius: 6,
              padding: "4px 10px",
              font: "500 12px 'DM Sans', sans-serif",
              fontFamily: "'DM Sans', sans-serif",
            }}
          >
            {running ? "Running…" : "▶ Run"}
          </button>
        </div>
      </div>

      {editing ? (
        <textarea
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          spellCheck={false}
          style={{
            width: "100%",
            minHeight: 150,
            padding: "12px 16px",
            background: HW2_COLOR.paper,
            border: "none",
            resize: "vertical",
            font: "500 12.5px 'DM Mono', monospace",
            color: HW2_COLOR.ink,
            lineHeight: 1.55,
            outline: "none",
            fontFamily: "'DM Mono', monospace",
            boxSizing: "border-box",
          }}
        />
      ) : (
        <pre
          style={{
            margin: 0,
            padding: "14px 16px",
            font: "400 12.5px 'DM Mono', monospace",
            color: HW2_COLOR.ink2,
            lineHeight: 1.55,
            background: HW2_COLOR.paper,
            overflowX: "auto",
          }}
        >
          {sql || "-- No query drafted yet."}
        </pre>
      )}

      {/* Draft watermark */}
      {answer.state !== "certified" && (
        <div
          style={{
            position: "absolute",
            inset: 0,
            pointerEvents: "none",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          <span
            style={{
              font: "700 52px 'DM Sans', sans-serif",
              color:
                answer.state === "demoted" ? HW2_COLOR.bad : HW2_COLOR.muted,
              opacity: 0.06,
              letterSpacing: "0.15em",
              transform: "rotate(-12deg)",
              textTransform: "uppercase",
            }}
          >
            {answer.state === "demoted" ? "Re-verify" : "Draft"}
          </span>
        </div>
      )}
    </div>
  );
}

// ─── Caveats card ─────────────────────────────────────────────────────────────

function CaveatsCard({ caveats }: { caveats: string[] }) {
  if (caveats.length === 0) return null;
  return (
    <div
      style={{
        padding: "12px 16px",
        background: HW2_COLOR.warnSoft,
        border: `1px solid ${HW2_COLOR.warn}44`,
        borderRadius: 10,
      }}
    >
      <div
        style={{
          font: "600 11px 'DM Sans', sans-serif",
          color: HW2_COLOR.warn,
          textTransform: "uppercase",
          letterSpacing: "0.06em",
          marginBottom: 8,
        }}
      >
        Caveats
      </div>
      {caveats.map((c, i) => (
        <p
          key={i}
          style={{
            font: "400 12.5px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink2,
            lineHeight: 1.5,
            margin: 0,
            paddingBottom: i < caveats.length - 1 ? 6 : 0,
          }}
        >
          {c}
        </p>
      ))}
    </div>
  );
}

// ─── Share panel ──────────────────────────────────────────────────────────────

function SharePanel({
  projectId,
  answer,
  onExportReport,
}: {
  projectId: string;
  answer: H2AnswerDraft;
  onExportReport: () => void;
}) {
  const router = useRouter();

  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 12,
        padding: "18px 20px",
      }}
    >
      <div
        style={{
          font: "600 11px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          letterSpacing: "0.06em",
          textTransform: "uppercase",
          marginBottom: 12,
        }}
      >
        Share
      </div>
      <div style={{ display: "grid", gap: 8 }}>
        <button
          onClick={onExportReport}
          style={{
            appearance: "none",
            cursor: "pointer",
            background: "#fff",
            border: `1px solid ${HW2_COLOR.rule2}`,
            borderRadius: 8,
            padding: "9px 14px",
            font: "500 13px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink2,
            textAlign: "left",
            fontFamily: "'DM Sans', sans-serif",
          }}
        >
          Export audit report (.md)
        </button>
        {answer.state !== "certified" && (
          <button
            onClick={() => router.push(`/h2/projects/${projectId}/resolve`)}
            style={{
              appearance: "none",
              cursor: "pointer",
              background: HW2_COLOR.blueSoft,
              color: HW2_COLOR.blue,
              border: `1px solid ${HW2_COLOR.blue}44`,
              borderRadius: 8,
              padding: "9px 14px",
              font: "500 13px 'DM Sans', sans-serif",
              textAlign: "left",
              fontFamily: "'DM Sans', sans-serif",
            }}
          >
            Resolve to certify →
          </button>
        )}
      </div>
      <div
        style={{
          marginTop: 14,
          paddingTop: 12,
          borderTop: `1px solid ${HW2_COLOR.rule}`,
          font: "400 11.5px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          lineHeight: 1.5,
        }}
      >
        Audit report is portable proof: the verdict + every contract&rsquo;s
        state + lineage + freshness, as Markdown.
      </div>
    </div>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function AnswerPage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();

  const [answers, setAnswers] = useState<H2AnswerDraft[]>([]);
  const [counts, setCounts] = useState({
    certified: 0,
    draft: 0,
    cannot_answer: 0,
  });
  const [loading, setLoading] = useState(true);
  const [drafting, setDrafting] = useState(false);
  const [activeIdx, setActiveIdx] = useState(0);
  const [report, setReport] = useState<string | null>(null);
  const [loadingReport, setLoadingReport] = useState(false);

  const loadDraft = async () => {
    const result = await h2.projects.answer.draft(id);
    setAnswers(result.answers);
    setCounts({
      certified: result.certified_count,
      draft: result.draft_count,
      cannot_answer: result.cannot_answer_count,
    });
    // Default to first demoted, else first draft, else first
    const demotedIdx = result.answers.findIndex((a) => a.state === "demoted");
    const draftIdx = result.answers.findIndex((a) => a.state === "draft");
    setActiveIdx(
      demotedIdx >= 0 ? demotedIdx : draftIdx >= 0 ? draftIdx : 0
    );
  };

  const redraft = async () => {
    setDrafting(true);
    try {
      await loadDraft();
    } finally {
      setDrafting(false);
    }
  };

  const fetchReport = async () => {
    setLoadingReport(true);
    try {
      const text = await h2.projects.report.get(id);
      setReport(text);
      // Trigger download
      const blob = new Blob([text], { type: "text/markdown" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${id}_audit_report.md`;
      a.click();
      URL.revokeObjectURL(url);
    } finally {
      setLoadingReport(false);
    }
  };

  useEffect(() => {
    loadDraft()
      .catch(() => {})
      .finally(() => setLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

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

  const activeAnswer = answers[activeIdx] ?? null;

  return (
    <div
      style={{
        maxWidth: 1180,
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
          Step 5 of 5 · Answer & Share
        </span>
        <div style={{ display: "flex", gap: 8 }}>
          <button
            onClick={redraft}
            disabled={drafting}
            style={{
              appearance: "none",
              cursor: drafting ? "default" : "pointer",
              background: "#fff",
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 8,
              padding: "6px 12px",
              font: "500 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink2,
              fontFamily: "'DM Sans', sans-serif",
              opacity: drafting ? 0.5 : 1,
            }}
          >
            {drafting ? "Drafting…" : "Redraft"}
          </button>
        </div>
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
        Answer your questions.
      </h2>
      <p
        style={{
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          marginBottom: 24,
          lineHeight: 1.5,
        }}
      >
        Each question carries its own evidence. The trust badge appears only
        when every contract passes and the insight itself is confident.
      </p>

      {/* Summary */}
      <div
        style={{
          display: "flex",
          gap: 16,
          marginBottom: 24,
          font: "500 13px 'DM Sans', sans-serif",
          flexWrap: "wrap",
        }}
      >
        <span style={{ color: HW2_COLOR.good }}>
          {counts.certified} certified
        </span>
        <span style={{ color: HW2_COLOR.faint }}>·</span>
        <span style={{ color: HW2_COLOR.muted }}>{counts.draft} draft</span>
        <span style={{ color: HW2_COLOR.faint }}>·</span>
        <span style={{ color: HW2_COLOR.warn }}>
          {counts.cannot_answer} can&rsquo;t answer
        </span>
      </div>

      {answers.length === 0 ? (
        <div
          style={{
            padding: "60px 40px",
            background: HW2_COLOR.surface,
            border: `1px solid ${HW2_COLOR.rule}`,
            borderRadius: 14,
            textAlign: "center",
          }}
        >
          <p
            style={{
              font: "500 14px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
            }}
          >
            No questions yet. Go back to Understand and accept some proposed
            questions.
          </p>
        </div>
      ) : (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "280px 1fr",
            gap: 18,
          }}
        >
          {/* Question sidebar */}
          <aside
            style={{
              display: "flex",
              flexDirection: "column",
              gap: 6,
              alignSelf: "flex-start",
            }}
          >
            <div
              style={{
                font: "600 10px 'DM Sans', sans-serif",
                color: HW2_COLOR.faint,
                letterSpacing: "0.08em",
                textTransform: "uppercase",
                padding: "0 4px",
                marginBottom: 4,
              }}
            >
              Questions
            </div>
            {answers.map((a, i) => (
              <QuestionItem
                key={a.question_id}
                idx={i}
                answer={a}
                active={i === activeIdx}
                onClick={() => setActiveIdx(i)}
              />
            ))}
          </aside>

          {/* Active question panel */}
          {activeAnswer && (
            <div style={{ display: "grid", gap: 14, minWidth: 0 }}>
              {/* Header */}
              <div>
                <div
                  style={{
                    font: "500 11px 'DM Mono', monospace",
                    color: HW2_COLOR.faint,
                    marginBottom: 6,
                  }}
                >
                  Question
                </div>
                <h3
                  style={{
                    font: "600 22px 'DM Sans', sans-serif",
                    letterSpacing: "-0.015em",
                    color: HW2_COLOR.ink,
                    lineHeight: 1.3,
                    margin: 0,
                    marginBottom: 10,
                  }}
                >
                  {activeAnswer.question_title}
                </h3>
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 10,
                    flexWrap: "wrap",
                  }}
                >
                  <QStatePill state={activeAnswer.state} />
                </div>

                {activeAnswer.state === "demoted" && (
                  <div
                    style={{
                      marginTop: 12,
                      padding: "12px 16px",
                      borderRadius: 10,
                      background: HW2_COLOR.badSoft,
                      border: `1px solid ${HW2_COLOR.bad}33`,
                      display: "flex",
                      alignItems: "center",
                      gap: 12,
                    }}
                  >
                    <span
                      style={{
                        width: 22,
                        height: 22,
                        borderRadius: "50%",
                        background: HW2_COLOR.bad,
                        color: "#fff",
                        display: "grid",
                        placeItems: "center",
                        font: "700 12px 'DM Sans', sans-serif",
                        flexShrink: 0,
                      }}
                    >
                      !
                    </span>
                    <div
                      style={{
                        flex: 1,
                        font: "400 13px 'DM Sans', sans-serif",
                        color: HW2_COLOR.ink2,
                        lineHeight: 1.5,
                      }}
                    >
                      <strong style={{ color: HW2_COLOR.bad }}>
                        Certification revoked.{" "}
                      </strong>
                      Re-verify needed before this answer can be trusted.
                    </div>
                    <button
                      onClick={() =>
                        router.push(`/h2/projects/${id}/resolve`)
                      }
                      style={{
                        appearance: "none",
                        cursor: "pointer",
                        background: "#fff",
                        border: `1px solid ${HW2_COLOR.rule2}`,
                        borderRadius: 7,
                        padding: "6px 12px",
                        font: "500 12px 'DM Sans', sans-serif",
                        color: HW2_COLOR.ink2,
                        fontFamily: "'DM Sans', sans-serif",
                        flexShrink: 0,
                      }}
                    >
                      Re-verify →
                    </button>
                  </div>
                )}
              </div>

              {activeAnswer.state === "cannot_answer" ? (
                <CannotAnswerCard answer={activeAnswer} />
              ) : (
                <>
                  <SqlCard answer={activeAnswer} />

                  {activeAnswer.caveats.length > 0 && (
                    <CaveatsCard caveats={activeAnswer.caveats} />
                  )}

                  {activeAnswer.confidence > 0 && (
                    <ConfidenceBar value={activeAnswer.confidence} />
                  )}

                  {/* Chart spec info */}
                  {activeAnswer.chart_spec?.type && (
                    <div
                      style={{
                        padding: "12px 16px",
                        background: HW2_COLOR.surface,
                        border: `1px solid ${HW2_COLOR.rule}`,
                        borderRadius: 10,
                        display: "flex",
                        alignItems: "center",
                        gap: 10,
                        font: "400 12.5px 'DM Sans', sans-serif",
                        color: HW2_COLOR.muted,
                      }}
                    >
                      <span
                        style={{
                          font: "600 11px 'DM Mono', monospace",
                          color: HW2_COLOR.faint,
                        }}
                      >
                        chart spec
                      </span>
                      <span>
                        {String(activeAnswer.chart_spec.type)} chart suggested
                      </span>
                    </div>
                  )}
                </>
              )}

              <SharePanel
                projectId={id}
                answer={activeAnswer}
                onExportReport={fetchReport}
              />

              {/* Report preview */}
              {report && (
                <div
                  style={{
                    background: HW2_COLOR.surface,
                    border: `1px solid ${HW2_COLOR.rule}`,
                    borderRadius: 12,
                    overflow: "hidden",
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      padding: "10px 16px",
                      background: HW2_COLOR.paper,
                      borderBottom: `1px solid ${HW2_COLOR.rule}`,
                    }}
                  >
                    <span
                      style={{
                        font: "600 12px 'DM Sans', sans-serif",
                        color: HW2_COLOR.ink,
                      }}
                    >
                      Audit report (Markdown)
                    </span>
                    <button
                      onClick={fetchReport}
                      disabled={loadingReport}
                      style={{
                        appearance: "none",
                        background: "transparent",
                        border: "none",
                        cursor: "pointer",
                        font: "500 11px 'DM Sans', sans-serif",
                        color: HW2_COLOR.blue,
                        fontFamily: "'DM Sans', sans-serif",
                      }}
                    >
                      {loadingReport ? "Downloading…" : "Download"}
                    </button>
                  </div>
                  <pre
                    style={{
                      padding: "14px 16px",
                      font: "400 11.5px 'DM Mono', monospace",
                      color: HW2_COLOR.ink2,
                      lineHeight: 1.6,
                      overflowX: "auto",
                      maxHeight: 360,
                      margin: 0,
                      whiteSpace: "pre-wrap",
                    }}
                  >
                    {report}
                  </pre>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
