"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import {
  h2,
  onHw2Event,
  HW2_RECOMPUTED,
  type H2ReadinessReport,
  type H2QuestionReadiness,
  type H2Contract,
} from "@/lib/h2api";
import { ReadinessRing, HW2_COLOR } from "@/components/h2/readiness-ring";
import { DataModelDiagram } from "@/components/h2/data-model";

// ─── State pill ───────────────────────────────────────────────────────────────

function QStatePill({
  state,
  size = "md",
}: {
  state: string;
  size?: "sm" | "md";
}) {
  const cfgs: Record<string, { color: string; bg: string; icon: string; label: string }> = {
    certified:    { color: HW2_COLOR.good,  bg: HW2_COLOR.goodSoft, icon: "✓", label: "Certified" },
    draft:        { color: HW2_COLOR.muted, bg: HW2_COLOR.chip,     icon: "○", label: "Draft" },
    demoted:      { color: HW2_COLOR.bad,   bg: HW2_COLOR.badSoft,  icon: "!", label: "Re-verify" },
    cannot_answer:{ color: HW2_COLOR.warn,  bg: HW2_COLOR.warnSoft, icon: "✗", label: "Can't answer" },
    pending:      { color: HW2_COLOR.faint, bg: HW2_COLOR.chip,     icon: "·", label: "Pending" },
  };
  const cfg = cfgs[state] ?? cfgs.pending;
  const p = size === "sm" ? "2px 8px" : "3px 10px";
  const fs = size === "sm" ? "9px" : "10px";

  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 5,
        padding: p,
        borderRadius: 4,
        background: cfg.bg,
        color: cfg.color,
        font: `700 ${fs} 'DM Sans', sans-serif`,
        letterSpacing: "0.07em",
        textTransform: "uppercase",
        whiteSpace: "nowrap",
      }}
    >
      <span style={{ font: `700 ${fs} 'DM Sans', sans-serif` }}>
        {cfg.icon}
      </span>
      {cfg.label}
    </span>
  );
}

// ─── Contract item ────────────────────────────────────────────────────────────

function ContractRow({ contract }: { contract: H2Contract }) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-start",
        gap: 12,
        padding: "10px 14px",
        borderRadius: 8,
        background: contract.passed ? HW2_COLOR.surface : HW2_COLOR.paper,
        border: `1px solid ${contract.passed ? HW2_COLOR.rule : HW2_COLOR.rule2}`,
      }}
    >
      <span
        style={{
          width: 18,
          height: 18,
          borderRadius: "50%",
          flexShrink: 0,
          background: contract.passed ? HW2_COLOR.goodSoft : HW2_COLOR.badSoft,
          color: contract.passed ? HW2_COLOR.good : HW2_COLOR.bad,
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          font: "700 11px 'DM Sans', sans-serif",
          marginTop: 1,
        }}
      >
        {contract.passed ? "✓" : "×"}
      </span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div
          style={{
            font: "500 13px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink2,
            lineHeight: 1.4,
          }}
        >
          {contract.contract_type}
        </div>
        <div
          style={{
            font: "400 11.5px 'DM Sans', sans-serif",
            color: contract.passed ? HW2_COLOR.muted : HW2_COLOR.bad,
            marginTop: 2,
            lineHeight: 1.4,
          }}
        >
          {contract.note}
        </div>
      </div>
    </div>
  );
}

// ─── Per-question verdict ─────────────────────────────────────────────────────

// Maps a failing evidence contract to a concrete, plain-English resolution path.
const RESOLUTION: Record<string, string> = {
  columns_present: "Add the missing column(s) to the source data.",
  meanings_locked: "Confirm and lock what the needed columns mean (open Schema & meaning).",
  nulls_acceptable: "Supply cleaner data — the needed columns have too many nulls.",
  coverage_sufficient: "Add more data — more rows or a longer time span.",
  consistent_grain: "Fix the duplicate-grain / join fan-out on the path.",
};

function resolutionPaths(contracts: H2Contract[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const c of contracts) {
    if (c.passed) continue;
    const g = RESOLUTION[c.contract_type] ?? c.note;
    if (g && !seen.has(g)) {
      seen.add(g);
      out.push(g);
    }
  }
  return out;
}

function QuestionVerdictRow({ question }: { question: H2QuestionReadiness }) {
  const [open, setOpen] = useState(false);
  const passing = question.contracts.filter((c) => c.passed).length;

  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${
          question.state === "demoted" ? HW2_COLOR.bad : HW2_COLOR.rule
        }`,
        borderRadius: 12,
        overflow: "hidden",
      }}
    >
      <button
        onClick={() => setOpen((v) => !v)}
        style={{
          appearance: "none",
          background: "transparent",
          border: "none",
          cursor: "pointer",
          width: "100%",
          padding: "16px 20px",
          textAlign: "left",
          display: "flex",
          alignItems: "center",
          gap: 16,
          fontFamily: "'DM Sans', sans-serif",
        }}
      >
        <ReadinessRing
          value={question.readiness_pct}
          certified={question.state === "certified"}
          demoted={question.state === "demoted"}
          size={40}
          stroke={3}
          showLabel={false}
          animate={false}
        />
        <div style={{ flex: 1, minWidth: 0 }}>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 10,
              marginBottom: 4,
            }}
          >
            <QStatePill state={question.state} size="sm" />
            <span
              style={{
                font: "500 11px 'DM Mono', monospace",
                color: HW2_COLOR.faint,
              }}
            >
              {passing}/{question.contracts.length} contracts pass
            </span>
          </div>
          <div
            style={{
              font: "600 15px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink,
              lineHeight: 1.35,
              letterSpacing: "-0.005em",
            }}
          >
            {question.title || question.summary || question.question_id.split(":").pop()}
          </div>
          {question.state === "draft" &&
            resolutionPaths(question.contracts).length > 0 && (
              <div
                style={{
                  font: "400 12px 'DM Sans', sans-serif",
                  color: HW2_COLOR.muted,
                  marginTop: 4,
                  lineHeight: 1.5,
                }}
              >
                <strong style={{ color: HW2_COLOR.ink2 }}>To certify: </strong>
                {resolutionPaths(question.contracts)[0]}
              </div>
            )}
          {question.state === "demoted" && (
            <div
              style={{
                font: "400 12px 'DM Sans', sans-serif",
                color: HW2_COLOR.bad,
                marginTop: 4,
              }}
            >
              Certification revoked — re-verify needed.
            </div>
          )}
        </div>
        <span
          style={{
            color: HW2_COLOR.faint,
            font: "500 11px 'DM Mono', monospace",
            flexShrink: 0,
          }}
        >
          {open ? "▾" : "▸"}
        </span>
      </button>

      {open && question.contracts.length > 0 && (
        <div
          style={{
            padding: "0 20px 18px 76px",
            display: "grid",
            gap: 8,
          }}
        >
          {question.contracts.map((c, i) => (
            <ContractRow key={i} contract={c} />
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Cannot-answer hero ───────────────────────────────────────────────────────

function CannotAnswerBlock({
  questions,
}: {
  questions: H2QuestionReadiness[];
}) {
  if (questions.length === 0) return null;

  return (
    <div
      style={{
        padding: "20px 22px",
        marginBottom: 24,
        background: "#fff",
        border: `1.5px solid ${HW2_COLOR.warn}`,
        borderRadius: 12,
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          marginBottom: 10,
        }}
      >
        <span
          style={{
            padding: "3px 9px",
            borderRadius: 4,
            background: HW2_COLOR.warnSoft,
            color: HW2_COLOR.warn,
            font: "700 10px 'DM Sans', sans-serif",
            letterSpacing: "0.08em",
            textTransform: "uppercase",
            display: "inline-flex",
            alignItems: "center",
            gap: 5,
          }}
        >
          <span>✗</span> Can&rsquo;t answer with this data
        </span>
        <span
          style={{
            font: "400 12px 'DM Sans', sans-serif",
            color: HW2_COLOR.muted,
          }}
        >
          {questions.length} question{questions.length !== 1 ? "s" : ""}
        </span>
      </div>
      {questions.map((q) => {
        const reasons = q.contracts
          .filter((c) => !c.passed)
          .map((c) => c.note)
          .filter(Boolean);
        const paths = resolutionPaths(q.contracts);
        return (
          <div
            key={q.question_id}
            style={{
              padding: "14px 0",
              borderTop: `1px solid ${HW2_COLOR.rule}`,
            }}
          >
            <div
              style={{
                font: "600 15px 'DM Sans', sans-serif",
                color: HW2_COLOR.ink,
                marginBottom: 6,
                letterSpacing: "-0.01em",
              }}
            >
              {q.title || q.question_id.split(":").pop()}
            </div>
            <div
              style={{
                font: "400 13px 'DM Sans', sans-serif",
                color: HW2_COLOR.ink2,
                lineHeight: 1.55,
              }}
            >
              <strong style={{ color: HW2_COLOR.warn }}>Why not: </strong>
              {q.summary}
            </div>
            {reasons.length > 0 && (
              <ul
                style={{
                  margin: "8px 0 0",
                  paddingLeft: 18,
                  font: "400 12.5px 'DM Sans', sans-serif",
                  color: HW2_COLOR.ink2,
                  lineHeight: 1.55,
                }}
              >
                {reasons.slice(0, 4).map((r, i) => (
                  <li key={i}>{r}</li>
                ))}
              </ul>
            )}
            {paths.length > 0 && (
              <div
                style={{
                  marginTop: 8,
                  font: "400 12.5px 'DM Sans', sans-serif",
                  color: HW2_COLOR.ink2,
                  lineHeight: 1.55,
                }}
              >
                <strong style={{ color: HW2_COLOR.good }}>To unblock: </strong>
                {paths.join(" · ")}
              </div>
            )}
            {q.needed_columns.length > 0 && (
              <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginTop: 8 }}>
                {q.needed_columns.map((c) => (
                  <span
                    key={c}
                    style={{
                      font: "500 11px 'DM Mono', monospace",
                      color: HW2_COLOR.muted,
                      padding: "1px 6px",
                      background: HW2_COLOR.chip,
                      borderRadius: 4,
                    }}
                  >
                    {c}
                  </span>
                ))}
              </div>
            )}
          </div>
        );
      })}
      <div
        style={{
          marginTop: 14,
          padding: "10px 14px",
          background: HW2_COLOR.warnSoft,
          borderRadius: 8,
          font: "400 12px 'DM Sans', sans-serif",
          color: HW2_COLOR.ink2,
          lineHeight: 1.5,
        }}
      >
        <strong style={{ color: HW2_COLOR.warn }}>
          This is the moment Headwater earns trust.
        </strong>{" "}
        A confident &ldquo;we can&rsquo;t answer that&rdquo; is more valuable
        than a confidently-wrong number.
      </div>
    </div>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function ReadinessPage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();

  const [report, setReport] = useState<H2ReadinessReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [evaluating, setEvaluating] = useState(false);
  const [certifying, setCertifying] = useState(false);
  const [demotions, setDemotions] = useState<string[]>([]);

  const evaluate = async () => {
    setEvaluating(true);
    try {
      const r = await h2.projects.readiness.evaluate(id);
      setReport(r);
      setDemotions([]);
    } finally {
      setEvaluating(false);
    }
  };

  const certify = async () => {
    setCertifying(true);
    try {
      const result = await h2.projects.certify.check(id);
      setDemotions(
        result.demotions.map(
          (d: { question_title: string }) => d.question_title
        )
      );
      await evaluate();
    } finally {
      setCertifying(false);
    }
  };

  useEffect(() => {
    h2.projects.readiness
      .evaluate(id)
      .then(setReport)
      .finally(() => setLoading(false));
  }, [id]);

  // Re-evaluate the verdict when a recompute lands.
  useEffect(() => onHw2Event(HW2_RECOMPUTED, evaluate), [id]);

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

  const askable =
    report?.questions.filter((q) => q.state !== "cannot_answer") ?? [];
  const cantAnswer =
    report?.questions.filter((q) => q.state === "cannot_answer") ?? [];

  return (
    <div
      style={{
        maxWidth: 900,
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
          Step 4 of 5 · Readiness
        </span>
        <div style={{ display: "flex", gap: 8 }}>
          <button
            onClick={evaluate}
            disabled={evaluating}
            style={{
              appearance: "none",
              cursor: evaluating ? "default" : "pointer",
              background: "#fff",
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 8,
              padding: "6px 12px",
              font: "500 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink2,
              fontFamily: "'DM Sans', sans-serif",
              opacity: evaluating ? 0.5 : 1,
            }}
          >
            {evaluating ? "Evaluating…" : "Re-evaluate"}
          </button>
          <button
            onClick={certify}
            disabled={certifying}
            style={{
              appearance: "none",
              cursor: certifying ? "default" : "pointer",
              background: HW2_COLOR.blue,
              color: "#fff",
              border: "1px solid transparent",
              borderRadius: 8,
              padding: "6px 12px",
              font: "500 12px 'DM Sans', sans-serif",
              fontFamily: "'DM Sans', sans-serif",
              opacity: certifying ? 0.5 : 1,
            }}
          >
            {certifying ? "Checking…" : "Certify check"}
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
        The verdict — per question.
      </h2>
      <p
        style={{
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          marginBottom: 24,
          lineHeight: 1.5,
        }}
      >
        Headwater certifies <em>answers</em>, not projects. The same project
        can hold a certified answer next to a Draft one — and a previously
        certified answer can be revoked when the data drifts.
      </p>

      {/* Data model */}
      {report?.source_name && (
        <div
          style={{
            background: HW2_COLOR.surface,
            border: `1px solid ${HW2_COLOR.rule}`,
            borderRadius: 12,
            padding: "16px 20px",
            marginBottom: 20,
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
            How the data connects
          </div>
          <p
            style={{
              font: "400 12.5px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              marginBottom: 12,
              lineHeight: 1.5,
            }}
          >
            Tables in scope, their join keys, and the relationships Headwater
            inferred. Solid edges are high-confidence; dashed need a look.
          </p>
          <DataModelDiagram sourceName={report.source_name} />
        </div>
      )}

      {/* Demotion alert */}
      {demotions.length > 0 && (
        <div
          style={{
            padding: "14px 18px",
            background: HW2_COLOR.badSoft,
            border: `1px solid ${HW2_COLOR.bad}44`,
            borderRadius: 10,
            marginBottom: 20,
          }}
        >
          <p
            style={{
              font: "600 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.bad,
              marginBottom: 6,
            }}
          >
            {demotions.length} question
            {demotions.length !== 1 ? "s" : ""} demoted
          </p>
          <ul style={{ paddingLeft: 18 }}>
            {demotions.map((d) => (
              <li
                key={d}
                style={{
                  font: "400 12px 'DM Sans', sans-serif",
                  color: HW2_COLOR.bad,
                }}
              >
                {d}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Summary counts */}
      {report && (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(3, 1fr)",
            gap: 12,
            marginBottom: 24,
          }}
        >
          {[
            {
              label: "Certified",
              count: report.certified_count,
              color: HW2_COLOR.good,
            },
            {
              label: "Draft",
              count: report.draft_count,
              color: HW2_COLOR.muted,
            },
            {
              label: "Can't answer",
              count: report.cannot_answer_count,
              color: HW2_COLOR.warn,
            },
          ].map((item) => (
            <div
              key={item.label}
              style={{
                background: HW2_COLOR.surface,
                border: `1px solid ${HW2_COLOR.rule}`,
                borderRadius: 10,
                padding: "14px 16px",
                textAlign: "center",
              }}
            >
              <div
                style={{
                  font: "700 26px 'DM Sans', sans-serif",
                  color: item.color,
                  letterSpacing: "-0.03em",
                }}
              >
                {item.count}
              </div>
              <div
                style={{
                  font: "500 11px 'DM Sans', sans-serif",
                  color: HW2_COLOR.muted,
                  marginTop: 3,
                }}
              >
                {item.label}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Askable question verdicts */}
      <div style={{ display: "grid", gap: 10, marginBottom: 24 }}>
        {askable.map((q) => (
          <QuestionVerdictRow key={q.question_id} question={q} />
        ))}
      </div>

      {/* Cannot-answer hero */}
      <CannotAnswerBlock questions={cantAnswer} />

      {/* The sacred badge rule */}
      <div
        style={{
          padding: "14px 18px",
          marginBottom: 24,
          background: HW2_COLOR.chip,
          borderRadius: 10,
          border: `1px solid ${HW2_COLOR.rule2}`,
          font: "400 12px 'DM Sans', sans-serif",
          color: HW2_COLOR.ink2,
          lineHeight: 1.55,
        }}
      >
        <strong style={{ color: HW2_COLOR.ink }}>The rule:</strong>{" "}
        Certification is recomputed from facts — locked columns + lineage · no
        blocking gap · structural integrity · no misleading items · consistent
        definition · confident insight. Not from clicks.{" "}
        <strong style={{ color: HW2_COLOR.ink }}>The badge is sacred.</strong>
      </div>

      {/* Actions */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 8,
        }}
      >
        <button
          onClick={() => router.push(`/h2/projects/${id}/resolve`)}
          style={{
            appearance: "none",
            cursor: "pointer",
            background: "#fff",
            border: `1px solid ${HW2_COLOR.rule2}`,
            borderRadius: 8,
            padding: "10px 16px",
            font: "500 13px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink2,
            fontFamily: "'DM Sans', sans-serif",
          }}
        >
          ← Improve readiness
        </button>
        <button
          onClick={() => router.push(`/h2/projects/${id}/answer`)}
          style={{
            appearance: "none",
            cursor: "pointer",
            background: HW2_COLOR.blue,
            color: "#fff",
            border: "1px solid transparent",
            borderRadius: 10,
            padding: "11px 22px",
            font: "600 14px 'DM Sans', sans-serif",
            fontFamily: "'DM Sans', sans-serif",
          }}
        >
          {report &&
          report.certified_count === report.questions.length &&
          report.questions.length > 0
            ? "View certified answers"
            : "Go to Answer →"}
        </button>
      </div>
    </div>
  );
}
