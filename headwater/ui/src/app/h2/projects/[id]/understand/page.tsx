"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import {
  h2,
  notifyInputChanged,
  type H2Question,
  type H2RelevantColumn,
  type H2EdaFinding,
} from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";
import { SchemaEditor } from "@/components/h2/schema-editor";

// ─── Primitives ──────────────────────────────────────────────────────────────

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        font: "600 11px 'DM Sans', sans-serif",
        color: HW2_COLOR.muted,
        textTransform: "uppercase",
        letterSpacing: "0.06em",
        marginBottom: 14,
      }}
    >
      {children}
    </div>
  );
}

// ─── Proposed question row ────────────────────────────────────────────────────

function ProposedQ({
  question,
  kept,
  onToggle,
}: {
  question: H2Question;
  kept: boolean;
  onToggle: () => void;
}) {
  const ans = question.answerability;
  const tone =
    ans === "answerable"
      ? {
          bg: HW2_COLOR.goodSoft,
          c: HW2_COLOR.good,
          icon: "✓",
          verdict: "Answerable",
        }
      : ans === "cannot_answer"
      ? {
          bg: HW2_COLOR.warnSoft,
          c: HW2_COLOR.warn,
          icon: "✗",
          verdict: "Can't answer",
        }
      : {
          bg: HW2_COLOR.blueSoft,
          c: HW2_COLOR.blue,
          icon: "⚠",
          verdict: "Answerable, with caveat",
        };

  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-start",
        gap: 12,
        padding: "12px 14px",
        borderRadius: 10,
        background: kept ? HW2_COLOR.surface : HW2_COLOR.paper,
        border: `1px solid ${kept ? HW2_COLOR.rule2 : HW2_COLOR.rule}`,
        opacity: kept ? 1 : 0.65,
        transition: "opacity 120ms",
      }}
    >
      <input
        type="checkbox"
        checked={kept}
        onChange={onToggle}
        style={{ accentColor: HW2_COLOR.blue, marginTop: 4, cursor: "pointer" }}
      />
      <div style={{ flex: 1, minWidth: 0 }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            marginBottom: 6,
            flexWrap: "wrap",
          }}
        >
          <span
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 5,
              padding: "2px 8px",
              borderRadius: 4,
              background: tone.bg,
              color: tone.c,
              font: "700 10px 'DM Sans', sans-serif",
              letterSpacing: "0.07em",
              textTransform: "uppercase",
            }}
          >
            <span>{tone.icon}</span>
            {tone.verdict}
          </span>
          <span
            style={{
              font: "500 11px 'DM Mono', monospace",
              color: HW2_COLOR.faint,
            }}
          >
            {(question.confidence * 100).toFixed(0)}% confidence
          </span>
        </div>
        <div
          style={{
            font: "500 14px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink,
            lineHeight: 1.4,
          }}
        >
          {question.title}
        </div>
        {question.question.reason && (
          <div
            style={{
              font: "400 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              marginTop: 5,
              lineHeight: 1.5,
            }}
          >
            <strong style={{ color: tone.c }}>
              {ans === "cannot_answer" ? "Why not: " : "Why: "}
            </strong>
            {question.question.reason}
          </div>
        )}
        {question.question.needed_columns &&
          question.question.needed_columns.length > 0 && (
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: 4,
                marginTop: 6,
              }}
            >
              {question.question.needed_columns.map((c) => (
                <span
                  key={c}
                  style={{
                    font: "500 11px 'DM Mono', monospace",
                    color: HW2_COLOR.faint,
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
    </div>
  );
}

// ─── Relevant columns ─────────────────────────────────────────────────────────

function RelevantCols({ cols }: { cols: H2RelevantColumn[] }) {
  const [showMore, setShowMore] = useState(false);
  const visible = showMore ? cols : cols.slice(0, 16);

  return (
    <div>
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
          marginBottom: 12,
        }}
      >
        <SectionLabel>Relevant to goal</SectionLabel>
        <span
          style={{
            font: "400 11px 'DM Sans', sans-serif",
            color: HW2_COLOR.faint,
          }}
        >
          {cols.length} columns
        </span>
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
        {visible.map((c) => (
          <span
            key={`${c.table_name}.${c.column_name}`}
            title={c.reason}
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 6,
              padding: "5px 9px",
              borderRadius: 6,
              background: HW2_COLOR.blueSoft,
              color: HW2_COLOR.blue,
              font: "500 12px 'DM Mono', monospace",
            }}
          >
            <span style={{ color: HW2_COLOR.muted }}>{c.table_name}.</span>
            {c.column_name}
          </span>
        ))}
      </div>
      {cols.length > 16 && (
        <button
          onClick={() => setShowMore((v) => !v)}
          style={{
            appearance: "none",
            background: "none",
            border: "none",
            cursor: "pointer",
            padding: 0,
            marginTop: 10,
            color: HW2_COLOR.muted,
            font: "500 12px 'DM Sans', sans-serif",
            display: "inline-flex",
            alignItems: "center",
            gap: 6,
            fontFamily: "'DM Sans', sans-serif",
          }}
        >
          <span
            style={{
              font: "500 11px 'DM Mono', monospace",
              color: HW2_COLOR.faint,
            }}
          >
            {showMore ? "▼" : "▶"}
          </span>
          {showMore ? "Show fewer" : `Show ${cols.length - 16} more`}
        </button>
      )}
    </div>
  );
}

// ─── EDA Finding row ──────────────────────────────────────────────────────────

function EdaFindingRow({ finding }: { finding: H2EdaFinding }) {
  const isCritical = finding.flags.includes("critical");
  return (
    <div
      style={{
        padding: "12px 16px",
        background: isCritical ? HW2_COLOR.badSoft : HW2_COLOR.surface,
        border: `1px solid ${isCritical ? HW2_COLOR.bad + "44" : HW2_COLOR.rule}`,
        borderRadius: 10,
      }}
    >
      <div style={{ display: "flex", alignItems: "flex-start", gap: 10 }}>
        <span
          style={{
            font: "600 10px 'DM Sans', sans-serif",
            color: isCritical ? HW2_COLOR.bad : HW2_COLOR.muted,
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            flexShrink: 0,
            marginTop: 2,
          }}
        >
          {finding.family}
        </span>
        <div style={{ flex: 1 }}>
          <p
            style={{
              font: "500 13px 'DM Sans', sans-serif",
              color: isCritical ? HW2_COLOR.bad : HW2_COLOR.ink,
            }}
          >
            {finding.title}
          </p>
          <div
            style={{
              display: "flex",
              gap: 12,
              marginTop: 4,
              font: "400 11px 'DM Mono', monospace",
              color: HW2_COLOR.faint,
            }}
          >
            <span>
              effect {(finding.effect_size * 100).toFixed(0)}%
            </span>
            <span>
              conf {(finding.confidence * 100).toFixed(0)}%
            </span>
            <span
              style={{
                font: "500 10.5px 'DM Mono', monospace",
                color: HW2_COLOR.muted,
              }}
            >
              {finding.col_ref}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Goal gate (Step 1 must be filled before Step 2) ──────────────────────────

function GoalGate({
  projectId,
  sourceName,
  onSaved,
}: {
  projectId: string;
  sourceName: string;
  onSaved: () => void;
}) {
  const [goal, setGoal] = useState("");
  const [rationale, setRationale] = useState("");
  const [suggesting, setSuggesting] = useState(false);
  const [saving, setSaving] = useState(false);

  const suggest = async () => {
    if (!sourceName) return;
    setSuggesting(true);
    try {
      const r = await h2.sources.suggestGoal(sourceName);
      setGoal(r.goal);
      setRationale(
        r.available
          ? r.rationale
          : "Suggested without a model — start Ollama for a data-aware goal."
      );
    } catch {
      setRationale("Could not reach the suggestion service.");
    } finally {
      setSuggesting(false);
    }
  };

  const save = async () => {
    if (goal.trim().length < 6) return;
    setSaving(true);
    try {
      await h2.projects.setGoal(projectId, goal.trim());
      onSaved();
    } finally {
      setSaving(false);
    }
  };

  return (
    <div style={{ maxWidth: 760, margin: "0 auto", padding: "32px 32px 80px", fontFamily: "'DM Sans', sans-serif" }}>
      <span
        style={{
          font: "600 11px 'DM Sans', sans-serif",
          color: HW2_COLOR.blue,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
        }}
      >
        Step 1 of 5 · Frame
      </span>
      <h2
        style={{
          font: "600 26px 'DM Sans', sans-serif",
          letterSpacing: "-0.02em",
          color: HW2_COLOR.ink,
          lineHeight: 1.25,
          marginTop: 8,
          marginBottom: 6,
        }}
      >
        What goal are we serving?
      </h2>
      <p style={{ font: "400 14px 'DM Sans', sans-serif", color: HW2_COLOR.muted, marginBottom: 20, lineHeight: 1.55 }}>
        This project has no goal yet. Define what you want to learn — or let
        Headwater infer one from the data — before we go further.
      </p>

      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
        <label style={{ font: "600 12px 'DM Sans', sans-serif", color: HW2_COLOR.muted }}>The goal *</label>
        <button
          type="button"
          onClick={suggest}
          disabled={!sourceName || suggesting}
          style={{
            appearance: "none",
            cursor: !sourceName || suggesting ? "default" : "pointer",
            background: HW2_COLOR.blueSoft,
            border: `1px solid ${HW2_COLOR.blue}44`,
            borderRadius: 7,
            padding: "5px 11px",
            font: "600 11.5px 'DM Sans', sans-serif",
            color: HW2_COLOR.blue,
            fontFamily: "'DM Sans', sans-serif",
            opacity: !sourceName || suggesting ? 0.5 : 1,
          }}
        >
          {suggesting ? "Thinking…" : "✦ Suggest from data"}
        </button>
      </div>
      <textarea
        value={goal}
        onChange={(e) => setGoal(e.target.value)}
        placeholder="e.g. Understand where delays occur in the end-to-end process"
        rows={3}
        style={{
          width: "100%",
          resize: "vertical",
          padding: "12px 16px",
          background: "#fff",
          border: `1px solid ${HW2_COLOR.rule2}`,
          borderRadius: 10,
          font: "500 16px 'DM Sans', sans-serif",
          color: HW2_COLOR.ink,
          lineHeight: 1.4,
          fontFamily: "'DM Sans', sans-serif",
          outline: "none",
          boxSizing: "border-box",
        }}
      />
      {rationale && (
        <p style={{ font: "400 12px 'DM Sans', sans-serif", color: HW2_COLOR.muted, marginTop: 8, lineHeight: 1.5 }}>
          <span style={{ color: HW2_COLOR.blue, fontWeight: 600 }}>Why: </span>
          {rationale}
        </p>
      )}
      <div style={{ marginTop: 20, display: "flex", justifyContent: "flex-end" }}>
        <button
          onClick={save}
          disabled={goal.trim().length < 6 || saving}
          style={{
            appearance: "none",
            cursor: goal.trim().length < 6 || saving ? "default" : "pointer",
            background: HW2_COLOR.blue,
            color: "#fff",
            border: "1px solid transparent",
            borderRadius: 10,
            padding: "11px 20px",
            font: "600 14px 'DM Sans', sans-serif",
            fontFamily: "'DM Sans', sans-serif",
            opacity: goal.trim().length < 6 || saving ? 0.5 : 1,
          }}
        >
          {saving ? "Saving…" : "Save goal & continue →"}
        </button>
      </div>
    </div>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function UnderstandPage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();

  const [questions, setQuestions] = useState<H2Question[]>([]);
  const [relevance, setRelevance] = useState<H2RelevantColumn[]>([]);
  const [eda, setEda] = useState<H2EdaFinding[]>([]);
  const [edaScore, setEdaScore] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [kept, setKept] = useState<Set<string>>(new Set());
  const [sourceName, setSourceName] = useState<string>("");
  const [showSchema, setShowSchema] = useState(false);
  const [goalStatement, setGoalStatement] = useState<string>("");
  const [goalLoaded, setGoalLoaded] = useState(false);

  const load = async () => {
    try {
      const project = await h2.projects.get(id);
      setSourceName(project.sources?.[0]?.source_name ?? "");
      setGoalStatement(project.goal?.statement ?? "");
      setGoalLoaded(true);
      const qs = project.questions ?? [];
      setQuestions(qs);
      // Initially keep all answerable questions
      setKept(
        new Set(
          qs
            .filter((q) => q.answerability !== "cannot_answer")
            .map((q) => q.id)
        )
      );
    } catch {
      // ignore
    } finally {
      setLoading(false);
    }
  };

  const rerunRelevance = async () => {
    setRefreshing(true);
    try {
      const rel = await h2.projects.rerunRelevance(id);
      setRelevance(rel.relevant_columns);
    } catch {
      // ignore
    } finally {
      setRefreshing(false);
    }
  };

  const runEda = async () => {
    setRunning(true);
    try {
      const result = await h2.projects.eda.run(id);
      setEda(result.top_findings);
      setEdaScore(result.insight_confidence_score);
    } catch {
      // ignore
    } finally {
      setRunning(false);
    }
  };

  const toggleKept = (qid: string) => {
    setKept((prev) => {
      const next = new Set(prev);
      next.has(qid) ? next.delete(qid) : next.add(qid);
      return next;
    });
  };

  const acceptAll = () => {
    setKept(
      new Set(
        questions
          .filter((q) => q.answerability !== "cannot_answer")
          .map((q) => q.id)
      )
    );
  };

  useEffect(() => {
    load();
    rerunRelevance();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

  const keptCount = [...kept].length;

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

  // Gate: Step 2 is only reachable once Step 1's goal is defined.
  if (goalLoaded && !goalStatement.trim()) {
    return (
      <GoalGate
        projectId={id}
        sourceName={sourceName}
        onSaved={() => {
          notifyInputChanged();
          setLoading(true);
          load().finally(() => setLoading(false));
        }}
      />
    );
  }

  return (
    <div
      style={{
        maxWidth: 980,
        margin: "0 auto",
        padding: "32px 32px 80px",
        fontFamily: "'DM Sans', sans-serif",
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
        Step 2 of 5 · Understand
      </span>
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
        Here&rsquo;s what this data is.
      </h2>
      <p
        style={{
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          marginBottom: 28,
        }}
      >
        Review the relevant columns and curate the proposed questions below.
        Open <strong style={{ color: HW2_COLOR.ink2 }}>Schema &amp; meaning</strong>{" "}
        to correct what columns mean — every edit feeds a refresh.
      </p>

      {/* Schema & meaning */}
      {sourceName && (
        <div style={{ marginBottom: 16 }}>
          <button
            onClick={() => setShowSchema((v) => !v)}
            style={{
              appearance: "none",
              cursor: "pointer",
              width: "100%",
              textAlign: "left",
              background: HW2_COLOR.surface,
              border: `1px solid ${HW2_COLOR.rule}`,
              borderRadius: 12,
              padding: "14px 20px",
              font: "600 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink,
              fontFamily: "'DM Sans', sans-serif",
              display: "flex",
              alignItems: "center",
              gap: 10,
            }}
          >
            <span style={{ color: HW2_COLOR.faint, font: "500 11px 'DM Mono', monospace" }}>
              {showSchema ? "▾" : "▸"}
            </span>
            Schema &amp; meaning
            <span
              style={{
                font: "400 12px 'DM Sans', sans-serif",
                color: HW2_COLOR.muted,
                fontWeight: 400,
              }}
            >
              — tables, editable column meanings, inferred relationships
            </span>
          </button>
          {showSchema && (
            <div style={{ marginTop: 12 }}>
              <SchemaEditor sourceName={sourceName} projectId={id} />
            </div>
          )}
        </div>
      )}

      {/* Relevant columns card */}
      {relevance.length > 0 && (
        <div
          style={{
            background: HW2_COLOR.surface,
            border: `1px solid ${HW2_COLOR.rule}`,
            borderRadius: 12,
            padding: "20px 24px",
            marginBottom: 16,
          }}
        >
          <RelevantCols cols={relevance} />
          <button
            onClick={rerunRelevance}
            disabled={refreshing}
            style={{
              appearance: "none",
              background: "transparent",
              border: "none",
              cursor: "pointer",
              marginTop: 12,
              padding: 0,
              color: HW2_COLOR.muted,
              font: "500 12px 'DM Sans', sans-serif",
              opacity: refreshing ? 0.5 : 1,
              fontFamily: "'DM Sans', sans-serif",
            }}
          >
            {refreshing ? "Re-running…" : "↺ Re-run relevance"}
          </button>
        </div>
      )}

      {/* EDA card */}
      <div
        style={{
          background: HW2_COLOR.surface,
          border: `1px solid ${HW2_COLOR.rule}`,
          borderRadius: 12,
          padding: "20px 24px",
          marginBottom: 20,
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: eda.length > 0 ? 16 : 0,
          }}
        >
          <SectionLabel>
            Data quality findings
            {edaScore !== null && (
              <span
                style={{
                  marginLeft: 10,
                  font: "500 10px 'DM Mono', monospace",
                  color: HW2_COLOR.faint,
                  textTransform: "none",
                  letterSpacing: 0,
                }}
              >
                insight confidence {(edaScore * 100).toFixed(0)}%
              </span>
            )}
          </SectionLabel>
          <button
            onClick={runEda}
            disabled={running}
            style={{
              appearance: "none",
              cursor: running ? "default" : "pointer",
              background: HW2_COLOR.chip,
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 8,
              padding: "6px 12px",
              font: "500 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink2,
              fontFamily: "'DM Sans', sans-serif",
              opacity: running ? 0.6 : 1,
            }}
          >
            {running ? "Running…" : "Run EDA"}
          </button>
        </div>
        {eda.length > 0 ? (
          <div style={{ display: "grid", gap: 8 }}>
            {eda.map((f, i) => (
              <EdaFindingRow key={i} finding={f} />
            ))}
          </div>
        ) : (
          <p
            style={{
              font: "400 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.faint,
            }}
          >
            Click &ldquo;Run EDA&rdquo; to analyse data quality patterns.
          </p>
        )}
      </div>

      {/* Proposed questions card */}
      <div
        style={{
          background: HW2_COLOR.surface,
          border: `1px solid ${HW2_COLOR.rule}`,
          borderRadius: 12,
          overflow: "hidden",
          marginBottom: 24,
        }}
      >
        <div
          style={{
            padding: "16px 22px",
            borderBottom: `1px solid ${HW2_COLOR.rule}`,
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 12,
          }}
        >
          <div>
            <div
              style={{
                font: "600 11px 'DM Sans', sans-serif",
                color: HW2_COLOR.blue,
                letterSpacing: "0.07em",
                textTransform: "uppercase",
              }}
            >
              Proposed questions
            </div>
            <div
              style={{
                font: "500 16px 'DM Sans', sans-serif",
                color: HW2_COLOR.ink,
                marginTop: 4,
              }}
            >
              {keptCount} of {questions.length} kept — curate or add your
              own.
            </div>
          </div>
          <button
            onClick={acceptAll}
            style={{
              appearance: "none",
              cursor: "pointer",
              background: "#fff",
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 8,
              padding: "6px 12px",
              font: "500 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink2,
              fontFamily: "'DM Sans', sans-serif",
            }}
          >
            Accept all answerable
          </button>
        </div>

        <div style={{ padding: "12px 22px 18px", display: "grid", gap: 8 }}>
          {questions.length === 0 ? (
            <p
              style={{
                font: "400 13px 'DM Sans', sans-serif",
                color: HW2_COLOR.faint,
                padding: "8px 0",
              }}
            >
              No questions proposed yet. The Frame stage generates these
              automatically.
            </p>
          ) : (
            questions.map((q) => (
              <ProposedQ
                key={q.id}
                question={q}
                kept={kept.has(q.id)}
                onToggle={() => toggleKept(q.id)}
              />
            ))
          )}
        </div>
      </div>

      {/* Continue */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "16px 20px",
          background: HW2_COLOR.surface,
          border: `1px solid ${HW2_COLOR.rule2}`,
          borderRadius: 12,
          gap: 14,
        }}
      >
        <div>
          <div
            style={{
              font: "600 14px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink,
            }}
          >
            Looks right?
          </div>
          <div
            style={{
              font: "400 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              marginTop: 2,
            }}
          >
            Specifics are resolved one at a time in the next step.
          </div>
        </div>
        <button
          onClick={() => router.push(`/h2/projects/${id}/resolve`)}
          style={{
            appearance: "none",
            cursor: "pointer",
            background: HW2_COLOR.blue,
            color: "#fff",
            border: "1px solid transparent",
            borderRadius: 8,
            padding: "10px 18px",
            font: "600 14px 'DM Sans', sans-serif",
            fontFamily: "'DM Sans', sans-serif",
          }}
        >
          Yes, continue →
        </button>
      </div>
    </div>
  );
}
