"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {
  h2,
  answerCache,
  onHw2Event,
  HW2_RECOMPUTED,
  type H2AnswerDraft,
  type H2AnswerRow,
  type H2AnswersResult,
} from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

// Session cache of the last answer payload per project, so navigating back to
// Answer doesn't redundantly re-run the pipeline (materialize + execute). A real
// input change fires HW2_RECOMPUTED, which refreshes + re-caches; Redraft forces
// a fresh run; certification re-caches its result. Lives in h2api (shared) so a
// mutation elsewhere (promoting a console query) can invalidate it.
const ANSWER_CACHE = answerCache;

// ─── State pill ───────────────────────────────────────────────────────────────

function QStatePill({ state }: { state: string }) {
  const cfgs: Record<string, { color: string; bg: string; icon: string; label: string }> = {
    certified:    { color: HW2_COLOR.good,  bg: HW2_COLOR.goodSoft, icon: "✓", label: "Certified" },
    doubtful:     { color: HW2_COLOR.warn,  bg: HW2_COLOR.warnSoft, icon: "⚠", label: "Doubtful" },
    pending:      { color: HW2_COLOR.muted, bg: HW2_COLOR.chip,     icon: "○", label: "Not certified" },
    cannot_answer:{ color: HW2_COLOR.warn,  bg: HW2_COLOR.warnSoft, icon: "✗", label: "Can't answer" },
  };
  const cfg = cfgs[state] ?? cfgs.pending;

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
          fontWeight: active ? 600 : 500,
          fontSize: 13,
          fontFamily: "'DM Sans', sans-serif",
          color: active ? HW2_COLOR.ink : HW2_COLOR.ink2,
          lineHeight: 1.35,
        }}
      >
        {answer.question_title}
      </div>
      <div
        title={confidenceBreakdownText(answer.confidence_breakdown)}
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

/** Human labels for confidence components. */
const CONF_LABEL: Record<string, string> = {
  readiness: "readiness",
  completeness: "completeness",
  verification: "judge",
};

/** Render one "label value" pair. The judge factor reads "unverified" at 0 so
 *  it isn't mistaken for "the judge scored 0" on a not-yet-judged answer. */
function confidenceParts(breakdown: Record<string, number>): string[] {
  return Object.entries(breakdown).map(([k, v]) => {
    if (k === "verification" && v === 0) return "judge unverified";
    return `${CONF_LABEL[k] ?? k} ${Math.round(v * 100)}%`;
  });
}

/** A readable derivation of the confidence number for the rail tooltip. */
function confidenceBreakdownText(
  breakdown: Record<string, number> | undefined
): string {
  if (!breakdown || Object.keys(breakdown).length === 0) {
    return "No evidence yet — run the query and certification.";
  }
  return "Calculated from " + confidenceParts(breakdown).join(" · ");
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

function SqlCard({
  answer,
  sourceName,
}: {
  answer: H2AnswerDraft;
  sourceName: string;
}) {
  const [editing, setEditing] = useState(false);
  const [sql, setSql] = useState(answer.sql_text ?? "");
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<{
    columns: string[];
    rows: H2AnswerRow[];
    row_count: number;
    truncated: boolean;
    error: string | null;
  } | null>(null);

  const run = async () => {
    if (!sql.trim() || !sourceName) return;
    setRunning(true);
    try {
      const r = await h2.query(sourceName, sql);
      setResult(r);
    } catch (e) {
      setResult({
        columns: [],
        rows: [],
        row_count: 0,
        truncated: false,
        error: e instanceof Error ? e.message : "Query failed.",
      });
    } finally {
      setRunning(false);
    }
  };

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
            onClick={run}
            disabled={running || !sql.trim() || !sourceName}
            title={!sourceName ? "Source not loaded yet" : "Execute this SQL read-only"}
            style={{
              appearance: "none",
              cursor: running || !sql.trim() || !sourceName ? "default" : "pointer",
              background: HW2_COLOR.blue,
              color: "#fff",
              border: "1px solid transparent",
              borderRadius: 6,
              padding: "4px 10px",
              font: "500 12px 'DM Sans', sans-serif",
              fontFamily: "'DM Sans', sans-serif",
              opacity: running || !sql.trim() || !sourceName ? 0.5 : 1,
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

      {result && (
        <div style={{ borderTop: `1px solid ${HW2_COLOR.rule}`, position: "relative", zIndex: 1 }}>
          {result.error ? (
            <div
              style={{
                padding: "12px 16px",
                font: "400 12.5px 'DM Mono', monospace",
                color: HW2_COLOR.bad,
                lineHeight: 1.5,
                background: HW2_COLOR.badSoft,
              }}
            >
              {result.error}
            </div>
          ) : result.columns.length === 0 ? (
            <div
              style={{
                padding: "12px 16px",
                font: "400 12.5px 'DM Sans', sans-serif",
                color: HW2_COLOR.muted,
              }}
            >
              No rows returned.
            </div>
          ) : (
            <>
              <div
                style={{
                  padding: "8px 16px",
                  font: "400 11px 'DM Mono', monospace",
                  color: HW2_COLOR.faint,
                  background: HW2_COLOR.surface,
                }}
              >
                {result.row_count.toLocaleString()} row{result.row_count === 1 ? "" : "s"}
                {result.truncated ? " · first 50 shown" : ""}
              </div>
              <div style={{ overflowX: "auto", maxHeight: 300, background: HW2_COLOR.surface }}>
                <table style={{ borderCollapse: "collapse", width: "100%" }}>
                  <thead>
                    <tr>
                      {result.columns.map((c) => (
                        <th
                          key={c}
                          style={{
                            position: "sticky",
                            top: 0,
                            textAlign: "left",
                            padding: "6px 14px",
                            background: HW2_COLOR.paper,
                            borderBottom: `1px solid ${HW2_COLOR.rule}`,
                            font: "600 11px 'DM Mono', monospace",
                            color: HW2_COLOR.ink2,
                            whiteSpace: "nowrap",
                          }}
                        >
                          {c}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.rows.slice(0, 50).map((row, ri) => (
                      <tr key={ri}>
                        {result.columns.map((c) => (
                          <td
                            key={c}
                            style={{
                              padding: "5px 14px",
                              borderBottom: `1px solid ${HW2_COLOR.rule}`,
                              font: "400 12px 'DM Mono', monospace",
                              color: HW2_COLOR.ink2,
                              whiteSpace: "nowrap",
                            }}
                          >
                            {row[c] === null || row[c] === undefined ? "—" : String(row[c])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
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
              color: HW2_COLOR.muted,
              opacity: 0.06,
              letterSpacing: "0.15em",
              transform: "rotate(-12deg)",
              textTransform: "uppercase",
            }}
          >
            {answer.state === "pending" ? "Not certified" : "Doubtful"}
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

// ─── Executed result: chart ───────────────────────────────────────────────────

function CardLabel({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        font: "600 11px 'DM Sans', sans-serif",
        color: HW2_COLOR.muted,
        textTransform: "uppercase",
        letterSpacing: "0.06em",
        marginBottom: 12,
      }}
    >
      {children}
    </div>
  );
}

// Honest stand-in when a chart was intended but the measure has no numeric
// values to plot (e.g. a text column that couldn't be aggregated to a number).
// We never render blank axes as if they were a result.
function UnplottablePanel({ measure, reason }: { measure: string; reason?: string }) {
  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${HW2_COLOR.warn}44`,
        borderRadius: 12,
        padding: "18px 20px",
      }}
    >
      <CardLabel>Visualization</CardLabel>
      <p
        style={{
          font: "500 13.5px 'DM Sans', sans-serif",
          color: HW2_COLOR.ink2,
          lineHeight: 1.55,
          margin: 0,
        }}
      >
        Nothing to plot — <code style={{ fontFamily: "'DM Mono', monospace" }}>{measure}</code>{" "}
        produced no numeric values, so this answer can&rsquo;t be charted or trusted yet.
      </p>
      {reason && (
        <p
          style={{
            font: "400 12.5px 'DM Sans', sans-serif",
            color: HW2_COLOR.muted,
            lineHeight: 1.55,
            marginTop: 8,
            marginBottom: 0,
          }}
        >
          {reason}
        </p>
      )}
    </div>
  );
}

// Trust tone for a stated finding, by answer state.
function findingTone(state: string): { c: string; bg: string; note: string } {
  if (state === "certified")
    return { c: HW2_COLOR.good, bg: HW2_COLOR.goodSoft, note: "Certified" };
  if (state === "doubtful")
    return { c: HW2_COLOR.warn, bg: HW2_COLOR.warnSoft, note: "Provisional — verify before sharing" };
  return { c: HW2_COLOR.muted, bg: HW2_COLOR.chip, note: "Not certified yet" };
}

// The lead of the answer panel: the takeaway in plain English, trust-toned.
function FindingCard({ answer }: { answer: H2AnswerDraft }) {
  if (!answer.finding_headline) return null;
  const tone = findingTone(answer.state);
  return (
    <div
      style={{
        background: "#fff",
        border: `1px solid ${HW2_COLOR.rule}`,
        borderLeft: `3px solid ${tone.c}`,
        borderRadius: 12,
        padding: "18px 20px",
      }}
    >
      <div
        style={{
          font: "700 9.5px 'DM Sans', sans-serif",
          letterSpacing: "0.09em",
          textTransform: "uppercase",
          color: tone.c,
          marginBottom: 8,
        }}
      >
        Finding · {tone.note}
      </div>
      <p
        style={{
          font: "600 19px 'DM Sans', sans-serif",
          letterSpacing: "-0.01em",
          color: HW2_COLOR.ink,
          lineHeight: 1.4,
          margin: 0,
        }}
      >
        {answer.finding_headline}
      </p>
      {answer.finding_support && (
        <p
          style={{
            font: "400 13.5px 'DM Sans', sans-serif",
            color: HW2_COLOR.muted,
            lineHeight: 1.5,
            margin: "8px 0 0",
          }}
        >
          {answer.finding_support}
        </p>
      )}
    </div>
  );
}

// Project-level overview: what the data shows across all answered questions.
function FindingsSummary({
  answers,
  onPick,
}: {
  answers: H2AnswerDraft[];
  onPick: (i: number) => void;
}) {
  const items = answers
    .map((a, i) => ({ a, i }))
    .filter(({ a }) => a.finding_headline);
  if (items.length === 0) return null;
  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 12,
        padding: "16px 20px",
        marginBottom: 24,
      }}
    >
      <CardLabel>What the data shows</CardLabel>
      <div style={{ display: "grid", gap: 8 }}>
        {items.map(({ a, i }) => {
          const tone = findingTone(a.state);
          return (
            <button
              key={a.question_id}
              onClick={() => onPick(i)}
              style={{
                appearance: "none",
                cursor: "pointer",
                textAlign: "left",
                background: "transparent",
                border: "none",
                padding: 0,
                display: "flex",
                alignItems: "baseline",
                gap: 10,
                fontFamily: "'DM Sans', sans-serif",
              }}
            >
              <span
                style={{
                  flexShrink: 0,
                  width: 7,
                  height: 7,
                  borderRadius: "50%",
                  background: tone.c,
                  transform: "translateY(-1px)",
                }}
              />
              <span
                style={{
                  font: "500 13.5px 'DM Sans', sans-serif",
                  color: HW2_COLOR.ink2,
                  lineHeight: 1.45,
                }}
              >
                {a.finding_headline}
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function ResultChart({ answer }: { answer: H2AnswerDraft }) {
  const spec = answer.chart_spec as { type?: string; x?: string; y?: string };
  const type = spec?.type;
  if (!type || type === "table" || !spec.x || !spec.y || answer.rows.length === 0) {
    return null;
  }
  const data = answer.rows;

  // A chart was intended, but if the measure is entirely non-numeric/null there
  // is nothing to draw — surface the truth instead of empty axes.
  const yKey = spec.y;
  const hasNumericY = data.some((r) => {
    const v = r[yKey];
    return typeof v === "number" && Number.isFinite(v);
  });
  if (!hasNumericY) {
    return <UnplottablePanel measure={yKey} reason={answer.caveats[0]} />;
  }

  // Relabel the category axis with resolved enum meanings (codes stay in the data).
  const xLabels = answer.value_labels?.[spec.x];
  const xTick = xLabels
    ? (v: unknown) => xLabels[String(v)] ?? String(v)
    : undefined;

  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 12,
        padding: "18px 18px 8px",
      }}
    >
      <CardLabel>Visualization</CardLabel>
      <ResponsiveContainer width="100%" height={280}>
        {type === "line" ? (
          <LineChart data={data} margin={{ top: 4, right: 12, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={HW2_COLOR.rule} />
            <XAxis
              dataKey={spec.x}
              tickFormatter={xTick}
              tick={{ fontSize: 11 }}
              stroke={HW2_COLOR.muted}
            />
            <YAxis tick={{ fontSize: 11 }} stroke={HW2_COLOR.muted} />
            <Tooltip labelFormatter={xTick} />
            <Line
              type="monotone"
              dataKey={spec.y}
              stroke={HW2_COLOR.blue}
              strokeWidth={2}
              dot={false}
            />
          </LineChart>
        ) : (
          <BarChart data={data} margin={{ top: 4, right: 12, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={HW2_COLOR.rule} />
            <XAxis
              dataKey={spec.x}
              tickFormatter={xTick}
              tick={{ fontSize: 11 }}
              stroke={HW2_COLOR.muted}
            />
            <YAxis tick={{ fontSize: 11 }} stroke={HW2_COLOR.muted} />
            <Tooltip labelFormatter={xTick} />
            <Bar dataKey={spec.y} fill={HW2_COLOR.blue} radius={[4, 4, 0, 0]} />
          </BarChart>
        )}
      </ResponsiveContainer>
    </div>
  );
}

// ─── Executed result: data table ───────────────────────────────────────────────

function ResultTable({ answer }: { answer: H2AnswerDraft }) {
  if (answer.columns.length === 0) return null;
  const rows = answer.rows.slice(0, 50);
  const labels = answer.value_labels ?? {};
  const fmt = (v: unknown) =>
    v === null || v === undefined
      ? "—"
      : typeof v === "number"
      ? Number.isInteger(v)
        ? v.toLocaleString()
        : v.toFixed(2)
      : String(v);
  // For coded columns, lead with the resolved meaning and keep the raw code.
  const cell = (c: string, v: unknown) => {
    const raw = fmt(v);
    const meaning =
      v === null || v === undefined ? undefined : labels[c]?.[String(v)];
    return meaning ? `${meaning} (${raw})` : raw;
  };

  return (
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
          alignItems: "baseline",
          justifyContent: "space-between",
          padding: "12px 16px",
          borderBottom: `1px solid ${HW2_COLOR.rule}`,
        }}
      >
        <span style={{ font: "600 12px 'DM Sans', sans-serif", color: HW2_COLOR.ink }}>
          Result data
        </span>
        <span style={{ font: "400 11px 'DM Mono', monospace", color: HW2_COLOR.faint }}>
          {answer.row_count.toLocaleString()} row{answer.row_count === 1 ? "" : "s"}
          {answer.truncated ? " · showing first 50" : ""}
        </span>
      </div>
      <div style={{ overflowX: "auto", maxHeight: 360 }}>
        <table style={{ borderCollapse: "collapse", width: "100%" }}>
          <thead>
            <tr>
              {answer.columns.map((c) => (
                <th
                  key={c}
                  style={{
                    position: "sticky",
                    top: 0,
                    textAlign: "left",
                    padding: "8px 14px",
                    background: HW2_COLOR.paper,
                    borderBottom: `1px solid ${HW2_COLOR.rule}`,
                    font: "600 11px 'DM Mono', monospace",
                    color: HW2_COLOR.ink2,
                    whiteSpace: "nowrap",
                  }}
                >
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, ri) => (
              <tr key={ri}>
                {answer.columns.map((c) => (
                  <td
                    key={c}
                    style={{
                      padding: "7px 14px",
                      borderBottom: `1px solid ${HW2_COLOR.rule}`,
                      font: "400 12px 'DM Mono', monospace",
                      color: HW2_COLOR.ink2,
                      whiteSpace: "nowrap",
                    }}
                  >
                    {cell(c, row[c])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Two-factor verdict panel ───────────────────────────────────────────────────

function VerdictFactor({
  label,
  pass,
  detail,
}: {
  label: string;
  pass: boolean;
  detail: string;
}) {
  return (
    <div style={{ display: "flex", alignItems: "flex-start", gap: 10 }}>
      <span
        style={{
          width: 18,
          height: 18,
          borderRadius: "50%",
          flexShrink: 0,
          marginTop: 1,
          display: "grid",
          placeItems: "center",
          background: pass ? HW2_COLOR.good : HW2_COLOR.chip,
          color: pass ? "#fff" : HW2_COLOR.muted,
          font: "700 10px 'DM Sans', sans-serif",
        }}
      >
        {pass ? "✓" : "○"}
      </span>
      <div>
        <div style={{ font: "600 12.5px 'DM Sans', sans-serif", color: HW2_COLOR.ink }}>
          {label}
        </div>
        <div style={{ font: "400 12px 'DM Sans', sans-serif", color: HW2_COLOR.muted }}>
          {detail}
        </div>
      </div>
    </div>
  );
}

// Human-readable judge state. "pending" = genuinely never run; "stale" = ran
// before but an input changed since, so its verdict no longer applies.
const JUDGE_LABEL: Record<string, { c: string; label: string }> = {
  certified:   { c: HW2_COLOR.good,  label: "Approved" },
  doubtful:    { c: HW2_COLOR.warn,  label: "Withheld — see reasons below" },
  reject:      { c: HW2_COLOR.warn,  label: "Rejected — see reasons below" },
  pending:     { c: HW2_COLOR.muted, label: "Not run yet — click Run certification" },
  stale:       { c: HW2_COLOR.warn,  label: "Inputs changed — re-run certification" },
  unavailable: { c: HW2_COLOR.muted, label: "No local model available" },
};

function JudgePanel({ answer }: { answer: H2AnswerDraft }) {
  const judgeTone =
    JUDGE_LABEL[answer.judge_verdict] ??
    { c: HW2_COLOR.warn, label: answer.judge_verdict };

  return (
    <div
      style={{
        background: HW2_COLOR.surface,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 12,
        padding: "16px 18px",
      }}
    >
      <CardLabel>Certification — both factors required</CardLabel>
      <div style={{ display: "grid", gap: 12 }}>
        <VerdictFactor
          label="Statistical readiness"
          pass={answer.statistical_pass}
          detail={
            answer.statistical_pass
              ? `Evidence contracts pass (${answer.readiness_pct}% cleared).`
              : "One or more evidence contracts failed."
          }
        />
        <VerdictFactor
          label="LLM judge"
          pass={answer.judge_verdict === "certified"}
          detail={`${judgeTone.label}${
            answer.judge_confidence > 0
              ? ` · ${Math.round(answer.judge_confidence * 100)}% confidence`
              : ""
          }`}
        />
      </div>
      {answer.judge_reasons.length > 0 && (
        <ul
          style={{
            margin: "12px 0 0",
            paddingLeft: 18,
            font: "400 12px 'DM Sans', sans-serif",
            color: HW2_COLOR.ink2,
            lineHeight: 1.5,
          }}
        >
          {answer.judge_reasons.map((r, i) => (
            <li key={i}>{r}</li>
          ))}
        </ul>
      )}
      <div
        style={{
          marginTop: 14,
          paddingTop: 12,
          borderTop: `1px solid ${HW2_COLOR.rule}`,
          font: "500 12px 'DM Mono', monospace",
          color: HW2_COLOR.ink2,
        }}
      >
        Confidence {Math.round(answer.confidence * 100)}%
        <span style={{ color: HW2_COLOR.faint, fontWeight: 400 }}>
          {answer.confidence_breakdown &&
          Object.keys(answer.confidence_breakdown).length > 0
            ? "  =  " + confidenceParts(answer.confidence_breakdown).join(" · ")
            : ""}
        </span>
      </div>
    </div>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function AnswerPage() {
  const { id } = useParams<{ id: string }>();

  const [sourceName, setSourceName] = useState("");
  const [answers, setAnswers] = useState<H2AnswerDraft[]>([]);
  const [counts, setCounts] = useState({
    certified: 0,
    doubtful: 0,
    pending: 0,
    cannot_answer: 0,
  });
  const [loading, setLoading] = useState(true);
  const [drafting, setDrafting] = useState(false);
  const [certifying, setCertifying] = useState(false);
  const [activeIdx, setActiveIdx] = useState(0);
  const [report, setReport] = useState<string | null>(null);
  const [loadingReport, setLoadingReport] = useState(false);

  const apply = (result: H2AnswersResult) => {
    setAnswers(result.answers);
    setCounts({
      certified: result.certified_count,
      doubtful: result.doubtful_count,
      pending: result.pending_count,
      cannot_answer: result.cannot_answer_count,
    });
    // Surface the first answer that still needs attention.
    const attentionIdx = result.answers.findIndex(
      (a) => a.state === "pending" || a.state === "doubtful"
    );
    setActiveIdx(attentionIdx >= 0 ? attentionIdx : 0);
  };

  const loadDraft = async () => {
    const result = await h2.projects.answer.draft(id);
    ANSWER_CACHE.set(id, result);
    apply(result);
  };

  const redraft = async () => {
    setDrafting(true);
    try {
      await loadDraft();
    } finally {
      setDrafting(false);
    }
  };

  const certify = async () => {
    setCertifying(true);
    try {
      const result = await h2.projects.answer.certify(id);
      ANSWER_CACHE.set(id, result);
      apply(result);
    } finally {
      setCertifying(false);
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
    // Reuse the cached payload on re-navigation (no redundant pipeline run);
    // only fetch (which executes) on a cold load. Recompute/Redraft refresh it.
    const cached = ANSWER_CACHE.get(id);
    if (cached) {
      apply(cached);
      setLoading(false);
      return;
    }
    loadDraft()
      .catch(() => {})
      .finally(() => setLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

  // The source name is needed to run ad-hoc SQL against the materialized source.
  useEffect(() => {
    h2.projects
      .get(id)
      .then((p) => setSourceName(p.sources?.[0]?.source_name ?? ""))
      .catch(() => {});
  }, [id]);

  // A recompute re-drafts and re-executes; pull the fresh answers/data in.
  useEffect(
    () => onHw2Event(HW2_RECOMPUTED, () => void loadDraft().catch(() => {})),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [id]
  );

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
  // If the judge has produced a verdict for any answer, the action is a re-run,
  // not a first run — mirrors the persisted truth ("pending" means never run).
  const judgedBefore = answers.some((a) =>
    ["certified", "doubtful", "reject", "stale"].includes(a.judge_verdict)
  );

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
            disabled={drafting || certifying}
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
          <button
            onClick={certify}
            disabled={certifying || drafting}
            title="Run the LLM judge over executed answers (can take a few seconds per question)"
            style={{
              appearance: "none",
              cursor: certifying ? "default" : "pointer",
              background: HW2_COLOR.blue,
              color: "#fff",
              border: "1px solid transparent",
              borderRadius: 8,
              padding: "6px 12px",
              font: "600 12px 'DM Sans', sans-serif",
              fontFamily: "'DM Sans', sans-serif",
              opacity: certifying ? 0.6 : 1,
            }}
          >
            {certifying
              ? "Certifying…"
              : judgedBefore
              ? "Re-run certification"
              : "Run certification"}
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
        <span style={{ color: HW2_COLOR.muted }}>{counts.pending} not certified</span>
        <span style={{ color: HW2_COLOR.faint }}>·</span>
        <span style={{ color: HW2_COLOR.warn }}>{counts.doubtful} doubtful</span>
        <span style={{ color: HW2_COLOR.faint }}>·</span>
        <span style={{ color: HW2_COLOR.warn }}>
          {counts.cannot_answer} can&rsquo;t answer
        </span>
      </div>

      {answers.length > 0 && <FindingsSummary answers={answers} onPick={setActiveIdx} />}

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

              </div>

              {activeAnswer.state === "cannot_answer" ? (
                <CannotAnswerCard answer={activeAnswer} />
              ) : (
                <>
                  <FindingCard answer={activeAnswer} />
                  <JudgePanel answer={activeAnswer} />

                  {activeAnswer.execution_error && (
                    <div
                      style={{
                        padding: "12px 16px",
                        background: HW2_COLOR.badSoft,
                        border: `1px solid ${HW2_COLOR.bad}44`,
                        borderRadius: 10,
                        font: "400 12.5px 'DM Mono', monospace",
                        color: HW2_COLOR.bad,
                        lineHeight: 1.5,
                      }}
                    >
                      Query failed: {activeAnswer.execution_error}
                    </div>
                  )}

                  <ResultChart answer={activeAnswer} />
                  <ResultTable answer={activeAnswer} />
                  {/* Key by question so the editor + run-result reset on switch —
                      never show one question's SQL beside another's data. */}
                  <SqlCard
                    key={activeAnswer.question_id}
                    answer={activeAnswer}
                    sourceName={sourceName}
                  />

                  {activeAnswer.caveats.length > 0 && (
                    <CaveatsCard caveats={activeAnswer.caveats} />
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
