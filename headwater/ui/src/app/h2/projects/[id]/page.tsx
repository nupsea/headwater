"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { h2, notifyInputChanged, type H2Project } from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";
import { InputsPanel } from "@/components/h2/inputs-panel";

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        font: "600 11px 'DM Sans', sans-serif",
        color: HW2_COLOR.muted,
        textTransform: "uppercase",
        letterSpacing: "0.06em",
        marginBottom: 10,
      }}
    >
      {children}
    </div>
  );
}

/** Project home — the "Frame" front door: goal, the inputs considered, scope,
 *  and a snapshot of what Headwater proposes, with a path into Understand. */
export default function ProjectHomePage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();

  const [project, setProject] = useState<H2Project | null>(null);
  const [loading, setLoading] = useState(true);
  const [editingGoal, setEditingGoal] = useState(false);
  const [goalDraft, setGoalDraft] = useState("");
  const [savingGoal, setSavingGoal] = useState(false);

  const load = () => {
    h2.projects
      .get(id)
      .then((p) => {
        setProject(p);
        setGoalDraft(p.goal?.statement ?? "");
      })
      .catch(() => setProject(null))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

  const saveGoal = async () => {
    if (goalDraft.trim().length < 6) return;
    setSavingGoal(true);
    try {
      await h2.projects.setGoal(id, goalDraft.trim());
      setEditingGoal(false);
      notifyInputChanged();
      load();
    } finally {
      setSavingGoal(false);
    }
  };

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

  if (!project) {
    return (
      <div style={{ maxWidth: 820, margin: "0 auto", padding: "32px" }}>
        <p style={{ font: "500 14px 'DM Sans', sans-serif", color: HW2_COLOR.bad }}>
          Project not found.
        </p>
      </div>
    );
  }

  const questions = project.questions ?? [];
  const answerable = questions.filter((q) => q.answerability === "answerable").length;
  const caveat = questions.filter((q) => q.answerability === "answerable_with_caveat").length;
  const cannot = questions.filter((q) => q.answerability === "cannot_answer").length;
  const scope = project.sources?.[0]?.selected_tables ?? [];
  const goal = project.goal?.statement ?? "";

  return (
    <div
      style={{
        maxWidth: 860,
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
        Step 1 of 5 · Frame
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
        The problem we&rsquo;re serving.
      </h2>
      <p
        style={{
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          marginBottom: 26,
          lineHeight: 1.55,
        }}
      >
        This is the project&rsquo;s front door: the goal, the inputs Headwater has
        considered, and what it can already propose. Refine the goal or feed more context
        any time — every change refreshes the whole workflow.
      </p>

      {/* Goal */}
      <div
        style={{
          background: HW2_COLOR.surface,
          border: `1px solid ${HW2_COLOR.rule}`,
          borderRadius: 12,
          padding: "18px 20px",
          marginBottom: 16,
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "baseline",
            justifyContent: "space-between",
            marginBottom: 10,
          }}
        >
          <SectionLabel>Goal</SectionLabel>
          {!editingGoal && (
            <button
              onClick={() => setEditingGoal(true)}
              style={{
                appearance: "none",
                background: "transparent",
                border: "none",
                cursor: "pointer",
                padding: 0,
                color: HW2_COLOR.blue,
                font: "500 12px 'DM Sans', sans-serif",
                fontFamily: "'DM Sans', sans-serif",
              }}
            >
              Edit
            </button>
          )}
        </div>
        {editingGoal ? (
          <div>
            <textarea
              value={goalDraft}
              onChange={(e) => setGoalDraft(e.target.value)}
              rows={3}
              style={{
                width: "100%",
                resize: "vertical",
                padding: "12px 14px",
                background: HW2_COLOR.paper,
                border: `1px solid ${HW2_COLOR.rule2}`,
                borderRadius: 10,
                font: "500 15px 'DM Sans', sans-serif",
                color: HW2_COLOR.ink,
                lineHeight: 1.4,
                outline: "none",
                boxSizing: "border-box",
                fontFamily: "'DM Sans', sans-serif",
              }}
            />
            <div style={{ marginTop: 10, display: "flex", gap: 8, justifyContent: "flex-end" }}>
              <button
                onClick={() => {
                  setEditingGoal(false);
                  setGoalDraft(goal);
                }}
                style={{
                  appearance: "none",
                  cursor: "pointer",
                  background: "#fff",
                  border: `1px solid ${HW2_COLOR.rule2}`,
                  borderRadius: 8,
                  padding: "8px 14px",
                  font: "500 13px 'DM Sans', sans-serif",
                  color: HW2_COLOR.ink2,
                  fontFamily: "'DM Sans', sans-serif",
                }}
              >
                Cancel
              </button>
              <button
                onClick={saveGoal}
                disabled={goalDraft.trim().length < 6 || savingGoal}
                style={{
                  appearance: "none",
                  cursor: goalDraft.trim().length < 6 || savingGoal ? "default" : "pointer",
                  background: HW2_COLOR.blue,
                  color: "#fff",
                  border: "1px solid transparent",
                  borderRadius: 8,
                  padding: "8px 16px",
                  font: "600 13px 'DM Sans', sans-serif",
                  fontFamily: "'DM Sans', sans-serif",
                  opacity: goalDraft.trim().length < 6 || savingGoal ? 0.5 : 1,
                }}
              >
                {savingGoal ? "Saving…" : "Save & re-propose"}
              </button>
            </div>
          </div>
        ) : (
          <p
            style={{
              font: "500 17px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink,
              lineHeight: 1.45,
              margin: 0,
            }}
          >
            {goal || (
              <span style={{ color: HW2_COLOR.faint, fontWeight: 400 }}>
                No goal set yet — click Edit to define what you want to learn.
              </span>
            )}
          </p>
        )}
      </div>

      {/* Inputs */}
      <div style={{ marginBottom: 16 }}>
        <InputsPanel projectId={id} onChanged={load} />
      </div>

      {/* Scope + proposal snapshot */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 16,
          marginBottom: 24,
        }}
      >
        <div
          style={{
            background: HW2_COLOR.surface,
            border: `1px solid ${HW2_COLOR.rule}`,
            borderRadius: 12,
            padding: "18px 20px",
          }}
        >
          <SectionLabel>Data in scope</SectionLabel>
          {scope.length === 0 ? (
            <p style={{ font: "400 13px 'DM Sans', sans-serif", color: HW2_COLOR.faint, margin: 0 }}>
              All tables from {project.sources?.[0]?.source_name ?? "the source"}.
            </p>
          ) : (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
              {scope.map((t) => (
                <span
                  key={t}
                  style={{
                    font: "500 12px 'DM Mono', monospace",
                    color: HW2_COLOR.blue,
                    padding: "3px 8px",
                    background: HW2_COLOR.blueSoft,
                    borderRadius: 4,
                  }}
                >
                  {t}
                </span>
              ))}
            </div>
          )}
        </div>

        <div
          style={{
            background: HW2_COLOR.surface,
            border: `1px solid ${HW2_COLOR.rule}`,
            borderRadius: 12,
            padding: "18px 20px",
          }}
        >
          <SectionLabel>What Headwater proposes</SectionLabel>
          {questions.length === 0 ? (
            <p style={{ font: "400 13px 'DM Sans', sans-serif", color: HW2_COLOR.faint, margin: 0 }}>
              No proposed questions yet — open Understand to generate them.
            </p>
          ) : (
            <div style={{ display: "flex", gap: 16, font: "500 13px 'DM Sans', sans-serif" }}>
              <span style={{ color: HW2_COLOR.good }}>{answerable} answerable</span>
              <span style={{ color: HW2_COLOR.blue }}>{caveat} with caveat</span>
              <span style={{ color: HW2_COLOR.warn }}>{cannot} can&rsquo;t</span>
            </div>
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
          <div style={{ font: "600 14px 'DM Sans', sans-serif", color: HW2_COLOR.ink }}>
            Ready to look closer?
          </div>
          <div style={{ font: "400 12px 'DM Sans', sans-serif", color: HW2_COLOR.muted, marginTop: 2 }}>
            Understand reconstructs the schema, relationships, and meanings — editable.
          </div>
        </div>
        <button
          onClick={() => router.push(`/h2/projects/${id}/understand`)}
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
          Continue to Understand →
        </button>
      </div>
    </div>
  );
}
