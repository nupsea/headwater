"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { h2, type H2Source } from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

function Field({
  label,
  value,
  onChange,
  placeholder,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
}) {
  return (
    <div>
      <label
        style={{
          font: "600 12px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          display: "block",
          marginBottom: 6,
        }}
      >
        {label}
      </label>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        style={{
          width: "100%",
          padding: "10px 14px",
          background: "#fff",
          border: `1px solid ${HW2_COLOR.rule2}`,
          borderRadius: 8,
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.ink,
          fontFamily: "'DM Sans', sans-serif",
          outline: "none",
          boxSizing: "border-box",
        }}
        onFocus={(e) => (e.currentTarget.style.borderColor = HW2_COLOR.blue)}
        onBlur={(e) => (e.currentTarget.style.borderColor = HW2_COLOR.rule2)}
      />
    </div>
  );
}

export default function NewProjectPage() {
  const router = useRouter();
  const [sources, setSources] = useState<H2Source[]>([]);
  const [tables, setTables] = useState<Array<{ table_name: string; row_count: number; description: string | null }>>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [goal, setGoal] = useState("");
  const [projectId, setProjectId] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [sourceName, setSourceName] = useState("");
  const [decision, setDecision] = useState("");
  const [targetMetric, setTargetMetric] = useState("");
  const [timeHorizon, setTimeHorizon] = useState("");
  const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set());
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [showTablePicker, setShowTablePicker] = useState(false);
  const [contextText, setContextText] = useState("");
  const [contextFiles, setContextFiles] = useState<File[]>([]);
  const [suggesting, setSuggesting] = useState(false);
  const [goalRationale, setGoalRationale] = useState("");

  const suggestGoal = async () => {
    if (!sourceName) return;
    setSuggesting(true);
    try {
      const r = await h2.sources.suggestGoal(sourceName);
      setGoal(r.goal);
      setGoalRationale(
        r.available
          ? r.rationale
          : "Suggested without a model — start Ollama for a data-aware goal."
      );
    } catch {
      setGoalRationale("Could not reach the suggestion service.");
    } finally {
      setSuggesting(false);
    }
  };

  useEffect(() => {
    h2.sources.list().then(setSources).catch(() => {});
  }, []);

  // Load tables when source is selected
  useEffect(() => {
    if (!sourceName) {
      setTables([]);
      setSelectedTables(new Set());
      return;
    }
    h2.sources
      .catalog(sourceName)
      .then((catalog) => {
        setTables(catalog);
        // Pre-select all tables
        setSelectedTables(new Set(catalog.map((t) => t.table_name)));
      })
      .catch(() => setTables([]));
  }, [sourceName]);

  const toggleTable = (name: string) => {
    setSelectedTables((prev) => {
      const next = new Set(prev);
      next.has(name) ? next.delete(name) : next.add(name);
      return next;
    });
  };

  const ready =
    goal.trim().length >= 6 &&
    projectId.trim().length > 0 &&
    sourceName.length > 0;

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!ready) return;
    setLoading(true);
    setError(null);
    try {
      const pid = projectId.trim();
      await h2.projects.frame({
        project_id: pid,
        source_name: sourceName,
        display_name: displayName || pid,
        goal: goal.trim(),
        decision: decision || undefined,
        target_metric: targetMetric || undefined,
        time_horizon: timeHorizon || undefined,
        selected_tables: selectedTables.size > 0 ? [...selectedTables] : undefined,
      });
      // Ingest any provided context so it is considered from the very start.
      const ingests: Promise<unknown>[] = [];
      if (contextText.trim()) {
        ingests.push(
          h2.projects.resources.ingest(
            pid,
            new File([contextText], "framing-context.md", { type: "text/markdown" })
          )
        );
      }
      for (const f of contextFiles) {
        ingests.push(h2.projects.resources.ingest(pid, f));
      }
      if (ingests.length) await Promise.allSettled(ingests);
      router.push(`/h2/projects/${pid}/understand`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to create project");
      setLoading(false);
    }
  };

  return (
    <div
      style={{
        maxWidth: 800,
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
          marginBottom: 6,
        }}
      >
        What goal are we serving?
      </h2>
      <p
        style={{
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          marginBottom: 28,
          lineHeight: 1.55,
        }}
      >
        State a goal and a scope. You don&rsquo;t need to know the questions
        yet —{" "}
        <strong style={{ color: HW2_COLOR.ink2 }}>
          Headwater will propose the questions this data can credibly answer
        </strong>{" "}
        on the next step, and flag the ones it can&rsquo;t.
      </p>

      <form onSubmit={submit}>
        {/* Goal */}
        <div style={{ marginBottom: 6 }}>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              marginBottom: 8,
            }}
          >
            <label
              style={{
                font: "600 12px 'DM Sans', sans-serif",
                color: HW2_COLOR.muted,
              }}
            >
              The goal *
            </label>
            <button
              type="button"
              onClick={suggestGoal}
              disabled={!sourceName || suggesting}
              title={
                sourceName
                  ? "Infer a goal from the selected data"
                  : "Select a data source first"
              }
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
              display: "block",
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
            onFocus={(e) =>
              (e.currentTarget.style.borderColor = HW2_COLOR.blue)
            }
            onBlur={(e) =>
              (e.currentTarget.style.borderColor = HW2_COLOR.rule2)
            }
          />
        </div>

        {goalRationale && (
          <p
            style={{
              font: "400 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              marginTop: 8,
              lineHeight: 1.5,
            }}
          >
            <span style={{ color: HW2_COLOR.blue, fontWeight: 600 }}>Why: </span>
            {goalRationale}
          </p>
        )}

        {/* Project ID + Display Name */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginTop: 16 }}>
          <Field
            label="Project ID *"
            value={projectId}
            onChange={setProjectId}
            placeholder="my_project_01"
          />
          <Field
            label="Display name"
            value={displayName}
            onChange={setDisplayName}
            placeholder="My Project"
          />
        </div>

        {/* Source selector */}
        <div style={{ marginTop: 20 }}>
          <label
            style={{
              font: "600 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              display: "block",
              marginBottom: 8,
            }}
          >
            Data source *
          </label>
          {sources.length === 0 ? (
            <div
              style={{
                padding: "12px 16px",
                background: HW2_COLOR.badSoft,
                border: `1px solid ${HW2_COLOR.bad}44`,
                borderRadius: 8,
                font: "500 13px 'DM Sans', sans-serif",
                color: HW2_COLOR.bad,
              }}
            >
              No sources available.{" "}
              <a
                href="/h2/sources/new"
                style={{ color: HW2_COLOR.bad, fontWeight: 600 }}
              >
                Connect one first.
              </a>
            </div>
          ) : (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              {sources.map((s) => {
                const active = sourceName === s.name;
                return (
                  <button
                    key={s.name}
                    type="button"
                    onClick={() => setSourceName(s.name)}
                    style={{
                      appearance: "none",
                      cursor: "pointer",
                      padding: "8px 14px",
                      background: active ? HW2_COLOR.blueSoft : "#fff",
                      border: `1.5px solid ${
                        active ? HW2_COLOR.blue : HW2_COLOR.rule2
                      }`,
                      borderRadius: 8,
                      font: "500 13px 'DM Sans', sans-serif",
                      color: active ? HW2_COLOR.blue : HW2_COLOR.ink2,
                      display: "inline-flex",
                      alignItems: "center",
                      gap: 8,
                    }}
                  >
                    <span
                      style={{
                        width: 6,
                        height: 6,
                        borderRadius: "50%",
                        background: s.latest_snapshot_id
                          ? HW2_COLOR.good
                          : HW2_COLOR.faint,
                      }}
                    />
                    {s.name}
                    <span
                      style={{
                        font: "400 10px 'DM Mono', monospace",
                        color: active ? HW2_COLOR.blue : HW2_COLOR.muted,
                      }}
                    >
                      {s.type}
                    </span>
                  </button>
                );
              })}
            </div>
          )}
        </div>

        {/* Advanced toggle */}
        <button
          type="button"
          onClick={() => setShowAdvanced((v) => !v)}
          style={{
            appearance: "none",
            background: "transparent",
            border: "none",
            cursor: "pointer",
            marginTop: 20,
            padding: 0,
            color: HW2_COLOR.muted,
            font: "500 13px 'DM Sans', sans-serif",
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
            {showAdvanced ? "▼" : "▶"}
          </span>
          Add detail (optional): decision · target metric · time horizon
        </button>

        {showAdvanced && (
          <div style={{ marginTop: 14, display: "grid", gap: 12 }}>
            <Field
              label="Decision you're making"
              value={decision}
              onChange={setDecision}
              placeholder="e.g. Where to add staff or change process"
            />
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr",
                gap: 12,
              }}
            >
              <Field
                label="Target metric"
                value={targetMetric}
                onChange={setTargetMetric}
                placeholder="e.g. Mean wait time"
              />
              <Field
                label="Time horizon"
                value={timeHorizon}
                onChange={setTimeHorizon}
                placeholder="e.g. Next quarter"
              />
            </div>
          </div>
        )}

        {/* Table scope */}
        {sourceName && tables.length > 0 && (
          <div style={{ marginTop: 28 }}>
            <div
              style={{
                display: "flex",
                alignItems: "baseline",
                justifyContent: "space-between",
                marginBottom: 10,
              }}
            >
              <label
                style={{
                  font: "600 12px 'DM Sans', sans-serif",
                  color: HW2_COLOR.muted,
                }}
              >
                Data scope · {selectedTables.size} table
                {selectedTables.size !== 1 ? "s" : ""} selected
              </label>
              <button
                type="button"
                onClick={() => setShowTablePicker((v) => !v)}
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
                {showTablePicker ? "Hide" : "Edit table picks"}
              </button>
            </div>

            {!showTablePicker ? (
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                {[...selectedTables].map((tname) => (
                  <span
                    key={tname}
                    style={{
                      font: "500 12px 'DM Mono', monospace",
                      color: HW2_COLOR.blue,
                      padding: "3px 8px",
                      background: HW2_COLOR.blueSoft,
                      borderRadius: 4,
                    }}
                  >
                    {tname}
                  </span>
                ))}
              </div>
            ) : (
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
                    padding: "8px 14px",
                    background: HW2_COLOR.paper,
                    borderBottom: `1px solid ${HW2_COLOR.rule}`,
                    font: "400 11px 'DM Sans', sans-serif",
                    color: HW2_COLOR.muted,
                  }}
                >
                  Pick the tables in scope. Profiles are reused — picking
                  doesn&rsquo;t re-scan.
                </div>
                <div
                  style={{ maxHeight: 320, overflowY: "auto", padding: "8px 6px" }}
                >
                  {tables.map((t) => {
                    const sel = selectedTables.has(t.table_name);
                    return (
                      <label
                        key={t.table_name}
                        style={{
                          display: "flex",
                          alignItems: "center",
                          gap: 10,
                          padding: "6px 12px",
                          cursor: "pointer",
                        }}
                      >
                        <input
                          type="checkbox"
                          checked={sel}
                          onChange={() => toggleTable(t.table_name)}
                          style={{ accentColor: HW2_COLOR.blue }}
                        />
                        <span
                          style={{
                            font: "500 12px 'DM Mono', monospace",
                            color: HW2_COLOR.ink2,
                            minWidth: 160,
                          }}
                        >
                          {t.table_name}
                        </span>
                        <span
                          style={{
                            flex: 1,
                            font: "400 12px 'DM Sans', sans-serif",
                            color: HW2_COLOR.muted,
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {t.description ?? ""}
                        </span>
                        <span
                          style={{
                            font: "400 11px 'DM Mono', monospace",
                            color: HW2_COLOR.faint,
                          }}
                        >
                          {t.row_count >= 1000
                            ? `${(t.row_count / 1000).toFixed(1)}k`
                            : t.row_count}
                        </span>
                      </label>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        )}

        {/* Context inputs */}
        <div style={{ marginTop: 28 }}>
          <label
            style={{
              font: "600 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              display: "block",
              marginBottom: 6,
            }}
          >
            Context &amp; inputs (optional)
          </label>
          <p
            style={{
              font: "400 12.5px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              lineHeight: 1.5,
              marginBottom: 10,
            }}
          >
            Paste a data dictionary, column definitions, or notes — or attach
            .md/.txt files. Headwater considers these when proposing and
            certifying answers. Markdown tables (column &rarr; meaning) map
            directly onto your data. (PDF coming later.)
          </p>
          <textarea
            value={contextText}
            onChange={(e) => setContextText(e.target.value)}
            placeholder={
              "e.g.\n| column | meaning |\n| --- | --- |\n| total_wait_time | service_start minus arrival_time |\n| patient_type | ER = emergency, OP = outpatient, IP = inpatient |"
            }
            rows={5}
            spellCheck={false}
            style={{
              width: "100%",
              resize: "vertical",
              padding: "12px 14px",
              background: "#fff",
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 10,
              font: "500 12.5px 'DM Mono', monospace",
              color: HW2_COLOR.ink,
              lineHeight: 1.5,
              outline: "none",
              boxSizing: "border-box",
            }}
          />
          <div style={{ marginTop: 10, display: "flex", alignItems: "center", gap: 12 }}>
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
                accept=".md,.markdown,.txt,.text"
                multiple
                onChange={(e) =>
                  setContextFiles(Array.from(e.target.files ?? []))
                }
                style={{ display: "none" }}
              />
            </label>
            {contextFiles.length > 0 && (
              <span
                style={{
                  font: "500 12px 'DM Mono', monospace",
                  color: HW2_COLOR.blue,
                }}
              >
                {contextFiles.map((f) => f.name).join(", ")}
              </span>
            )}
          </div>
        </div>

        {/* Error */}
        {error && (
          <div
            style={{
              marginTop: 20,
              padding: "12px 16px",
              background: HW2_COLOR.badSoft,
              border: `1px solid ${HW2_COLOR.bad}44`,
              borderRadius: 8,
              font: "500 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.bad,
            }}
          >
            {error}
          </div>
        )}

        {/* CTA */}
        <div
          style={{
            marginTop: 36,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <span
            style={{
              font: "400 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.faint,
            }}
          >
            {!ready
              ? "Add a goal, project ID, and select a source."
              : "Headwater will read the scope and propose questions next."}
          </span>
          <button
            type="submit"
            disabled={!ready || loading}
            style={{
              appearance: "none",
              cursor: ready && !loading ? "pointer" : "default",
              background: HW2_COLOR.blue,
              color: "#fff",
              border: "1px solid transparent",
              borderRadius: 10,
              padding: "11px 20px",
              font: "600 14px 'DM Sans', sans-serif",
              opacity: !ready || loading ? 0.5 : 1,
              transition: "opacity 120ms",
            }}
          >
            {loading ? "Generating…" : "Generate understanding →"}
          </button>
        </div>
      </form>
    </div>
  );
}
