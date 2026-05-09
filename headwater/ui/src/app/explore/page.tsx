"use client";

import { useEffect, useRef, useState } from "react";
import {
  api,
  type InsightFamilyDiagnostic,
  type SuggestedQuestion,
  type StatisticalInsight,
  type DataInsight,
  type SemanticHighlight,
  type ExplorationResult,
  type DimensionOption,
} from "@/lib/api";
import { ResultChart } from "@/components/result-chart";
import { SqlViewer } from "@/components/sql-viewer";
import { DisambiguationUI } from "@/components/disambiguation-ui";
import { useProjects } from "@/lib/project-context";

const SOURCE_COLORS: Record<string, string> = {
  business: "bg-emerald-100 text-emerald-800 border-emerald-200 dark:bg-emerald-900/30 dark:text-emerald-300 dark:border-emerald-700",
  mart: "bg-blue-100 text-blue-800 border-blue-200 dark:bg-blue-900/30 dark:text-blue-300 dark:border-blue-700",
  relationship: "bg-purple-100 text-purple-800 border-purple-200 dark:bg-purple-900/30 dark:text-purple-300 dark:border-purple-700",
  quality: "bg-amber-100 text-amber-800 border-amber-200 dark:bg-amber-900/30 dark:text-amber-300 dark:border-amber-700",
  semantic: "bg-green-100 text-green-800 border-green-200 dark:bg-green-900/30 dark:text-green-300 dark:border-green-700",
  statistical: "bg-rose-100 text-rose-800 border-rose-200 dark:bg-rose-900/30 dark:text-rose-300 dark:border-rose-700",
  catalog: "bg-teal-100 text-teal-800 border-teal-200 dark:bg-teal-900/30 dark:text-teal-300 dark:border-teal-700",
  cross_table: "bg-indigo-100 text-indigo-800 border-indigo-200 dark:bg-indigo-900/30 dark:text-indigo-300 dark:border-indigo-700",
};

const SEVERITY_COLORS: Record<string, string> = {
  info: "border-l-[var(--accent)] bg-[color-mix(in_srgb,var(--accent)_10%,var(--card))]",
  warning: "border-l-[var(--warning)] bg-[color-mix(in_srgb,var(--warning)_10%,var(--card))]",
  critical: "border-l-[var(--danger)] bg-[color-mix(in_srgb,var(--danger)_10%,var(--card))]",
};

const SOURCE_LABELS: Record<string, string> = {
  business: "Business signal",
  mart: "Model insight",
  relationship: "Relationship",
  quality: "Data quality",
  semantic: "Schema-driven",
  statistical: "Statistical",
  catalog: "Catalog",
  cross_table: "Cross-table",
};

function questionShape(question: string) {
  const q = question.toLowerCase();
  if (q.includes("changed over time") || q.startsWith("how has ")) return "Trend";
  if (q.startsWith("which ") && q.includes(" highest ")) return "Top segment";
  if (q.startsWith("how many ")) return "Volume";
  if (q.includes("distribution of")) return "Distribution";
  if (q.startsWith("what is the average ")) return "Comparison";
  return "Explore";
}

function questionWhy(question: SuggestedQuestion) {
  const q = question.question.toLowerCase();
  if (q.includes("changed over time")) {
    return "Best first check for movement, seasonality, or operational shifts.";
  }
  if (q.startsWith("which ") && q.includes(" highest ")) {
    return "Useful for finding the segment or driver that matters most right now.";
  }
  if (q.startsWith("how many ")) {
    return "Good for quickly sizing demand, activity, or concentration.";
  }
  if (q.includes("distribution of")) {
    return "Useful when the underlying metric varies across many records.";
  }
  if (question.source === "cross_table") {
    return "Combines tables to surface business context instead of a single-table view.";
  }
  return "Curated from the current schema, models, and detected signals.";
}

function pickFeaturedSuggestions(suggestions: SuggestedQuestion[]) {
  const order = ["business", "cross_table", "mart", "semantic", "catalog", "relationship", "quality"];
  const ranked = [...suggestions].sort((a, b) => {
    const sourceDelta = order.indexOf(a.source) - order.indexOf(b.source);
    if (sourceDelta !== 0) return sourceDelta;
    const shapeDelta =
      (questionShape(a.question) === "Distribution" ? 1 : 0) -
      (questionShape(b.question) === "Distribution" ? 1 : 0);
    if (shapeDelta !== 0) return shapeDelta;
    return a.question.length - b.question.length;
  });
  return ranked.slice(0, 3);
}

function summarizeDiagnostics(diagnostics: InsightFamilyDiagnostic[]) {
  const counts = { generated: 0, skipped: 0, failed: 0 };
  const reasons = new Map<string, number>();
  const families = new Set<string>();

  for (const diagnostic of diagnostics) {
    counts[diagnostic.status] += 1;
    families.add(diagnostic.family);
    if (diagnostic.reason) {
      reasons.set(diagnostic.reason, (reasons.get(diagnostic.reason) || 0) + 1);
    }
  }

  return {
    counts,
    families: families.size,
    topReasons: Array.from(reasons.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 4),
  };
}

export default function ExplorePage() {
  const { activeProjectId } = useProjects();
  const activeProjectIdRef = useRef<string | null>(activeProjectId);
  const [suggestions, setSuggestions] = useState<SuggestedQuestion[]>([]);
  const [businessInsights, setBusinessInsights] = useState<DataInsight[]>([]);
  const [semanticHighlights, setSemanticHighlights] = useState<SemanticHighlight[]>([]);
  const [insights, setInsights] = useState<StatisticalInsight[]>([]);
  const [suggestionDiagnostics, setSuggestionDiagnostics] = useState<
    InsightFamilyDiagnostic[]
  >([]);
  const [insightDiagnostics, setInsightDiagnostics] = useState<
    InsightFamilyDiagnostic[]
  >([]);
  const [question, setQuestion] = useState("");
  const [result, setResult] = useState<ExplorationResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [filterCategory, setFilterCategory] = useState("all");
  const [showSql, setShowSql] = useState(false);
  const [showRepairHistory, setShowRepairHistory] = useState(false);
  const [showTable, setShowTable] = useState(false);
  const [suggestionsLoading, setSuggestionsLoading] = useState(false);
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insightsLoaded, setInsightsLoaded] = useState(false);
  const [activeTab, setActiveTab] = useState<"questions" | "insights">(
    "questions"
  );

  const [reviewPct, setReviewPct] = useState(100);

  useEffect(() => {
    activeProjectIdRef.current = activeProjectId;
  }, [activeProjectId]);

  const loadSuggestions = async (force = false) => {
    if (!activeProjectId) return;
    if (suggestionsLoading && !force) return;
    setSuggestionsLoading(true);
    const projectId = activeProjectId;
    try {
      const res = await api.exploreSuggestions(projectId);
      if (activeProjectIdRef.current !== projectId) return;
      setSuggestions(res.suggestions || []);
      setBusinessInsights(res.business_insights || []);
      setSemanticHighlights(res.semantic_highlights || []);
      setInsights(res.insights || []);
      setSuggestionDiagnostics(res.diagnostics || []);
      if (typeof res.review_pct === "number") setReviewPct(res.review_pct);
      setError("");
    } catch (e) {
      if (activeProjectIdRef.current !== projectId) return;
      const msg = String(e instanceof Error ? e.message : e);
      if (msg.includes("400") || msg.toLowerCase().includes("no discovery")) {
        setError(
          "No data to ask about yet. Connect a source on the Sources page and run the pipeline first."
        );
      } else {
        setError(msg);
      }
    } finally {
      if (activeProjectIdRef.current === projectId) {
        setSuggestionsLoading(false);
      }
    }
  };

  useEffect(() => {
    let cancelled = false;
    queueMicrotask(() => {
      if (cancelled) return;
      setSuggestions([]);
      setBusinessInsights([]);
      setSemanticHighlights([]);
      setInsights([]);
      setSuggestionDiagnostics([]);
      setInsightDiagnostics([]);
      setQuestion("");
      setResult(null);
      setLoading(false);
      setError("");
      setFilterCategory("all");
      setShowSql(false);
      setShowRepairHistory(false);
      setShowTable(false);
      setSuggestionsLoading(false);
      setInsightsLoading(false);
      setInsightsLoaded(false);
      setReviewPct(100);
    });
    void loadSuggestions();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeProjectId]);

  const loadInsights = async (force = false) => {
    if (!activeProjectId) return;
    if ((insightsLoaded || insightsLoading) && !force) {
      return;
    }
    setInsightsLoading(true);
    const projectId = activeProjectId;
    try {
      const res = await api.exploreInsights(projectId);
      if (activeProjectIdRef.current !== projectId) return;
      setBusinessInsights(res.business_insights || []);
      setSemanticHighlights(res.semantic_highlights || []);
      setInsights(res.insights || []);
      setInsightDiagnostics(res.diagnostics || []);
      setInsightsLoaded(true);
    } catch (e) {
      if (activeProjectIdRef.current !== projectId) return;
      setError(e instanceof Error ? e.message : String(e));
      setInsightsLoaded(true);
    } finally {
      if (activeProjectIdRef.current === projectId) setInsightsLoading(false);
    }
  };

  const refreshAll = async () => {
    await Promise.all([loadSuggestions(true), loadInsights(true)]);
  };

  const askQuestion = async (q: string) => {
    if (!activeProjectId) return;
    const projectId = activeProjectId;
    setLoading(true);
    setError("");
    setResult(null);
    setShowTable(false);
    setShowSql(false);
    setShowRepairHistory(false);
    setQuestion(q);
    try {
      const res = await api.exploreAsk(q, projectId);
      if (activeProjectIdRef.current === projectId) setResult(res);
    } catch (e) {
      if (activeProjectIdRef.current === projectId) {
        setError(e instanceof Error ? e.message : String(e));
      }
    }
    if (activeProjectIdRef.current === projectId) setLoading(false);
  };

  const categories = [
    "all",
    ...Array.from(new Set(suggestions.map((s) => s.category))),
  ];
  const filtered =
    filterCategory === "all"
      ? suggestions
      : suggestions.filter((s) => s.category === filterCategory);
  const featuredSuggestions = pickFeaturedSuggestions(filtered);
  const featuredQuestions = new Set(featuredSuggestions.map((s) => s.question));
  const remainingSuggestions = filtered.filter((s) => !featuredQuestions.has(s.question));

  if (error && !suggestions.length) {
    return (
      <div>
        <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-1">
          Analyze
        </div>
        <h1 className="text-2xl font-bold mb-4">Ask a question</h1>
        <div className="bg-card border border-border rounded-lg p-8 max-w-xl">
          <h2 className="text-lg font-semibold mb-2">Nothing to ask about yet</h2>
          <p className="text-sm text-muted mb-4">{error}</p>
          <div className="flex gap-2">
            <a
              href="/sources"
              className="px-4 py-2 bg-accent text-white rounded-md text-sm font-medium"
            >
              Connect a source →
            </a>
            <button
              onClick={() => void loadSuggestions()}
              className="px-4 py-2 border border-border rounded-md text-sm font-medium hover:bg-background"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  const suggestionSummary = summarizeDiagnostics(suggestionDiagnostics);
  const insightSummary = summarizeDiagnostics(insightDiagnostics);
  const validationSummary = summarizeDiagnostics([
    ...suggestionDiagnostics,
    ...insightDiagnostics,
  ]);

  return (
    <div>
      <div className="mb-6 flex flex-col gap-4">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold mb-2">Explore Data</h1>
            <p className="text-muted text-sm max-w-2xl">
              Ask natural language questions about your data. The system decomposes
              questions into metrics and dimensions from the semantic catalog, then
              generates deterministic SQL.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <button
              onClick={() => void loadSuggestions(true)}
              disabled={suggestionsLoading}
              className="px-3 py-1.5 border border-border rounded text-xs font-medium bg-card hover:border-foreground disabled:opacity-50"
            >
              {suggestionsLoading ? "Refreshing questions..." : "Refresh questions"}
            </button>
            <button
              onClick={() => void loadInsights(true)}
              disabled={insightsLoading}
              className="px-3 py-1.5 border border-border rounded text-xs font-medium bg-card hover:border-foreground disabled:opacity-50"
            >
              {insightsLoading ? "Refreshing insights..." : "Refresh insights"}
            </button>
            <button
              onClick={() => void refreshAll()}
              disabled={suggestionsLoading || insightsLoading}
              className="px-3 py-1.5 bg-foreground text-background rounded text-xs font-medium disabled:opacity-50"
            >
              {suggestionsLoading || insightsLoading ? "Refreshing all..." : "Refresh all"}
            </button>
          </div>
        </div>

        <div className="grid gap-3 lg:grid-cols-[1.2fr_0.8fr]">
          <div className="rounded-lg border border-border bg-card p-4">
            <div className="flex flex-wrap items-center justify-between gap-3 mb-3">
              <h2 className="text-xs font-semibold uppercase tracking-wider text-muted">
                Validation
              </h2>
              <span className="text-xs text-muted">
                {validationSummary.families} family types observed
              </span>
            </div>
            <div className="flex flex-wrap gap-2">
              <span className="px-2.5 py-1 rounded-full text-xs border border-border bg-background">
                {validationSummary.counts.generated} generated
              </span>
              <span className="px-2.5 py-1 rounded-full text-xs border border-border bg-background">
                {validationSummary.counts.skipped} skipped
              </span>
              <span className="px-2.5 py-1 rounded-full text-xs border border-border bg-background">
                {validationSummary.counts.failed} failed
              </span>
              <span className="px-2.5 py-1 rounded-full text-xs border border-border bg-background">
                {suggestions.length} questions
              </span>
              <span className="px-2.5 py-1 rounded-full text-xs border border-border bg-background">
                {insights.length} insights
              </span>
            </div>
            <div className="mt-3 space-y-1 text-xs text-muted">
              {validationSummary.topReasons.length === 0 ? (
                <p>No diagnostic reasons reported for the current scope.</p>
              ) : (
                validationSummary.topReasons.map(([reason, count]) => (
                  <div key={reason} className="flex items-start justify-between gap-4">
                    <span className="min-w-0 flex-1">{reason}</span>
                    <span className="shrink-0 font-mono">{count}</span>
                  </div>
                ))
              )}
            </div>
          </div>
          <div className="rounded-lg border border-border bg-card p-4">
            <h2 className="text-xs font-semibold uppercase tracking-wider text-muted mb-3">
              Current Run
            </h2>
            <div className="grid grid-cols-3 gap-2 text-xs">
              <div className="rounded border border-border bg-background p-2">
                <div className="text-muted uppercase tracking-wider">Questions</div>
                <div className="mt-1 font-semibold">{suggestions.length}</div>
              </div>
              <div className="rounded border border-border bg-background p-2">
                <div className="text-muted uppercase tracking-wider">Insights</div>
                <div className="mt-1 font-semibold">{insights.length}</div>
              </div>
              <div className="rounded border border-border bg-background p-2">
                <div className="text-muted uppercase tracking-wider">Warnings</div>
                <div className="mt-1 font-semibold">
                  {suggestionSummary.counts.skipped + insightSummary.counts.skipped}
                </div>
              </div>
            </div>
            <p className="mt-3 text-xs text-muted">
              Refresh questions after framing or model changes. Refresh insights after
              re-running the pipeline or updating source data.
            </p>
          </div>
        </div>
      </div>

      {/* Soft review indicator (non-blocking) */}
      {reviewPct < 100 && (
        <div className="mb-6 p-3 bg-[color-mix(in_srgb,var(--accent)_10%,var(--card))] border border-border rounded-lg flex items-center justify-between">
          <div>
            <span className="text-sm text-foreground">
              {Math.round(reviewPct)}% of tables reviewed in the dictionary.
              Reviewing more tables improves accuracy.
            </span>
          </div>
          <a
            href="/discovery"
            className="px-3 py-1 bg-blue-600 text-white rounded text-xs font-medium hover:bg-blue-700 transition-colors shrink-0 ml-4"
          >
            Review Discovery
          </a>
        </div>
      )}

      {/* Question input */}
      <div className="mb-6">
        <div className="flex gap-2">
          <input
            type="text"
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && question.trim()) askQuestion(question);
            }}
            placeholder="Ask a question about your data..."
            className="flex-1 px-4 py-2 border border-border rounded-lg bg-background text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <button
            onClick={() => askQuestion(question)}
            disabled={!question.trim() || loading}
            className="px-4 py-2 bg-foreground text-background rounded-lg text-sm font-medium disabled:opacity-50 hover:opacity-90 transition-opacity"
          >
            {loading ? "Analyzing..." : "Ask"}
          </button>
        </div>
      </div>

      {/* Result panel */}
      {result && (
        <div className="mb-8 border border-border rounded-lg bg-card">
          <div className="p-4 border-b border-border">
            <div className="flex items-center justify-between">
              <h2 className="font-semibold">{result.question}</h2>
              <div className="flex items-center gap-3 text-xs text-muted">
                <span>{result.row_count} rows</span>
                {result.repaired && (
                  <span className="px-2 py-0.5 rounded bg-[var(--success)]/10 text-[var(--success)] border border-[var(--success)]/20">
                    auto-repaired
                  </span>
                )}
                {result.visualization && (
                  <span className="px-2 py-0.5 rounded bg-background border border-border">
                    {result.visualization.chart_type}
                  </span>
                )}
                <button
                  onClick={() => setShowSql(!showSql)}
                  className="underline hover:text-foreground"
                >
                  {showSql ? "Hide SQL" : "Show SQL"}
                </button>
                {result.repair_history.length > 0 && (
                  <button
                    onClick={() => setShowRepairHistory(!showRepairHistory)}
                    className="underline hover:text-foreground"
                  >
                    {showRepairHistory ? "Hide Repair Log" : "Repair Log"}
                  </button>
                )}
              </div>
            </div>

            {/* Decomposition explanation */}
            {result.explanation && (
              <p className="text-sm text-muted mt-2">{result.explanation}</p>
            )}

            {/* Resolution metadata */}
            {!result.error && result.data.length > 0 && (
              <div className="flex items-center gap-2 mt-2 flex-wrap">
                {result.repaired && (
                  <span className="px-2 py-0.5 rounded text-[10px] font-medium bg-[var(--success)]/10 text-[var(--success)] border border-[var(--success)]/20">
                    Auto-repaired
                  </span>
                )}
                {result.options.length === 0 && (
                  <span className="px-2 py-0.5 rounded text-[10px] font-medium bg-[var(--success)]/10 text-[var(--success)] border border-[var(--success)]/20">
                    Catalog-resolved
                  </span>
                )}
              </div>
            )}

            {result.error && (
              <p className="text-sm text-[var(--danger)] mt-2">{result.error}</p>
            )}
          </div>

          {/* Disambiguation options */}
          {result.options && result.options.length > 0 && (
            <DisambiguationUI
              options={result.options}
              loading={loading}
              question={result.question}
              onSelect={(opt: DimensionOption) => {
                const q = result.question.replace(
                  /by\s+\S+/i,
                  `by ${opt.display_name}`
                );
                askQuestion(
                  q !== result.question
                    ? q
                    : `${result.question} (${opt.display_name})`
                );
              }}
            />
          )}

          {/* Warnings */}
          {result.warnings && result.warnings.length > 0 && (
            <div className="px-4 py-3 border-b border-border">
              <div className="space-y-2">
                {result.warnings.map((w, i) => (
                  <div
                    key={i}
                    className="flex items-start gap-2 p-3 rounded-lg bg-[color-mix(in_srgb,var(--warning)_10%,var(--card))] border border-[var(--warning)]/20"
                  >
                    <span className="text-[var(--warning)] shrink-0 mt-0.5 text-sm">!</span>
                    <p className="text-sm text-foreground">{w}</p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Follow-up suggestions from decomposition */}
          {result.suggestions && result.suggestions.length > 0 && (
            <div className="px-4 py-3 border-b border-border bg-background">
              <div className="text-xs font-semibold text-muted uppercase tracking-wider mb-2">
                Related questions
              </div>
              <div className="flex flex-wrap gap-2">
                {result.suggestions.map((s, i) => (
                  <button
                    key={i}
                    onClick={() => askQuestion(s)}
                    disabled={loading}
                    className="px-3 py-1 text-xs border border-border rounded-full bg-card hover:border-foreground transition-colors disabled:opacity-50"
                  >
                    {s}
                  </button>
                ))}
              </div>
            </div>
          )}

          {showSql && result.sql && (
            <div className="p-4 border-b border-border">
              <SqlViewer sql={result.sql} />
            </div>
          )}

          {showRepairHistory && result.repair_history.length > 0 && (
            <div className="p-4 border-b border-border bg-[color-mix(in_srgb,var(--warning)_10%,var(--card))]">
              <h3 className="text-xs font-semibold text-[var(--warning)] uppercase tracking-wider mb-3">
                Repair History ({result.repair_history.length} attempt{result.repair_history.length > 1 ? "s" : ""})
              </h3>
              <div className="space-y-3">
                {result.repair_history.map((attempt, i) => (
                  <div key={i} className="text-xs border border-border rounded p-3 bg-card">
                    <div className="font-medium text-foreground mb-1">
                      Attempt {i + 1}
                    </div>
                    <pre className="font-mono text-[11px] bg-background p-2 rounded mb-2 overflow-auto whitespace-pre-wrap">
                      {attempt.sql}
                    </pre>
                    <div className="text-[var(--danger)]">
                      {attempt.error}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Chart visualization */}
          {result.visualization &&
            result.visualization.chart_type !== "table" &&
            result.visualization.chart_type !== "kpi" &&
            result.data.length > 0 && (
              <div className="border-b border-border">
                <ResultChart spec={result.visualization} data={result.data} />
              </div>
            )}

          {/* Data table -- shown by default for table/kpi type, toggled for charts */}
          {result.data.length > 0 && (
            <>
              {result.visualization &&
                result.visualization.chart_type !== "table" &&
                result.visualization.chart_type !== "kpi" && (
                  <div className="px-4 py-2 border-b border-border bg-background">
                    <button
                      onClick={() => setShowTable(!showTable)}
                      className="text-xs text-muted underline hover:text-foreground"
                    >
                      {showTable
                        ? "Hide data table"
                        : `Show data table (${result.row_count} rows)`}
                    </button>
                  </div>
                )}
              {(showTable ||
                !result.visualization ||
                result.visualization.chart_type === "table") && (
                <div className="overflow-auto max-h-96">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border bg-background">
                        {Object.keys(result.data[0]).map((col) => (
                          <th
                            key={col}
                            className="px-3 py-2 text-left font-medium text-muted"
                          >
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {result.data.slice(0, 100).map((row, i) => (
                        <tr
                          key={i}
                          className="border-b border-border last:border-0"
                        >
                          {Object.values(row).map((val, j) => (
                            <td key={j} className="px-3 py-2 font-mono text-xs">
                              {val === null ? (
                                <span className="text-muted italic">null</span>
                              ) : typeof val === "number" ? (
                                Number.isInteger(val) ? (
                                  val.toLocaleString()
                                ) : (
                                  val.toFixed(2)
                                )
                              ) : (
                                String(val)
                              )}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}

          {result.visualization &&
            result.visualization.chart_type === "kpi" &&
            result.data.length === 1 && (
              <div className="p-6 flex gap-6 flex-wrap">
                {Object.entries(result.data[0]).map(([key, val]) => (
                  <div key={key} className="text-center">
                    <div className="text-xs text-muted uppercase tracking-wider">
                      {key.replace(/_/g, " ")}
                    </div>
                    <div className="text-3xl font-bold mt-1">
                      {typeof val === "number"
                        ? Number.isInteger(val)
                          ? val.toLocaleString()
                          : val.toFixed(2)
                        : String(val)}
                    </div>
                  </div>
                ))}
              </div>
            )}
        </div>
      )}

      {/* Tabs: Suggested Questions / Statistical Insights */}
      <div className="flex gap-4 mb-4 border-b border-border">
        <button
          onClick={() => setActiveTab("questions")}
          className={`pb-2 text-sm font-medium transition-colors ${
            activeTab === "questions"
              ? "border-b-2 border-foreground text-foreground"
              : "text-muted hover:text-foreground"
          }`}
        >
          Suggested Questions ({suggestions.length})
        </button>
        <button
          onClick={() => {
            setActiveTab("insights");
            void loadInsights();
          }}
          className={`pb-2 text-sm font-medium transition-colors ${
            activeTab === "insights"
              ? "border-b-2 border-foreground text-foreground"
              : "text-muted hover:text-foreground"
          }`}
        >
          Insights ({semanticHighlights.length + businessInsights.length + insights.length})
        </button>
      </div>

      {/* Suggested Questions tab */}
      {activeTab === "questions" && (
        <div>
          {/* Category filter */}
          <div className="flex gap-2 mb-4 flex-wrap">
            {categories.map((cat) => (
              <button
                key={cat}
                onClick={() => setFilterCategory(cat)}
                className={`px-3 py-1 rounded-full text-xs border transition-colors ${
                  filterCategory === cat
                    ? "bg-foreground text-background border-foreground"
                    : "bg-background text-muted border-border hover:border-foreground"
                }`}
              >
                {cat === "all" ? "All" : cat}
              </button>
            ))}
          </div>

          {featuredSuggestions.length > 0 && (
            <div className="mb-6">
              <div className="flex items-end justify-between gap-4 mb-3">
                <div>
                  <div className="text-xs font-semibold uppercase tracking-wider text-muted">
                    Start Here
                  </div>
                  <div className="text-sm text-muted mt-1">
                    Lead questions chosen to surface business movement, dominant segments,
                    and joined context first.
                  </div>
                </div>
              </div>
              <div className="grid gap-3 lg:grid-cols-3">
                {featuredSuggestions.map((s, i) => (
                  <button
                    key={`featured-${s.question}`}
                    onClick={() => askQuestion(s.question)}
                    disabled={loading}
                    className={`text-left border border-border rounded-lg bg-card hover:border-foreground transition-colors disabled:opacity-50 ${
                      i === 0 ? "lg:col-span-2 p-5" : "p-4"
                    }`}
                  >
                    <div className="flex flex-wrap items-center gap-2 mb-3">
                      <span className="px-2 py-0.5 rounded text-[10px] border border-border bg-background text-muted">
                        {i === 0 ? "Featured" : `Pick ${i + 1}`}
                      </span>
                      <span
                        className={`px-2 py-0.5 rounded text-[10px] border ${
                          SOURCE_COLORS[s.source] || "bg-gray-100 text-gray-600"
                        }`}
                      >
                        {SOURCE_LABELS[s.source] || s.source}
                      </span>
                      <span className="px-2 py-0.5 rounded text-[10px] border border-border bg-background text-muted">
                        {questionShape(s.question)}
                      </span>
                    </div>
                    <div className={`${i === 0 ? "text-base" : "text-sm"} font-semibold mb-2`}>
                      {s.question}
                    </div>
                    <div className="text-xs text-muted mb-3">{questionWhy(s)}</div>
                    <div className="flex flex-wrap items-center gap-2 text-[10px] text-muted">
                      <span>{s.category}</span>
                      {s.relevant_tables.length > 0 && (
                        <span>{s.relevant_tables.join(", ")}</span>
                      )}
                    </div>
                  </button>
                ))}
              </div>
            </div>
          )}

          {remainingSuggestions.length > 0 && (
            <div>
              <div className="text-xs font-semibold uppercase tracking-wider text-muted mb-3">
                More Questions
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                {remainingSuggestions.map((s, i) => (
                  <button
                    key={i}
                    onClick={() => askQuestion(s.question)}
                    disabled={loading}
                    className="text-left p-4 border border-border rounded-lg bg-card hover:border-foreground transition-colors disabled:opacity-50"
                  >
                    <div className="flex flex-wrap items-center gap-2 mb-2">
                      <span
                        className={`px-2 py-0.5 rounded text-[10px] border ${
                          SOURCE_COLORS[s.source] || "bg-gray-100 text-gray-600"
                        }`}
                      >
                        {SOURCE_LABELS[s.source] || s.source}
                      </span>
                      <span className="text-[10px] text-muted">{questionShape(s.question)}</span>
                    </div>
                    <div className="text-sm font-medium mb-2">{s.question}</div>
                    <div className="text-xs text-muted mb-2">{questionWhy(s)}</div>
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-[10px] text-muted">{s.category}</span>
                      {s.relevant_tables.length > 0 && (
                        <span className="text-[10px] text-muted">
                          {s.relevant_tables.join(", ")}
                        </span>
                      )}
                    </div>
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Statistical Insights tab */}
      {activeTab === "insights" && (
        <div className="space-y-3">
          {insightsLoading ? (
            <p className="text-muted text-sm">
              Looking for business and statistical signals...
            </p>
          ) : semanticHighlights.length === 0 &&
            businessInsights.length === 0 &&
            insights.length === 0 ? (
            <p className="text-muted text-sm">
              No material signals detected yet. Run the
              pipeline to materialize models and surface insights.
            </p>
          ) : (
            <div className="space-y-3">
              {semanticHighlights.length > 0 && (
                <div className="space-y-3">
                  <div className="text-xs font-semibold uppercase tracking-wider text-muted">
                    Semantic Findings
                  </div>
                  {semanticHighlights.map((highlight) => (
                    <div
                      key={highlight.id}
                      className={`border-l-4 rounded-r-lg p-4 ${
                        SEVERITY_COLORS[highlight.severity] || SEVERITY_COLORS.info
                      }`}
                    >
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1">
                          <div className="flex flex-wrap items-center gap-2 mb-1">
                            <div className="text-sm font-medium">{highlight.title}</div>
                            <span className="px-2 py-0.5 rounded text-[10px] border border-border bg-background text-muted">
                              {highlight.decision_lens}
                            </span>
                          </div>
                          <div className="text-sm text-muted mb-2">{highlight.detail}</div>
                          <div className="flex gap-3 text-xs text-muted flex-wrap">
                            <span>Table: {highlight.table}</span>
                            <span>Type: {highlight.insight_type.replace(/_/g, " ")}</span>
                            {highlight.confidence_level && (
                              <span>Confidence: {highlight.confidence_level}</span>
                            )}
                          </div>
                        </div>
                        <div className="text-right shrink-0 text-xs text-muted">
                          {highlight.support_count !== null && (
                            <div>{highlight.support_count.toLocaleString()} rows</div>
                          )}
                          <div>{highlight.metadata_signals.glossary_terms} glossary</div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
              {businessInsights.length > 0 && (
                <div className="space-y-3">
                  <div className="text-xs font-semibold uppercase tracking-wider text-muted">
                    Business Signals
                  </div>
                  {businessInsights.map((insight) => (
                    <div
                      key={insight.id}
                      className="rounded-lg border border-border bg-card p-4"
                    >
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1">
                          <div className="text-sm font-medium mb-1">{insight.title}</div>
                          <div className="text-sm text-muted mb-2">{insight.detail}</div>
                          <div className="flex gap-3 text-xs text-muted flex-wrap">
                            <span>Table: {insight.table}</span>
                            <span>View: {insight.chart_type}</span>
                            <span>Category: {insight.category}</span>
                          </div>
                        </div>
                        <div className="text-right shrink-0">
                          <div className="text-lg font-bold">
                            {insight.value.toFixed(1)}
                            {insight.unit}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
              {insights.length > 0 && (
                <div className="space-y-3">
                  <div className="text-xs font-semibold uppercase tracking-wider text-muted">
                    Supporting Statistical Signals
                  </div>
              {insights.map((insight, i) => (
                <div
                  key={i}
                  className={`border-l-4 rounded-r-lg p-4 ${
                    SEVERITY_COLORS[insight.severity] || SEVERITY_COLORS.info
                  }`}
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1">
                      <div className="text-sm font-medium mb-1">
                        {insight.description}
                      </div>
                      <div className="flex gap-3 text-xs text-muted flex-wrap">
                        <span>Table: {insight.table_name}</span>
                        <span>Type: {insight.insight_type.replace(/_/g, " ")}</span>
                        {insight.p_value !== null && (
                          <span>p-value: {insight.p_value.toFixed(4)}</span>
                        )}
                        {insight.confidence_level && (
                          <span>Confidence: {insight.confidence_level}</span>
                        )}
                        {insight.time_period && (
                          <span>Period: {insight.time_period}</span>
                        )}
                      </div>
                    </div>
                    <div className="text-right shrink-0">
                      <div className="text-lg font-bold">
                        {insight.magnitude > 0 ? "+" : ""}
                        {insight.magnitude.toFixed(1)}%
                      </div>
                      {insight.z_score !== null && (
                        <div className="text-xs text-muted">
                          z={insight.z_score.toFixed(1)}
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              ))}
                </div>
              )}
            </div>
          )}
          {(suggestionDiagnostics.length > 0 || insightDiagnostics.length > 0) && (
            <div className="mt-6 rounded-lg border border-border bg-card p-4">
              <div className="flex items-center justify-between gap-3 mb-3">
                <div>
                  <h3 className="text-xs font-semibold uppercase tracking-wider text-muted">
                    Execution Diagnostics
                  </h3>
                  <p className="text-xs text-muted mt-1">
                    Why each insight family generated, skipped, or failed in this scope.
                  </p>
                </div>
                <span className="text-xs text-muted">
                  {suggestionDiagnostics.length + insightDiagnostics.length} entries
                </span>
              </div>
              <div className="space-y-2">
                {[...suggestionDiagnostics, ...insightDiagnostics]
                  .slice(0, 8)
                  .map((diag, idx) => (
                    <div
                      key={`${diag.schema_name}:${diag.physical_table}:${diag.family}:${idx}`}
                      className="rounded border border-border bg-background p-3 text-xs"
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="font-semibold text-foreground">
                          {diag.physical_table}
                        </span>
                        <span className="text-muted">{diag.family}</span>
                        <span className="text-muted">• {diag.status}</span>
                        {diag.generated_count > 0 && (
                          <span className="text-muted">
                            • {diag.generated_count} generated
                          </span>
                        )}
                      </div>
                      {diag.reason && (
                        <div className="mt-1 text-muted">{diag.reason}</div>
                      )}
                    </div>
                  ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
