"use client";

import { useEffect, useState } from "react";
import {
  api,
  type SuggestedQuestion,
  type StatisticalInsight,
  type ExplorationResult,
} from "@/lib/api";
import { ResultChart } from "@/components/result-chart";
import { SqlViewer } from "@/components/sql-viewer";

const SOURCE_COLORS: Record<string, string> = {
  mart: "bg-blue-100 text-blue-800 border-blue-200",
  relationship: "bg-purple-100 text-purple-800 border-purple-200",
  quality: "bg-amber-100 text-amber-800 border-amber-200",
  semantic: "bg-green-100 text-green-800 border-green-200",
  statistical: "bg-rose-100 text-rose-800 border-rose-200",
};

const SEVERITY_COLORS: Record<string, string> = {
  info: "border-l-blue-400 bg-blue-50",
  warning: "border-l-amber-400 bg-amber-50",
  critical: "border-l-red-400 bg-red-50",
};

export default function ExplorePage() {
  const [suggestions, setSuggestions] = useState<SuggestedQuestion[]>([]);
  const [insights, setInsights] = useState<StatisticalInsight[]>([]);
  const [question, setQuestion] = useState("");
  const [result, setResult] = useState<ExplorationResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [filterCategory, setFilterCategory] = useState("all");
  const [showSql, setShowSql] = useState(false);
  const [showRepairHistory, setShowRepairHistory] = useState(false);
  const [showTable, setShowTable] = useState(false);
  const [activeTab, setActiveTab] = useState<"questions" | "insights">(
    "questions"
  );

  const [reviewRequired, setReviewRequired] = useState(false);

  useEffect(() => {
    api
      .exploreSuggestions()
      .then((res: Record<string, unknown>) => {
        setSuggestions(
          (res.suggestions as typeof suggestions) || []
        );
        setInsights(
          (res.insights as typeof insights) || []
        );
        if (res.review_required) setReviewRequired(true);
      })
      .catch(() => setError("Run the pipeline from the Dashboard first."));
  }, []);

  const askQuestion = async (q: string) => {
    setLoading(true);
    setError("");
    setResult(null);
    setShowTable(false);
    setShowSql(false);
    setShowRepairHistory(false);
    setQuestion(q);
    try {
      const res = await api.exploreAsk(q);
      setResult(res);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setLoading(false);
  };

  const categories = [
    "all",
    ...Array.from(new Set(suggestions.map((s) => s.category))),
  ];
  const filtered =
    filterCategory === "all"
      ? suggestions
      : suggestions.filter((s) => s.category === filterCategory);

  if (error && !suggestions.length) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-4">Explore Data</h1>
        <div className="bg-card border border-border rounded-lg p-8 max-w-xl mx-auto text-center">
          <h2 className="text-lg font-semibold mb-2">No Data to Explore Yet</h2>
          <p className="text-sm text-muted mb-4">
            Ask natural language questions about your data and get instant answers
            with visualizations. The explorer works on top of your materialized
            staging and mart models.
          </p>
          <p className="text-sm text-muted mb-4">
            Run the full pipeline from the Dashboard first, or use the CLI:
          </p>
          <div className="bg-background border border-border rounded p-4 text-left text-sm font-mono text-muted">
            <p className="mb-1">headwater demo</p>
            <p>headwater discover --source /path/to/data</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <h1 className="text-2xl font-bold mb-2">Explore Data</h1>
      <p className="text-muted text-sm mb-6">
        Ask natural language questions about your data. The system generates SQL
        from your curated metadata and executes it against the analytical
        database.
      </p>

      {/* Dictionary review gate */}
      {reviewRequired && (
        <div className="mb-6 p-4 bg-amber-50 border border-amber-200 rounded-lg">
          <h3 className="text-sm font-semibold text-amber-800 mb-1">
            Review Required
          </h3>
          <p className="text-sm text-amber-900 mb-2">
            Some tables have not been reviewed yet. The explorer only generates
            queries for reviewed tables. Review table metadata in the Data
            Dictionary to enable full exploration.
          </p>
          <a
            href="/dictionary"
            className="inline-block px-3 py-1.5 bg-amber-600 text-white rounded text-sm font-medium hover:bg-amber-700 transition-colors"
          >
            Go to Data Dictionary
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
                  <span className="px-2 py-0.5 rounded bg-green-100 text-green-800 border border-green-200">
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
            {result.error && (
              <p className="text-sm text-red-600 mt-2">{result.error}</p>
            )}
          </div>

          {result.warnings && result.warnings.length > 0 && (
            <div className="px-4 py-3 border-b border-border bg-amber-50 border-l-4 border-l-amber-400">
              <div className="text-xs font-semibold text-amber-800 uppercase tracking-wider mb-1">
                Grounding Warning
              </div>
              {result.warnings.map((w, i) => (
                <p key={i} className="text-sm text-amber-900">{w}</p>
              ))}
            </div>
          )}

          {showSql && result.sql && (
            <div className="p-4 border-b border-border">
              <SqlViewer sql={result.sql} />
            </div>
          )}

          {showRepairHistory && result.repair_history.length > 0 && (
            <div className="p-4 border-b border-border bg-amber-50">
              <h3 className="text-xs font-semibold text-amber-800 uppercase tracking-wider mb-3">
                Repair History ({result.repair_history.length} attempt{result.repair_history.length > 1 ? "s" : ""})
              </h3>
              <div className="space-y-3">
                {result.repair_history.map((attempt, i) => (
                  <div key={i} className="text-xs border border-amber-200 rounded p-3 bg-white">
                    <div className="font-medium text-amber-900 mb-1">
                      Attempt {i + 1}
                    </div>
                    <pre className="font-mono text-[11px] bg-amber-50 p-2 rounded mb-2 overflow-auto whitespace-pre-wrap">
                      {attempt.sql}
                    </pre>
                    <div className="text-red-700">
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
          onClick={() => setActiveTab("insights")}
          className={`pb-2 text-sm font-medium transition-colors ${
            activeTab === "insights"
              ? "border-b-2 border-foreground text-foreground"
              : "text-muted hover:text-foreground"
          }`}
        >
          Statistical Insights ({insights.length})
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

          {/* Questions grid */}
          <div className="grid gap-3 md:grid-cols-2">
            {filtered.map((s, i) => (
              <button
                key={i}
                onClick={() => askQuestion(s.question)}
                disabled={loading}
                className="text-left p-4 border border-border rounded-lg bg-card hover:border-foreground transition-colors disabled:opacity-50"
              >
                <div className="text-sm font-medium mb-2">{s.question}</div>
                <div className="flex items-center gap-2">
                  <span
                    className={`px-2 py-0.5 rounded text-[10px] border ${
                      SOURCE_COLORS[s.source] || "bg-gray-100 text-gray-600"
                    }`}
                  >
                    {s.source}
                  </span>
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

      {/* Statistical Insights tab */}
      {activeTab === "insights" && (
        <div className="space-y-3">
          {insights.length === 0 ? (
            <p className="text-muted text-sm">
              No statistically significant patterns detected yet. Run the
              pipeline to materialize models and surface insights.
            </p>
          ) : (
            insights.map((insight, i) => (
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
            ))
          )}
        </div>
      )}
    </div>
  );
}
