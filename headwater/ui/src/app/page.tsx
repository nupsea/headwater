"use client";

import { useEffect, useState } from "react";
import {
  api,
  type StatusResponse,
  type InsightsResponse,
  type PipelineRunResponse,
} from "@/lib/api";
import { WorkflowProgress } from "@/components/workflow-progress";
import { DataSummary } from "@/components/data-summary";
import { AdvisoryActions } from "@/components/advisory-actions";
import { CompletenessChart } from "@/components/completeness-chart";
import { DomainMap } from "@/components/domain-map";
import { RelationshipDiagram } from "@/components/relationship-diagram";

export default function DashboardPage() {
  const [, setStatus] = useState<StatusResponse | null>(null);
  const [insights, setInsights] = useState<InsightsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [phase, setPhase] = useState("");
  const [sourcePath, setSourcePath] = useState("/data/sample");
  const [error, setError] = useState("");

  const refresh = async () => {
    try {
      const s = await api.status();
      setStatus(s);
      if (s.discovered) {
        const ins = await api.insights();
        setInsights(ins);
      }
    } catch {
      /* server not ready */
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  const runFullPipeline = async () => {
    setLoading(true);
    setError("");
    setPhase("Discovering, profiling, modeling, and validating...");
    try {
      const result: PipelineRunResponse = await api.pipelineRun(sourcePath);
      setPhase(
        `Done: ${result.tables_discovered} tables, ` +
          `${result.profiles} profiles, ${result.relationships} relationships, ` +
          `${result.quality_passed}/${result.quality_total} quality checks passed.`
      );
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setPhase("");
    }
    setLoading(false);
  };

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold">Headwater</h1>
          <p className="text-sm text-muted mt-0.5">
            Advisory data platform -- discover, profile, model, validate
          </p>
        </div>
        {/* Re-run inline in header when data exists */}
        {insights && (
          <div className="flex items-center gap-3">
            <input
              type="text"
              value={sourcePath}
              onChange={(e) => setSourcePath(e.target.value)}
              className="w-56 px-3 py-1.5 border border-border rounded bg-background text-xs font-mono"
            />
            <button
              onClick={runFullPipeline}
              disabled={loading}
              className="px-4 py-1.5 bg-accent text-white rounded text-xs font-medium hover:opacity-90 disabled:opacity-50 whitespace-nowrap"
            >
              {loading ? "Running..." : "Re-run"}
            </button>
          </div>
        )}
      </div>

      {/* Pipeline launcher (first run) */}
      {!insights && (
        <div className="bg-card border border-border rounded-lg p-8 mb-6 text-center max-w-xl mx-auto">
          <h2 className="text-lg font-semibold mb-2">
            Discover Your Data
          </h2>
          <p className="text-sm text-muted mb-6">
            Point Headwater at a dataset. In seconds, get a profiled,
            documented, relationship-mapped, quality-baselined analytical
            warehouse -- ready for review.
          </p>
          <div className="flex items-end gap-3 justify-center">
            <div className="text-left">
              <label className="text-xs text-muted block mb-1">
                Data source path
              </label>
              <input
                type="text"
                value={sourcePath}
                onChange={(e) => setSourcePath(e.target.value)}
                className="w-72 px-3 py-2 border border-border rounded bg-background text-sm font-mono"
              />
            </div>
            <button
              onClick={runFullPipeline}
              disabled={loading}
              className="px-6 py-2 bg-accent text-white rounded text-sm font-medium hover:opacity-90 disabled:opacity-50"
            >
              {loading ? "Running..." : "Run Full Pipeline"}
            </button>
          </div>
          {phase && (
            <div className="text-sm mt-4 p-3 bg-background rounded border border-border">
              {phase}
            </div>
          )}
          {error && (
            <div className="text-sm mt-4 p-3 bg-danger/10 rounded border border-danger/30 text-danger">
              {error}
            </div>
          )}
        </div>
      )}

      {/* Full dashboard */}
      {insights && (
        <div className="space-y-5">
          {/* Row 1: Workflow progress bar */}
          <WorkflowProgress workflow={insights.workflow} />

          {/* Row 2: Visual metrics -- the "wow we found all this" moment */}
          <DataSummary
            profile={insights.data_profile}
            overview={insights.overview}
          />

          {/* Row 3: Completeness chart + Next Steps side by side */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
            <CompletenessChart tables={insights.table_health} />
            <AdvisoryActions actions={insights.advisory_actions} />
          </div>

          {/* Row 4: Domain map + Relationships side by side */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
            <DomainMap domains={insights.domains} />
            <RelationshipDiagram
              tables={insights.table_health}
              relationships={insights.relationship_map}
            />
          </div>
        </div>
      )}
    </div>
  );
}
