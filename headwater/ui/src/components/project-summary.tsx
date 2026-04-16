"use client";

import { type Project, type ProjectProgress } from "@/lib/api";
import { ConfidenceDot } from "@/components/confidence-dot";

const MATURITY_STAGES = ["raw", "profiled", "documented", "modeled", "production"] as const;

const MATURITY_LABELS: Record<string, string> = {
  raw: "Raw",
  profiled: "Profiled",
  documented: "Documented",
  modeled: "Modeled",
  production: "Production",
};

function ProgressFraction({
  label,
  done,
  total,
  sub,
}: {
  label: string;
  done: number;
  total: number;
  sub?: string;
}) {
  const pct = total > 0 ? (done / total) * 100 : 0;
  return (
    <div>
      <div className="flex items-baseline justify-between mb-1">
        <span className="text-xs text-muted">{label}</span>
        <span className="text-sm font-bold font-mono">
          {done}/{total}
        </span>
      </div>
      <div className="h-1 bg-border rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all ${
            pct >= 100
              ? "bg-green-500"
              : pct >= 50
              ? "bg-accent"
              : "bg-yellow-500"
          }`}
          style={{ width: `${Math.min(pct, 100)}%` }}
        />
      </div>
      {sub && <div className="text-[10px] text-muted mt-0.5">{sub}</div>}
    </div>
  );
}

export function ProjectSummary({
  project,
  progress,
}: {
  project: Project;
  progress?: ProjectProgress;
}) {
  const p = progress;
  const stageIdx = MATURITY_STAGES.indexOf(
    project.maturity as (typeof MATURITY_STAGES)[number]
  );

  return (
    <div className="bg-card border border-border rounded-lg p-5">
      {/* Header row: name + maturity + confidence */}
      <div className="flex items-start justify-between mb-4">
        <div>
          <h2 className="text-lg font-semibold">{project.display_name}</h2>
          {project.description && (
            <p className="text-xs text-muted mt-1 max-w-xl line-clamp-2">
              {project.description}
            </p>
          )}
        </div>
        <div className="text-right shrink-0 ml-4">
          <div className="text-3xl font-bold">
            {Math.round(project.catalog_confidence * 100)}%
          </div>
          <div className="text-[10px] text-muted uppercase tracking-wider">
            Catalog Confidence
          </div>
          <ConfidenceDot value={project.catalog_confidence} />
        </div>
      </div>

      {/* Maturity gauge */}
      <div className="mb-5">
        <div className="flex items-center gap-1 mb-1.5">
          {MATURITY_STAGES.map((stage, i) => (
            <div key={stage} className="flex-1 flex flex-col items-center">
              <div
                className={`h-2 w-full rounded-sm transition-colors ${
                  i <= stageIdx
                    ? i === stageIdx
                      ? "bg-accent"
                      : "bg-accent/60"
                    : "bg-border"
                }`}
              />
              <span
                className={`text-[9px] mt-1 ${
                  i === stageIdx
                    ? "text-accent font-semibold"
                    : i < stageIdx
                    ? "text-muted"
                    : "text-border"
                }`}
              >
                {MATURITY_LABELS[stage]}
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* Progress grid */}
      {p && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          <ProgressFraction
            label="Tables Reviewed"
            done={p.tables_reviewed}
            total={p.tables_discovered}
            sub={`${p.tables_profiled} profiled, ${p.tables_modeled} modeled`}
          />
          <ProgressFraction
            label="Columns Confirmed"
            done={p.columns_confirmed}
            total={p.columns_total}
            sub={`${p.columns_described} described`}
          />
          <ProgressFraction
            label="Metrics Confirmed"
            done={p.metrics_confirmed ?? 0}
            total={p.metrics_defined}
          />
          <ProgressFraction
            label="Dimensions Confirmed"
            done={p.dimensions_confirmed ?? 0}
            total={p.dimensions_defined}
          />
          <ProgressFraction
            label="Relationships"
            done={p.relationships_confirmed}
            total={p.relationships_detected}
          />
          <ProgressFraction
            label="Quality Contracts"
            done={p.contracts_enforcing}
            total={p.quality_contracts}
            sub={p.contracts_enforcing > 0 ? `${p.contracts_enforcing} enforcing` : "observing"}
          />
          <div className="col-span-2">
            <div className="flex items-baseline justify-between mb-1">
              <span className="text-xs text-muted">Catalog Coverage</span>
              <span className="text-sm font-bold font-mono">
                {Math.round(p.catalog_coverage * 100)}%
              </span>
            </div>
            <div className="h-1 bg-border rounded-full overflow-hidden">
              <div
                className="h-full bg-accent rounded-full transition-all"
                style={{ width: `${p.catalog_coverage * 100}%` }}
              />
            </div>
            <div className="text-[10px] text-muted mt-0.5">
              Analytical columns represented in the semantic catalog
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
