import type { DataProfile, InsightsResponse } from "@/lib/api";

/* SVG donut chart for a single percentage */
function DonutGauge({
  pct,
  label,
  sub,
  color,
  size = 88,
}: {
  pct: number;
  label: string;
  sub: string;
  color: string;
  size?: number;
}) {
  const r = (size - 10) / 2;
  const circ = 2 * Math.PI * r;
  const offset = circ - (pct / 100) * circ;

  return (
    <div className="flex flex-col items-center">
      <svg width={size} height={size} className="-rotate-90">
        <circle
          cx={size / 2}
          cy={size / 2}
          r={r}
          fill="none"
          stroke="var(--border)"
          strokeWidth={6}
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={r}
          fill="none"
          stroke={color}
          strokeWidth={6}
          strokeLinecap="round"
          strokeDasharray={circ}
          strokeDashoffset={offset}
          className="transition-all duration-700"
        />
      </svg>
      <div className="text-lg font-bold font-mono -mt-14">{pct}%</div>
      <div className="text-xs font-medium mt-6">{label}</div>
      <div className="text-[11px] text-muted">{sub}</div>
    </div>
  );
}

/* Big number card */
function BigStat({
  value,
  label,
  sub,
}: {
  value: string;
  label: string;
  sub: string;
}) {
  return (
    <div className="text-center">
      <div className="text-3xl font-bold font-mono">{value}</div>
      <div className="text-xs font-medium mt-1">{label}</div>
      <div className="text-[11px] text-muted">{sub}</div>
    </div>
  );
}

export function DataSummary({
  profile,
  overview,
}: {
  profile: DataProfile;
  overview: InsightsResponse["overview"];
}) {
  const qualityPct = profile.quality.pass_rate_pct ?? 0;
  const fkPct = profile.fk_integrity.avg_integrity_pct ?? 0;

  return (
    <div className="bg-card border border-border rounded-lg p-6">
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-6 items-start">
        {/* Big numbers */}
        <BigStat
          value={`${overview.total_tables}`}
          label="Tables Discovered"
          sub={`${overview.total_columns} columns`}
        />
        <BigStat
          value={overview.total_rows.toLocaleString()}
          label="Total Rows"
          sub={`${overview.total_cells.toLocaleString()} cells`}
        />

        {/* Visual gauges */}
        <DonutGauge
          pct={profile.completeness_pct}
          label="Completeness"
          sub="cells with values"
          color="var(--success)"
        />
        <DonutGauge
          pct={
            Math.round(
              (profile.pk_coverage.tables_with_pk /
                Math.max(profile.pk_coverage.total_tables, 1)) *
                100
            )
          }
          label="PK Coverage"
          sub={`${profile.pk_coverage.tables_with_pk} of ${profile.pk_coverage.total_tables} tables`}
          color="var(--accent)"
        />
        <DonutGauge
          pct={fkPct}
          label="FK Integrity"
          sub={
            profile.fk_integrity.total_relationships > 0
              ? `${profile.fk_integrity.total_relationships} relationships`
              : "none detected"
          }
          color="var(--warning)"
        />
        <DonutGauge
          pct={qualityPct}
          label="Quality Checks"
          sub={
            profile.quality.total > 0
              ? `${profile.quality.passed} of ${profile.quality.total} pass`
              : "not yet run"
          }
          color={qualityPct >= 90 ? "var(--success)" : qualityPct >= 70 ? "var(--warning)" : "var(--danger)"}
        />
      </div>
    </div>
  );
}
