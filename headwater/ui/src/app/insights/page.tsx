"use client";

import { useEffect, useState } from "react";
import {
  api,
  type InsightsResponse,
  type StatisticalInsight,
  type SuggestedQuestion,
  type ExploreSuggestionsResponse,
} from "@/lib/api";

/* ────── Colour helpers ────── */

const DOMAIN_COLORS: Record<string, string> = {
  Environmental: "#4f46e5",
  Operational: "#0891b2",
  Compliance: "#d97706",
  Geographic: "#16a34a",
};

const SEVERITY_BADGE: Record<
  string,
  { bg: string; fg: string; border: string }
> = {
  critical: { bg: "#fef2f2", fg: "#dc2626", border: "#fecaca" },
  warning: { bg: "#fffbeb", fg: "#d97706", border: "#fde68a" },
  info: { bg: "#eff6ff", fg: "#2563eb", border: "#dbeafe" },
};

export default function InsightsPage() {
  const [insights, setInsights] = useState<InsightsResponse | null>(null);
  const [statInsights, setStatInsights] = useState<StatisticalInsight[]>([]);
  const [error, setError] = useState("");

  useEffect(() => {
    api
      .insights()
      .then(setInsights)
      .catch(() =>
        setError("Run the pipeline from the Dashboard first.")
      );

    api
      .exploreSuggestions()
      .then((res: ExploreSuggestionsResponse) => {
        setStatInsights(res.insights || []);
      })
      .catch(() => {});
  }, []);

  if (error) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-4">Insights</h1>
        <div className="bg-card border border-border rounded-lg p-8 max-w-xl mx-auto text-center">
          <h2 className="text-lg font-semibold mb-2">No Data Yet</h2>
          <p className="text-sm text-muted mb-4">
            The Insights page shows data health, key findings, anomalies, and
            quality metrics after you run the Headwater pipeline.
          </p>
        </div>
      </div>
    );
  }

  if (!insights) {
    return (
      <div>
        <h1 className="text-2xl font-bold mb-4">Insights</h1>
        <div className="space-y-4">
          <div className="h-32 bg-card border border-border rounded-xl animate-pulse" />
          <div className="grid grid-cols-2 gap-4">
            <div className="h-24 bg-card border border-border rounded-xl animate-pulse" />
            <div className="h-24 bg-card border border-border rounded-xl animate-pulse" />
          </div>
        </div>
      </div>
    );
  }

  /* ────── Computed values ────── */

  const dp = insights.data_profile;
  const ov = insights.overview;
  const healthScore = Math.round(
    (dp.completeness_pct * 0.3 +
      (dp.quality.pass_rate_pct ?? 100) * 0.4 +
      (dp.pk_coverage.tables_with_pk / Math.max(1, dp.pk_coverage.total_tables)) * 100 * 0.15 +
      ((dp.fk_integrity.avg_integrity_pct ?? 100)) * 0.15)
  );

  const qualityPassed = dp.quality.passed;
  const qualityTotal = dp.quality.total;
  const completeness = dp.completeness_pct;
  const catalogConf = insights.catalog_health?.catalog_confidence ?? 72;

  /* ────── Key findings from column issues ────── */

  interface Finding {
    id: string;
    type: "error" | "warning" | "info" | "success";
    icon: string;
    title: string;
    detail: string;
    table: string;
    action: string;
  }

  const keyFindings: Finding[] = [];

  // High null columns
  if (dp.high_null_columns > 0) {
    const nullCols = insights.null_analysis.filter((n) => n.null_rate > 0.1);
    if (nullCols.length > 0) {
      const worst = nullCols.sort((a, b) => b.null_rate - a.null_rate)[0];
      keyFindings.push({
        id: "null1",
        type: "error",
        icon: "⚠",
        title: `${Math.round(worst.null_rate * 100)}% data missing in ${worst.column}`,
        detail: `${worst.table}.${worst.column} has ${worst.null_count.toLocaleString()} null values out of ${worst.total_rows.toLocaleString()} rows.`,
        table: worst.table,
        action: "Review source",
      });
    }
  }

  // Weak FK integrity
  insights.relationship_map
    .filter((r) => r.integrity < 1.0)
    .slice(0, 2)
    .forEach((r, i) => {
      keyFindings.push({
        id: `fk${i}`,
        type: "warning",
        icon: "↑",
        title: `${Math.round(r.integrity * 100)}% integrity: ${r.from_table} → ${r.to_table}`,
        detail: `Foreign key ${r.from_column} → ${r.to_table}.${r.to_column} has ${Math.round(r.integrity * 100)}% referential integrity. JOINs will lose ${Math.round((1 - r.integrity) * 100)}% of rows.`,
        table: r.from_table,
        action: "View relationship",
      });
    });

  // Perfect integrity
  const perfectFKs = insights.relationship_map.filter(
    (r) => r.integrity === 1.0
  );
  if (perfectFKs.length > 0) {
    keyFindings.push({
      id: "fk_perfect",
      type: "success",
      icon: "✓",
      title: `${perfectFKs.length} relationships with 100% integrity`,
      detail: `${perfectFKs.length} of ${insights.relationship_map.length} foreign key relationships resolve perfectly. Strong foundation for cross-domain analysis.`,
      table: "schema",
      action: "View model",
    });
  }

  // Column issues
  insights.column_issues.slice(0, 2).forEach((ci, i) => {
    const worst = ci.issues[0];
    if (worst) {
      keyFindings.push({
        id: `ci${i}`,
        type: worst.severity === "error" ? "error" : "info",
        icon: worst.severity === "error" ? "⚠" : "⏱",
        title: `${ci.column}: ${worst.message}`,
        detail: worst.detail,
        table: ci.table,
        action: "Explore",
      });
    }
  });

  const findingColors: Record<string, [string, string, string]> = {
    error: ["#fef2f2", "#dc2626", "#fecaca"],
    warning: ["#fffbeb", "#d97706", "#fde68a"],
    info: ["#eff6ff", "#2563eb", "#dbeafe"],
    success: ["#f0fdf4", "#16a34a", "#bbf7d0"],
  };

  /* ────── Domain breakdown ────── */

  const domains = Object.entries(insights.domains).map(([name, d]) => ({
    name,
    tables: d.tables.length,
    rows: d.total_rows,
    color: DOMAIN_COLORS[name] || "#64748b",
  }));

  /* ────── Quality contracts per table ────── */

  type QualityByTable = { table: string; count: number; status: "pass" | "fail" };
  const qualityByTable: QualityByTable[] = [];
  const tableSet = new Set<string>();
  insights.table_health.forEach((t) => {
    if (!tableSet.has(t.name)) {
      tableSet.add(t.name);
      qualityByTable.push({ table: t.name, count: t.column_count, status: "pass" });
    }
  });

  return (
    <div style={{ maxWidth: 900 }}>
      {/* Header */}
      <div style={{ marginBottom: 28 }}>
        <div
          style={{
            fontSize: 10,
            fontWeight: 700,
            color: "#94a3b8",
            textTransform: "uppercase",
            letterSpacing: "0.08em",
            marginBottom: 6,
          }}
        >
          Insights & Exploration
        </div>
        <h1
          style={{
            fontSize: 24,
            fontWeight: 700,
            letterSpacing: "-0.03em",
            color: "var(--foreground)",
          }}
        >
          What your data is telling you
        </h1>
        <p
          style={{
            fontSize: 14,
            color: "#64748b",
            marginTop: 6,
            lineHeight: 1.6,
          }}
        >
          Auto-generated from {ov.total_tables} profiled tables ·{" "}
          {ov.total_rows.toLocaleString()} records ·{" "}
          {ov.total_relationships} relationships ·{" "}
          {qualityTotal} quality contracts.
        </p>
      </div>

      {/* Health score row */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "auto 1fr 1fr 1fr",
          gap: 16,
          marginBottom: 28,
          alignItems: "center",
        }}
      >
        {/* Donut ring */}
        <div style={{ position: "relative", width: 120, height: 120, flexShrink: 0 }}>
          <div
            style={{
              width: 120,
              height: 120,
              borderRadius: "50%",
              background: `conic-gradient(#4f46e5 0deg ${healthScore * 3.6}deg, var(--border) ${healthScore * 3.6}deg 360deg)`,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
            }}
          >
            <div
              style={{
                width: 86,
                height: 86,
                borderRadius: "50%",
                background: "var(--background)",
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
              }}
            >
              <span
                style={{
                  fontSize: 22,
                  fontWeight: 800,
                  color: "var(--foreground)",
                  letterSpacing: "-0.04em",
                  lineHeight: 1,
                }}
              >
                {healthScore}%
              </span>
              <span
                style={{
                  fontSize: 9,
                  color: "#94a3b8",
                  fontWeight: 600,
                  textTransform: "uppercase",
                  letterSpacing: "0.06em",
                }}
              >
                health
              </span>
            </div>
          </div>
        </div>

        {/* Metric tiles */}
        {[
          {
            label: "Completeness",
            value: `${completeness}%`,
            sub: `${ov.total_columns} columns`,
            color: completeness >= 80 ? "#16a34a" : "#d97706",
            bg: completeness >= 80 ? "#f0fdf4" : "#fffbeb",
            border: completeness >= 80 ? "#bbf7d0" : "#fde68a",
          },
          {
            label: "Quality",
            value: `${dp.quality.pass_rate_pct ?? 0}%`,
            sub: `${qualityPassed}/${qualityTotal} passing`,
            color: (dp.quality.pass_rate_pct ?? 0) >= 90 ? "#16a34a" : "#d97706",
            bg: (dp.quality.pass_rate_pct ?? 0) >= 90 ? "#f0fdf4" : "#fffbeb",
            border: (dp.quality.pass_rate_pct ?? 0) >= 90 ? "#bbf7d0" : "#fde68a",
          },
          {
            label: "Catalog",
            value: `${Math.round(catalogConf)}%`,
            sub: "confidence",
            color: catalogConf >= 70 ? "#16a34a" : "#d97706",
            bg: catalogConf >= 70 ? "#f0fdf4" : "#fffbeb",
            border: catalogConf >= 70 ? "#bbf7d0" : "#fde68a",
          },
        ].map((m) => (
          <div
            key={m.label}
            style={{
              padding: "16px 18px",
              background: m.bg,
              border: `1px solid ${m.border}`,
              borderRadius: 12,
            }}
          >
            <div
              style={{
                fontSize: 11,
                fontWeight: 600,
                color: m.color,
                textTransform: "uppercase",
                letterSpacing: "0.07em",
                marginBottom: 4,
              }}
            >
              {m.label}
            </div>
            <div
              style={{
                fontSize: 28,
                fontWeight: 800,
                color: "var(--foreground)",
                letterSpacing: "-0.04em",
                lineHeight: 1,
              }}
            >
              {m.value}
            </div>
            <div style={{ fontSize: 11, color: m.color, marginTop: 4 }}>
              {m.sub}
            </div>
          </div>
        ))}
      </div>

      {/* Key findings */}
      {keyFindings.length > 0 && (
        <div style={{ marginBottom: 28 }}>
          <div
            style={{
              fontSize: 10,
              fontWeight: 700,
              color: "#94a3b8",
              textTransform: "uppercase",
              letterSpacing: "0.08em",
              marginBottom: 10,
            }}
          >
            Key findings
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr 1fr",
              gap: 12,
            }}
          >
            {keyFindings.map((f) => {
              const [bg, fg, border] =
                findingColors[f.type] || findingColors.info;
              return (
                <div
                  key={f.id}
                  style={{
                    padding: "16px 18px",
                    background: bg,
                    border: `1px solid ${border}`,
                    borderRadius: 12,
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      alignItems: "flex-start",
                      gap: 10,
                      marginBottom: 8,
                    }}
                  >
                    <div
                      style={{
                        width: 28,
                        height: 28,
                        borderRadius: 8,
                        background: fg,
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        flexShrink: 0,
                      }}
                    >
                      <span style={{ color: "#fff", fontSize: 14 }}>
                        {f.icon}
                      </span>
                    </div>
                    <div
                      style={{
                        fontWeight: 600,
                        fontSize: 13,
                        color: "var(--foreground)",
                        lineHeight: 1.3,
                      }}
                    >
                      {f.title}
                    </div>
                  </div>
                  <p
                    style={{
                      fontSize: 12,
                      color: "#475569",
                      lineHeight: 1.6,
                      margin: "0 0 10px",
                    }}
                  >
                    {f.detail}
                  </p>
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                    }}
                  >
                    <span
                      style={{
                        fontSize: 10,
                        color: fg,
                        fontFamily: "var(--font-geist-mono), monospace",
                        background: `${fg}18`,
                        padding: "2px 8px",
                        borderRadius: 4,
                      }}
                    >
                      {f.table}
                    </span>
                    <a
                      href="/discovery"
                      style={{
                        fontSize: 11,
                        color: fg,
                        background: "none",
                        border: "none",
                        cursor: "pointer",
                        textDecoration: "none",
                      }}
                    >
                      {f.action} →
                    </a>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Anomalies + Domain breakdown */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 20,
          marginBottom: 28,
        }}
      >
        {/* Statistical anomalies */}
        <div
          style={{
            background: "var(--card)",
            border: "1px solid var(--border)",
            borderRadius: 12,
            padding: "18px 20px",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              marginBottom: 14,
            }}
          >
            <div
              style={{
                width: 8,
                height: 8,
                borderRadius: "50%",
                background:
                  statInsights.length > 0 ? "#dc2626" : "#16a34a",
              }}
            />
            <div
              style={{
                fontSize: 10,
                fontWeight: 700,
                color: "#94a3b8",
                textTransform: "uppercase",
                letterSpacing: "0.08em",
              }}
            >
              {statInsights.length > 0
                ? `${statInsights.length} anomalies detected`
                : "No anomalies detected"}
            </div>
          </div>
          {statInsights.length > 0
            ? statInsights.slice(0, 5).map((a, i) => {
                const sev = SEVERITY_BADGE[a.severity] || SEVERITY_BADGE.info;
                return (
                  <div
                    key={i}
                    style={{
                      display: "flex",
                      alignItems: "flex-start",
                      gap: 10,
                      paddingBottom: i < Math.min(statInsights.length, 5) - 1 ? 12 : 0,
                      marginBottom: i < Math.min(statInsights.length, 5) - 1 ? 12 : 0,
                      borderBottom:
                        i < Math.min(statInsights.length, 5) - 1
                          ? "1px solid var(--border)"
                          : "none",
                    }}
                  >
                    <div
                      style={{
                        width: 6,
                        height: 6,
                        borderRadius: "50%",
                        flexShrink: 0,
                        marginTop: 5,
                        background: sev.fg,
                      }}
                    />
                    <div style={{ flex: 1 }}>
                      <div
                        style={{
                          fontSize: 12,
                          fontWeight: 600,
                          color: "var(--foreground)",
                        }}
                      >
                        {a.metric}
                      </div>
                      <div
                        style={{ fontSize: 11, color: "#64748b", marginTop: 2 }}
                      >
                        {a.description}
                      </div>
                      <div
                        style={{
                          fontSize: 10,
                          color: "#94a3b8",
                          marginTop: 2,
                          fontFamily: "var(--font-geist-mono), monospace",
                        }}
                      >
                        {a.table_name}
                      </div>
                    </div>
                    <span
                      style={{
                        fontSize: 9,
                        fontWeight: 700,
                        padding: "2px 7px",
                        borderRadius: 4,
                        flexShrink: 0,
                        background: sev.bg,
                        color: sev.fg,
                        textTransform: "uppercase",
                      }}
                    >
                      {a.severity}
                    </span>
                  </div>
                );
              })
            : (
              <div style={{ fontSize: 12, color: "#94a3b8", padding: "8px 0" }}>
                All metrics within normal range. Run the pipeline to surface statistical patterns.
              </div>
            )}
        </div>

        {/* Domain breakdown */}
        <div
          style={{
            background: "var(--card)",
            border: "1px solid var(--border)",
            borderRadius: 12,
            padding: "18px 20px",
          }}
        >
          <div
            style={{
              fontSize: 10,
              fontWeight: 700,
              color: "#94a3b8",
              textTransform: "uppercase",
              letterSpacing: "0.08em",
              marginBottom: 14,
            }}
          >
            Domain data distribution
          </div>
          {domains.length > 0 ? (
            <div>
              {domains.map((d) => {
                const maxRows = Math.max(...domains.map((x) => x.rows));
                const pct = (d.rows / maxRows) * 100;
                return (
                  <div
                    key={d.name}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 10,
                      marginBottom: 10,
                    }}
                  >
                    <div
                      style={{
                        width: 100,
                        fontSize: 11,
                        color: "#475569",
                        textAlign: "right",
                        flexShrink: 0,
                        lineHeight: 1.3,
                      }}
                    >
                      {d.name}
                    </div>
                    <div
                      style={{
                        flex: 1,
                        height: 20,
                        background: "var(--border)",
                        borderRadius: 4,
                        overflow: "hidden",
                      }}
                    >
                      <div
                        style={{
                          height: "100%",
                          width: `${pct}%`,
                          background: d.color,
                          borderRadius: 4,
                          transition: "width 0.5s",
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "flex-end",
                          paddingRight: 6,
                        }}
                      >
                        <span
                          style={{
                            fontSize: 10,
                            color: "#fff",
                            fontWeight: 700,
                          }}
                        >
                          {d.rows.toLocaleString()}
                        </span>
                      </div>
                    </div>
                    <div
                      style={{
                        fontSize: 10,
                        color: "#94a3b8",
                        width: 60,
                        textAlign: "right",
                      }}
                    >
                      {d.tables} table{d.tables !== 1 ? "s" : ""}
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <p style={{ fontSize: 12, color: "#94a3b8" }}>
              No domain classification available yet.
            </p>
          )}
        </div>
      </div>

      {/* Did you know */}
      <div style={{ marginBottom: 24 }}>
        <div
          style={{
            fontSize: 10,
            fontWeight: 700,
            color: "#94a3b8",
            textTransform: "uppercase",
            letterSpacing: "0.08em",
            marginBottom: 10,
          }}
        >
          Data Summary
        </div>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(4, 1fr)",
            gap: 12,
          }}
        >
          {[
            {
              stat: `${ov.total_tables}`,
              label: "Tables profiled",
              sub: `${ov.total_columns} columns across all tables`,
            },
            {
              stat: `${ov.total_relationships}`,
              label: "Relationships detected",
              sub: `${perfectFKs.length} with 100% integrity`,
            },
            {
              stat: `${dp.high_null_columns}`,
              label: "High-null columns",
              sub: `Columns with >10% null values`,
            },
            {
              stat: `${dp.constant_columns}`,
              label: "Constant columns",
              sub: "Single-value columns (potential noise)",
            },
          ].map((f, i) => (
            <div
              key={i}
              style={{
                padding: "16px 16px",
                background: "var(--card)",
                border: "1px solid var(--border)",
                borderRadius: 12,
                textAlign: "center",
              }}
            >
              <div
                style={{
                  fontSize: 24,
                  fontWeight: 800,
                  color: "#4f46e5",
                  letterSpacing: "-0.04em",
                  marginBottom: 6,
                }}
              >
                {f.stat}
              </div>
              <div
                style={{
                  fontSize: 12,
                  fontWeight: 600,
                  color: "var(--foreground)",
                  marginBottom: 4,
                }}
              >
                {f.label}
              </div>
              <div
                style={{
                  fontSize: 11,
                  color: "#94a3b8",
                  lineHeight: 1.4,
                }}
              >
                {f.sub}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Quality contracts summary */}
      <div
        style={{
          background: "var(--card)",
          border: "1px solid var(--border)",
          borderRadius: 12,
          padding: "18px 20px",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: 12,
          }}
        >
          <div
            style={{
              fontSize: 10,
              fontWeight: 700,
              color: "#94a3b8",
              textTransform: "uppercase",
              letterSpacing: "0.08em",
            }}
          >
            Quality contracts
          </div>
          <span
            style={{
              fontSize: 12,
              color: qualityPassed === qualityTotal ? "#16a34a" : "#d97706",
              fontWeight: 600,
            }}
          >
            {qualityPassed} / {qualityTotal} passing
          </span>
        </div>
        <div
          style={{
            display: "flex",
            gap: 8,
            flexWrap: "wrap",
          }}
        >
          {qualityByTable.map((c) => (
            <div
              key={c.table}
              style={{
                padding: "6px 12px",
                background: c.status === "pass" ? "#f0fdf4" : "#fef2f2",
                border: `1px solid ${c.status === "pass" ? "#bbf7d0" : "#fecaca"}`,
                borderRadius: 20,
                display: "flex",
                alignItems: "center",
                gap: 6,
              }}
            >
              <div
                style={{
                  width: 5,
                  height: 5,
                  borderRadius: "50%",
                  background: c.status === "pass" ? "#16a34a" : "#dc2626",
                }}
              />
              <span
                style={{
                  fontSize: 11,
                  fontFamily: "var(--font-geist-mono), monospace",
                  color: c.status === "pass" ? "#166534" : "#991b1b",
                }}
              >
                {c.table}
              </span>
              <span
                style={{
                  fontSize: 10,
                  color: c.status === "pass" ? "#4ade80" : "#fca5a5",
                }}
              >
                ·
              </span>
              <span
                style={{
                  fontSize: 11,
                  color: c.status === "pass" ? "#166534" : "#991b1b",
                  fontWeight: 600,
                }}
              >
                {c.count}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
