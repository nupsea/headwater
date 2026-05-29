"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { h2, type H2Project, type H2Source } from "@/lib/h2api";
import { ReadinessRing, HW2_COLOR } from "@/components/h2/readiness-ring";

function computeReadout(project: H2Project) {
  const questions = project.questions ?? [];
  const total = questions.length;
  if (total === 0) return { pct: 0, certifiedCount: 0, total: 0 };
  const certifiedCount = questions.filter((q) => q.status === "certified").length;
  return { pct: Math.round((certifiedCount / total) * 100), certifiedCount, total };
}

export default function H2HomePage() {
  const router = useRouter();
  const [projects, setProjects] = useState<H2Project[]>([]);
  const [sources, setSources] = useState<H2Source[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([h2.projects.list(), h2.sources.list()])
      .then(([p, s]) => {
        setProjects(p);
        setSources(s);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          height: "60vh",
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
        }}
      >
        Loading…
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ maxWidth: 640, margin: "48px auto", padding: "0 32px" }}>
        <div
          style={{
            padding: "14px 18px",
            background: HW2_COLOR.badSoft,
            border: `1px solid ${HW2_COLOR.bad}44`,
            borderRadius: 10,
            font: "500 13px 'DM Sans', sans-serif",
            color: HW2_COLOR.bad,
          }}
        >
          {error}
        </div>
      </div>
    );
  }

  return (
    <div
      style={{
        maxWidth: 900,
        margin: "0 auto",
        padding: "36px 32px 80px",
        fontFamily: "'DM Sans', sans-serif",
      }}
    >
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          marginBottom: 32,
        }}
      >
        <div>
          <span
            style={{
              font: "600 11px 'DM Sans', sans-serif",
              color: HW2_COLOR.blue,
              letterSpacing: "0.08em",
              textTransform: "uppercase",
            }}
          >
            Workspace
          </span>
          <h1
            style={{
              font: "600 26px 'DM Sans', sans-serif",
              letterSpacing: "-0.02em",
              color: HW2_COLOR.ink,
              lineHeight: 1.25,
              marginTop: 6,
              marginBottom: 6,
            }}
          >
            {projects.length > 0
              ? `${projects.length} active project${projects.length !== 1 ? "s" : ""}`
              : "Start a project"}
          </h1>
          <p
            style={{
              font: "400 14px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              lineHeight: 1.55,
              maxWidth: 520,
            }}
          >
            A project is a business goal. Headwater proposes the questions this
            data can credibly answer, then helps you certify each one.
          </p>
        </div>

        <button
          onClick={() => router.push("/h2/projects/new")}
          style={{
            appearance: "none",
            cursor: "pointer",
            background: HW2_COLOR.ink,
            color: "#fff",
            border: "1px solid transparent",
            borderRadius: 10,
            padding: "11px 20px",
            font: "600 14px 'DM Sans', sans-serif",
            whiteSpace: "nowrap",
            flexShrink: 0,
          }}
        >
          + New project
        </button>
      </div>

      {/* Projects grid */}
      {projects.length === 0 ? (
        <div
          style={{
            border: `1.5px dashed ${HW2_COLOR.rule2}`,
            borderRadius: 14,
            padding: "56px 40px",
            textAlign: "center",
            marginBottom: 32,
          }}
        >
          <div
            style={{
              font: "500 15px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              marginBottom: 12,
            }}
          >
            No projects yet
          </div>
          <p
            style={{
              font: "400 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.faint,
              lineHeight: 1.55,
              marginBottom: 20,
              maxWidth: 380,
              margin: "0 auto 20px",
            }}
          >
            Connect a source, then create a project to have Headwater propose
            questions and evaluate data readiness.
          </p>
          <button
            onClick={() =>
              sources.length === 0
                ? router.push("/h2/sources/new")
                : router.push("/h2/projects/new")
            }
            style={{
              appearance: "none",
              cursor: "pointer",
              background: HW2_COLOR.blue,
              color: "#fff",
              border: "1px solid transparent",
              borderRadius: 10,
              padding: "10px 20px",
              font: "600 14px 'DM Sans', sans-serif",
            }}
          >
            {sources.length === 0 ? "Connect a source first" : "Create first project"}
          </button>
        </div>
      ) : (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(340px, 1fr))",
            gap: 14,
            marginBottom: 36,
          }}
        >
          {projects.map((p) => (
            <ProjectCard
              key={p.id}
              project={p}
              onClick={() => router.push(`/h2/projects/${p.id}/understand`)}
            />
          ))}
        </div>
      )}

      {/* Sources section */}
      <section>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: 12,
          }}
        >
          <span
            style={{
              font: "600 11px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              letterSpacing: "0.07em",
              textTransform: "uppercase",
            }}
          >
            Connected sources
          </span>
          <button
            onClick={() => router.push("/h2/sources/new")}
            style={{
              appearance: "none",
              background: "transparent",
              border: "none",
              cursor: "pointer",
              padding: 0,
              font: "500 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.blue,
            }}
          >
            + Connect source
          </button>
        </div>

        {sources.length === 0 ? (
          <p
            style={{
              font: "400 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.faint,
            }}
          >
            No sources connected yet.
          </p>
        ) : (
          <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
            {sources.map((s) => (
              <div
                key={s.name}
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 8,
                  padding: "8px 12px",
                  background: HW2_COLOR.surface,
                  border: `1px solid ${HW2_COLOR.rule}`,
                  borderRadius: 8,
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
                <span
                  style={{
                    font: "600 13px 'DM Sans', sans-serif",
                    color: HW2_COLOR.ink,
                  }}
                >
                  {s.name}
                </span>
                <span
                  style={{
                    font: "500 10px 'DM Mono', monospace",
                    color: HW2_COLOR.muted,
                  }}
                >
                  {s.type}
                </span>
                {s.latest_snapshot_id && (
                  <span
                    style={{
                      font: "500 10px 'DM Sans', sans-serif",
                      color: HW2_COLOR.good,
                    }}
                  >
                    profiled
                  </span>
                )}
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

function ProjectCard({
  project,
  onClick,
}: {
  project: H2Project;
  onClick: () => void;
}) {
  const ro = computeReadout(project);
  const goal = project.goal?.statement || project.description || "—";

  return (
    <button
      onClick={onClick}
      style={{
        appearance: "none",
        cursor: "pointer",
        textAlign: "left",
        background: HW2_COLOR.surface,
        border: `1px solid ${HW2_COLOR.rule}`,
        borderRadius: 14,
        padding: "18px 20px",
        transition: "border-color 120ms, box-shadow 120ms",
        fontFamily: "'DM Sans', sans-serif",
      }}
      onMouseEnter={(e) => {
        (e.currentTarget as HTMLButtonElement).style.borderColor = HW2_COLOR.blue;
        (e.currentTarget as HTMLButtonElement).style.boxShadow =
          "0 2px 8px rgba(43,95,217,0.08)";
      }}
      onMouseLeave={(e) => {
        (e.currentTarget as HTMLButtonElement).style.borderColor = HW2_COLOR.rule;
        (e.currentTarget as HTMLButtonElement).style.boxShadow = "none";
      }}
    >
      <div
        style={{ display: "flex", alignItems: "flex-start", gap: 14, marginBottom: 12 }}
      >
        <ReadinessRing
          value={ro.pct}
          certified={ro.certifiedCount === ro.total && ro.total > 0}
          demoted={false}
          size={36}
          stroke={3.5}
          showLabel={false}
          animate={false}
        />
        <div style={{ flex: 1, minWidth: 0 }}>
          <div
            style={{
              font: "600 15px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink,
              letterSpacing: "-0.01em",
              marginBottom: 4,
            }}
          >
            {project.display_name}
          </div>
          <p
            style={{
              font: "400 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              lineHeight: 1.5,
              display: "-webkit-box",
              WebkitLineClamp: 2,
              WebkitBoxOrient: "vertical",
              overflow: "hidden",
            }}
          >
            {goal}
          </p>
        </div>
      </div>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          paddingTop: 12,
          borderTop: `1px solid ${HW2_COLOR.rule}`,
        }}
      >
        <span
          style={{
            font: "500 11px 'DM Mono', monospace",
            color: HW2_COLOR.muted,
          }}
        >
          {ro.certifiedCount}/{ro.total} certified
        </span>
        <span
          style={{
            font: "400 11px 'DM Sans', sans-serif",
            color: HW2_COLOR.faint,
          }}
        >
          {new Date(project.updated_at).toLocaleDateString()}
        </span>
      </div>
    </button>
  );
}
