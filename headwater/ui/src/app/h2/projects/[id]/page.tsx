"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { h2, type H2Project, type H2ReadinessReport, trustBadge } from "@/lib/h2api";

const STEPS = [
  { label: "Understand", href: (id: string) => `/h2/projects/${id}/understand` },
  { label: "Resolve", href: (id: string) => `/h2/projects/${id}/resolve` },
  { label: "Readiness", href: (id: string) => `/h2/projects/${id}/readiness` },
  { label: "Answer", href: (id: string) => `/h2/projects/${id}/answer` },
];

export default function ProjectPage() {
  const { id } = useParams<{ id: string }>();
  const [project, setProject] = useState<H2Project | null>(null);
  const [readiness, setReadiness] = useState<H2ReadinessReport | null>(null);
  const [resolveCount, setResolveCount] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      h2.projects.get(id),
      h2.projects.readiness.evaluate(id).catch(() => null),
      h2.projects.resolve.list(id).catch(() => []),
    ]).then(([p, r, rv]) => {
      setProject(p as H2Project);
      setReadiness(r as H2ReadinessReport | null);
      setResolveCount((rv as unknown[]).length);
    }).finally(() => setLoading(false));
  }, [id]);

  if (loading) return <div className="p-8 text-gray-500">Loading…</div>;
  if (!project) return <div className="p-8 text-red-600">Project not found.</div>;

  const badge = trustBadge(readiness);
  const goal = project.goal?.statement || project.description;

  return (
    <div className="max-w-3xl mx-auto p-8">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-start justify-between">
          <div>
            <Link href="/h2" className="text-xs text-gray-400 hover:text-gray-600">
              ← Projects
            </Link>
            <h1 className="text-xl font-semibold text-gray-900 mt-1">
              {project.display_name}
            </h1>
            <p className="text-sm text-gray-600 mt-1">{goal}</p>
          </div>
          <div className="text-right shrink-0 ml-4">
            <span className={`text-sm font-medium ${badge.color}`}>
              {badge.label} {badge.pct > 0 ? `${badge.pct}%` : ""}
            </span>
            <div className="text-xs text-gray-400 mt-1">trust</div>
          </div>
        </div>

        {/* Goal details */}
        {(project.goal?.decision || project.goal?.target_metric || project.goal?.time_horizon) && (
          <div className="mt-3 flex flex-wrap gap-3 text-xs text-gray-500">
            {project.goal.decision && (
              <span className="border border-gray-200 rounded px-2 py-0.5">
                Decision: {project.goal.decision}
              </span>
            )}
            {project.goal.target_metric && (
              <span className="border border-gray-200 rounded px-2 py-0.5">
                Metric: {project.goal.target_metric}
              </span>
            )}
            {project.goal.time_horizon && (
              <span className="border border-gray-200 rounded px-2 py-0.5">
                Horizon: {project.goal.time_horizon}
              </span>
            )}
          </div>
        )}
      </div>

      {/* Step navigation */}
      <div className="border border-gray-200 rounded-lg p-1 mb-8 flex gap-1">
        {STEPS.map((step, i) => (
          <Link
            key={step.label}
            href={step.href(id)}
            className="flex-1 text-center py-2 px-3 text-sm rounded-md hover:bg-gray-50 text-gray-600 hover:text-gray-900 transition-colors"
          >
            <span className="text-gray-400 mr-1">{i + 1}.</span>
            {step.label}
          </Link>
        ))}
      </div>

      {/* Quick stats */}
      <div className="grid grid-cols-3 gap-4 mb-8">
        <StatCard
          label="Questions"
          value={(project.questions?.length ?? 0).toString()}
          href={`/h2/projects/${id}/understand`}
        />
        <StatCard
          label="Resolve items"
          value={resolveCount.toString()}
          href={`/h2/projects/${id}/resolve`}
          alert={resolveCount > 0}
        />
        <StatCard
          label="Certified"
          value={readiness ? `${readiness.certified_count}/${readiness.questions.length}` : "—"}
          href={`/h2/projects/${id}/readiness`}
        />
      </div>

      {/* Quick actions */}
      <div className="space-y-2">
        <h2 className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-3">
          Quick actions
        </h2>
        <Link
          href={`/h2/projects/${id}/understand`}
          className="flex items-center justify-between border border-gray-200 rounded-lg px-4 py-3 hover:bg-gray-50"
        >
          <span className="text-sm text-gray-700">View proposed questions</span>
          <span className="text-gray-400">→</span>
        </Link>
        <Link
          href={`/h2/projects/${id}/resolve`}
          className="flex items-center justify-between border border-gray-200 rounded-lg px-4 py-3 hover:bg-gray-50"
        >
          <span className="text-sm text-gray-700">
            Resolve gaps{resolveCount > 0 && <span className="ml-2 bg-orange-100 text-orange-700 text-xs px-2 py-0.5 rounded-full">{resolveCount}</span>}
          </span>
          <span className="text-gray-400">→</span>
        </Link>
        <Link
          href={`/h2/projects/${id}/readiness`}
          className="flex items-center justify-between border border-gray-200 rounded-lg px-4 py-3 hover:bg-gray-50"
        >
          <span className="text-sm text-gray-700">View readiness verdict</span>
          <span className="text-gray-400">→</span>
        </Link>
        <Link
          href={`/h2/projects/${id}/answer`}
          className="flex items-center justify-between border border-gray-200 rounded-lg px-4 py-3 hover:bg-gray-50"
        >
          <span className="text-sm text-gray-700">See answer drafts</span>
          <span className="text-gray-400">→</span>
        </Link>
      </div>
    </div>
  );
}

function StatCard({ label, value, href, alert = false }: { label: string; value: string; href: string; alert?: boolean }) {
  return (
    <Link
      href={href}
      className="border border-gray-200 rounded-lg p-4 text-center hover:border-blue-300 transition-colors"
    >
      <div className={`text-2xl font-semibold ${alert ? "text-orange-600" : "text-gray-800"}`}>
        {value}
      </div>
      <div className="text-xs text-gray-500 mt-1">{label}</div>
    </Link>
  );
}
