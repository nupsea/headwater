"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { h2, type H2Project, type H2Source, trustBadge } from "@/lib/h2api";

export default function H2HomePage() {
  const [projects, setProjects] = useState<H2Project[]>([]);
  const [sources, setSources] = useState<H2Source[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([h2.projects.list(), h2.sources.list()])
      .then(([p, s]) => { setProjects(p); setSources(s); })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="p-8 text-gray-500">Loading…</div>;
  if (error) return <div className="p-8 text-red-600">{error}</div>;

  return (
    <div className="max-w-4xl mx-auto p-8">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900">Headwater 2</h1>
          <p className="text-sm text-gray-500 mt-1">
            Goal-anchored data readiness workspace
          </p>
        </div>
        <Link
          href="/h2/projects/new"
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
        >
          + New project
        </Link>
      </div>

      {/* Projects */}
      <section className="mb-8">
        <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wide mb-3">
          Projects
        </h2>
        {projects.length === 0 ? (
          <div className="border border-dashed border-gray-300 rounded-lg p-8 text-center text-gray-500">
            <p>No projects yet.</p>
            <p className="text-sm mt-1">
              <Link href="/h2/projects/new" className="text-blue-600 underline">
                Create your first project
              </Link>{" "}
              to get started.
            </p>
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            {projects.map(p => (
              <ProjectCard key={p.id} project={p} />
            ))}
          </div>
        )}
      </section>

      {/* Sources */}
      <section>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wide">
            Shared sources
          </h2>
          <Link href="/h2/sources/new" className="text-sm text-blue-600 hover:underline">
            + Connect source
          </Link>
        </div>
        {sources.length === 0 ? (
          <p className="text-sm text-gray-400">
            No sources connected yet.{" "}
            <Link href="/h2/sources/new" className="text-blue-600 underline">
              Connect a source
            </Link>{" "}
            before creating a project.
          </p>
        ) : (
          <div className="flex flex-wrap gap-3">
            {sources.map(s => (
              <div
                key={s.name}
                className="border border-gray-200 rounded-lg px-4 py-3 text-sm"
              >
                <span className="font-medium text-gray-800">{s.name}</span>
                <span className="ml-2 text-gray-400 text-xs">{s.type}</span>
                {s.latest_snapshot_id && (
                  <span className="ml-2 text-green-600 text-xs">profiled</span>
                )}
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

function ProjectCard({ project }: { project: H2Project }) {
  const badge = trustBadge(null);
  const goal = project.goal?.statement || project.description || "—";

  return (
    <Link
      href={`/h2/projects/${project.id}`}
      className="block border border-gray-200 rounded-lg p-5 hover:border-blue-300 hover:shadow-sm transition-all"
    >
      <div className="flex items-start justify-between">
        <div className="flex-1 min-w-0">
          <h3 className="font-medium text-gray-900 truncate">{project.display_name}</h3>
          <p className="text-sm text-gray-500 mt-1 line-clamp-2">{goal}</p>
        </div>
        <span className={`ml-3 text-xs font-medium shrink-0 ${badge.color}`}>
          {badge.label}
        </span>
      </div>
      <div className="mt-3 text-xs text-gray-400">
        Updated {new Date(project.updated_at).toLocaleDateString()}
      </div>
    </Link>
  );
}
