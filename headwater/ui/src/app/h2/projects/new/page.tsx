"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { h2, type H2Source } from "@/lib/h2api";

export default function NewProjectPage() {
  const router = useRouter();
  const [sources, setSources] = useState<H2Source[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [form, setForm] = useState({
    project_id: "",
    source_name: "",
    display_name: "",
    goal: "",
    decision: "",
    target_metric: "",
    time_horizon: "",
  });

  useEffect(() => {
    h2.sources.list().then(setSources).catch(() => {});
  }, []);

  const set = (k: keyof typeof form) => (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) =>
    setForm(f => ({ ...f, [k]: e.target.value }));

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!form.project_id || !form.source_name || !form.goal) return;
    setLoading(true);
    setError(null);
    try {
      await h2.projects.frame({
        project_id: form.project_id,
        source_name: form.source_name,
        display_name: form.display_name || form.project_id,
        goal: form.goal,
        decision: form.decision || undefined,
        target_metric: form.target_metric || undefined,
        time_horizon: form.time_horizon || undefined,
      });
      router.push(`/h2/projects/${form.project_id}`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to create project");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-2xl mx-auto p-8">
      <div className="mb-6">
        <h1 className="text-xl font-semibold text-gray-900">New project</h1>
        <p className="text-sm text-gray-500 mt-1">
          A project is a business problem. Start with the goal.
        </p>
      </div>

      <form onSubmit={submit} className="space-y-5">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            What problem are you solving? *
          </label>
          <textarea
            value={form.goal}
            onChange={set("goal")}
            required
            rows={3}
            placeholder="e.g. Understand where delays occur in the registration process"
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Project ID *
            </label>
            <input
              value={form.project_id}
              onChange={set("project_id")}
              required
              placeholder="reg_workflow_01"
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Display name
            </label>
            <input
              value={form.display_name}
              onChange={set("display_name")}
              placeholder="Registration Workflow"
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Data source *
          </label>
          {sources.length === 0 ? (
            <p className="text-sm text-red-600">
              No sources available.{" "}
              <a href="/h2/sources/new" className="underline">Connect one first.</a>
            </p>
          ) : (
            <select
              value={form.source_name}
              onChange={set("source_name")}
              required
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">Select a source…</option>
              {sources.map(s => (
                <option key={s.name} value={s.name}>
                  {s.name} ({s.type})
                </option>
              ))}
            </select>
          )}
        </div>

        <details className="border border-gray-200 rounded-md p-3">
          <summary className="text-sm text-gray-500 cursor-pointer">
            Add detail (optional)
          </summary>
          <div className="mt-3 space-y-3">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Decision this project informs
              </label>
              <input
                value={form.decision}
                onChange={set("decision")}
                placeholder="e.g. Identify bottleneck steps for process improvement"
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
              />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Target metric
                </label>
                <input
                  value={form.target_metric}
                  onChange={set("target_metric")}
                  placeholder="wait_time"
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Time horizon
                </label>
                <input
                  value={form.time_horizon}
                  onChange={set("time_horizon")}
                  placeholder="weekly"
                  className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
                />
              </div>
            </div>
          </div>
        </details>

        {error && (
          <div className="text-sm text-red-600 border border-red-200 bg-red-50 rounded px-3 py-2">
            {error}
          </div>
        )}

        <div className="flex gap-3 pt-2">
          <button
            type="button"
            onClick={() => router.back()}
            className="px-4 py-2 border border-gray-300 text-sm rounded-md hover:bg-gray-50"
          >
            Cancel
          </button>
          <button
            type="submit"
            disabled={loading || !form.goal || !form.project_id || !form.source_name}
            className="px-5 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-50"
          >
            {loading ? "Generating…" : "Generate →"}
          </button>
        </div>
      </form>
    </div>
  );
}
