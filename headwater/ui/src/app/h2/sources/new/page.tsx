"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { h2 } from "@/lib/h2api";

export default function ConnectSourcePage() {
  const router = useRouter();
  const [path, setPath] = useState("");
  const [sourceType, setSourceType] = useState("");
  const [name, setName] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!path) return;
    setLoading(true);
    setError(null);
    try {
      await h2.sources.discover(path, sourceType || undefined, name || undefined);
      router.push("/h2");
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Failed to connect source");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-xl mx-auto p-8">
      <div className="mb-6">
        <Link href="/h2" className="text-xs text-gray-400 hover:text-gray-600">
          ← Home
        </Link>
        <h1 className="text-xl font-semibold text-gray-900 mt-1">Connect a source</h1>
        <p className="text-sm text-gray-500 mt-1">
          Sources are profiled once and shared across projects.
        </p>
      </div>

      <form onSubmit={submit} className="space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            File path or connection string *
          </label>
          <input
            value={path}
            onChange={e => setPath(e.target.value)}
            required
            placeholder="/data/my_source  or  postgres://user:pass@host/db"
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Type (optional)
            </label>
            <select
              value={sourceType}
              onChange={e => setSourceType(e.target.value)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
            >
              <option value="">Auto-detect</option>
              <option value="csv">CSV</option>
              <option value="json">JSON</option>
              <option value="parquet">Parquet</option>
              <option value="duckdb">DuckDB</option>
              <option value="sqlite">SQLite</option>
              <option value="postgres">PostgreSQL</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Name (optional)
            </label>
            <input
              value={name}
              onChange={e => setName(e.target.value)}
              placeholder="my_source"
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
            />
          </div>
        </div>

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
            disabled={loading || !path}
            className="px-5 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-50"
          >
            {loading ? "Profiling…" : "Connect & profile"}
          </button>
        </div>
      </form>
    </div>
  );
}
