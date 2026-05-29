"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { h2 } from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";

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
      setError(
        err instanceof Error ? err.message : "Failed to connect source"
      );
      setLoading(false);
    }
  };

  const inputStyle = {
    width: "100%",
    padding: "10px 14px",
    background: "#fff",
    border: `1px solid ${HW2_COLOR.rule2}`,
    borderRadius: 8,
    font: "400 14px 'DM Sans', sans-serif",
    color: HW2_COLOR.ink,
    fontFamily: "'DM Sans', sans-serif",
    outline: "none",
    boxSizing: "border-box" as const,
  };

  return (
    <div
      style={{
        maxWidth: 600,
        margin: "0 auto",
        padding: "36px 32px 80px",
        fontFamily: "'DM Sans', sans-serif",
      }}
    >
      <span
        style={{
          font: "600 11px 'DM Sans', sans-serif",
          color: HW2_COLOR.blue,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
        }}
      >
        Connect
      </span>
      <h1
        style={{
          font: "600 26px 'DM Sans', sans-serif",
          letterSpacing: "-0.02em",
          color: HW2_COLOR.ink,
          lineHeight: 1.25,
          marginTop: 8,
          marginBottom: 6,
        }}
      >
        Connect a source
      </h1>
      <p
        style={{
          font: "400 14px 'DM Sans', sans-serif",
          color: HW2_COLOR.muted,
          marginBottom: 28,
          lineHeight: 1.55,
        }}
      >
        Sources are profiled once and shared across all projects.
        Point to a file path or connection string.
      </p>

      <form onSubmit={submit} style={{ display: "grid", gap: 18 }}>
        <div>
          <label
            style={{
              font: "600 12px 'DM Sans', sans-serif",
              color: HW2_COLOR.muted,
              display: "block",
              marginBottom: 8,
            }}
          >
            File path or connection string *
          </label>
          <input
            value={path}
            onChange={(e) => setPath(e.target.value)}
            required
            placeholder="/data/my_source  or  postgres://user:pass@host/db"
            style={inputStyle}
            onFocus={(e) =>
              (e.currentTarget.style.borderColor = HW2_COLOR.blue)
            }
            onBlur={(e) =>
              (e.currentTarget.style.borderColor = HW2_COLOR.rule2)
            }
          />
        </div>

        <div
          style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}
        >
          <div>
            <label
              style={{
                font: "600 12px 'DM Sans', sans-serif",
                color: HW2_COLOR.muted,
                display: "block",
                marginBottom: 8,
              }}
            >
              Type (optional)
            </label>
            <select
              value={sourceType}
              onChange={(e) => setSourceType(e.target.value)}
              style={{
                ...inputStyle,
                cursor: "pointer",
              }}
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
            <label
              style={{
                font: "600 12px 'DM Sans', sans-serif",
                color: HW2_COLOR.muted,
                display: "block",
                marginBottom: 8,
              }}
            >
              Name (optional)
            </label>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my_source"
              style={inputStyle}
              onFocus={(e) =>
                (e.currentTarget.style.borderColor = HW2_COLOR.blue)
              }
              onBlur={(e) =>
                (e.currentTarget.style.borderColor = HW2_COLOR.rule2)
              }
            />
          </div>
        </div>

        {error && (
          <div
            style={{
              padding: "12px 16px",
              background: HW2_COLOR.badSoft,
              border: `1px solid ${HW2_COLOR.bad}44`,
              borderRadius: 8,
              font: "500 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.bad,
            }}
          >
            {error}
          </div>
        )}

        {/* Hint */}
        <div
          style={{
            padding: "12px 16px",
            background: HW2_COLOR.chip,
            borderRadius: 8,
            font: "400 12px 'DM Sans', sans-serif",
            color: HW2_COLOR.muted,
            lineHeight: 1.5,
          }}
        >
          Profiling reads column names, types, row counts and statistics. No
          row data is sent to any LLM.
        </div>

        <div
          style={{ display: "flex", gap: 10, paddingTop: 4 }}
        >
          <button
            type="button"
            onClick={() => router.back()}
            style={{
              appearance: "none",
              cursor: "pointer",
              background: "#fff",
              border: `1px solid ${HW2_COLOR.rule2}`,
              borderRadius: 8,
              padding: "10px 18px",
              font: "500 13px 'DM Sans', sans-serif",
              color: HW2_COLOR.ink2,
              fontFamily: "'DM Sans', sans-serif",
            }}
          >
            Cancel
          </button>
          <button
            type="submit"
            disabled={loading || !path}
            style={{
              appearance: "none",
              cursor: loading || !path ? "default" : "pointer",
              background: HW2_COLOR.blue,
              color: "#fff",
              border: "1px solid transparent",
              borderRadius: 8,
              padding: "10px 20px",
              font: "600 14px 'DM Sans', sans-serif",
              fontFamily: "'DM Sans', sans-serif",
              opacity: loading || !path ? 0.5 : 1,
              transition: "opacity 120ms",
            }}
          >
            {loading ? "Profiling…" : "Connect & profile →"}
          </button>
        </div>
      </form>
    </div>
  );
}
