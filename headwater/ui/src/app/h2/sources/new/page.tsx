"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { h2 } from "@/lib/h2api";
import { HW2_COLOR } from "@/components/h2/readiness-ring";
import { useH2Context } from "@/app/h2/layout";

// Database source types are addressed by a connection URI built from
// structured credential fields rather than a raw connection string.
const DB_DEFAULT_PORT: Record<string, string> = {
  postgres: "5432",
  redshift: "5439",
  mysql: "3306",
};

function buildDbUri(
  scheme: string,
  host: string,
  port: string,
  database: string,
  schema: string,
  user: string,
  password: string,
): string {
  const enc = encodeURIComponent;
  const cred = user
    ? `${enc(user)}${password ? `:${enc(password)}` : ""}@`
    : "";
  if (scheme === "snowflake") {
    // Snowflake is addressed by account (no port); warehouse/role via query.
    let uri = `snowflake://${cred}${host.trim()}/${enc(database.trim())}`;
    if (schema.trim()) uri += `/${enc(schema.trim())}`;
    return uri;
  }
  const effPort = port.trim() || DB_DEFAULT_PORT[scheme] || "5432";
  let uri = `${scheme}://${cred}${host.trim()}:${effPort}/${enc(database.trim())}`;
  if (schema.trim()) uri += `/${enc(schema.trim())}`;
  return uri;
}

function withQuery(uri: string, params: Record<string, string>): string {
  const entries = Object.entries(params).filter(([, v]) => v.trim() !== "");
  if (entries.length === 0) return uri;
  const qs = entries
    .map(([k, v]) => `${k}=${encodeURIComponent(v.trim())}`)
    .join("&");
  return `${uri}?${qs}`;
}

export default function ConnectSourcePage() {
  const router = useRouter();
  const { setActiveSource, reload } = useH2Context();
  const [sourceType, setSourceType] = useState("");
  const [name, setName] = useState("");

  // File / connection-string mode
  const [path, setPath] = useState("");

  // Database mode (postgres / redshift / mysql / snowflake)
  const [host, setHost] = useState("");
  const [port, setPort] = useState("");
  const [database, setDatabase] = useState("");
  const [schema, setSchema] = useState("");
  const [user, setUser] = useState("");
  const [password, setPassword] = useState("");
  // Snowflake-specific
  const [warehouse, setWarehouse] = useState("");
  const [role, setRole] = useState("");

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const isSnowflake = sourceType === "snowflake";
  const isDb =
    sourceType === "postgres" ||
    sourceType === "redshift" ||
    sourceType === "mysql" ||
    isSnowflake;
  const dbReady = isDb && host.trim() !== "" && database.trim() !== "";
  const canSubmit = isDb ? dbReady : path.trim() !== "";

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!canSubmit) return;
    setLoading(true);
    setError(null);
    try {
      let target = isDb
        ? buildDbUri(sourceType, host, port, database, schema, user, password)
        : path;
      if (isSnowflake) {
        target = withQuery(target, { warehouse, role });
      }
      const r = await h2.sources.discover(
        target,
        sourceType || undefined,
        name || undefined,
      );
      // The new source becomes the workspace context. Warehouses land on the
      // catalog with the table picker open (nothing is ingested yet); file
      // sources land on their freshly profiled catalog.
      setActiveSource(r.source_name);
      reload();
      router.push(
        `/h2/sources/${encodeURIComponent(r.source_name)}${isDb ? "?browse=1" : ""}`
      );
    } catch (err: unknown) {
      setError(
        err instanceof Error ? err.message : "Failed to connect source",
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

  const labelStyle = {
    font: "600 12px 'DM Sans', sans-serif",
    color: HW2_COLOR.muted,
    display: "block",
    marginBottom: 8,
  };

  const focusOn = (e: React.FocusEvent<HTMLInputElement>) =>
    (e.currentTarget.style.borderColor = HW2_COLOR.blue);
  const focusOff = (e: React.FocusEvent<HTMLInputElement>) =>
    (e.currentTarget.style.borderColor = HW2_COLOR.rule2);

  const field = (
    label: string,
    value: string,
    setter: (v: string) => void,
    opts: { placeholder?: string; type?: string; required?: boolean } = {},
  ) => (
    <div>
      <label style={labelStyle}>
        {label}
        {opts.required ? " *" : ""}
      </label>
      <input
        value={value}
        onChange={(e) => setter(e.target.value)}
        placeholder={opts.placeholder}
        type={opts.type ?? "text"}
        autoComplete="off"
        style={inputStyle}
        onFocus={focusOn}
        onBlur={focusOff}
      />
    </div>
  );

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
        Sources are profiled once and shared across all projects. Point to a
        file path, or pick a database type and enter its connection details.
      </p>

      <form onSubmit={submit} style={{ display: "grid", gap: 18 }}>
        <div
          style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}
        >
          <div>
            <label style={labelStyle}>Type</label>
            <select
              value={sourceType}
              onChange={(e) => setSourceType(e.target.value)}
              style={{ ...inputStyle, cursor: "pointer" }}
            >
              <option value="">Auto-detect (file)</option>
              <option value="csv">CSV</option>
              <option value="json">JSON</option>
              <option value="parquet">Parquet</option>
              <option value="duckdb">DuckDB</option>
              <option value="sqlite">SQLite</option>
              <option value="postgres">PostgreSQL</option>
              <option value="redshift">Redshift</option>
              <option value="mysql">MySQL</option>
              <option value="snowflake">Snowflake</option>
            </select>
          </div>
          <div>
            <label style={labelStyle}>Name (optional)</label>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder={isDb ? "defaults to host" : "my_source"}
              style={inputStyle}
              onFocus={focusOn}
              onBlur={focusOff}
            />
          </div>
        </div>

        {isDb ? (
          <>
            {field(
              isSnowflake ? "Account" : "Host",
              host,
              setHost,
              {
                placeholder: isSnowflake
                  ? "myorg-myaccount.snowflakecomputing.com"
                  : "cluster.abc123.us-east-1.redshift.amazonaws.com",
                required: true,
              },
            )}
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr",
                gap: 14,
              }}
            >
              {isSnowflake
                ? field("Warehouse", warehouse, setWarehouse, {
                    placeholder: "COMPUTE_WH",
                  })
                : field("Port", port, setPort, {
                    placeholder: DB_DEFAULT_PORT[sourceType] ?? "5432",
                  })}
              {field("Database", database, setDatabase, {
                placeholder: isSnowflake ? "ANALYTICS" : "dev",
                required: true,
              })}
            </div>
            {isSnowflake ? (
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "1fr 1fr",
                  gap: 14,
                }}
              >
                {field("Schema (optional)", schema, setSchema, {
                  placeholder: "PUBLIC",
                })}
                {field("Role (optional)", role, setRole, {
                  placeholder: "ANALYST",
                })}
              </div>
            ) : (
              field("Schema (optional)", schema, setSchema, {
                placeholder: "public",
              })
            )}
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr",
                gap: 14,
              }}
            >
              {field("User", user, setUser, {
                placeholder: isSnowflake ? "ANALYST_USER" : "awsuser",
              })}
              {field("Password", password, setPassword, {
                placeholder: "••••••••",
                type: "password",
              })}
            </div>
          </>
        ) : (
          field("File path or connection string", path, setPath, {
            placeholder: "/data/my_source  or  postgres://user:pass@host/db",
            required: true,
          })
        )}

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
          {isDb
            ? "Credentials are used to connect and list the catalog — no rows are copied. Pick the tables to ingest after connecting. No row data is sent to any LLM."
            : "Profiling reads column names, types, row counts and statistics. No row data is sent to any LLM."}
        </div>

        <div style={{ display: "flex", gap: 10, paddingTop: 4 }}>
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
            disabled={loading || !canSubmit}
            style={{
              appearance: "none",
              cursor: loading || !canSubmit ? "default" : "pointer",
              background: HW2_COLOR.blue,
              color: "#fff",
              border: "1px solid transparent",
              borderRadius: 8,
              padding: "10px 20px",
              font: "600 14px 'DM Sans', sans-serif",
              fontFamily: "'DM Sans', sans-serif",
              opacity: loading || !canSubmit ? 0.5 : 1,
              transition: "opacity 120ms",
            }}
          >
            {loading
              ? isDb
                ? "Connecting…"
                : "Profiling…"
              : isDb
                ? "Connect & browse →"
                : "Connect & profile →"}
          </button>
        </div>
      </form>
    </div>
  );
}
