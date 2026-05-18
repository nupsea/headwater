"use client";

import { useEffect, useMemo, useState } from "react";
import {
  api,
  type ConnectorType,
  type SourceCreatePayload,
  type SourcePreviewResponse,
} from "@/lib/api";
import { useToast } from "@/components/toast";

type Props = {
  open: boolean;
  onClose: () => void;
  onCreated: () => void;
};

/* ------------------------------------------------------------------ */
/*  Warehouse / OLAP connector ids that show schema filter options     */
/* ------------------------------------------------------------------ */
const FILTERABLE_CONNECTORS = new Set([
  "redshift",
  "snowflake",
  "postgres",
  "mysql",
  "bigquery",
  "databricks",
  "trino",
]);
const DB_CONNECTORS = new Set([
  "postgres",
  "mysql",
  "snowflake",
  "redshift",
]);
const FILE_CONNECTORS = new Set(["json", "csv", "duckdb", "sqlite"]);

/* ------------------------------------------------------------------ */
/*  Number formatter                                                  */
/* ------------------------------------------------------------------ */
function fmt(n: number | null | undefined): string {
  if (n == null) return "—";
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

/* ================================================================== */
/*  Main wizard component                                             */
/* ================================================================== */

export function ConnectorWizard({ open, onClose, onCreated }: Props) {
  const { toast } = useToast();
  const [step, setStep] = useState(1);
  const [chosen, setChosen] = useState<ConnectorType | null>(null);
  const [search, setSearch] = useState("");
  const [connectors, setConnectors] = useState<ConnectorType[]>([]);
  const [submitting, setSubmitting] = useState(false);
  const [testing, setTesting] = useState(false);
  const [previewing, setPreviewing] = useState(false);
  const [preview, setPreview] = useState<SourcePreviewResponse | null>(null);
  const [testOk, setTestOk] = useState<boolean | null>(null);
  const [testError, setTestError] = useState<string | null>(null);

  const [form, setForm] = useState<SourceCreatePayload>({
    name: "my-new-source",
    type: "postgres",
    display_name: "",
    host: "",
    uri: "",
    path: "",
    auto_sync: true,
    config: {
      max_tables: 50,
      sample_rows: 10000,
    },
  });

  /* Filter-specific state */
  const [includeSchemas, setIncludeSchemas] = useState("");
  const [excludeSchemas, setExcludeSchemas] = useState("");
  const [includeTables, setIncludeTables] = useState("");
  const [excludeTables, setExcludeTables] = useState("");

  useEffect(() => {
    if (!open) return;
    api
      .connectorCatalog()
      .then((r) => setConnectors(r.connectors))
      .catch(() => {});
  }, [open]);

  useEffect(() => {
    if (!open) {
      setStep(1);
      setChosen(null);
      setSearch("");
      setTestOk(null);
      setTestError(null);
      setPreview(null);
      setIncludeSchemas("");
      setExcludeSchemas("");
      setIncludeTables("");
      setExcludeTables("");
    }
  }, [open]);

  const grouped = useMemo(() => {
    const filtered = connectors.filter((c) =>
      c.name.toLowerCase().includes(search.toLowerCase())
    );
    return filtered.reduce<Record<string, ConnectorType[]>>((acc, c) => {
      (acc[c.category] ||= []).push(c);
      return acc;
    }, {});
  }, [connectors, search]);

  const showSchemaFilter = chosen ? FILTERABLE_CONNECTORS.has(chosen.id) : false;
  const totalSteps = showSchemaFilter ? 4 : 3;

  /* Build the config including schema filter */
  const buildConfig = () => {
    const cfg: Record<string, unknown> = {
      max_tables: form.config?.max_tables ?? 50,
      sample_rows: form.config?.sample_rows ?? 10000,
    };
    const inc = includeSchemas
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    const exc = excludeSchemas
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    const incT = includeTables
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    const excT = excludeTables
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    if (inc.length) cfg.include_schemas = inc;
    if (exc.length) cfg.exclude_schemas = exc;
    if (incT.length) cfg.include_tables = incT;
    if (excT.length) cfg.exclude_tables = excT;
    return cfg;
  };

  /* ---- Step 2→3: Register + validate + preview ---- */
  const validateAndPreview = async () => {
    if (!chosen) return;
    setTesting(true);
    setTestOk(null);
    setTestError(null);
    setPreview(null);

    try {
      /* First, register the source (so preview can connect). */
      await api.createSource({
        name: form.name,
        type: chosen.id,
        display_name: form.display_name || form.name,
        host: form.host || undefined,
        uri: form.uri || undefined,
        path: form.path || undefined,
        auto_sync: false, // not syncing yet
        config: buildConfig(),
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      /* If already exists we can still proceed */
      if (!msg.includes("already") && !msg.includes("UNIQUE")) {
        setTestError(msg);
        setTesting(false);
        return;
      }
    }

    /* Test connection */
    try {
      const result = await api.testSource(form.name);
      if (result.status !== "ok") {
        setTestOk(false);
        setTestError(result.detail || "Connection test failed.");
        setTesting(false);
        return;
      }
      setTestOk(true);
    } catch (e) {
      setTestOk(false);
      setTestError(e instanceof Error ? e.message : String(e));
      setTesting(false);
      return;
    }

    /* Now fetch the preview */
    setPreviewing(true);
    try {
      const pv = await api.previewSource(form.name);
      setPreview(pv);
      setStep(showSchemaFilter ? 3 : 2.5);
    } catch (e) {
      setTestError(
        `Connection succeeded but preview failed: ${
          e instanceof Error ? e.message : String(e)
        }`
      );
    }
    setTesting(false);
    setPreviewing(false);
  };

  /* ---- Final step: confirm and sync ---- */
  const confirmAndSync = async () => {
    if (!chosen) return;
    setSubmitting(true);
    try {
      if (form.auto_sync) {
        toast(`Starting sync for ${chosen.name}…`, "info");
        await api.syncSource(form.name);
        toast(`Synced ${form.display_name || form.name}`, "success");
      } else {
        toast(`${chosen.name} source ready`, "success");
      }
      onCreated();
      onClose();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      toast(`Sync failed: ${msg}`, "error");
    }
    setSubmitting(false);
  };

  if (!open) return null;

  /* Determine which visual step we are on */
  const displayStep = step === 2.5 ? 3 : step;

  const stepLabels = showSchemaFilter
    ? ["Choose", "Configure", "Preview", "Confirm"]
    : ["Choose", "Configure", "Confirm"];

  const uriHint = (() => {
    if (!chosen) return "";
    switch (chosen.id) {
      case "redshift":
        return "redshift://user:pass@cluster-endpoint:5439/database";
      case "snowflake":
        return "snowflake://user:pass@account/db/schema?warehouse=WH";
      case "postgres":
        return "postgresql://user:pass@host:port/db";
      case "mysql":
        return "mysql://user:pass@host:port/db";
      default:
        return "";
    }
  })();

  return (
    <div
      className="fixed inset-0 bg-black/40 backdrop-blur-sm z-50 flex items-center justify-center p-5"
      onClick={onClose}
    >
      <div
        className="bg-card rounded-xl w-full max-w-2xl max-h-[85vh] overflow-hidden flex flex-col shadow-2xl border border-border"
        onClick={(e) => e.stopPropagation()}
      >
        {/* -------- Header -------- */}
        <div className="px-6 py-4 border-b border-border flex items-center justify-between">
          <div className="flex-1">
            <div className="flex gap-1.5 mb-2">
              {stepLabels.map((label, i) => (
                <div key={label} className="flex items-center gap-1.5">
                  <div
                    className={`w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-bold transition-colors ${
                      i + 1 <= displayStep
                        ? "bg-accent text-white"
                        : "bg-border text-muted"
                    }`}
                  >
                    {i + 1 < displayStep ? "✓" : i + 1}
                  </div>
                  <span
                    className={`text-[11px] font-medium ${
                      i + 1 === displayStep
                        ? "text-foreground"
                        : "text-muted"
                    }`}
                  >
                    {label}
                  </span>
                  {i < stepLabels.length - 1 && (
                    <div className="w-4 h-px bg-border mx-0.5" />
                  )}
                </div>
              ))}
            </div>
            <div className="text-base font-bold">
              {step === 1 && "Choose a connector"}
              {step === 2 &&
                `Configure ${chosen?.name ?? ""}`}
              {(step === 3 || step === 2.5) && "Review discovery preview"}
              {step === 4 && "Confirm & sync"}
            </div>
          </div>
          <button
            onClick={onClose}
            className="w-7 h-7 rounded-md bg-background hover:bg-border text-muted text-base"
            aria-label="Close"
          >
            ×
          </button>
        </div>

        {/* -------- Body -------- */}
        <div className="px-6 py-5 overflow-y-auto flex-1">
          {/* == STEP 1: Choose connector == */}
          {step === 1 && (
            <>
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search connectors…"
                className="w-full px-3 py-2 border border-border rounded-md bg-background text-sm mb-4"
              />
              {Object.entries(grouped).map(([cat, types]) => (
                <div key={cat} className="mb-4">
                  <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-2">
                    {cat}
                  </div>
                  <div className="grid grid-cols-3 gap-2">
                    {types.map((t) => (
                      <button
                        key={t.id}
                        onClick={() => {
                          if (!t.supported) {
                            toast(
                              `${t.name} is ${t.status ?? "planned"} and not supported in this build.`,
                              "info"
                            );
                            return;
                          }
                          setChosen(t);
                          setForm((f) => ({ ...f, type: t.id }));
                          setStep(2);
                        }}
                        className={`flex items-center gap-2.5 p-2.5 border rounded-lg text-left transition-colors ${
                          t.supported
                            ? "border-border hover:border-accent hover:bg-background"
                            : "border-border opacity-60 cursor-not-allowed"
                        }`}
                      >
                        <div
                          className="w-8 h-8 rounded-md flex items-center justify-center font-mono font-bold shrink-0"
                          style={{
                            background: t.color,
                            color: t.lightGlyph ? "#0f172a" : "#ffffff",
                            fontSize: t.glyph.length > 1 ? 10 : 13,
                          }}
                        >
                          {t.glyph}
                        </div>
                        <div className="min-w-0">
                          <div className="text-[13px] font-medium truncate">
                            {t.name}
                          </div>
                          {!t.supported && (
                            <div className="text-[10px] text-muted">
                              {t.status ?? "planned"}
                            </div>
                          )}
                        </div>
                      </button>
                    ))}
                  </div>
                </div>
              ))}
            </>
          )}

          {/* == STEP 2: Configure == */}
          {step === 2 && chosen && (
            <div className="flex flex-col gap-3.5">
              <Field
                label="Connection name"
                hint="Display name in Headwater"
                value={form.display_name ?? ""}
                onChange={(v) =>
                  setForm((f) => ({ ...f, display_name: v }))
                }
              />
              <Field
                label="Internal id (no spaces)"
                value={form.name}
                onChange={(v) => setForm((f) => ({ ...f, name: v }))}
              />
              {DB_CONNECTORS.has(chosen.id) ? (
                <Field
                  label="Connection URI"
                  hint={uriHint}
                  value={form.uri ?? ""}
                  onChange={(v) => setForm((f) => ({ ...f, uri: v }))}
                />
              ) : null}
              {FILE_CONNECTORS.has(chosen.id) ? (
                <Field
                  label="Path"
                  hint={
                    chosen.id === "duckdb" || chosen.id === "sqlite"
                      ? "Absolute path to a local database file"
                      : "Absolute path to a directory of files"
                  }
                  value={form.path ?? ""}
                  onChange={(v) => setForm((f) => ({ ...f, path: v }))}
                />
              ) : null}

              {/* Schema filter section for warehouse connectors */}
              {showSchemaFilter && (
                <div className="mt-1 pt-3 border-t border-border">
                  <div className="flex items-center gap-2 mb-3">
                    <div className="w-5 h-5 rounded bg-accent/10 flex items-center justify-center text-accent text-[11px] font-bold">
                      ⊞
                    </div>
                    <span className="text-[13px] font-semibold text-foreground">
                      Schema &amp; table filters
                    </span>
                    <span className="text-[10px] text-muted">
                      (leave blank to discover all)
                    </span>
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <Field
                      label="Include schemas"
                      hint="comma-separated"
                      value={includeSchemas}
                      onChange={setIncludeSchemas}
                      placeholder="analytics, reporting"
                    />
                    <Field
                      label="Exclude schemas"
                      hint="comma-separated"
                      value={excludeSchemas}
                      onChange={setExcludeSchemas}
                      placeholder="staging, scratch"
                    />
                    <Field
                      label="Include tables"
                      hint="glob patterns"
                      value={includeTables}
                      onChange={setIncludeTables}
                      placeholder="dim_*, fact_*"
                    />
                    <Field
                      label="Exclude tables"
                      hint="glob patterns"
                      value={excludeTables}
                      onChange={setExcludeTables}
                      placeholder="*_tmp, *_backup"
                    />
                  </div>
                </div>
              )}

              <div className="grid grid-cols-2 gap-3 pt-3 border-t border-border">
                <Field
                  label="Max tables"
                  hint="sync limit"
                  value={String(form.config?.max_tables ?? 50)}
                  onChange={(v) =>
                    setForm((f) => ({
                      ...f,
                      config: {
                        ...(f.config ?? {}),
                        max_tables: Number(v) || 50,
                      },
                    }))
                  }
                />
                <Field
                  label="Sample rows"
                  hint="per table"
                  value={String(form.config?.sample_rows ?? 10000)}
                  onChange={(v) =>
                    setForm((f) => ({
                      ...f,
                      config: {
                        ...(f.config ?? {}),
                        sample_rows: Number(v) || 10000,
                      },
                    }))
                  }
                />
              </div>

              {/* Test/preview feedback */}
              {testError && (
                <div className="mt-2 px-3 py-2 rounded-lg bg-danger/10 border border-danger/30 text-[12px] text-danger">
                  {testError}
                </div>
              )}
              {testOk && !preview && (
                <div className="mt-2 px-3 py-2 rounded-lg bg-success/10 border border-success/30 text-[12px] text-success">
                  ✓ Connection verified. Loading preview…
                </div>
              )}
            </div>
          )}

          {/* == STEP 3 (or 2.5 for non-filterable): Preview == */}
          {(step === 3 || step === 2.5) && preview && (
            <div className="flex flex-col gap-4">
              {/* Summary cards */}
              <div className="grid grid-cols-3 gap-3">
                <PreviewCard
                  label="Schemas"
                  value={String(preview.schemas_found)}
                  detail={
                    preview.schemas.length > 0
                      ? preview.schemas.slice(0, 5).join(", ") +
                        (preview.schemas.length > 5
                          ? ` +${preview.schemas.length - 5} more`
                          : "")
                      : "—"
                  }
                />
                <PreviewCard
                  label="Tables found"
                  value={String(preview.tables_found)}
                  detail={`${preview.tables_considered} will be ingested`}
                  accent
                />
                <PreviewCard
                  label="Est. rows"
                  value={fmt(preview.total_estimated_rows)}
                  detail={
                    preview.has_row_estimates
                      ? "from catalog stats"
                      : "estimates not available"
                  }
                />
              </div>

              {/* Sampling summary */}
              <div className="px-4 py-3 rounded-lg bg-accent/5 border border-accent/20">
                <div className="flex items-center gap-2 mb-1.5">
                  <span className="text-accent text-sm">◈</span>
                  <span className="text-[13px] font-semibold text-foreground">
                    Sampling plan
                  </span>
                </div>
                <div className="text-[12px] text-muted leading-relaxed">
                  Headwater will sample up to{" "}
                  <span className="font-mono text-foreground font-medium">
                    {fmt(preview.sample_rows_per_table)}
                  </span>{" "}
                  rows per table across{" "}
                  <span className="font-mono text-foreground font-medium">
                    {preview.tables_considered}
                  </span>{" "}
                  table(s).
                  {preview.tables_skipped > 0 && (
                    <>
                      {" "}
                      <span className="text-warning">
                        {preview.tables_skipped} table(s) will be skipped
                      </span>{" "}
                      due to the max_tables limit.
                    </>
                  )}
                </div>
              </div>

              {/* Schema filter applied */}
              {preview.config.schema_filter && (
                <div className="px-4 py-2 rounded-lg bg-background border border-border text-[11px]">
                  <span className="font-semibold text-foreground">
                    Active filters:{" "}
                  </span>
                  {Object.entries(preview.config.schema_filter).map(
                    ([k, v]) => (
                      <span key={k} className="text-muted mr-3">
                        {k}={Array.isArray(v) ? v.join(", ") : String(v)}
                      </span>
                    )
                  )}
                </div>
              )}

              {/* Table list */}
              <div>
                <div className="text-[11px] font-bold uppercase tracking-wider text-muted mb-2">
                  Tables to ingest ({preview.tables_considered})
                </div>
                <div className="max-h-48 overflow-y-auto rounded-lg border border-border">
                  <table className="w-full text-[12px]">
                    <thead>
                      <tr className="border-b border-border bg-background">
                        <th className="text-left px-3 py-1.5 font-medium text-muted">
                          Table
                        </th>
                        <th className="text-right px-3 py-1.5 font-medium text-muted">
                          Est. rows
                        </th>
                        <th className="text-right px-3 py-1.5 font-medium text-muted">
                          Sample
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {preview.tables.map((t, i) => (
                        <tr
                          key={t.name}
                          className={
                            i % 2 === 0 ? "" : "bg-background/50"
                          }
                        >
                          <td className="px-3 py-1.5 font-mono text-foreground truncate max-w-[240px]">
                            {t.name}
                          </td>
                          <td className="px-3 py-1.5 text-right text-muted font-mono">
                            {fmt(t.estimated_rows)}
                          </td>
                          <td className="px-3 py-1.5 text-right text-muted font-mono">
                            {t.estimated_rows != null
                              ? fmt(
                                  Math.min(
                                    t.estimated_rows,
                                    preview.sample_rows_per_table
                                  )
                                )
                              : fmt(preview.sample_rows_per_table)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Skipped tables */}
              {preview.tables_skipped > 0 && (
                <div className="text-[11px] text-muted">
                  <span className="font-semibold text-warning">
                    Skipped:
                  </span>{" "}
                  {preview.tables_skipped_names.join(", ")}
                  {preview.tables_skipped > 20 &&
                    ` +${preview.tables_skipped - 20} more`}
                </div>
              )}

              {/* Auto-sync toggle */}
              <label className="flex items-center gap-2 text-[13px] text-foreground mt-1">
                <input
                  type="checkbox"
                  checked={form.auto_sync ?? true}
                  onChange={(e) =>
                    setForm((f) => ({
                      ...f,
                      auto_sync: e.target.checked,
                    }))
                  }
                />
                Proceed with sync after confirmation
              </label>
            </div>
          )}

          {/* == STEP 4 (final confirm step for filterable) — we skip to confirm directly == */}
        </div>

        {/* -------- Footer -------- */}
        <div className="px-6 py-4 border-t border-border flex justify-between gap-2">
          <button
            onClick={() => {
              if (step > 1) {
                if (step === 3 || step === 2.5) setStep(2);
                else if (step === 4) setStep(3);
                else setStep(1);
              } else {
                onClose();
              }
            }}
            className="px-3 py-1.5 border border-border rounded-md text-sm text-muted hover:bg-background"
          >
            {step > 1 ? "← Back" : "Cancel"}
          </button>
          <div className="flex gap-2">
            {step === 2 && (
              <button
                onClick={validateAndPreview}
                disabled={testing || previewing}
                className="px-4 py-1.5 bg-accent text-white rounded-md text-sm font-medium hover:opacity-90 disabled:opacity-50 flex items-center gap-2"
              >
                {testing || previewing ? (
                  <>
                    <span className="inline-block w-3.5 h-3.5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                    {testing
                      ? "Validating…"
                      : "Loading preview…"}
                  </>
                ) : (
                  "Validate & preview →"
                )}
              </button>
            )}
            {(step === 3 || step === 2.5) && preview && (
              <button
                onClick={confirmAndSync}
                disabled={submitting}
                className="px-4 py-1.5 bg-success text-white rounded-md text-sm font-medium hover:opacity-90 disabled:opacity-50 flex items-center gap-2"
              >
                {submitting ? (
                  <>
                    <span className="inline-block w-3.5 h-3.5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                    Syncing…
                  </>
                ) : form.auto_sync ? (
                  `Confirm & sync ${preview.tables_considered} tables`
                ) : (
                  "Confirm"
                )}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Reusable sub-components                                           */
/* ------------------------------------------------------------------ */

function PreviewCard({
  label,
  value,
  detail,
  accent,
}: {
  label: string;
  value: string;
  detail: string;
  accent?: boolean;
}) {
  return (
    <div
      className={`px-4 py-3 rounded-lg border ${
        accent
          ? "border-accent/30 bg-accent/5"
          : "border-border bg-background"
      }`}
    >
      <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-1">
        {label}
      </div>
      <div
        className={`text-xl font-bold font-mono ${
          accent ? "text-accent" : "text-foreground"
        }`}
      >
        {value}
      </div>
      <div className="text-[11px] text-muted mt-0.5 truncate">{detail}</div>
    </div>
  );
}

function Field({
  label,
  hint,
  value,
  onChange,
  placeholder,
}: {
  label: string;
  hint?: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
}) {
  return (
    <div>
      <div className="flex gap-2 items-baseline mb-1.5">
        <label className="text-[13px] font-medium text-foreground">
          {label}
        </label>
        {hint && <span className="text-[11px] text-muted">{hint}</span>}
      </div>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="w-full px-3 py-2 border border-border rounded-md bg-background text-sm font-mono placeholder:text-muted/50"
      />
    </div>
  );
}
