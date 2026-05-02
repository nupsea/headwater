"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  api,
  type ConnectionTestResult,
  type ConnectorType,
  type Project,
  type SourceCreatePayload,
} from "@/lib/api";
import { useToast } from "@/components/toast";

interface CreateProjectDialogProps {
  open: boolean;
  onClose: () => void;
  onCreated: (project: Project) => void;
}

type Step = 1 | 2 | 3;

type ConnectionForm = {
  source_name: string;
  display_name: string;
  host: string;
  port: string;
  database: string;
  schema: string;
  warehouse: string;
  role: string;
  user: string;
  password: string;
  path: string;
  auto_sync: boolean;
  max_tables: number;
  sample_rows: number;
};

const DEFAULT_FORM: ConnectionForm = {
  source_name: "",
  display_name: "",
  host: "",
  port: "",
  database: "",
  schema: "",
  warehouse: "",
  role: "",
  user: "",
  password: "",
  path: "",
  auto_sync: true,
  max_tables: 50,
  sample_rows: 10_000,
};

const CONNECTOR_HINTS: Record<string, { title: string; bullets: string[] }> = {
  snowflake: {
    title: "Snowflake connection guide",
    bullets: [
      "Use the Snowflake account identifier in the host field.",
      "Set warehouse, database, and schema explicitly so Headwater can test and browse safely.",
      "Keep the role narrow. Read-only access is enough for discovery and ingestion.",
    ],
  },
  postgres: {
    title: "Postgres connection guide",
    bullets: [
      "Use a read-only user with access to the schemas you want to evaluate.",
      "Provide the database name and host before you test the connection.",
      "Start with the default port 5432 unless your instance uses a custom port.",
    ],
  },
  mysql: {
    title: "MySQL connection guide",
    bullets: [
      "Use a read-only user for discovery and profiling.",
      "Specify the database name and port. The default port is 3306.",
      "Keep the connection scoped to the schemas you want Headwater to evaluate.",
    ],
  },
  sqlite: {
    title: "SQLite connection guide",
    bullets: [
      "Point to a local `.sqlite` or `.db` file on disk.",
      "Headwater opens the database read-only and never mutates it.",
      "Use this for embedded or hand-off analysis databases.",
    ],
  },
  duckdb: {
    title: "DuckDB connection guide",
    bullets: [
      "Point to a local DuckDB file on disk.",
      "Headwater opens the file read-only for catalog and profiling work.",
      "This is a good fit for local analytical datasets and shared extracts.",
    ],
  },
  json: {
    title: "JSON source guide",
    bullets: [
      "Point to a directory that contains the JSON files for the project.",
      "Use a stable folder root so source sync can discover files consistently.",
      "Headwater will treat the directory as a file source, not a database.",
    ],
  },
  csv: {
    title: "CSV source guide",
    bullets: [
      "Point to a directory that contains the CSV files for the project.",
      "Use a stable folder root so source sync can discover files consistently.",
      "Headwater will treat the directory as a file source, not a database.",
    ],
  },
};

export function CreateProjectDialog({
  open,
  onClose,
  onCreated,
}: CreateProjectDialogProps) {
  const { toast } = useToast();
  const [step, setStep] = useState<Step>(1);
  const [projectName, setProjectName] = useState("");
  const [projectDescription, setProjectDescription] = useState("");
  const [connectors, setConnectors] = useState<ConnectorType[]>([]);
  const [selectedConnector, setSelectedConnector] = useState<ConnectorType | null>(null);
  const [search, setSearch] = useState("");
  const [form, setForm] = useState<ConnectionForm>(DEFAULT_FORM);
  const [preflight, setPreflight] = useState<ConnectionTestResult | null>(null);
  const preflightRef = useRef<ConnectionTestResult | null>(null);
  const [testing, setTesting] = useState(false);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState("");

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
      setProjectName("");
      setProjectDescription("");
      setSelectedConnector(null);
      setSearch("");
      setForm(DEFAULT_FORM);
      setPreflight(null);
      preflightRef.current = null;
      setTesting(false);
      setCreating(false);
      setError("");
    }
  }, [open]);

  useEffect(() => {
    if (!selectedConnector) return;
    setForm((current) => {
      const next = { ...current };
      if (!next.display_name) next.display_name = selectedConnector.name;
      if (!next.source_name) {
        const slug = slugify(projectName || selectedConnector.name);
        next.source_name = `${slug}-${selectedConnector.id}`;
      }
      if (selectedConnector.id === "snowflake") {
        if (!next.schema) next.schema = "PUBLIC";
        if (!next.port) next.port = "";
      }
      if (selectedConnector.id === "postgres" && !next.port) next.port = "5432";
      if (selectedConnector.id === "mysql" && !next.port) next.port = "3306";
      return next;
    });
  }, [projectName, selectedConnector]);

  useEffect(() => {
    if (preflightRef.current) {
      setPreflight(null);
      preflightRef.current = null;
    }
  }, [
    projectName,
    form.auto_sync,
    form.database,
    form.display_name,
    form.host,
    form.max_tables,
    form.password,
    form.path,
    form.port,
    form.role,
    form.sample_rows,
    form.schema,
    form.source_name,
    form.user,
    form.warehouse,
  ]);

  const groupedConnectors = useMemo(() => {
    const filtered = connectors.filter(
      (connector) =>
        connector.name.toLowerCase().includes(search.toLowerCase()) ||
        connector.id.toLowerCase().includes(search.toLowerCase())
    );
    return filtered.reduce<Record<string, ConnectorType[]>>((acc, connector) => {
      (acc[connector.category] ||= []).push(connector);
      return acc;
    }, {});
  }, [connectors, search]);

  if (!open) return null;

  const selectConnector = (connector: ConnectorType) => {
    if (!connector.supported) {
      toast(
        `${connector.name} is ${connector.status} and not supported in this build.`,
        "info"
      );
      return;
    }
    setSelectedConnector(connector);
    setPreflight(null);
    preflightRef.current = null;
    setError("");
    setStep(2);
  };

  const testConnection = async () => {
    if (!selectedConnector) {
      setError("Choose a connector first.");
      return;
    }
    const target = buildConnectionTarget(selectedConnector.id, form);
    if (!target) {
      setError("Fill the connection fields before testing.");
      return;
    }
    setTesting(true);
    setError("");
    setPreflight(null);
    try {
      const result = await api.testConnection(target, selectedConnector.id);
      setPreflight(result);
      preflightRef.current = result;
      toast(
        result.status === "ok"
          ? `Connection validated: ${result.tables} table(s) found`
          : "Connection test failed",
        result.status === "ok" ? "success" : "error"
      );
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      setError(message);
      toast(`Connection test failed: ${message}`, "error");
    } finally {
      setTesting(false);
    }
  };

  const createProjectOnly = async () => {
    if (!projectName.trim()) {
      setError("Project name is required.");
      return;
    }
    setCreating(true);
    setError("");
    try {
      const project = await api.createProject({
        display_name: projectName.trim(),
        description: projectDescription.trim() || undefined,
      });
      toast("Project created", "success");
      onCreated(project);
      onClose();
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      setError(message);
      toast(`Project creation failed: ${message}`, "error");
    } finally {
      setCreating(false);
    }
  };

  const createAndIngest = async () => {
    if (!projectName.trim()) {
      setError("Project name is required.");
      return;
    }
    if (!selectedConnector) {
      setError("Choose a connector or create the project only.");
      return;
    }
    if (!preflight || preflight.status !== "ok") {
      setError("Test the connection before creating the source.");
      return;
    }
    const target = buildConnectionTarget(selectedConnector.id, form);
    if (!target) {
      setError("Fill the connection fields before creating the source.");
      return;
    }

    setCreating(true);
    setError("");
    try {
      const project = await api.createProject({
        display_name: projectName.trim(),
        description: projectDescription.trim() || undefined,
      });
      const sourcePayload: SourceCreatePayload = {
        name: form.source_name.trim() || slugify(`${project.display_name}-${selectedConnector.id}`),
        type: selectedConnector.id,
        display_name: form.display_name.trim() || `${project.display_name} ${selectedConnector.name}`,
        auto_sync: form.auto_sync,
        config: {
          max_tables: form.max_tables,
          sample_rows: form.sample_rows,
          connector: selectedConnector.id,
          connection: {
            host: form.host.trim() || undefined,
            port: form.port.trim() || undefined,
            database: form.database.trim() || undefined,
            schema: form.schema.trim() || undefined,
            warehouse: form.warehouse.trim() || undefined,
            role: form.role.trim() || undefined,
            path: form.path.trim() || undefined,
          },
        },
      };
      const connectionValue = buildConnectionValue(selectedConnector.id, form);
      if ("path" in connectionValue) {
        sourcePayload.path = connectionValue.path;
      }
      if ("uri" in connectionValue) {
        sourcePayload.uri = connectionValue.uri;
      }
      if ("host" in connectionValue) {
        sourcePayload.host = connectionValue.host;
      }
      if (selectedConnector.id === "snowflake" || selectedConnector.id === "postgres" || selectedConnector.id === "mysql") {
        sourcePayload.host = form.host.trim() || undefined;
      }
      const source = await api.createSource(sourcePayload);
      const persistedTest = await api.testSource(source.name);
      if (persistedTest.status !== "ok") {
        await api.deleteSource(source.name).catch(() => {});
        throw new Error(persistedTest.detail || "Persisted source validation failed.");
      }
      if (form.auto_sync) {
        toast(`Connected ${source.display_name || source.name}; starting sync`, "info");
        await api.syncSource(source.name);
      }
      onCreated(project);
      toast(
        form.auto_sync
          ? `Created project and ingested ${source.display_name || source.name}`
          : `Created project and connected ${source.display_name || source.name}`,
        "success"
      );
      onClose();
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      setError(message);
      toast(`Setup failed: ${message}`, "error");
    } finally {
      setCreating(false);
    }
  };

  const guide = selectedConnector ? CONNECTOR_HINTS[selectedConnector.id] : null;
  const connectionPreview =
    selectedConnector ? previewConnectionValue(selectedConnector.id, form) : "";
  const canTest = Boolean(selectedConnector && buildConnectionTarget(selectedConnector.id, form));
  const canConnect = Boolean(selectedConnector && preflight?.status === "ok");

  return (
    <div
      className="fixed inset-0 z-50 bg-black/40 backdrop-blur-sm flex items-center justify-center p-5"
      onClick={onClose}
    >
      <div
        className="w-full max-w-5xl max-h-[90vh] overflow-hidden bg-card border border-border rounded-xl shadow-2xl flex flex-col"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="px-6 py-4 border-b border-border flex items-center justify-between">
          <div>
            <div className="text-[10px] font-bold uppercase tracking-wider text-muted">
              Guided setup
            </div>
            <div className="text-lg font-semibold">
              Start a project, connect a source, and ingest safely
            </div>
          </div>
          <button
            onClick={onClose}
            className="h-8 w-8 rounded-md border border-border text-muted hover:text-foreground hover:bg-background"
            aria-label="Close"
          >
            ×
          </button>
        </div>

        <div className="px-6 py-4 border-b border-border flex gap-2 text-[11px]">
          {[
            "Project",
            "Source",
            "Test",
            "Ingest",
          ].map((label, index) => {
            const current = index + 1;
            const active = step === current;
            const done = step > current;
            return (
              <div
                key={label}
                className={`flex items-center gap-2 px-3 py-1.5 rounded-full border ${
                  active
                    ? "border-accent bg-accent/10 text-accent"
                    : done
                      ? "border-green-200 bg-green-50 text-green-700 dark:border-green-900 dark:bg-green-950/30 dark:text-green-300"
                      : "border-border text-muted"
                }`}
              >
                <span className="font-mono text-[10px]">{current}</span>
                <span>{label}</span>
              </div>
            );
          })}
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-[1.2fr_0.8fr] gap-0 flex-1 min-h-0">
          <div className="p-6 overflow-y-auto">
            {error && (
              <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/30 dark:text-red-300">
                {error}
              </div>
            )}

            {step === 1 && (
              <div className="space-y-4">
                <SectionTitle
                  title="Project details"
                  detail="Create the workspace that will hold the review queue, models, and insight work."
                />
                <Field
                  label="Project name"
                  value={projectName}
                  onChange={(value) => setProjectName(value)}
                  placeholder="Retail analytics"
                />
                <Field
                  label="Description"
                  value={projectDescription}
                  onChange={(value) => setProjectDescription(value)}
                  placeholder="Optional short description"
                />
              </div>
            )}

            {step === 2 && (
              <div className="space-y-5">
                <SectionTitle
                  title="Choose a source"
                  detail="Pick the warehouse, database, or file source you want to connect for this project."
                />
                <input
                  value={search}
                  onChange={(event) => setSearch(event.target.value)}
                  placeholder="Search connector types"
                  className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm"
                />
                <div className="space-y-4">
                  {Object.entries(groupedConnectors).map(([category, items]) => (
                    <div key={category}>
                      <div className="mb-2 text-[10px] font-bold uppercase tracking-wider text-muted">
                        {category}
                      </div>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                        {items.map((connector) => {
                          const active = selectedConnector?.id === connector.id;
                          return (
                            <button
                              key={connector.id}
                              onClick={() => selectConnector(connector)}
                              className={`flex items-center gap-3 rounded-lg border px-3 py-3 text-left transition-colors ${
                                active
                                  ? "border-accent bg-accent/5"
                                  : connector.supported
                                    ? "border-border hover:border-accent/40 hover:bg-background"
                                    : "border-border opacity-60 cursor-not-allowed"
                              }`}
                            >
                              <div
                                className="h-10 w-10 shrink-0 rounded-md flex items-center justify-center font-mono text-sm font-bold"
                                style={{
                                  background: connector.color,
                                  color: connector.lightGlyph ? "#0f172a" : "#ffffff",
                                }}
                              >
                                {connector.glyph}
                              </div>
                              <div className="min-w-0">
                                <div className="flex items-center gap-2">
                                  <span className="text-sm font-medium truncate">
                                    {connector.name}
                                  </span>
                                  {!connector.supported && (
                                    <span className="rounded-full border border-border px-1.5 py-0.5 text-[9px] uppercase tracking-wider text-muted">
                                      {connector.status}
                                    </span>
                                  )}
                                </div>
                                <div className="text-[11px] text-muted">
                                  {connector.category}
                                </div>
                              </div>
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {step === 3 && selectedConnector && (
              <div className="space-y-5">
                <SectionTitle
                  title="Connection inputs"
                  detail="Fill the fields for the selected connector, test the connection, then create the source and ingest."
                />

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                  <Field
                    label="Source id"
                    value={form.source_name}
                    onChange={(value) => setForm((current) => ({ ...current, source_name: value }))}
                    placeholder={slugify(`${projectName || "project"}-${selectedConnector.id}`)}
                  />
                  <Field
                    label="Display name"
                    value={form.display_name}
                    onChange={(value) => setForm((current) => ({ ...current, display_name: value }))}
                    placeholder={`${projectName || "Project"} ${selectedConnector.name}`}
                  />
                </div>

                {selectedConnector.id === "snowflake" && (
                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                    <Field
                      label="Account"
                      value={form.host}
                      onChange={(value) => setForm((current) => ({ ...current, host: value }))}
                      placeholder="acme-org.us-east-1"
                    />
                    <Field
                      label="Warehouse"
                      value={form.warehouse}
                      onChange={(value) => setForm((current) => ({ ...current, warehouse: value }))}
                      placeholder="ANALYTICS_WH"
                    />
                    <Field
                      label="Database"
                      value={form.database}
                      onChange={(value) => setForm((current) => ({ ...current, database: value }))}
                      placeholder="WAREHOUSE_DB"
                    />
                    <Field
                      label="Schema"
                      value={form.schema}
                      onChange={(value) => setForm((current) => ({ ...current, schema: value }))}
                      placeholder="PUBLIC"
                    />
                    <Field
                      label="Role"
                      value={form.role}
                      onChange={(value) => setForm((current) => ({ ...current, role: value }))}
                      placeholder="HEADWATER_READONLY"
                    />
                    <Field
                      label="User"
                      value={form.user}
                      onChange={(value) => setForm((current) => ({ ...current, user: value }))}
                      placeholder="headwater_ro"
                    />
                    <Field
                      label="Password"
                      type="password"
                      value={form.password}
                      onChange={(value) => setForm((current) => ({ ...current, password: value }))}
                      placeholder="••••••••"
                    />
                  </div>
                )}

                {selectedConnector.id === "postgres" ||
                selectedConnector.id === "mysql" ? (
                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                    <Field
                      label="Host"
                      value={form.host}
                      onChange={(value) => setForm((current) => ({ ...current, host: value }))}
                      placeholder="db.example.com"
                    />
                    <Field
                      label="Port"
                      value={form.port}
                      onChange={(value) => setForm((current) => ({ ...current, port: value }))}
                      placeholder={selectedConnector.id === "mysql" ? "3306" : "5432"}
                    />
                    <Field
                      label="Database"
                      value={form.database}
                      onChange={(value) => setForm((current) => ({ ...current, database: value }))}
                      placeholder="analytics"
                    />
                    <Field
                      label="User"
                      value={form.user}
                      onChange={(value) => setForm((current) => ({ ...current, user: value }))}
                      placeholder="read_only"
                    />
                    <Field
                      label="Password"
                      type="password"
                      value={form.password}
                      onChange={(value) => setForm((current) => ({ ...current, password: value }))}
                      placeholder="••••••••"
                    />
                  </div>
                ) : null}

                {selectedConnector.id === "sqlite" ||
                selectedConnector.id === "duckdb" ||
                selectedConnector.id === "json" ||
                selectedConnector.id === "csv" ? (
                  <Field
                    label="Path"
                    value={form.path}
                    onChange={(value) => setForm((current) => ({ ...current, path: value }))}
                    placeholder={
                      selectedConnector.id === "json" || selectedConnector.id === "csv"
                        ? "/path/to/data"
                        : "/path/to/database.db"
                    }
                  />
                ) : null}

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 border-t border-border pt-4">
                  <Field
                    label="Max tables"
                    value={String(form.max_tables)}
                    onChange={(value) =>
                      setForm((current) => ({
                        ...current,
                        max_tables: Number(value) || 50,
                      }))
                    }
                    type="number"
                    placeholder="50"
                  />
                  <Field
                    label="Sample rows"
                    value={String(form.sample_rows)}
                    onChange={(value) =>
                      setForm((current) => ({
                        ...current,
                        sample_rows: Number(value) || 10_000,
                      }))
                    }
                    type="number"
                    placeholder="10000"
                  />
                </div>

                <label className="flex items-center gap-2 text-sm text-foreground">
                  <input
                    type="checkbox"
                    checked={form.auto_sync}
                    onChange={(event) =>
                      setForm((current) => ({
                        ...current,
                        auto_sync: event.target.checked,
                      }))
                    }
                  />
                  Run sync after connection is created
                </label>
              </div>
            )}
          </div>

          <div className="border-t xl:border-t-0 xl:border-l border-border bg-background/50 p-6 overflow-y-auto">
            <div className="space-y-5">
              <div>
                <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-2">
                  Connection guide
                </div>
                {guide ? (
                  <>
                    <div className="text-sm font-semibold mb-2">{guide.title}</div>
                    <ul className="space-y-2">
                      {guide.bullets.map((bullet) => (
                        <li key={bullet} className="text-[12px] text-muted leading-5">
                          {bullet}
                        </li>
                      ))}
                    </ul>
                  </>
                ) : (
                  <div className="text-sm text-muted">
                    Pick a supported connector to see the setup instructions.
                  </div>
                )}
              </div>

              {selectedConnector && (
                <div>
                  <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-2">
                    Connection preview
                  </div>
                  <div className="rounded-lg border border-border bg-card px-3 py-2 text-[11px] font-mono break-all">
                    {connectionPreview || "Fill the connection fields to preview the target."}
                  </div>
                </div>
              )}

              {preflight && (
                <div
                  className={`rounded-lg border px-3 py-2 ${
                    preflight.status === "ok"
                      ? "border-green-200 bg-green-50 text-green-700 dark:border-green-900 dark:bg-green-950/30 dark:text-green-300"
                      : "border-red-200 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950/30 dark:text-red-300"
                  }`}
                >
                  <div className="text-sm font-semibold mb-1">
                    {preflight.status === "ok" ? "Connection ready" : "Connection failed"}
                  </div>
                  <div className="text-[12px] leading-5">{preflight.detail}</div>
                  {preflight.status === "ok" && preflight.table_names?.length ? (
                    <div className="mt-2 text-[11px] text-inherit/80">
                      Example tables: {preflight.table_names.slice(0, 4).join(", ")}
                    </div>
                  ) : null}
                </div>
              )}

              <div className="rounded-lg border border-border bg-card px-3 py-3 text-[12px] text-muted space-y-2">
                <div className="font-semibold text-foreground">What happens next</div>
                <div>1. Headwater creates the project workspace.</div>
                <div>2. The source is registered with the connection details you entered.</div>
                <div>3. The connection is tested again from the persisted source record.</div>
                <div>4. If sync is enabled, Headwater ingests tables using bounded sampling.</div>
              </div>
            </div>
          </div>
        </div>

        <div className="px-6 py-4 border-t border-border flex flex-wrap items-center justify-between gap-3">
          <div className="flex gap-2">
            {step > 1 && (
              <button
                onClick={() => setStep((current) => Math.max(1, current - 1) as Step)}
                className="rounded-md border border-border px-3 py-2 text-sm text-muted hover:bg-background"
              >
                Back
              </button>
            )}
            <button
              onClick={onClose}
              className="rounded-md border border-border px-3 py-2 text-sm text-muted hover:bg-background"
            >
              Cancel
            </button>
          </div>

          <div className="flex gap-2">
            {step === 1 && (
              <>
                <button
                  onClick={createProjectOnly}
                  disabled={creating || !projectName.trim()}
                  className="rounded-md border border-border px-4 py-2 text-sm font-medium hover:bg-background disabled:opacity-50"
                >
                  {creating ? "Creating..." : "Create project only"}
                </button>
                <button
                  onClick={() => setStep(2)}
                  disabled={!projectName.trim()}
                  className="rounded-md bg-accent px-4 py-2 text-sm font-medium text-white hover:opacity-90 disabled:opacity-50"
                >
                  Continue
                </button>
              </>
            )}

            {step === 2 && (
              <button
                onClick={() => setStep(3)}
                disabled={!selectedConnector}
                className="rounded-md bg-accent px-4 py-2 text-sm font-medium text-white hover:opacity-90 disabled:opacity-50"
              >
                Continue to test
              </button>
            )}

            {step === 3 && selectedConnector && (
              <>
                <button
                  onClick={testConnection}
                  disabled={testing || !canTest}
                  className="rounded-md border border-border px-4 py-2 text-sm font-medium hover:bg-background disabled:opacity-50"
                >
                  {testing ? "Testing..." : "Test connection"}
                </button>
                <button
                  onClick={createProjectOnly}
                  disabled={creating || !projectName.trim()}
                  className="rounded-md border border-border px-4 py-2 text-sm font-medium hover:bg-background disabled:opacity-50"
                >
                  {creating ? "Creating..." : "Project only"}
                </button>
                <button
                  onClick={createAndIngest}
                  disabled={creating || !canConnect}
                  className="rounded-md bg-accent px-4 py-2 text-sm font-medium text-white hover:opacity-90 disabled:opacity-50"
                >
                  {creating
                    ? form.auto_sync
                      ? "Creating & syncing..."
                      : "Creating..."
                    : form.auto_sync
                      ? "Create project & sync"
                      : "Create project & connect"}
                </button>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function Field({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  type?: string;
}) {
  return (
    <div>
      <label className="mb-1.5 block text-[12px] font-medium text-foreground">
        {label}
      </label>
      <input
        type={type}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm"
      />
    </div>
  );
}

function SectionTitle({
  title,
  detail,
}: {
  title: string;
  detail: string;
}) {
  return (
    <div>
      <div className="text-[10px] font-bold uppercase tracking-wider text-muted mb-1">
        Setup step
      </div>
      <div className="text-lg font-semibold">{title}</div>
      <p className="mt-1 text-sm text-muted">{detail}</p>
    </div>
  );
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function buildConnectionTarget(
  connectorId: string,
  form: ConnectionForm
): string | null {
  const built = buildConnectionValue(connectorId, form);
  if ("uri" in built) return built.uri;
  if ("path" in built) return built.path;
  return null;
}

function buildConnectionValue(
  connectorId: string,
  form: ConnectionForm
): { uri: string } | { path: string } | { host: string } {
  if (connectorId === "snowflake") {
    const user = encodeURIComponent(form.user.trim());
    const password = encodeURIComponent(form.password.trim());
    const host = form.host.trim();
    const database = form.database.trim();
    const schema = form.schema.trim();
    const params = new URLSearchParams();
    if (form.warehouse.trim()) params.set("warehouse", form.warehouse.trim());
    if (form.role.trim()) params.set("role", form.role.trim());
    const query = params.toString();
    return {
      uri: `snowflake://${user}:${password}@${host}/${database}/${schema}${query ? `?${query}` : ""}`,
    };
  }
  if (connectorId === "postgres") {
    const user = encodeURIComponent(form.user.trim());
    const password = encodeURIComponent(form.password.trim());
    const host = form.host.trim();
    const port = form.port.trim() || "5432";
    const database = form.database.trim();
    return {
      uri: `postgresql://${user}:${password}@${host}:${port}/${database}`,
    };
  }
  if (connectorId === "mysql") {
    const user = encodeURIComponent(form.user.trim());
    const password = encodeURIComponent(form.password.trim());
    const host = form.host.trim();
    const port = form.port.trim() || "3306";
    const database = form.database.trim();
    return {
      uri: `mysql://${user}:${password}@${host}:${port}/${database}`,
    };
  }
  if (connectorId === "sqlite") {
    return { path: form.path.trim() };
  }
  if (connectorId === "duckdb") {
    return { path: form.path.trim() };
  }
  return { path: form.path.trim() };
}

function previewConnectionValue(
  connectorId: string,
  form: ConnectionForm
): string {
  const built = buildConnectionValue(connectorId, form);
  if ("uri" in built) {
    return built.uri.replace(/:\/\/([^:]+):([^@]+)@/, "://$1:***@");
  }
  return "path" in built ? built.path : "";
}
