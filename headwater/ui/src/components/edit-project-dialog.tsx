"use client";

import { useEffect, useMemo, useState } from "react";
import {
  api,
  type ConnectionTestResult,
  type ConnectorType,
  type Project,
  type SourceDetail,
} from "@/lib/api";
import { ConnectionTestResultPanel } from "@/components/connection-test-result";
import { useToast } from "@/components/toast";

type EditProjectDialogProps = {
  open: boolean;
  project: Project | null;
  onClose: () => void;
  onSaved: (project: Project) => Promise<void> | void;
};

type ConnectionForm = {
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
  include_schemas: string;
  exclude_schemas: string;
  include_tables: string;
  exclude_tables: string;
};

type ConnectionStringField =
  | "host"
  | "port"
  | "database"
  | "schema"
  | "warehouse"
  | "role"
  | "user"
  | "path";

const DEFAULT_FORM: ConnectionForm = {
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
  sample_rows: 10000,
  include_schemas: "",
  exclude_schemas: "",
  include_tables: "",
  exclude_tables: "",
};

const FILTERABLE_CONNECTORS = new Set(["redshift", "snowflake", "postgres", "mysql"]);
const DATABASE_CONNECTORS = new Set(["postgres", "mysql", "snowflake", "redshift"]);

export function EditProjectDialog({
  open,
  project,
  onClose,
  onSaved,
}: EditProjectDialogProps) {
  const { toast } = useToast();
  const [connectors, setConnectors] = useState<ConnectorType[]>([]);
  const [projectName, setProjectName] = useState("");
  const [description, setDescription] = useState("");
  const [source, setSource] = useState<SourceDetail | null>(null);
  const [selectedConnectorId, setSelectedConnectorId] = useState("");
  const [form, setForm] = useState<ConnectionForm>(DEFAULT_FORM);
  const [rerunAfterSave, setRerunAfterSave] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [error, setError] = useState("");
  const [testResult, setTestResult] = useState<ConnectionTestResult | null>(null);

  const sourceName = project?.sources?.[0] ?? "";

  useEffect(() => {
    if (!open) return;
    api
      .connectorCatalog()
      .then((response) => setConnectors(response.connectors.filter((item) => item.supported)))
      .catch(() => {});
  }, [open]);

  useEffect(() => {
    if (!open || !project) return;
    setProjectName(project.display_name);
    setDescription(project.description || "");
    setError("");
    setTestResult(null);
    setSource(null);
    setForm(DEFAULT_FORM);
    setSelectedConnectorId("");
    if (!sourceName) return;
    api
      .source(sourceName)
      .then((detail) => {
        setSource(detail);
        setSelectedConnectorId(detail.type);
        setForm(readConnectionForm(detail));
      })
      .catch((e) => {
        const message = e instanceof Error ? e.message : String(e);
        setError(message);
      });
  }, [open, project, sourceName]);

  const selectedConnector = useMemo(
    () => connectors.find((item) => item.id === selectedConnectorId) ?? null,
    [connectors, selectedConnectorId]
  );

  const showFilters = selectedConnector ? FILTERABLE_CONNECTORS.has(selectedConnector.id) : false;
  const canTest = Boolean(selectedConnector && buildConnectionTarget(selectedConnector.id, form));

  if (!open || !project) return null;

  const testConnection = async () => {
    if (!selectedConnector) return;
    const target = buildConnectionTarget(selectedConnector.id, form);
    if (!target) {
      setError("Fill the connection fields before testing.");
      return;
    }
    setTesting(true);
    setError("");
    setTestResult(null);
    try {
      const result = await api.testConnection(target, selectedConnector.id, buildFilterConfig(form));
      setTestResult(result);
      toast(
        result.status === "ok" ? `Connection validated: ${result.tables} table(s) found` : "Connection test failed",
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

  const save = async () => {
    if (!projectName.trim()) {
      setError("Project name is required.");
      return;
    }
    setSaving(true);
    setError("");
    try {
      let updatedProject = await api.updateProject(project.id, {
        display_name: projectName.trim(),
        description: description.trim(),
        sources: project.sources,
      });

      if (sourceName && selectedConnector) {
        const connectionValue = resolveConnectionValue(selectedConnector.id, form, source);
        if (!connectionValue) {
          const message =
            "Re-enter the source password before saving connection changes.";
          setError(message);
          toast(message, "error");
          return;
        }
        const sourceBody = {
          type: selectedConnector.id,
          display_name: form.display_name.trim() || source?.display_name || sourceName,
          host: form.host.trim() || undefined,
          auto_sync: form.auto_sync,
          ...(connectionValue.kind === "uri"
            ? { uri: connectionValue.value }
            : connectionValue.kind === "path"
              ? { path: connectionValue.value }
              : {}),
          config: {
            max_tables: form.max_tables,
            sample_rows: form.sample_rows,
            ...buildFilterConfig(form),
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
        await api.updateSource(sourceName, sourceBody);
        if (rerunAfterSave) {
          toast(`Re-ingesting ${sourceName}`, "info");
          await api.syncSource(sourceName);
        }
      }

      updatedProject = await api.project(project.id);
      toast(
        rerunAfterSave && sourceName ? "Project updated and re-ingested" : "Project updated",
        "success"
      );
      await onSaved(updatedProject);
      onClose();
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      setError(message);
      toast(`Update failed: ${message}`, "error");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div
      className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-5"
      onClick={onClose}
    >
      <div
        className="bg-card border border-border rounded-lg shadow-xl w-full max-w-4xl max-h-[90vh] overflow-hidden flex flex-col"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="px-5 py-4 border-b border-border flex items-center justify-between">
          <div>
            <h2 className="text-base font-semibold">Edit project</h2>
            <p className="text-sm text-muted mt-1">
              Update the linked source filters or connection, then optionally re-run ingestion.
            </p>
          </div>
          <button
            onClick={onClose}
            className="h-8 w-8 rounded-md border border-border text-muted hover:text-foreground hover:bg-background"
            aria-label="Close"
          >
            ×
          </button>
        </div>

        <div className="p-5 overflow-y-auto space-y-5">
          {error ? (
            <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
              {error}
            </div>
          ) : null}

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
            <Field label="Project name" value={projectName} onChange={setProjectName} />
            <Field
              label="Primary source id"
              value={sourceName || "No source linked"}
              onChange={() => {}}
              readOnly
            />
          </div>

          <div>
            <label className="mb-1.5 block text-[12px] font-medium text-foreground">
              Description
            </label>
            <textarea
              value={description}
              onChange={(event) => setDescription(event.target.value)}
              rows={3}
              className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm resize-none"
            />
          </div>

          {sourceName ? (
            <>
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                <SelectField
                  label="Connector"
                  value={selectedConnectorId}
                  onChange={setSelectedConnectorId}
                  options={connectors.map((item) => ({ value: item.id, label: item.name }))}
                />
                <Field
                  label="Source display name"
                  value={form.display_name}
                  onChange={(value) => setForm((current) => ({ ...current, display_name: value }))}
                />
              </div>

              {selectedConnector && DATABASE_CONNECTORS.has(selectedConnector.id) ? (
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                  <Field
                    label={selectedConnector.id === "snowflake" ? "Account" : "Host"}
                    value={form.host}
                    onChange={(value) => setForm((current) => ({ ...current, host: value }))}
                  />
                  <Field
                    label="Port"
                    value={form.port}
                    onChange={(value) => setForm((current) => ({ ...current, port: value }))}
                  />
                  <Field
                    label="Database"
                    value={form.database}
                    onChange={(value) => setForm((current) => ({ ...current, database: value }))}
                  />
                  {selectedConnector.id === "snowflake" ? (
                    <Field
                      label="Schema"
                      value={form.schema}
                      onChange={(value) => setForm((current) => ({ ...current, schema: value }))}
                    />
                  ) : null}
                  {selectedConnector.id === "snowflake" ? (
                    <Field
                      label="Warehouse"
                      value={form.warehouse}
                      onChange={(value) =>
                        setForm((current) => ({ ...current, warehouse: value }))
                      }
                    />
                  ) : null}
                  {selectedConnector.id === "snowflake" ? (
                    <Field
                      label="Role"
                      value={form.role}
                      onChange={(value) => setForm((current) => ({ ...current, role: value }))}
                    />
                  ) : null}
                  <Field
                    label="User"
                    value={form.user}
                    onChange={(value) => setForm((current) => ({ ...current, user: value }))}
                  />
                  <Field
                    label="Password"
                    type="password"
                    value={form.password}
                    onChange={(value) => setForm((current) => ({ ...current, password: value }))}
                  />
                </div>
              ) : (
                <Field
                  label="Path"
                  value={form.path}
                  onChange={(value) => setForm((current) => ({ ...current, path: value }))}
                />
              )}

              {showFilters ? (
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                  <Field
                    label="Include schema or qualified patterns"
                    value={form.include_schemas}
                    onChange={(value) =>
                      setForm((current) => ({ ...current, include_schemas: value }))
                    }
                    placeholder="data.dim*, prst.*, view.*"
                  />
                  <Field
                    label="Exclude schema or qualified patterns"
                    value={form.exclude_schemas}
                    onChange={(value) =>
                      setForm((current) => ({ ...current, exclude_schemas: value }))
                    }
                    placeholder="staging.*, tmp_*"
                  />
                  <Field
                    label="Include tables"
                    value={form.include_tables}
                    onChange={(value) =>
                      setForm((current) => ({ ...current, include_tables: value }))
                    }
                    placeholder="fact_*, dim_*"
                  />
                  <Field
                    label="Exclude tables"
                    value={form.exclude_tables}
                    onChange={(value) =>
                      setForm((current) => ({ ...current, exclude_tables: value }))
                    }
                    placeholder="*_tmp"
                  />
                </div>
              ) : null}

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                <Field
                  label="Max tables"
                  type="number"
                  value={String(form.max_tables)}
                  onChange={(value) =>
                    setForm((current) => ({ ...current, max_tables: Number(value) || 50 }))
                  }
                />
                <Field
                  label="Sample rows"
                  type="number"
                  value={String(form.sample_rows)}
                  onChange={(value) =>
                    setForm((current) => ({ ...current, sample_rows: Number(value) || 10000 }))
                  }
                />
              </div>

              <div className="flex flex-wrap gap-4 text-sm">
                <Checkbox
                  label="Auto-sync source"
                  checked={form.auto_sync}
                  onChange={(checked) => setForm((current) => ({ ...current, auto_sync: checked }))}
                />
                <Checkbox
                  label="Re-ingest after save"
                  checked={rerunAfterSave}
                  onChange={setRerunAfterSave}
                />
              </div>

              <div className="flex items-center gap-2">
                <button
                  onClick={testConnection}
                  disabled={testing || !canTest}
                  className="px-3 py-1.5 border border-border rounded-md text-sm font-medium hover:bg-background disabled:opacity-50"
                >
                  {testing ? "Testing..." : "Test connection"}
                </button>
              </div>
              <ConnectionTestResultPanel result={testResult} />
            </>
          ) : (
            <div className="rounded-lg border border-border bg-background px-3 py-3 text-sm text-muted">
              This project has no linked source yet. You can still update the project metadata here.
            </div>
          )}
        </div>

        <div className="px-5 py-4 border-t border-border flex justify-end gap-2">
          <button
            onClick={onClose}
            className="px-3 py-1.5 border border-border rounded-md text-sm text-muted hover:text-foreground"
          >
            Cancel
          </button>
          <button
            onClick={save}
            disabled={saving}
            className="px-4 py-1.5 bg-accent text-white rounded-md text-sm font-medium disabled:opacity-50"
          >
            {saving ? "Saving..." : rerunAfterSave && sourceName ? "Save and re-ingest" : "Save"}
          </button>
        </div>
      </div>
    </div>
  );
}

function Checkbox({
  label,
  checked,
  onChange,
}: {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
}) {
  return (
    <label className="flex items-center gap-2">
      <input
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
      />
      {label}
    </label>
  );
}

function Field({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
  readOnly = false,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  type?: string;
  readOnly?: boolean;
}) {
  return (
    <div>
      <label className="mb-1.5 block text-[12px] font-medium text-foreground">{label}</label>
      <input
        type={type}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        readOnly={readOnly}
        className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm read-only:text-muted"
      />
    </div>
  );
}

function SelectField({
  label,
  value,
  onChange,
  options,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  options: { value: string; label: string }[];
}) {
  return (
    <div>
      <label className="mb-1.5 block text-[12px] font-medium text-foreground">{label}</label>
      <select
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm"
      >
        <option value="">Choose connector</option>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  );
}

function buildFilterConfig(form: ConnectionForm) {
  const config: Record<string, string[]> = {};
  const includeSchemas = splitCsv(form.include_schemas);
  const excludeSchemas = splitCsv(form.exclude_schemas);
  const includeTables = splitCsv(form.include_tables);
  const excludeTables = splitCsv(form.exclude_tables);
  if (includeSchemas.length) config.include_schemas = includeSchemas;
  if (excludeSchemas.length) config.exclude_schemas = excludeSchemas;
  if (includeTables.length) config.include_tables = includeTables;
  if (excludeTables.length) config.exclude_tables = excludeTables;
  return config;
}

function splitCsv(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function readConnectionForm(source: SourceDetail): ConnectionForm {
  const config = source.config ?? {};
  const connection = asRecord(config.connection);
  const parsed = source.uri ? parseConnectionUri(source.type, source.uri) : {};
  const parsedPassword = stringValue(parsed.password);
  return {
    display_name: source.display_name || source.name,
    host: stringValue(connection.host) || stringValue(parsed.host),
    port: stringValue(connection.port) || stringValue(parsed.port),
    database: stringValue(connection.database) || stringValue(parsed.database),
    schema: stringValue(connection.schema) || stringValue(parsed.schema),
    warehouse: stringValue(connection.warehouse) || stringValue(parsed.warehouse),
    role: stringValue(connection.role) || stringValue(parsed.role),
    user: stringValue(parsed.user),
    password: parsedPassword == "***" ? "" : parsedPassword,
    path: source.path || stringValue(connection.path),
    auto_sync: source.auto_sync,
    max_tables: numberValue(config.max_tables, 50),
    sample_rows: numberValue(config.sample_rows, 10000),
    include_schemas: joinList(config.include_schemas),
    exclude_schemas: joinList(config.exclude_schemas),
    include_tables: joinList(config.include_tables),
    exclude_tables: joinList(config.exclude_tables),
  };
}

function joinList(value: unknown): string {
  return Array.isArray(value) ? value.map((item) => String(item)).join(", ") : "";
}

function numberValue(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function buildConnectionTarget(connectorId: string, form: ConnectionForm): string | null {
  const built = buildConnectionValue(connectorId, form);
  if ("uri" in built) return built.uri;
  if ("path" in built) return built.path;
  return null;
}

function resolveConnectionValue(
  connectorId: string,
  form: ConnectionForm,
  source: SourceDetail | null
): { kind: "uri" | "path" | "preserve_uri"; value?: string } | null {
  if (!DATABASE_CONNECTORS.has(connectorId)) {
    return { kind: "path", value: form.path.trim() };
  }

  const password = form.password.trim();
  if (password) {
    const built = buildConnectionValue(connectorId, form);
    return "uri" in built ? { kind: "uri", value: built.uri } : null;
  }

  const baseline = source ? readConnectionForm(source) : DEFAULT_FORM;
  if (_sameConnectionDetails(connectorId, form, baseline)) {
    return { kind: "preserve_uri" };
  }

  return null;
}

function buildConnectionValue(
  connectorId: string,
  form: ConnectionForm
): { uri: string } | { path: string } {
  if (connectorId === "snowflake") {
    const params = new URLSearchParams();
    if (form.warehouse.trim()) params.set("warehouse", form.warehouse.trim());
    if (form.role.trim()) params.set("role", form.role.trim());
    const query = params.toString();
    return {
      uri: `snowflake://${encodeURIComponent(form.user.trim())}:${encodeURIComponent(
        form.password.trim()
      )}@${form.host.trim()}/${form.database.trim()}/${form.schema.trim()}${
        query ? `?${query}` : ""
      }`,
    };
  }
  if (connectorId === "postgres") {
    return {
      uri: `postgresql://${encodeURIComponent(form.user.trim())}:${encodeURIComponent(
        form.password.trim()
      )}@${form.host.trim()}:${form.port.trim() || "5432"}/${form.database.trim()}`,
    };
  }
  if (connectorId === "mysql") {
    return {
      uri: `mysql://${encodeURIComponent(form.user.trim())}:${encodeURIComponent(
        form.password.trim()
      )}@${form.host.trim()}:${form.port.trim() || "3306"}/${form.database.trim()}`,
    };
  }
  if (connectorId === "redshift") {
    return {
      uri: `redshift://${encodeURIComponent(form.user.trim())}:${encodeURIComponent(
        form.password.trim()
      )}@${form.host.trim()}:${form.port.trim() || "5439"}/${form.database.trim()}`,
    };
  }
  return { path: form.path.trim() };
}

function _sameConnectionDetails(
  connectorId: string,
  current: ConnectionForm,
  baseline: ConnectionForm
): boolean {
  const fields: ConnectionStringField[] = [
    "host",
    "port",
    "database",
    "user",
  ];
  const snowflakeFields: ConnectionStringField[] = ["schema", "warehouse", "role"];
  const fileFields: ConnectionStringField[] = ["path"];

  const compare = (field: ConnectionStringField) =>
    current[field].trim() === baseline[field].trim();

  if (!DATABASE_CONNECTORS.has(connectorId)) {
    return fileFields.every(compare);
  }
  if (!fields.every(compare)) return false;
  if (connectorId === "snowflake" && !snowflakeFields.every(compare)) return false;
  return true;
}

function parseConnectionUri(type: string, value: string): Record<string, string> {
  try {
    const url = new URL(value);
    const details: Record<string, string> = {
      host: decodeURIComponent(url.hostname),
      port: decodeURIComponent(url.port),
      user: decodeURIComponent(url.username),
      password: decodeURIComponent(url.password),
    };
    const parts = url.pathname.split("/").filter(Boolean).map((item) => decodeURIComponent(item));
    if (type === "snowflake") {
      details.database = parts[0] || "";
      details.schema = parts[1] || "";
      details.warehouse = decodeURIComponent(url.searchParams.get("warehouse") || "");
      details.role = decodeURIComponent(url.searchParams.get("role") || "");
    } else {
      details.database = parts[0] || "";
    }
    return details;
  } catch {
    return {};
  }
}
