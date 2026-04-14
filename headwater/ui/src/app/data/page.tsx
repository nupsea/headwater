"use client";

import { useEffect, useState } from "react";
import {
  api,
  type CatalogTable,
  type CatalogResponse,
  type DataPreviewResponse,
  type DataQueryResponse,
} from "@/lib/api";

type ActiveTab = "preview" | "query";

function ResultTable({
  columns,
  data,
}: {
  columns: string[];
  data: Record<string, unknown>[];
}) {
  if (columns.length === 0 || data.length === 0) return null;

  return (
    <div className="overflow-auto flex-1 border border-border rounded-lg">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-background sticky top-0 z-10">
            {columns.map((col) => (
              <th
                key={col}
                className="px-3 py-2 text-left font-medium text-muted whitespace-nowrap"
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={i} className="border-b border-border last:border-0 hover:bg-background/50">
              {columns.map((col) => {
                const val = row[col];
                return (
                  <td key={col} className="px-3 py-1.5 font-mono text-xs whitespace-nowrap">
                    {val === null || val === undefined ? (
                      <span className="text-muted italic">null</span>
                    ) : typeof val === "number" ? (
                      Number.isInteger(val) ? val.toLocaleString() : val.toFixed(2)
                    ) : (
                      String(val)
                    )}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function DataPage() {
  const [activeTab, setActiveTab] = useState<ActiveTab>("preview");
  const [catalogOpen, setCatalogOpen] = useState(true);

  // Catalog state
  const [schemas, setSchemas] = useState<string[]>([]);
  const [catalog, setCatalog] = useState<CatalogTable[]>([]);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);
  const [collapsedSchemas, setCollapsedSchemas] = useState<Set<string>>(() => new Set());
  const [catalogLoading, setCatalogLoading] = useState(true);
  const [catalogError, setCatalogError] = useState("");

  // Table Preview state
  const [selectedTable, setSelectedTable] = useState("");
  const [rowLimit, setRowLimit] = useState(100);
  const [previewResult, setPreviewResult] = useState<DataPreviewResponse | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState("");

  // SQL Query state
  const [sql, setSql] = useState("");
  const [queryResult, setQueryResult] = useState<DataQueryResponse | null>(null);
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState("");

  // Track client mount to avoid hydration mismatches
  const [mounted, setMounted] = useState(false);

  // Load catalog from DuckDB on mount
  useEffect(() => {
    setMounted(true);
    setCatalogLoading(true);
    api
      .dataCatalog()
      .then((res: CatalogResponse) => {
        setSchemas(res.schemas);
        setCatalog(res.tables);
        if (res.tables.length > 0) {
          setSelectedTable(res.tables[0].qualified_name);
        }
      })
      .catch(() => setCatalogError("Run the pipeline from the Dashboard first."))
      .finally(() => setCatalogLoading(false));
  }, []);

  const toggleSchema = (schema: string) => {
    setCollapsedSchemas((prev) => {
      const next = new Set(prev);
      if (next.has(schema)) {
        next.delete(schema);
      } else {
        next.add(schema);
      }
      return next;
    });
  };

  const toggleExpand = (qualifiedName: string) => {
    setExpandedTable((prev) => (prev === qualifiedName ? null : qualifiedName));
  };

  const selectTableFromCatalog = (qualifiedName: string, tableName: string) => {
    setSelectedTable(qualifiedName);
    setActiveTab("preview");
    loadPreviewFor(tableName);
  };

  const insertTableRef = (qualifiedName: string) => {
    setSql((prev) =>
      prev ? prev + " " + qualifiedName : `SELECT * FROM ${qualifiedName} LIMIT 100`
    );
    setActiveTab("query");
  };

  const loadPreviewFor = async (tableName: string) => {
    if (!tableName) return;
    setPreviewLoading(true);
    setPreviewError("");
    setPreviewResult(null);
    try {
      const res = await api.dataPreview(tableName, rowLimit);
      setPreviewResult(res);
    } catch (e) {
      setPreviewError(e instanceof Error ? e.message : String(e));
    }
    setPreviewLoading(false);
  };

  const loadPreview = () => {
    const entry = catalog.find((t) => t.qualified_name === selectedTable);
    if (entry) loadPreviewFor(entry.table_name);
  };

  const runQuery = async () => {
    if (!sql.trim()) return;
    setQueryLoading(true);
    setQueryError("");
    setQueryResult(null);
    try {
      const res = await api.dataQuery(sql);
      if (res.error) {
        setQueryError(res.error);
      }
      setQueryResult(res);
    } catch (e) {
      setQueryError(e instanceof Error ? e.message : String(e));
    }
    setQueryLoading(false);
  };

  const LIMITS = [25, 50, 100, 200, 500];

  const tablesBySchema = schemas.map((s) => ({
    schema: s,
    tables: catalog.filter((t) => t.schema === s),
  }));

  return (
    // Break out of the parent max-w-7xl container to use full width
    <div className="-mx-6 -my-6 flex h-[calc(100vh-3.25rem)]">
      {/* Collapsible Catalog sidebar */}
      {catalogOpen ? (
        <div className="w-60 flex-shrink-0 border-r border-border bg-card flex flex-col">
          <div className="px-3 py-2 border-b border-border flex items-center justify-between">
            <div>
              <h2 className="text-xs font-semibold">Catalog</h2>
              <p className="text-xs text-muted">
                {catalog.length} table{catalog.length !== 1 ? "s" : ""} / {schemas.length} schema{schemas.length !== 1 ? "s" : ""}
              </p>
            </div>
            <button
              onClick={() => setCatalogOpen(false)}
              className="text-muted hover:text-foreground text-xs px-1"
              title="Collapse catalog"
            >
              &laquo;
            </button>
          </div>
          <div className="flex-1 overflow-y-auto text-sm">
            {catalogLoading && (
              <p className="text-xs text-muted p-3">Loading...</p>
            )}
            {catalogError && (
              <p className="text-xs text-red-600 p-3">{catalogError}</p>
            )}
            {tablesBySchema.map(({ schema, tables }) => (
              <div key={schema}>
                <button
                  onClick={() => toggleSchema(schema)}
                  className="w-full flex items-center gap-2 px-3 py-1.5 bg-background border-b border-border text-xs font-semibold text-muted hover:text-foreground transition-colors"
                >
                  <span className="w-3 text-center">
                    {collapsedSchemas.has(schema) ? ">" : "v"}
                  </span>
                  <span className="uppercase tracking-wide">{schema}</span>
                  <span className="ml-auto font-normal">{tables.length}</span>
                </button>

                {!collapsedSchemas.has(schema) &&
                  tables.map((table) => (
                    <div
                      key={table.qualified_name}
                      className="border-b border-border last:border-0"
                    >
                      <div
                        className={`flex items-center gap-1.5 px-3 py-1 cursor-pointer hover:bg-background transition-colors ${
                          selectedTable === table.qualified_name ? "bg-background" : ""
                        }`}
                      >
                        <button
                          onClick={() => toggleExpand(table.qualified_name)}
                          className="text-xs text-muted w-3 flex-shrink-0 text-center"
                        >
                          {expandedTable === table.qualified_name ? "v" : ">"}
                        </button>
                        <div className="flex-1 min-w-0">
                          <button
                            onClick={() =>
                              selectTableFromCatalog(
                                table.qualified_name,
                                table.table_name
                              )
                            }
                            className="block text-xs font-medium truncate text-left w-full hover:text-blue-600 transition-colors"
                            title={table.qualified_name}
                          >
                            {table.table_name}
                          </button>
                          <div className="flex items-center gap-2 text-[10px] text-muted">
                            {table.row_count !== null && (
                              <span>{table.row_count.toLocaleString()} rows</span>
                            )}
                            <span>{table.column_count} cols</span>
                          </div>
                        </div>
                        <button
                          onClick={() => insertTableRef(table.qualified_name)}
                          title="Insert into SQL editor"
                          className="text-[10px] text-muted hover:text-foreground flex-shrink-0 px-0.5"
                        >
                          SQL
                        </button>
                      </div>

                      {expandedTable === table.qualified_name && (
                        <div className="pl-7 pr-3 pb-1.5">
                          {table.columns.length === 0 ? (
                            <p className="text-xs text-muted italic">No columns</p>
                          ) : (
                            <ul className="space-y-0">
                              {table.columns.map((col) => (
                                <li
                                  key={col.name}
                                  className="flex items-center justify-between text-[10px] py-0.5"
                                >
                                  <span className="font-mono truncate">
                                    {col.name}
                                  </span>
                                  <span className="text-muted ml-2 flex-shrink-0 font-mono">
                                    {col.dtype}
                                  </span>
                                </li>
                              ))}
                            </ul>
                          )}
                        </div>
                      )}
                    </div>
                  ))}
              </div>
            ))}
          </div>
        </div>
      ) : (
        <button
          onClick={() => setCatalogOpen(true)}
          className="w-8 flex-shrink-0 border-r border-border bg-card flex items-center justify-center text-muted hover:text-foreground transition-colors"
          title="Show catalog"
        >
          &raquo;
        </button>
      )}

      {/* Right: Main content */}
      <div className="flex-1 min-w-0 flex flex-col p-4 overflow-hidden">
        {/* Header row: title + tabs inline */}
        <div className="flex items-center gap-6 mb-3 flex-shrink-0">
          <h1 className="text-lg font-bold">Data Viewer</h1>
          <div className="flex gap-4 border-b border-border">
            <button
              onClick={() => setActiveTab("preview")}
              className={`pb-1.5 text-sm font-medium transition-colors ${
                activeTab === "preview"
                  ? "border-b-2 border-foreground text-foreground"
                  : "text-muted hover:text-foreground"
              }`}
            >
              Table Preview
            </button>
            <button
              onClick={() => setActiveTab("query")}
              className={`pb-1.5 text-sm font-medium transition-colors ${
                activeTab === "query"
                  ? "border-b-2 border-foreground text-foreground"
                  : "text-muted hover:text-foreground"
              }`}
            >
              SQL Query
            </button>
          </div>
        </div>

        {/* Table Preview tab */}
        {activeTab === "preview" && (
          <div className="flex-1 flex flex-col min-h-0">
            <div className="flex items-end gap-3 mb-3 flex-wrap flex-shrink-0">
              <div>
                <label className="block text-xs text-muted mb-1">Table</label>
                <select
                  value={selectedTable}
                  onChange={(e) => setSelectedTable(e.target.value)}
                  className="px-3 py-1.5 border border-border rounded-lg bg-background text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  {catalog.length === 0 && (
                    <option value="">No tables available</option>
                  )}
                  {tablesBySchema.map(({ schema, tables }) =>
                    tables.length > 0 ? (
                      <optgroup key={schema} label={schema}>
                        {tables.map((t) => (
                          <option key={t.qualified_name} value={t.qualified_name}>
                            {t.table_name}
                          </option>
                        ))}
                      </optgroup>
                    ) : null
                  )}
                </select>
              </div>
              <div>
                <label className="block text-xs text-muted mb-1">Rows</label>
                <select
                  value={rowLimit}
                  onChange={(e) => setRowLimit(Number(e.target.value))}
                  className="px-3 py-1.5 border border-border rounded-lg bg-background text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  {LIMITS.map((n) => (
                    <option key={n} value={n}>
                      {n}
                    </option>
                  ))}
                </select>
              </div>
              <button
                onClick={loadPreview}
                disabled={!mounted || !selectedTable || previewLoading}
                className="px-4 py-1.5 bg-foreground text-background rounded-lg text-sm font-medium disabled:opacity-50 hover:opacity-90 transition-opacity"
              >
                {previewLoading ? "Loading..." : "Load"}
              </button>
              {previewResult && (
                <span className="text-xs text-muted ml-2">
                  {previewResult.row_count} of {previewResult.total_rows.toLocaleString()} rows
                  <span className="font-mono ml-2">{previewResult.sql}</span>
                </span>
              )}
            </div>

            {previewError && (
              <div className="mb-3 p-2 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700 flex-shrink-0">
                {previewError}
              </div>
            )}

            {previewResult && (
              <ResultTable
                columns={previewResult.columns}
                data={previewResult.data}
              />
            )}
          </div>
        )}

        {/* SQL Query tab */}
        {activeTab === "query" && (
          <div className="flex-1 flex flex-col min-h-0">
            <div className="mb-3 flex-shrink-0">
              <textarea
                value={sql}
                onChange={(e) => setSql(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && (e.metaKey || e.ctrlKey) && sql.trim()) {
                    runQuery();
                  }
                }}
                rows={3}
                placeholder="SELECT * FROM staging.stg_sites LIMIT 50"
                className="w-full px-3 py-2 border border-border rounded-lg bg-background text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 resize-y"
              />
              <div className="flex items-center justify-between mt-1.5">
                <span className="text-xs text-muted">
                  Cmd+Enter to run
                  {queryResult && !queryResult.error && (
                    <span className="ml-4">{queryResult.row_count} rows returned</span>
                  )}
                </span>
                <button
                  onClick={runQuery}
                  disabled={!mounted || !sql.trim() || queryLoading}
                  className="px-4 py-1.5 bg-foreground text-background rounded-lg text-sm font-medium disabled:opacity-50 hover:opacity-90 transition-opacity"
                >
                  {queryLoading ? "Running..." : "Run Query"}
                </button>
              </div>
            </div>

            {queryError && (
              <div className="mb-3 p-2 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700 flex-shrink-0">
                {queryError}
              </div>
            )}

            {queryResult && !queryResult.error && (
              <ResultTable
                columns={queryResult.columns}
                data={queryResult.data}
              />
            )}
          </div>
        )}
      </div>
    </div>
  );
}
