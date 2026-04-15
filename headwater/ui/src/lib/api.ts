const BASE = "/api";

async function fetchJSON<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${url}`, init);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`API ${res.status}: ${body}`);
  }
  return res.json() as Promise<T>;
}

// ---------- Types ----------

export interface StatusResponse {
  status: string;
  discovered: boolean;
  tables: number;
  staging_models: number;
  mart_models: number;
  contracts: number;
  executed: number;
  dictionary_reviewed: number;
  dictionary_complete: boolean;
}

export interface ColumnInfo {
  name: string;
  dtype: string;
  nullable: boolean;
  is_primary_key: boolean;
  description: string | null;
  semantic_type: string | null;
}

export interface TableDetail {
  name: string;
  row_count: number;
  columns: ColumnInfo[];
  description: string | null;
  domain: string | null;
  tags: string[];
}

export interface ColumnProfile {
  table_name: string;
  column_name: string;
  dtype: string;
  null_count: number;
  null_rate: number;
  distinct_count: number;
  uniqueness_ratio: number;
  min_value: number | null;
  max_value: number | null;
  mean: number | null;
  median: number | null;
  min_length: number | null;
  max_length: number | null;
  top_values: [string, number][] | null;
  detected_pattern: string | null;
}

export interface ModelSummary {
  name: string;
  model_type: string;
  status: string;
  description: string;
  source_tables: string[];
  questions: string[];
  assumptions: string[];
}

export interface ModelDetail extends ModelSummary {
  sql: string;
  depends_on: string[];
}

export interface ContractSummary {
  id: string;
  model_name: string;
  column_name: string | null;
  rule_type: string;
  severity: string;
  confidence: number;
  status: string;
  description: string;
}

export interface QualityCheckResult {
  rule_id: string;
  model_name: string;
  passed: boolean;
  message: string;
}

// ---------- Data Profile ----------

export interface PKCoverage {
  tables_with_pk: number;
  total_tables: number;
  description: string;
}

export interface FKIntegrity {
  avg_integrity_pct: number | null;
  total_relationships: number;
  description: string;
}

export interface QualityMetric {
  passed: number;
  total: number;
  pass_rate_pct: number | null;
  description: string;
}

export interface DataProfile {
  completeness_pct: number;
  pk_coverage: PKCoverage;
  fk_integrity: FKIntegrity;
  quality: QualityMetric;
  high_null_columns: number;
  constant_columns: number;
  total_columns_profiled: number;
}

// ---------- Workflow ----------

export interface WorkflowPhase {
  key: string;
  label: string;
  status: "complete" | "active" | "pending";
  detail: string;
}

export interface Workflow {
  phases: WorkflowPhase[];
  current_phase: string;
}

// ---------- Advisory Actions ----------

export interface AdvisoryAction {
  phase: string;
  priority: "blocking" | "recommended" | "informational" | "success";
  title: string;
  detail: string;
  link: string;
}

// ---------- Insights types ----------

export interface TableHealth {
  name: string;
  row_count: number;
  column_count: number;
  domain: string | null;
  description: string | null;
  completeness: number;
  avg_null_rate: number;
  pk_columns: string[];
  fk_columns: { column: string; references: string }[];
  has_relationships: boolean;
}

export interface ColumnIssue {
  table: string;
  column: string;
  dtype: string;
  issues: {
    type: string;
    severity: string;
    message: string;
    detail: string;
  }[];
}

export interface NullEntry {
  table: string;
  column: string;
  null_rate: number;
  null_count: number;
  total_rows: number;
}

export interface UniquenessEntry {
  table: string;
  column: string;
  uniqueness_ratio: number;
  distinct_count: number;
  is_pk_candidate: boolean;
}

export interface PatternEntry {
  table: string;
  column: string;
  pattern: string;
}

export interface RelationshipEntry {
  from_table: string;
  from_column: string;
  to_table: string;
  to_column: string;
  type: string;
  confidence: number;
  integrity: number;
}

export interface ModelSuggestion {
  type: string;
  title: string;
  detail: string;
}

export interface InsightsResponse {
  data_profile: DataProfile;
  workflow: Workflow;
  advisory_actions: AdvisoryAction[];
  overview: {
    total_tables: number;
    total_columns: number;
    total_rows: number;
    total_cells: number;
    total_relationships: number;
    completeness_pct: number;
    total_profiles: number;
    total_contracts: number;
  };
  domains: Record<string, { tables: string[]; total_rows: number }>;
  table_health: TableHealth[];
  column_issues: ColumnIssue[];
  null_analysis: NullEntry[];
  uniqueness_analysis: UniquenessEntry[];
  patterns_found: PatternEntry[];
  relationship_map: RelationshipEntry[];
  quality_summary: {
    total: number;
    passed: number;
    failed: number;
    pass_rate: number;
  } | null;
  model_suggestions: ModelSuggestion[];
  catalog_health: CatalogHealth | null;
}

export interface PipelineRunResponse {
  tables_loaded: number;
  tables_discovered: number;
  profiles: number;
  relationships: number;
  staging_models: number;
  mart_models: number;
  contracts: number;
  models_executed: number;
  models_succeeded: number;
  quality_total: number;
  quality_passed: number;
  quality_failed: number;
}

// ---------- Explorer types ----------

export interface SuggestedQuestion {
  question: string;
  source: "mart" | "relationship" | "quality" | "semantic" | "statistical";
  category: string;
  relevant_tables: string[];
  sql_hint: string | null;
}

export interface StatisticalInsight {
  metric: string;
  table_name: string;
  insight_type:
    | "temporal_anomaly"
    | "period_comparison"
    | "correlation"
    | "distribution_shift";
  description: string;
  magnitude: number;
  z_score: number | null;
  p_value: number | null;
  confidence_level: string | null;
  time_period: string | null;
  comparison_baseline: string | null;
  severity: "info" | "warning" | "critical";
}

export interface VisualizationSpec {
  chart_type: "kpi" | "bar" | "line" | "scatter" | "table" | "heatmap";
  title: string;
  x_axis: string | null;
  y_axis: string | null;
  group_by: string | null;
  description: string;
}

export interface RepairAttempt {
  sql: string;
  error: string;
}

export interface DimensionOption {
  dimension_name: string;
  display_name: string;
  description: string;
  sample_values: string[];
  confidence: number;
}

export interface ExplorationResult {
  question: string;
  sql: string;
  data: Record<string, unknown>[];
  row_count: number;
  visualization: VisualizationSpec | null;
  error: string | null;
  warnings: string[];
  suggestions: string[];
  explanation: string;
  options: DimensionOption[];
  repaired: boolean;
  repair_history: RepairAttempt[];
}

export interface ExploreSuggestionsResponse {
  suggestions: SuggestedQuestion[];
  insights: StatisticalInsight[];
  review_pct: number;
}

// ---------- Drift types (US-402, US-403) ----------

export interface ColumnChange {
  column_name: string;
  change_type: "added" | "removed" | "type_changed" | "nullability_changed";
  before: string | null;
  after: string | null;
}

export interface TableChange {
  table_name: string;
  change_type: "added" | "removed" | "columns_changed";
  column_changes: ColumnChange[];
}

export interface DriftReport {
  id: number;
  source_name: string;
  run_id_from: number | null;
  run_id_to: number;
  diff_json: string;
  diff: {
    source_name: string;
    run_id_from: number | null;
    run_id_to: number;
    no_changes: boolean;
    tables_added: string[];
    tables_removed: string[];
    tables_changed: TableChange[];
    detected_at: string;
  };
  detected_at: string;
  acknowledged: number;
}

export interface DriftReportsResponse {
  reports: DriftReport[];
  message?: string;
}

// ---------- Confidence types (US-302, US-303) ----------

export interface ConfidenceMetrics {
  description_acceptance_rate: number | null;
  description_sample_size: number;
  description_reason: string | null;
  model_edit_distance_avg: number | null;
  model_edit_distance_sample_size: number;
  contract_precision: number | null;
  contract_precision_sample_size: number;
}

// ---------- Data Dictionary types ----------

export interface DictColumn {
  name: string;
  dtype: string;
  nullable: boolean;
  is_primary_key: boolean;
  is_foreign_key: boolean;
  fk_references: string | null;
  semantic_type: string | null;
  role: string | null;
  description: string | null;
  confidence: number;
  locked: boolean;
  needs_review: boolean;
}

export interface DictTable {
  name: string;
  source_name: string;
  row_count: number;
  description: string | null;
  domain: string | null;
  review_status: "pending" | "in_review" | "reviewed" | "skipped";
  columns: DictColumn[];
  relationships: RelationshipEntry[];
  questions: string[];
}

export interface DictReviewSummary {
  total: number;
  reviewed: number;
  pending: number;
  in_review: number;
  skipped: number;
  pct_complete: number;
}

export interface ColumnReviewPayload {
  name: string;
  semantic_type?: string | null;
  role?: string | null;
  description?: string | null;
  is_primary_key?: boolean | null;
}

export interface TableReviewPayload {
  columns: ColumnReviewPayload[];
  table_description?: string | null;
  table_domain?: string | null;
  confirm: boolean;
}

// ---------- Data Viewer types ----------

export interface DataPreviewResponse {
  columns: string[];
  data: Record<string, unknown>[];
  row_count: number;
  total_rows: number;
  sql: string;
  table_name: string;
}

export interface DataQueryResponse {
  columns: string[];
  data: Record<string, unknown>[];
  row_count: number;
  sql: string;
  error: string | null;
}

export interface CatalogColumn {
  name: string;
  dtype: string;
}

export interface CatalogTable {
  schema: string;
  table_name: string;
  qualified_name: string;
  row_count: number | null;
  column_count: number;
  columns: CatalogColumn[];
}

export interface CatalogResponse {
  schemas: string[];
  tables: CatalogTable[];
  total: number;
}

// ---------- v2: Project types ----------

export interface ProjectProgress {
  tables_discovered: number;
  tables_profiled: number;
  tables_reviewed: number;
  tables_modeled: number;
  tables_mart_ready: number;
  columns_total: number;
  columns_described: number;
  columns_confirmed: number;
  relationships_detected: number;
  relationships_confirmed: number;
  metrics_defined: number;
  metrics_confirmed: number;
  dimensions_defined: number;
  dimensions_confirmed: number;
  quality_contracts: number;
  contracts_enforcing: number;
  catalog_coverage: number;
}

export interface Project {
  id: string;
  slug: string;
  display_name: string;
  description: string;
  maturity: "raw" | "profiled" | "documented" | "modeled" | "production";
  maturity_score: number;
  catalog_confidence: number;
  created_at: string;
  updated_at: string;
  progress?: ProjectProgress;
}

// ---------- v2: Catalog types ----------

export interface CatalogMetric {
  name: string;
  project_id: string;
  display_name: string;
  description: string;
  expression: string;
  column_name: string | null;
  table_name: string;
  agg_type: string;
  synonyms: string[];
  confidence: number;
  status: "proposed" | "confirmed" | "rejected";
  source: "heuristic" | "llm" | "human";
}

export interface CatalogDimension {
  name: string;
  project_id: string;
  display_name: string;
  description: string;
  column_name: string;
  table_name: string;
  dtype: string;
  synonyms: string[];
  sample_values: string[];
  cardinality: number;
  confidence: number;
  status: "proposed" | "confirmed" | "rejected";
  source: "heuristic" | "llm" | "human";
  join_path: string | null;
  join_nullable: boolean;
}

export interface CatalogEntity {
  name: string;
  project_id: string;
  display_name: string;
  description: string;
  table_name: string;
  row_semantics: string;
  metrics: string[];
  dimensions: string[];
  temporal_grain: string | null;
}

export interface CatalogReviewResponse {
  metrics: CatalogMetric[];
  dimensions: CatalogDimension[];
  entities: CatalogEntity[];
  summary: {
    metrics_total: number;
    metrics_confirmed: number;
    metrics_rejected: number;
    dimensions_total: number;
    dimensions_confirmed: number;
    dimensions_rejected: number;
  };
}

// ---------- v2: Graph types ----------

export interface GraphNode {
  id: string;
  row_count: number;
  domain: string;
  description: string;
}

export interface GraphEdge {
  source: string;
  target: string;
  from_column: string;
  to_column: string;
  rel_type: string;
  confidence: number;
  ref_integrity: number;
  nullable: boolean;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface GraphPatterns {
  conformed_dimensions: { name: string; connection_count: number; connected_tables: string[] }[];
  star_schemas: { hub: string; spokes: string[]; spoke_count: number }[];
  chains: { path: string[]; hop_count: number }[];
  nullable_warnings: { from_table: string; from_column: string; to_table: string; to_column: string; ref_integrity: number }[];
}

// ---------- v2: Settings types ----------

export interface LLMSettings {
  provider: string;
  model: string;
  ollama_base_url: string;
  openai_compat_base_url: string | null;
  has_api_key: boolean;
  has_openai_compat_key: boolean;
}

export interface LLMSettingsUpdate {
  provider?: "none" | "anthropic" | "ollama" | "openai_compat";
  model?: string;
  api_key?: string;
  ollama_base_url?: string;
  openai_compat_base_url?: string;
  openai_compat_api_key?: string;
}

// ---------- v2: Catalog health (from insights) ----------

export interface CatalogHealth {
  metrics_total: number;
  metrics_confirmed: number;
  dimensions_total: number;
  dimensions_confirmed: number;
  entities_total: number;
  catalog_confidence: number;
  catalog_coverage: number;
  maturity: string;
  maturity_score: number;
}

// ---------- v2: Decomposition types ----------

export interface MetricMatch {
  metric_name: string;
  display_name: string;
  expression: string;
  table: string;
  confidence: number;
  strategy: "keyword" | "embedding" | "llm";
}

export interface DimensionMatch {
  dimension_name: string;
  display_name: string;
  column: string;
  table: string;
  join_path: string | null;
  confidence: number;
  strategy: "keyword" | "embedding" | "llm";
  is_filter: boolean;
  filter_value: string | null;
}

export interface DimensionOption {
  dimension_name: string;
  display_name: string;
  description: string;
  sample_values: string[];
  confidence: number;
}

export interface DecompositionResult {
  status: "resolved" | "options" | "outside_scope";
  entity: string | null;
  metrics: MetricMatch[];
  dimensions: DimensionMatch[];
  sql: string | null;
  explanation: string;
  warnings: string[];
  suggestions: string[];
  options: DimensionOption[];
  outside_catalog: string[];
  confidence: number;
  resolution_mode: "catalog" | "exploratory" | null;
}

// ---------- API calls ----------

export const api = {
  status: () => fetchJSON<StatusResponse>("/status"),

  pipelineRun: (sourcePath: string, sourceType = "auto") =>
    fetchJSON<PipelineRunResponse>(
      `/pipeline/run?source_path=${encodeURIComponent(sourcePath)}&source_type=${sourceType}`,
      { method: "POST" }
    ),

  insights: () => fetchJSON<InsightsResponse>("/insights"),

  table: (name: string) => fetchJSON<TableDetail>(`/tables/${name}`),

  tableProfile: (name: string) =>
    fetchJSON<ColumnProfile[]>(`/tables/${name}/profile`),

  models: () => fetchJSON<ModelSummary[]>("/models"),

  model: (name: string) => fetchJSON<ModelDetail>(`/models/${name}`),

  approveModel: (name: string) =>
    fetchJSON<{ name: string; status: string }>(`/models/${name}/approve`, {
      method: "POST",
    }),

  rejectModel: (name: string) =>
    fetchJSON<{ name: string; status: string }>(`/models/${name}/reject`, {
      method: "POST",
    }),

  contracts: () => fetchJSON<ContractSummary[]>("/contracts"),

  // Data Dictionary
  dictionary: () =>
    fetchJSON<{ tables: DictTable[] }>("/dictionary"),

  dictionaryTable: (name: string) =>
    fetchJSON<DictTable>(`/dictionary/${name}`),

  dictionarySummary: () =>
    fetchJSON<DictReviewSummary>("/dictionary/summary"),

  reviewTable: (name: string, body: TableReviewPayload) =>
    fetchJSON<{ table: string; review_status: string; columns_updated: number }>(
      `/dictionary/${name}/review`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }
    ),

  skipTable: (name: string) =>
    fetchJSON<{ table: string; review_status: string }>(
      `/dictionary/${name}/skip`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" }
    ),

  confirmAllTables: () =>
    fetchJSON<{ confirmed: number; total: number }>("/dictionary/confirm-all", {
      method: "POST",
    }),

  // Data Viewer
  dataCatalog: () => fetchJSON<CatalogResponse>("/data/catalog"),

  dataPreview: (tableName: string, limit = 100) =>
    fetchJSON<DataPreviewResponse>(
      `/data/${tableName}/preview?limit=${limit}`
    ),

  dataQuery: (sql: string) =>
    fetchJSON<DataQueryResponse>("/data/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql }),
    }),

  exploreSuggestions: () =>
    fetchJSON<ExploreSuggestionsResponse>("/explore/suggestions"),

  exploreAsk: (question: string) =>
    fetchJSON<ExplorationResult>("/explore/ask", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
    }),

  // Drift (US-402, US-403)
  driftReports: (source?: string) =>
    fetchJSON<DriftReportsResponse>(
      `/drift${source ? `?source=${encodeURIComponent(source)}` : ""}`
    ),

  driftLatest: (source?: string) =>
    fetchJSON<DriftReport | { report: null; message: string }>(
      `/drift?latest=true${source ? `&source=${encodeURIComponent(source)}` : ""}`
    ),

  acknowledgeDrift: (reportId: number) =>
    fetchJSON<{ report_id: number; acknowledged: boolean }>(
      `/drift/${reportId}/acknowledge`,
      { method: "PATCH" }
    ),

  // Confidence (US-302, US-303)
  confidence: (source?: string) =>
    fetchJSON<ConfidenceMetrics>(
      `/confidence${source ? `?source=${encodeURIComponent(source)}` : ""}`
    ),

  // v2: Projects
  projects: () => fetchJSON<{ projects: Project[] }>("/projects"),

  project: (id: string) => fetchJSON<Project>(`/projects/${id}`),

  projectProgress: (id: string) =>
    fetchJSON<{ project_id: string; progress: ProjectProgress; maturity: string; maturity_score: number }>(
      `/projects/${id}/progress`
    ),

  projectCatalog: (id: string) =>
    fetchJSON<{ project_id: string; metrics: CatalogMetric[]; dimensions: CatalogDimension[]; entities: CatalogEntity[] }>(
      `/projects/${id}/catalog`
    ),

  deleteProject: (id: string) =>
    fetchJSON<{ deleted: string }>(`/projects/${id}`, { method: "DELETE" }),

  // v2: Catalog review
  catalogReview: () => fetchJSON<CatalogReviewResponse>("/dictionary/catalog"),

  reviewMetric: (name: string, action: "confirmed" | "rejected") =>
    fetchJSON<{ metric: string; status: string; confidence: number }>(
      `/dictionary/catalog/metrics/${name}`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action }),
      }
    ),

  reviewDimension: (name: string, action: "confirmed" | "rejected", synonyms?: string[]) =>
    fetchJSON<{ dimension: string; status: string; confidence: number }>(
      `/dictionary/catalog/dimensions/${name}`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action, synonyms }),
      }
    ),

  // v2: Graph
  graphData: () => fetchJSON<GraphData>("/graph/data"),

  graphPatterns: () => fetchJSON<GraphPatterns>("/graph/patterns"),

  graphJoinPath: (from: string, to: string) =>
    fetchJSON<{ from_table: string; to_table: string; path: unknown[] | null; hop_count?: number }>(
      `/graph/join-path?from_table=${encodeURIComponent(from)}&to_table=${encodeURIComponent(to)}`
    ),

  // v2: Settings
  llmSettings: () => fetchJSON<LLMSettings>("/settings/llm"),

  updateLLMSettings: (body: LLMSettingsUpdate) =>
    fetchJSON<LLMSettings>("/settings/llm", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
};
