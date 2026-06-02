// Headwater 2 API client

const BASE = "/api/h2";

async function fetchJSON<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${url}`, init);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`H2 API ${res.status}: ${body}`);
  }
  return res.json() as Promise<T>;
}

async function post<T>(url: string, body?: unknown): Promise<T> {
  return fetchJSON<T>(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body != null ? JSON.stringify(body) : undefined,
  });
}

// ── Types ──────────────────────────────────────────────────────────────────

export interface H2Source {
  name: string;
  type: string;
  path: string | null;
  uri: string | null;
  latest_snapshot_id: string | null;
  created_at: string;
  updated_at: string;
}

export interface H2CatalogColumn {
  column_name: string;
  dtype: string;
  semantic_type: string;
  description: string | null;
  locked: boolean;
  ordinal: number;
  profile_summary: Record<string, unknown>;
}

export interface H2CatalogTable {
  table_name: string;
  row_count: number;
  description: string | null;
  columns: H2CatalogColumn[];
}

export interface H2Relationship {
  from_table: string;
  from_column: string;
  to_table: string;
  to_column: string;
  rel_type: string;
  confidence: number;
  referential_integrity: number;
}

export interface H2Project {
  id: string;
  slug: string;
  display_name: string;
  description: string;
  goal: {
    statement: string;
    decision?: string;
    target_metric?: string;
    entities?: string[];
    time_horizon?: string;
  };
  created_at: string;
  updated_at: string;
  questions?: H2Question[];
  sources?: Array<{ source_name: string; selected_tables: string[] }>;
}

export interface H2Question {
  id: string;
  project_id: string;
  title: string;
  answerability: "answerable" | "answerable_with_caveat" | "cannot_answer";
  confidence: number;
  status: string;
  question: {
    reason?: string;
    needed_columns?: string[];
    col_roles?: Record<string, string>;
  };
}

export interface H2ResolveCard {
  card_id: string;
  issue_kind: string;
  priority: "high" | "medium" | "low";
  title: string;
  body: string;
  status?: "open" | "deferred" | "resolved";
  defined?: boolean;
  definition?: string;
  /** "input" = an actionable ask; "limitation" = an informational data gap. */
  category?: "input" | "limitation";
  /** Detailed reasoning (e.g. judge findings), shown behind a "Why" disclosure. */
  why?: string[];
  /** Concrete code values (e.g. ["A","H","S","D"]) rendered as chips. */
  values?: string[];
  /** Proposed parse-to-minutes derivation for an unusable (text) measure. */
  derivation?: {
    kind: string;
    unit: string;
    detected: { id: string; label: string };
    options: { id: string; label: string }[];
    samples: string[];
  } | null;
  affected_questions: string[];
  affected_titles?: string[];
  contract_impacts: string[];
}

export interface H2Contract {
  contract_type: string;
  passed: boolean;
  note: string;
}

export interface H2QuestionReadiness {
  question_id: string;
  state: "certified" | "draft" | "cannot_answer" | "demoted";
  readiness_pct: number;
  summary: string;
  title: string;
  needed_columns: string[];
  contracts: H2Contract[];
}

export interface H2ReadinessReport {
  project_id: string;
  source_name: string;
  source_snapshot_id: string | null;
  certified_count: number;
  draft_count: number;
  cannot_answer_count: number;
  questions: H2QuestionReadiness[];
}

export type H2AnswerRow = Record<string, string | number | boolean | null>;

export interface H2AnswerDraft {
  question_id: string;
  question_title: string;
  state: "certified" | "doubtful" | "pending" | "cannot_answer";
  confidence: number;
  sql_text: string | null;
  chart_spec: Record<string, unknown>;
  columns: string[];
  rows: H2AnswerRow[];
  row_count: number;
  truncated: boolean;
  result_stats: Record<string, unknown>;
  readiness_pct: number;
  statistical_pass: boolean;
  judge_verdict:
    | "certified"
    | "doubtful"
    | "reject"
    | "unavailable"
    | "pending"
    | "stale";
  judge_confidence: number;
  judge_reasons: string[];
  caveats: string[];
  execution_error: string | null;
  /** column -> { raw code: human meaning } from resolved enum mappings. */
  value_labels?: Record<string, Record<string, string>>;
}

export interface H2AnswersResult {
  certified_count: number;
  doubtful_count: number;
  pending_count: number;
  cannot_answer_count: number;
  answers: H2AnswerDraft[];
}

export interface H2EdaFinding {
  col_ref: string;
  family: string;
  title: string;
  confidence: number;
  effect_size: number;
  flags: string[];
}

export interface H2Resource {
  path: string;
  format: string;
  ingested_at: string;
  sensitivity?: string | null;
  claims_created?: number;
  claims_updated?: number;
  conflicts_detected?: number;
}

export interface H2ResourceIngest {
  resource_path: string;
  resource_format: string;
  sensitivity: string;
  sensitivity_notes: string[];
  claims_created: number;
  claims_updated: number;
  claims_skipped_locked: number;
  conflicts_detected: number;
  notes: string[];
}

export interface H2RelevantColumn {
  table_name: string;
  column_name: string;
  semantic_role: string | null;
  score: number;
  reason: string;
}

export interface H2ProposedQuestion {
  question_id: string;
  title: string;
  answerability: string;
  reason: string;
  needed_columns: string[];
  confidence: number;
}

// ── Sources ────────────────────────────────────────────────────────────────

export const h2 = {
  sources: {
    list: () => fetchJSON<H2Source[]>("/sources"),
    get: (name: string) =>
      fetchJSON<H2Source & { tables: unknown[]; latest_snapshot: unknown }>(`/sources/${name}`),
    discover: (path: string, sourceType?: string, name?: string) =>
      post<{ snapshot_id: string; table_count: number }>("/sources", {
        path, source_type: sourceType, name,
      }),
    catalog: (name: string, table?: string, projectId?: string) => {
      const params = new URLSearchParams();
      if (table) params.set("table", table);
      if (projectId) params.set("project_id", projectId);
      const qs = params.toString();
      return fetchJSON<H2CatalogTable[]>(`/sources/${name}/catalog${qs ? `?${qs}` : ""}`);
    },
    relationships: (name: string) =>
      fetchJSON<H2Relationship[]>(`/sources/${name}/relationships`),
    updateColumn: (
      sourceName: string,
      tableName: string,
      columnName: string,
      update: { description?: string; semantic_type?: string; dtype?: string; locked?: boolean }
    ) =>
      fetchJSON<{ updated: string }>(
        `/sources/${sourceName}/catalog/${tableName}/${columnName}`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(update),
        }
      ),
    suggestGoal: (name: string) =>
      post<{ goal: string; rationale: string; available: boolean }>(
        `/sources/${name}/suggest-goal`
      ),
    generateDescriptions: (name: string, overwrite = false) =>
      post<{ updated: number; available: boolean; note?: string }>(
        `/sources/${name}/generate-descriptions?overwrite=${overwrite}`
      ),
  },

  projects: {
    list: () => fetchJSON<H2Project[]>("/projects"),
    get: (id: string) => fetchJSON<H2Project>(`/projects/${id}`),
    frame: (req: {
      project_id: string;
      source_name: string;
      display_name: string;
      goal: string;
      decision?: string;
      target_metric?: string;
      entities?: string[];
      time_horizon?: string;
      selected_tables?: string[];
    }) =>
      post<{
        project_id: string;
        relevant_columns: H2RelevantColumn[];
        proposed_questions: H2ProposedQuestion[];
        notes: string[];
      }>("/projects", req),
    setGoal: (id: string, goal: string) =>
      post<{ project_id: string; goal: string }>(`/projects/${id}/goal`, { goal }),
    rerunRelevance: (id: string) =>
      post<{
        relevant_columns: H2RelevantColumn[];
        proposed_questions: H2ProposedQuestion[];
      }>(`/projects/${id}/relevance`),
    setQuestionDisposition: (id: string, questionId: string, dropped: boolean) =>
      post<{ question_id: string; status: string; dropped: boolean }>(
        `/projects/${id}/questions/${encodeURIComponent(questionId)}/disposition`,
        { dropped }
      ),

    resolve: {
      build: (id: string) => post<H2ResolveCard[]>(`/projects/${id}/resolve`),
      define: (id: string, cardId: string, markdown: string) =>
        post<{
          bound: boolean;
          claim_type?: string;
          table?: string;
          column?: string;
          reason?: string;
        }>(`/projects/${id}/resolve/${encodeURIComponent(cardId)}/define`, {
          markdown,
        }),
      list: (id: string) => fetchJSON<H2ResolveCard[]>(`/projects/${id}/resolve`),
      setDisposition: (id: string, cardId: string, status: "open" | "deferred" | "resolved") =>
        post<{ card_id: string; status: string }>(
          `/projects/${id}/resolve/${encodeURIComponent(cardId)}/disposition`,
          { status }
        ),
      suggest: (id: string, cardId: string) =>
        post<{ available: boolean; markdown: string; note: string }>(
          `/projects/${id}/resolve/${encodeURIComponent(cardId)}/suggest`
        ),
      derive: (id: string, cardId: string, formatId: string) =>
        post<{ applied: boolean; format?: string; unit?: string; reason?: string }>(
          `/projects/${id}/resolve/${encodeURIComponent(cardId)}/derive`,
          { format_id: formatId }
        ),
    },

    readiness: {
      evaluate: (id: string) => post<H2ReadinessReport>(`/projects/${id}/readiness`),
    },

    eda: {
      run: (id: string) =>
        post<{ findings_count: number; insight_confidence_score: number; top_findings: H2EdaFinding[] }>(
          `/projects/${id}/eda`
        ),
    },

    answer: {
      draft: (id: string) => post<H2AnswersResult>(`/projects/${id}/answer`),
      certify: (id: string) => post<H2AnswersResult>(`/projects/${id}/answer/certify`),
    },

    state: (id: string) =>
      fetchJSON<{
        project_id: string;
        stale: boolean;
        never_computed: boolean;
        impacted_count: number;
        last_recomputed_at: string | null;
      }>(`/projects/${id}/state`),
    recompute: (id: string) =>
      post<{
        certified_count: number;
        doubtful_count: number;
        pending_count: number;
        cannot_answer_count: number;
        recomputed_at: string;
      }>(`/projects/${id}/recompute`),

    certify: {
      check: (id: string) =>
        post<{ demotions: Array<{ question_id: string; question_title: string; drift_summary: string }>; newly_certified: string[]; has_drift: boolean }>(
          `/projects/${id}/certify`
        ),
    },

    report: {
      get: (id: string) => fetch(`${BASE}/projects/${id}/report`).then(r => r.text()),
    },

    resources: {
      list: (id: string) => fetchJSON<H2Resource[]>(`/projects/${id}/resources`),
      ingest: async (id: string, file: File, lock = false): Promise<H2ResourceIngest> => {
        const form = new FormData();
        form.append("file", file);
        const res = await fetch(`${BASE}/projects/${id}/resources?lock=${lock}`, {
          method: "POST",
          body: form,
        });
        if (!res.ok) throw new Error(`H2 API ${res.status}: ${await res.text()}`);
        return res.json() as Promise<H2ResourceIngest>;
      },
    },
  },

  query: (sourceName: string, sql: string) =>
    post<{
      columns: string[];
      rows: H2AnswerRow[];
      row_count: number;
      truncated: boolean;
      error: string | null;
    }>("/query", { source_name: sourceName, sql }),
};

// ── Helpers ────────────────────────────────────────────────────────────────

/** Signal that a project input changed so the recompute banner re-checks state. */
export const HW2_INPUT_CHANGED = "hw2:inputchanged";
export function notifyInputChanged(): void {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new Event(HW2_INPUT_CHANGED));
  }
}

/** Signal that a recompute finished so every open view re-fetches its derived
 *  state — the seamless alternative to a full page reload. */
export const HW2_RECOMPUTED = "hw2:recomputed";
export function notifyRecomputed(): void {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new Event(HW2_RECOMPUTED));
  }
}

/** Subscribe a callback to one of the HW2 events; returns an unsubscribe fn.
 *  Convenience for page effects that reload on recompute. */
export function onHw2Event(event: string, handler: () => void): () => void {
  if (typeof window === "undefined") return () => {};
  window.addEventListener(event, handler);
  return () => window.removeEventListener(event, handler);
}

export function trustBadge(readiness?: H2ReadinessReport | null): {
  label: string;
  pct: number;
  color: string;
} {
  if (!readiness) return { label: "Not started", pct: 0, color: "text-gray-400" };
  const total = readiness.questions.length;
  if (total === 0) return { label: "No questions", pct: 0, color: "text-gray-400" };
  const certified = readiness.certified_count;
  const pct = Math.round((certified / total) * 100);
  if (pct === 100) return { label: "Certified", pct, color: "text-green-600" };
  if (pct >= 60) return { label: "Forming", pct, color: "text-yellow-600" };
  if (pct >= 20) return { label: "Low", pct, color: "text-orange-500" };
  return { label: "Not started", pct, color: "text-gray-400" };
}

export function stateColor(state: string): string {
  switch (state) {
    case "certified": return "text-green-700 bg-green-50 border-green-200";
    case "draft": return "text-yellow-700 bg-yellow-50 border-yellow-200";
    case "cannot_answer": return "text-red-700 bg-red-50 border-red-200";
    case "demoted": return "text-orange-700 bg-orange-50 border-orange-200";
    default: return "text-gray-600 bg-gray-50 border-gray-200";
  }
}

export function priorityColor(priority: string): string {
  switch (priority) {
    case "high": return "text-red-700 bg-red-50";
    case "medium": return "text-yellow-700 bg-yellow-50";
    default: return "text-gray-600 bg-gray-50";
  }
}
