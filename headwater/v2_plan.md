# Headwater v2: Semantic Intelligence & UX Overhaul

## Development Plan for CTO Review

**Team**: 1 Senior Data Engineer + Claude Agents
**Date**: April 2026
**Status**: Architecture finalized, ready for implementation

---

## 1. Context: Why This Overhaul

The CTO demo revealed that the app's core value proposition -- semantic understanding
of data -- was not working. A simple question ("Count of complaints by county") picked
wrong columns and returned a confident-looking wrong answer.

Root cause analysis revealed this was not a bug but an architectural gap:
- The explorer does text-to-SQL via keyword matching (the anti-pattern)
- Rich semantic metadata exists (business descriptions, narratives, semantic groups)
  but is never used by the explorer
- No formal ontology (metrics, dimensions, entities) exists
- No semantic search capability exists
- The dashboard shows static data and lacks holistic project tracking
- The data dictionary provides minimal descriptions
- Relationships have no visual graph representation

This plan addresses ALL of these issues as a cohesive overhaul.

---

## 2. Research Foundation

Architecture informed by:
- **dbt Semantic Layer Benchmark (2026)**: Ontology-driven decomposition achieves
  98-100% accuracy vs 84-90% for text-to-SQL. Errors > misinformation.
- **CHESS (BIRD #1)**: Multi-stage schema linking via LSH + embeddings + descriptions.
  Schema selection is the highest-impact component (+6.12% accuracy).
- **MetricFlow**: Metrics/dimensions defined in YAML, SQL generated deterministically.
  LLM decomposes, never generates SQL.
- **Google db-context-enrichment**: Column descriptions are foundational for text-to-SQL.
- **sentence-transformers**: all-MiniLM-L6-v2 for lightweight semantic similarity.
- **LanceDB**: Arrow-native vector store for multi-dataset embedding persistence.
- **Kuzu**: Embedded graph DB for relationship pattern discovery.

---

## 3. New Concepts

### 3.1 Project (Data Group)

A **Project** is the top-level container for a dataset or data group. It replaces
the current flat `sources` table with a richer entity that tracks maturity and progress.

```python
class Project(BaseModel):
    id: str                      # Auto-generated UUID
    slug: str                    # URL-safe name: "riverton-env-health"
    display_name: str            # "Riverton Environmental Health"
    description: str             # Auto-generated summary of the dataset
    sources: list[SourceConfig]  # One or more data sources
    created_at: datetime
    updated_at: datetime

    # Maturity tracking
    maturity: Literal["raw", "profiled", "documented", "modeled", "production"]
    maturity_score: float        # 0.0-1.0 composite score

    # Progress tracking (dynamically updated)
    progress: ProjectProgress

    # Catalog reference
    catalog_confidence: float    # Overall semantic catalog quality

class ProjectProgress(BaseModel):
    tables_discovered: int
    tables_profiled: int
    tables_reviewed: int         # Dictionary confirmed
    tables_modeled: int          # Has staging model
    tables_mart_ready: int       # Has approved mart
    columns_total: int
    columns_described: int       # Have meaningful descriptions
    columns_confirmed: int       # Human-locked
    relationships_detected: int
    relationships_confirmed: int
    metrics_defined: int
    dimensions_defined: int
    quality_contracts: int
    contracts_enforcing: int     # Past observation mode
    catalog_coverage: float      # % of analytical columns in catalog
    last_activity: datetime
```

**Maturity levels:**
```
raw        -> Data loaded, not yet profiled
profiled   -> Schema extracted, stats computed, relationships detected
documented -> Dictionary reviewed, descriptions confirmed, catalog generated
modeled    -> Staging + mart models generated and approved
production -> Quality contracts enforcing, drift monitoring active
```

### 3.2 Semantic Catalog (Ontology)

```python
class MetricDefinition(BaseModel):
    name: str                    # "complaint_count"
    display_name: str            # "Total Complaints"
    description: str             # "Count of all complaint records"
    expression: str              # "COUNT(*)"
    column: str | None = None
    table: str
    agg_type: Literal["count", "sum", "avg", "min", "max", "count_distinct"]
    filters: list[str] = []
    synonyms: list[str] = []
    confidence: float = 0.5
    status: Literal["proposed", "confirmed", "rejected"] = "proposed"
    source: Literal["heuristic", "llm", "human"] = "heuristic"

class DimensionDefinition(BaseModel):
    name: str                    # "zone_geography"
    display_name: str            # "Zone / Geographic Area"
    description: str
    column: str                  # "name"
    table: str                   # "zones"
    dtype: str
    expression: str | None = None  # For CASE WHEN derived dimensions
    synonyms: list[str] = []
    hierarchy: list[str] = []   # coarse -> fine
    sample_values: list[str] = []
    cardinality: int = 0
    confidence: float = 0.5
    status: Literal["proposed", "confirmed", "rejected"] = "proposed"
    source: Literal["heuristic", "llm", "human"] = "heuristic"
    join_path: str | None = None
    join_nullable: bool = False  # True if FK can be NULL

class EntityDefinition(BaseModel):
    name: str
    display_name: str
    description: str
    table: str
    row_semantics: str
    metrics: list[str] = []
    dimensions: list[str] = []
    temporal_grain: str | None = None
    synonyms: list[str] = []

class SemanticCatalog(BaseModel):
    metrics: list[MetricDefinition] = []
    dimensions: list[DimensionDefinition] = []
    entities: list[EntityDefinition] = []
    generated_at: datetime
    generation_source: Literal["heuristic", "llm", "hybrid"] = "heuristic"
    confidence: float = 0.5
```

### 3.3 Decomposition Result

```python
class DecompositionResult(BaseModel):
    status: Literal["resolved", "options", "outside_scope"]
    entity: str | None = None
    metrics: list[MetricMatch] = []
    dimensions: list[DimensionMatch] = []
    sql: str | None = None
    explanation: str = ""        # How the query was interpreted
    warnings: list[str] = []    # NULL caveats, join notes
    suggestions: list[str] = [] # Follow-up questions
    options: list[DimensionOption] = []  # When disambiguation needed
    outside_catalog: list[str] = []
    confidence: float = 0.0
    resolution_mode: Literal["catalog", "exploratory"] | None = None
```

---

## 4. Architecture: Component Map

### 4.1 New Backend Components

```
headwater/headwater/
  core/
    models.py            MODIFY  Add Project, SemanticCatalog, Decomposition models
    metadata.py          MODIFY  Add project + catalog tables, LanceDB + Kuzu init
    vector_store.py      CREATE  LanceDB connection factory + embedding helpers
    graph_store.py       CREATE  Kuzu connection factory + pattern queries
  analyzer/
    catalog.py           CREATE  SemanticCatalog builder (heuristic + LLM)
    eval.py              CREATE  Catalog quality evaluation
  explorer/
    decomposition.py     CREATE  QueryDecomposer: 3-strategy resolution
    schema_context.py    CREATE  Rich metadata context for fallback
    nl_to_sql.py         MODIFY  Wire decomposer as Priority 0
    suggestions.py       MODIFY  Generate from catalog (metric x dimension)
  api/routes/
    project.py           CREATE  Project CRUD, dashboard data, maturity tracking
    explore.py           MODIFY  DecompositionResult, /select-option, feedback
    dictionary.py        MODIFY  Include catalog review, richer descriptions
    discovery.py         MODIFY  Trigger catalog + graph build after discovery
    insights.py          MODIFY  Project-aware holistic insights
    graph.py             CREATE  Relationship graph data for visualization
```

### 4.2 New Frontend Components

```
ui/src/app/
  page.tsx               MODIFY  Project dashboard with left panel
  discovery/page.tsx     MODIFY  Key columns view + detailed tab
  dictionary/page.tsx    MODIFY  Rich descriptions, catalog review section
  models/page.tsx        MODIFY  Graph visualization tab (Kuzu-powered)
  quality/page.tsx       MODIFY  Redesigned quality dashboard
  explore/page.tsx       MODIFY  Disambiguation UI, explanation panel

ui/src/components/
  project-sidebar.tsx    CREATE  Left panel: project list + summary
  project-summary.tsx    CREATE  Maturity gauge, progress tracker
  relationship-graph.tsx CREATE  Interactive graph visualization (D3/force)
  catalog-review.tsx     CREATE  Metric/dimension review cards
  disambiguation-ui.tsx  CREATE  Option selection cards
  quality-dashboard.tsx  CREATE  Redesigned quality overview
  key-columns-view.tsx   CREATE  Highlighted important columns
```

### 4.3 New Dependencies

```toml
[project.dependencies]
# Existing deps unchanged, plus:
lancedb = ">=0.15"               # Vector store (Arrow-native)
sentence-transformers = ">=3.0"  # Embedding model
kuzu = ">=0.7"                   # Embedded graph DB
```

All three are embedded (no external servers), lightweight, Python-native.

---

## 5. Detailed Flows With Concrete Examples

### 5.1 Ingest & Discovery Flow

```
USER: Uploads 8 JSON files (Riverton Environmental Health dataset)
        |
   [1] Connector loads into DuckDB (existing, unchanged)
        complaints (3000), zones (25), sites (500), sensors (832),
        readings (49302), inspections (1243), incidents (5000), programs (10)
        |
   [2] Profiler extracts stats (existing, unchanged)
        Each column gets: null_rate, distinct_count, top_values, min/max, etc.
        FK relationships detected: 12 relationships across 8 tables
        |
   [3] Semantic analyzer enriches (existing, unchanged)
        Heuristic + LLM: descriptions, roles, semantic types, narratives
        |
   [4] **NEW: Kuzu graph build**
        Load tables as nodes, FKs as edges with properties:
          (complaints)-[:FK {col: "zone_id", nullable: false, integrity: 1.0}]->(zones)
          (complaints)-[:FK {col: "related_site_id", nullable: true, integrity: 0.28}]->(sites)
          (readings)-[:FK {col: "sensor_id"}]->(sensors)-[:FK {col: "site_id"}]->(sites)
          ...
        Run pattern discovery:
          CONFORMED DIMENSIONS: zones (connected to 4 fact tables)
          STAR SCHEMA: zones hub, sites secondary hub
          CHAINS: readings -> sensors -> sites -> zones (4 hops)
          NULLABLE FK WARNING: complaints.related_site_id (72% NULL)
        |
   [5] **NEW: Catalog generation**
        From semantic metadata + graph patterns + LLM:

        METRICS (auto-generated):
          complaint_count:        COUNT(*) from complaints
          avg_inspection_score:   AVG("score") from inspections
          reading_value:          AVG("value") from readings
          incident_count:         COUNT(*) from incidents
          resolution_rate:        COUNT("resolution_date")/COUNT(*) from complaints
          critical_violation_rate: SUM("critical_violations")/COUNT(*) from inspections
          ...

        DIMENSIONS (auto-generated with synonyms):
          complaint_category:  complaints.category
            synonyms: [type, kind, category, complaint type]
            values: [noise, water_quality, pest, air_quality, waste, other]

          zone_geography:      zones.name
            synonyms: [county, borough, district, zone, area, neighborhood, region]
            values: [Downtown Core, Industrial Park, Riverside, ...]
            join_path: complaints.zone_id -> zones.zone_id
            hierarchy: [state, city, zone_geography]

          zone_type:           zones.type
            synonyms: [land use, area type, zone classification]
            values: [urban_commercial, residential, industrial, ...]

          sensor_measurement:  sensors.sensor_type
            synonyms: [measurement, parameter, pollutant, what is measured]
            values: [pm25, temperature, noise, ozone, ...]

          site_facility_type:  sites.site_type
            synonyms: [facility, venue, building type, establishment]
            values: [air_monitoring_station, food_establishment, school, ...]
          ...

        ENTITIES:
          complaints: "Environmental health complaints from residents"
            metrics: [complaint_count, resolution_rate, avg_resolution_days]
            dimensions: [complaint_category, zone_geography, zone_type,
                         complaint_priority, complaint_source, complaint_status]

          inspections: "Site inspections by environmental health inspectors"
            metrics: [avg_inspection_score, inspection_count, critical_violation_rate]
            dimensions: [inspection_type, inspection_result, site_facility_type,
                         zone_geography, zone_type]
          ...
        |
   [6] **NEW: Embedding index in LanceDB**
        Each metric/dimension/entity embedded with all-MiniLM-L6-v2:
          text = "Zone Geography. Administrative zones for monitoring.
                  Synonyms: county, borough, district. Values: Downtown Core, ..."
          -> 384-dim vector stored in LanceDB table "catalog_entries"
        |
   [7] **NEW: Catalog evaluation**
        Coverage: 87% (most analytical columns in catalog)
        Synonym test: 9/10 synthetic questions resolved correctly
        SQL validity: 100% (all expressions parse in DuckDB)
        Ambiguity: 1 warning (zone_geography and zone_type both match "area")
        Overall: 0.85 / 1.0
        |
   [8] **NEW: Project created**
        slug: "riverton-env-health"
        display_name: "Riverton Environmental Health"
        description: "Environmental health monitoring data for Riverton, Columbia.
          8 tables covering complaints, inspections, sensor readings, health
          incidents, monitoring sites, sensors, zones, and programs. 59.9K total
          records spanning 2024."
        maturity: "profiled" (will advance as user reviews dictionary)
        |
   ALL AUTOMATIC -- no user input required up to this point
```

### 5.2 Data Dictionary Flow (Enhanced)

**Current problem**: Dictionary shows column name and dtype. "zone_id" just says "zone_id".

**New dictionary experience:**

```
TABLE: complaints (3,000 rows)                         Maturity: [Profiled]
"Environmental health complaints filed by Riverton residents.
 Each row is one complaint filing."

KEY COLUMNS (highlighted, most important first):
  +------------------------------------------------------------------+
  | category     varchar  [dimension]  6 values     confidence: 0.95 |
  |   "Type of environmental complaint"                              |
  |   noise (20%), water_quality (15%), air_quality (12%), ...       |
  +------------------------------------------------------------------+
  | zone_id      varchar  [FK -> zones] 25 zones    confidence: 0.99 |
  |   "Links complaint to geographic zone for area-based analysis"   |
  |   Joins to: zones.name, zones.type, zones.population            |
  +------------------------------------------------------------------+
  | priority     varchar  [dimension]  4 values     confidence: 0.90 |
  |   "Severity: urgent > high > medium > low"                      |
  +------------------------------------------------------------------+
  | date_filed   date     [temporal]   365 days     confidence: 0.98 |
  |   "When complaint was submitted. Range: 2024-01-01 to 2024-12-31"|
  +------------------------------------------------------------------+
  | status       varchar  [dimension]  5 values     confidence: 0.92 |
  |   "Workflow state: open -> investigating -> resolved/closed"     |
  +------------------------------------------------------------------+

  [Show all 16 columns >>]                    (detailed view in second tab)

PROPOSED METRICS:                              [Confirm] [Edit] [Reject]
  complaint_count     COUNT(*)                 confidence: 0.95
  resolution_rate     COUNT(resolution_date)/COUNT(*)     conf: 0.80
    ! resolution_date is NULL for 45% of rows (unresolved complaints)

PROPOSED DIMENSIONS:                           [Confirm] [Edit] [Reject]
  zone_geography      zones.name via zone_id   confidence: 0.85
    Synonyms: county, district, zone, area, neighborhood
    [+ Add synonym]
  complaint_category  complaints.category      confidence: 0.95
    Synonyms: type, kind, category
    [+ Add synonym]

RELATIONSHIPS:
  zone_id -> zones.zone_id (many-to-one, 100% integrity)  [Confirmed]
  related_site_id -> sites.site_id (many-to-one, 28% integrity)
    ! 72% of complaints have no linked site             [Needs review]
```

**Key differences from current:**
- Key columns surfaced first (dimensions + temporal + FK, sorted by importance)
- Actual descriptions, not just column names
- Sample values shown inline
- Metric/dimension proposals with confirm/edit/reject
- Synonym management
- Relationship quality warnings
- Two tabs: "Key Columns" (curated) and "All Columns" (full detail)

### 5.3 Query Decomposition Flow (Detailed)

```
USER: "Show me average inspection scores by zone type for food establishments"
        |
   [1] KEYWORD EXTRACTION (no LLM)
        content_words: [inspection, scores, zone, type, food, establishments]
        intent: AVERAGE
        subject: [inspection, scores]
        predicate: [zone, type, food, establishments]
        |
   [2] ENTITY RESOLUTION
        Strategy A (keyword): "inspection" matches entity "inspections"
        -> entity: inspections (confidence 0.95)
        |
   [3] METRIC RESOLUTION
        Strategy A: "scores" stem-matches "avg_inspection_score" synonym
        -> metric: avg_inspection_score = AVG("score") (confidence 0.85)
        |
   [4] DIMENSION RESOLUTION
        Strategy A: compound "zone_type" exact match in catalog
        -> dimension: zone_type (confidence 0.95)

        "food establishments" -> no dimension match
        Strategy B (embedding): embed("food establishments")
          cosine_sim with catalog entries:
            site_facility_type: 0.82 (description mentions "food_establishment")
            zone_type: 0.35
          -> Strategy B suggests: FILTER on site_facility_type = 'food_establishment'
        "for X" grammar pattern -> confirms FILTER intent, not GROUP BY
        |
   [5] JOIN PATH (from catalog, pre-computed at build time)
        inspections.site_id -> sites.site_id (1 hop, not nullable)
        sites.zone_id -> zones.zone_id (2 hops total, not nullable)
        Filter: sites.site_type = 'food_establishment' (same join path)
        |
   [6] SQL GENERATION (deterministic from catalog)
        SELECT z."type" AS zone_type,
               ROUND(AVG(i."score"), 2) AS avg_inspection_score,
               COUNT(*) AS inspection_count
        FROM staging.stg_inspections i
        JOIN staging.stg_sites s ON i."site_id" = s."site_id"
        JOIN staging.stg_zones z ON s."zone_id" = z."zone_id"
        WHERE s."site_type" = 'food_establishment'
        GROUP BY z."type"
        ORDER BY avg_inspection_score DESC
        |
   [7] EXECUTION + EXPLANATION
        "Averaged inspection scores by zone type, filtered to food establishments.
         Path: inspections -> sites -> zones (2 joins).
         'food establishments' interpreted as a filter on site type."
        |
   [8] FOLLOW-UP SUGGESTIONS (from catalog cross-products)
        "You might also explore:
         - Inspection scores by zone name (25 zones)
         - Critical violations by zone type for food establishments
         - Inspection trend over time for food establishments"
```

### 5.4 Handling JOINs, CASE WHEN, NULLs

**JOINs**: Pre-computed in catalog. Nullable FKs use LEFT JOIN with warning.
```
"Note: 72% of complaints have no linked site. Using LEFT JOIN --
 site-related columns will be NULL for those records."
```

**CASE WHEN**: Derived dimensions stored in catalog with `expression` field.
```python
DimensionDefinition(
    name="priority_severity",
    expression='CASE WHEN "priority" IN (\'urgent\',\'high\') THEN \'Critical\' '
               'WHEN "priority" = \'medium\' THEN \'Moderate\' ELSE \'Low\' END',
    synonyms=["severity", "severity level", "urgency"],
)
```

**NULLs**: Profile null_rates drive warnings.
```
User: "Average resolution time by zone"
System: "resolution_date is NULL for 45% of complaints (unresolved cases).
         This metric reflects only resolved complaints.
         Suggestion: Also include resolution_rate to see full picture."
SQL includes: WHERE "resolution_date" IS NOT NULL
```

**Value-based filters**: "unresolved complaints" -> WHERE status NOT IN ('resolved', 'closed')
```
System detects: "unresolved" matches top_values of status column
-> Adds filter, explains: "Filtered to status: open, investigating, referred"
```

---

## 6. Dashboard & UX Overhaul

### 6.1 Project Dashboard (Left Panel + Main View)

```
+--LEFT PANEL--+--MAIN DASHBOARD----------------------------------------+
|              |                                                         |
| PROJECTS     | Riverton Environmental Health                          |
|              | slug: riverton-env-health                              |
| [+] New      |                                                         |
|              | MATURITY: [====>    ] Documented (3/5)                  |
| > Riverton   |                                                         |
|   Env Health | "Environmental health monitoring data for Riverton.     |
|   [====]     |  8 tables, 59.9K records, 12 relationships detected.   |
|   Documented |  Covers complaints, inspections, sensor readings,       |
|              |  health incidents, and monitoring programs."             |
| > Sales      |                                                         |
|   Pipeline   | PROGRESS                                                |
|   [==]       | Tables:    8/8 discovered, 8/8 profiled, 5/8 reviewed  |
|   Profiled   | Columns:   89/126 described, 42/126 confirmed           |
|              | Metrics:   12 defined, 8 confirmed                     |
|              | Dimensions: 15 defined, 10 confirmed                   |
|              | Relationships: 12 detected, 9 confirmed                |
|              | Quality:   168 contracts, 45 enforcing                 |
|              | Catalog:   0.85 confidence                             |
|              |                                                         |
|              | WORKFLOW                                                |
|              | [Discover: DONE] -> [Dictionary: 5/8] ->               |
|              | [Models: 3 pending] -> [Quality: observing]            |
|              |                                                         |
|              | RECENT ACTIVITY                                        |
|              | - 2h ago: Confirmed zones table in dictionary           |
|              | - 1d ago: Approved mart_complaint_summary model         |
|              | - 2d ago: Discovery completed (8 tables, 59.9K rows)   |
|              |                                                         |
|              | ATTENTION NEEDED                                        |
|              | ! 3 mart models awaiting review                        |
|              | ! 2 columns with low confidence (needs dictionary)     |
|              | ! 1 relationship with 28% integrity (investigate)      |
|              |                                                         |
|              | DOMAIN MAP          | HEALTH SCORECARD                 |
|              | [Environmental:5]   | Completeness: 94%               |
|              | [Monitoring:3]      | Descriptions: 71%               |
|              |                     | Catalog coverage: 85%           |
+--------------+---------------------------------------------------------+
```

**Dynamic updates**: Every user action (confirm column, approve model, answer question)
updates the progress counters and maturity level in real-time.

### 6.2 Discovery Page (Key Columns + Detail Tab)

**Tab 1: Key Columns** (default view)
- Only shows dimensions, metrics, temporal, FK columns
- Sorted by analytical importance (dimensions first, then metrics, then temporal)
- Each column has inline description, sample values, confidence badge
- FK columns show what they join to

**Tab 2: Full Schema** (detailed view)
- All columns with complete profiling stats
- Existing discovery view, enhanced with descriptions

### 6.3 Models & Lineage (Graph Visualization)

**New tab: Relationship Graph**

Interactive force-directed graph (D3.js or similar):
```
                    [programs]
                        |
                        | linked_program_id
                        v
[zones] <--zone_id-- [incidents] --reporting_facility_id--> [sites]
  ^                                                           ^  |
  |--zone_id-- [complaints] --related_site_id(dashed)--------+  |
  |                                                              |
  +--zone_id-- [sites] --site_id--> [sensors] --sensor_id--> [readings]
                  ^
                  +--site_id-- [inspections]
```

- Nodes: tables (sized by row count)
- Edges: FK relationships (solid = high integrity, dashed = low/nullable)
- Click node: expand to show columns, metrics, dimensions
- Click edge: show relationship detail (cardinality, integrity %, FK columns)
- Drill down: click a node to see its entity definition, metrics, dimensions
- Color coding: by domain or by maturity (reviewed = green, pending = yellow)
- Kuzu powers the backend queries for graph data

**Existing tabs enhanced:**
- Lineage diagram updated when catalog links metrics to marts
- Browse All shows catalog-linked metrics alongside model SQL

### 6.4 Quality & Metrics (Redesigned Dashboard)

**Current problem**: Just tabs with lists. No prioritization or actionability.

**New design:**

```
QUALITY SCORECARD
+-------------------+-------------------+-------------------+
| COMPLETENESS      | DATA QUALITY      | CATALOG HEALTH    |
| 94.2%             | 12 issues         | 0.85 confidence   |
| [==========> ]    | 3 critical        | 87% coverage      |
| 7 cols have nulls | 5 warnings        | 12 metrics        |
|                   | 4 info            | 15 dimensions     |
+-------------------+-------------------+-------------------+

NEEDS ATTENTION (sorted by impact)
  ! complaints.latitude: 15% NULL -- geographic analysis affected
  ! complaints.related_site_id: 72% NULL -- site joins will lose data
  ! inspections.follow_up_date: 40% NULL -- follow-up tracking incomplete

CLEAN & RELIABLE
  zones: 0% nulls, all columns profiled, dictionary confirmed
  programs: 0% nulls, all columns profiled
  sensors: 2% nulls (calibration_status only)

CONTRACTS
  [Enforcing: 45]  [Observing: 123]  [Proposed: 0]
  Recent violations: 2 (both in observation mode, no action needed)

METRICS DEFINED
  12 metrics across 5 entities
  Highest confidence: complaint_count (0.95), avg_inspection_score (0.92)
  Needs review: resolution_rate (0.75), critical_violation_rate (0.78)
```

---

## 7. Technology Stack (Complete)

| Layer | Technology | Purpose |
|---|---|---|
| Language | Python 3.11+ | Backend |
| Package manager | uv | Dependencies |
| Data processing | Polars | Arrow-native profiling |
| Analytical DB | DuckDB | OLAP, model materialization |
| Metadata store | SQLite | Metadata, decisions, audit |
| Vector store | **LanceDB** | Catalog embeddings, semantic search |
| Graph store | **Kuzu** | Relationship patterns, lineage |
| Embeddings | **sentence-transformers** | all-MiniLM-L6-v2 (384-dim) |
| API | FastAPI + uvicorn | Backend API |
| LLM | Anthropic SDK / Ollama | Semantic enrichment, decomposition |
| SQL templates | Jinja2 | Model generation |
| Validation | Pydantic v2 | Domain models |
| Frontend | Next.js 16 + React 19 | UI |
| Styling | Tailwind CSS 4 | UI styling |
| Charts | Recharts | Data visualization |
| Graph viz | **D3.js / react-force-graph** | Relationship graph |
| Testing | pytest | Backend tests |
| Linting | ruff | Code quality |

---

## 8. LLM Strategy: Tiered Provider Model

### 8.1 Three Tiers (Progressive Enhancement)

The system works at ALL tiers. Each tier improves quality but none is required.

| Tier | Provider | Model | Use Case | Cost |
|------|----------|-------|----------|------|
| **0: Heuristic** | `none` | -- | Catalog from patterns + profiles. No LLM. | Free |
| **1: Local LLM** | `ollama` | `llama3.1:8b` (default) | Synonym expansion, basic decomposition, descriptions | Free (CPU/GPU) |
| **2: Cloud LLM** | `anthropic` | `claude-sonnet-4-20250514` | Deep semantic inference, complex decomposition, eval | API cost |

**Default**: Tier 0 (heuristic-only). Works out of the box with no setup.

### 8.2 What Each Tier Powers

| Capability | Tier 0 (Heuristic) | Tier 1 (Local) | Tier 2 (Cloud) |
|-----------|---------------------|-----------------|-----------------|
| Column descriptions | Pattern-based ("ID column", "timestamp") | Contextual ("Zone identifier linking complaints to geographic areas") | Rich business-level ("Administrative zone where the complaint was filed, used for area-based aggregation and resource allocation") |
| Synonym generation | Built-in families (~60 entries) | LLM-expanded (+20-40 per dataset) | Deep contextual (+50-80, domain-aware) |
| Catalog confidence | 0.4-0.6 typical | 0.6-0.8 typical | 0.8-0.95 typical |
| Query decomposition | Keyword + embedding only | + LLM disambiguation | + complex multi-entity reasoning |
| Catalog evaluation | Coverage + SQL validity only | + synthetic question test | + semantic coherence scoring |

### 8.3 User-Switchable Configuration

**Current** (env vars only):
```bash
HEADWATER_LLM_PROVIDER=ollama
HEADWATER_LLM_MODEL=llama3.1:8b
```

**New**: Add UI settings panel + support for OpenAI-compatible endpoints:
```python
# core/config.py additions
llm_provider: Literal["none", "anthropic", "ollama", "openai_compat"] = "none"
openai_compat_base_url: str | None = None   # For vLLM, Together, Groq, etc.
openai_compat_api_key: str | None = None
```

**API endpoint**: `GET/PUT /settings/llm` -- lets the UI read/update provider config.
User can switch providers mid-session; catalog regeneration is triggered if provider
tier changes upward.

### 8.4 Model Recommendations by Budget

| Budget | Setup | Expected Catalog Quality |
|--------|-------|-------------------------|
| $0 | Ollama + llama3.1:8b (or mistral:7b) | Good (0.6-0.8 confidence) |
| $5-20/month | Anthropic Haiku for enrichment, Sonnet for eval | Very good (0.8-0.9) |
| $20-50/month | Anthropic Sonnet for everything | Excellent (0.85-0.95) |
| Enterprise | User's own model via OpenAI-compat endpoint | Depends on model |

---

## 9. Modeling Technique: Ontology-Driven Star Schema

### 9.1 Current State

The generator uses **archetype-based pattern matching** with 3 archetypes:
- `period_comparison` (temporal + LAG window functions)
- `entity_summary` (fact + dimension JOIN, GROUP BY ALL)
- `simple_aggregation` (single-table AVG/SUM/COUNT)

This works mechanically but has no semantic understanding. The PatternMatcher
guesses join columns from naming conventions (`{table}_id`).

### 9.2 New Approach: The Catalog IS the Schema

The semantic catalog naturally defines a **star schema**:
- **Entities** = fact tables (complaints, inspections, readings)
- **Dimensions** = dimension tables with join paths (zones, sites, sensors)
- **Metrics** = pre-defined aggregation expressions on facts
- **Join paths** = pre-computed from Kuzu graph with integrity scores

This replaces the PatternMatcher. Instead of guessing archetypes from column
types, the catalog explicitly declares what each table is and how it connects.

### 9.3 Why Star Schema (Not Data Vault or Others)

| Technique | Fit for Headwater | Why |
|-----------|-------------------|-----|
| **Star Schema** | Best fit | Maps directly to metric/dimension ontology. Simple to understand and query. 90% of analytical questions are "metric by dimension". |
| **Data Vault** | Poor fit | Solves auditability/historization -- problems we don't have. Adds hubs/links/satellites complexity that obscures analytical value. |
| **Wide/Flat** | Partial fit | Works for single-table analysis but loses relationship context. Already covered by `simple_aggregation` archetype. |
| **Activity Schema** | Future consideration | Good for event-based data (readings, incidents). Could be a 4th archetype if needed. |

### 9.4 What Changes in the Generator

The existing generator stays for **staging models** (mechanical, unchanged).
For **mart models**, the catalog replaces PatternMatcher:

```
BEFORE: PatternMatcher -> archetype detection -> f-string SQL
AFTER:  Catalog entities -> metric/dimension combos -> deterministic SQL from catalog
```

Mart proposals are generated from catalog cross-products:
- For each entity, generate one mart per meaningful dimension combination
- Join paths come from catalog (pre-computed, validated)
- Metric expressions come from catalog (pre-defined, validated)
- The user reviews the ONTOLOGY (metrics, dimensions), not the SQL

The SQL is a mechanical consequence of the ontology -- if the ontology is right,
the SQL is right. This is the MetricFlow insight.

---

## 10. Confirmation Flow: Non-Blocking by Default

### 10.1 Design Principle

**Only mart model approval blocks execution.** Everything else is non-blocking.
The explorer works immediately with whatever confidence level exists. Users improve
quality incrementally; all views reflect changes in real-time.

### 10.2 Confirmation Tiers

| Element | Auto-generated | User can edit anytime | Blocks exploration | Blocks execution |
|---------|---------------|----------------------|-------------------|-----------------|
| Column descriptions | Yes | Yes | No | No |
| Semantic types/roles | Yes | Yes | No | No |
| Relationships | Yes (detected) | Yes (confirm/reject) | No | No |
| Metrics (proposed) | Yes | Yes (confirm/edit/reject) | No | No |
| Dimensions (proposed) | Yes | Yes (confirm/edit/reject) | No | No |
| Synonyms | Yes | Yes (add/remove) | No | No |
| Table review status | Starts "pending" | Yes | **No** | No |
| Staging models | Auto-approved | No (mechanical) | No | No |
| **Mart models** | Starts "proposed" | Yes (approve/reject) | No | **Yes** |

### 10.3 Confidence-Based UX (Not Gates)

Instead of blocking, confidence drives **visual indicators**:

- **High confidence (>=0.8)**: Green badge. Explorer auto-resolves.
- **Medium confidence (0.4-0.8)**: Yellow badge. Explorer presents options.
  Dictionary shows "Suggested -- confirm to improve accuracy".
- **Low confidence (<0.4)**: Red badge. Explorer explains what's available.
  Dictionary shows "Needs review -- description may be inaccurate".

**Key change**: The current dictionary review gate (`reviewed_tables` check in
`explore.py`) is **removed**. Exploration works from day one. Reviewing the
dictionary improves accuracy but never blocks access.

### 10.4 State Propagation

When a user confirms a column, locks a description, adds a synonym, or confirms
a metric/dimension:
1. Persisted immediately to SQLite metadata
2. Catalog entry updated (confidence boosted, source changed to "human")
3. LanceDB embedding re-indexed for that entry
4. All API responses reflect the change on next request
5. Dashboard progress counters update
6. Maturity level may advance (e.g., profiled -> documented)

No page refresh required -- the frontend polls or uses the existing refresh pattern.

### 10.5 What Triggers Maturity Transitions

```
raw        -> profiled:    Automatic after discovery + profiling completes
profiled   -> documented:  >=60% of columns have descriptions (auto or confirmed)
                           AND catalog generated with >=0.5 confidence
documented -> modeled:     >=1 mart model approved
modeled    -> production:  >=1 contract in "enforcing" status
```

All transitions are automatic based on thresholds. No manual "advance maturity" button.

---

## 11. Gap Analysis: Earlier Plans vs v2

### 11.1 Items Added to v2 (Were Missing)

| Item | Source | Action |
|------|--------|--------|
| Companion doc scanning | semantic_dictionary.md memory | **Add to Phase 2** -- scan source dirs for .md/.txt/.csv alongside data files. CompanionDoc model already exists. Wire into ingest. |
| Confidence metrics dashboard | US-302/303/305 | **Add to Phase 5** -- surface description acceptance rate, model edit distance, contract precision in project health scorecard |
| Schema drift alerts | US-402/403 | **Add to Phase 5** -- surface drift_reports in dashboard "Attention Needed" section |
| LLM settings UI | New requirement | **Add to Phase 5** -- settings panel for provider/model/key switching |

### 11.2 Explicitly Deferred (Not in v2 Scope)

| Item | Original Phase | Why Deferred |
|------|---------------|--------------|
| Observe mode connectors (Snowflake, BigQuery) | Phase 2 | Requires warehouse SDKs + auth; v2 focuses on semantic layer |
| Catalog integrations (Glue, Unity) | Phase 2+ | Major connector work; orthogonal to semantic overhaul |
| dbt export format | Phase 3 | Nice-to-have; catalog ontology is the prerequisite |
| Nessie branching for approvals | Phase 3 | Requires Iceberg stack; overkill for current scale |
| Notifications (Slack, email) | Phase 3 | External service integrations; not core value |
| Scheduled discovery runs | Phase 3+ | Requires daemon/job management |
| Cloud warehouse backends | Phase 5 | DuckDB sufficient for advisory use case |
| RBAC / multi-user | Phase 5+ | Enterprise feature; single-user focus for now |

### 11.3 Preserved from Earlier Plans (Already in v2)

- Semantic locks (I-6) -- confirmed columns skip re-enrichment
- Advisory boundary (I-4) -- mart models always require human review
- Quality contracts in observation mode (I-5)
- Arrow-native data flow (I-2)
- LLM audit logging -- all calls tracked in metadata

---

## 12. Implementation Plan

### Phase 1: Foundation (Core Models + Stores)

**Files:**
- `core/models.py` -- Add Project, SemanticCatalog, MetricDefinition,
  DimensionDefinition, EntityDefinition, DecompositionResult
- `core/vector_store.py` -- LanceDB connection factory, embedding helpers
- `core/graph_store.py` -- Kuzu connection factory, schema loading, pattern queries
- `core/metadata.py` -- Add projects table, catalog table
- `core/config.py` -- Add vector/graph store path settings, OpenAI-compat provider

**Tests:** test_models.py additions, test_vector_store.py, test_graph_store.py

### Phase 2: Catalog Generation

**Files:**
- `analyzer/catalog.py` -- Heuristic + LLM catalog builder, synonym families,
  embedding index population
- `analyzer/eval.py` -- Catalog quality evaluation
- `analyzer/semantic.py` -- Wire catalog build after analysis
- `analyzer/companion.py` -- Companion doc scanning (wire existing CompanionDoc model
  into directory scan during ingest)

**Tests:** test_catalog.py, test_eval.py

### Phase 3: Query Decomposition Engine

**Files:**
- `explorer/decomposition.py` -- QueryDecomposer with 3-strategy resolution
  (keyword + embedding + LLM) + deterministic SQL generation from catalog
- `explorer/schema_context.py` -- Rich context for exploratory fallback
- `explorer/nl_to_sql.py` -- Wire decomposer as Priority 0, remove dictionary
  review gate (exploration always available)
- `explorer/suggestions.py` -- Catalog-based suggestion generation

**Tests:** test_decomposition.py (including "complaints by county" test case)

### Phase 4: API Layer

**Files:**
- `api/routes/project.py` -- Project CRUD, maturity tracking, progress
- `api/routes/explore.py` -- DecompositionResult, /select-option, feedback,
  remove reviewed_tables gate
- `api/routes/dictionary.py` -- Catalog review section, richer descriptions,
  non-blocking confirm/edit (state propagation)
- `api/routes/discovery.py` -- Trigger catalog + graph build, companion doc scan
- `api/routes/graph.py` -- Relationship graph data for visualization
- `api/routes/insights.py` -- Project-aware holistic dashboard data
- `api/routes/settings.py` -- LLM provider/model/key read and update

### Phase 5: Frontend Overhaul

**Files:**
- `ui/src/components/project-sidebar.tsx` -- Left panel with project list
- `ui/src/components/project-summary.tsx` -- Maturity gauge, progress tracker,
  confidence metrics (US-302/303/305), drift alerts (US-402/403)
- `ui/src/app/page.tsx` -- Redesigned dashboard with project context
- `ui/src/app/discovery/page.tsx` -- Key columns + detailed tabs
- `ui/src/app/dictionary/page.tsx` -- Rich descriptions, catalog review,
  non-blocking confidence badges (green/yellow/red)
- `ui/src/app/models/page.tsx` -- Relationship graph tab (D3)
- `ui/src/app/quality/page.tsx` -- Redesigned quality scorecard
- `ui/src/app/explore/page.tsx` -- Disambiguation UI, explanations, no gate
- `ui/src/app/settings/page.tsx` -- LLM provider settings panel
- `ui/src/components/relationship-graph.tsx` -- Interactive graph
- `ui/src/components/disambiguation-ui.tsx` -- Option cards
- `ui/src/lib/api.ts` -- New types and endpoints

### Phase 6: Evaluation & Polish

- Catalog evaluation integration into dashboard
- End-to-end testing with sample dataset
- Press release and RFP updates
- Verify non-blocking flow: discovery -> explore immediately (no gates)

---

## 13. Verification Plan

1. **Unit tests**: All existing 99 tests pass + new test files
2. **Catalog test**: Discover sample data -> catalog has zone_geography with "county"
   in synonyms, correct metrics for all entities
3. **Decomposition test**: "complaints by county" -> resolves to zone_geography,
   generates correct JOIN SQL
4. **Disambiguation test**: "complaints by area" -> returns options with zone_geography
   recommended
5. **NULL handling test**: "resolution time by zone" -> SQL includes WHERE NOT NULL,
   warning about 45% NULL rate
6. **Graph test**: Kuzu identifies zones as conformed dimension across 4 tables
7. **Non-blocking test**: Discovery completes -> explorer works immediately without
   any dictionary review. Low-confidence answers show warnings, not errors.
8. **State propagation test**: Confirm a column in dictionary -> explorer results
   for that column show updated confidence. Dashboard progress increments.
9. **Tiered LLM test**: System works with provider=none (heuristic), produces
   valid catalog with confidence 0.4-0.6. Switch to ollama -> confidence improves.
10. **End-to-end**: Full pipeline run -> dashboard shows project with maturity tracking ->
    dictionary shows rich descriptions + catalog -> explorer answers "complaints by county"
11. **Lint**: `cd headwater && uv run ruff check .` clean
12. **Format**: `cd headwater && uv run ruff format .` clean
