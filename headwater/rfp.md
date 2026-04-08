# Headwater: Project Proposal & Technical RFP

**"Where your data story begins."**

| | |
|---|---|
| **Product** | Headwater -- Advisory Data Platform |
| **Version** | 1.0 (Proposal) |
| **Date** | April 2026 |
| **License** | Apache 2.0 |
| **Status** | Pre-release / Seeking community contributors |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement & Market Opportunity](#2-problem-statement--market-opportunity)
3. [Product Vision](#3-product-vision)
4. [Architecture](#4-architecture)
5. [Competitive Positioning](#5-competitive-positioning)
6. [The Semantic Layer Question](#6-the-semantic-layer-question)
7. [Technology Stack](#7-technology-stack)
8. [Roadmap & Timeline](#8-roadmap--timeline)
9. [Go-to-Market Strategy](#9-go-to-market-strategy)
10. [Investment & Budget](#10-investment--budget)
11. [Risk Assessment](#11-risk-assessment)
12. [OSS Governance & Community](#12-oss-governance--community)
13. [Appendices](#13-appendices)

---

## 1. Executive Summary

Data professionals are navigating a paradox. AI-powered tooling promises to automate everything, yet over 50% of a data engineer's time still goes to maintenance -- keeping pipelines running, chasing schema changes, writing documentation nobody reads, and answering ad-hoc questions that interrupt deep work. The gap is not a lack of tools. It is a lack of tools that are reliable, transparent, and trustworthy enough to actually use. Data engineers have seen the hallucinations. They have tried the "magic" demos that break on real data. What they need is a tool that earns their confidence incrementally -- proving its value through measurable accuracy, not marketing claims.

**Headwater** is an open-source advisory companion for data professionals. It handles the repetitive groundwork -- discovery, profiling, documentation, quality baselines, boilerplate model generation -- so that data engineers, analytics engineers, and analysts can focus on the work that actually requires their expertise: business logic, data modeling decisions, stakeholder collaboration, and architecture.

Headwater does not replace data professionals. It makes them more productive by eliminating mechanical toil. Connect it to any data source -- an existing data catalog (AWS Glue, Databricks Unity Catalog, Iceberg REST), a production OLTP database, or flat files -- and Headwater discovers, profiles, documents, and proposes quality contracts and analytical models for your review. You bring the domain knowledge and judgment; Headwater brings the scaffolding and the ongoing monitoring.

Critically, Headwater **tracks its own accuracy over time**. Every suggestion it makes -- descriptions, quality contracts, model proposals -- is evaluated against your approvals, edits, and rejections. Acceptance rates, false-positive rates on quality alerts, and edit-distance on model proposals are tracked and surfaced. This gives data teams a quantitative basis for trusting (or not trusting) Headwater's suggestions, rather than asking them to take it on faith.

**Why now**: Three shifts converged. LLMs can infer semantic meaning from schema and data patterns with useful accuracy -- but only when used with hardened context and human review, not as autonomous agents. DuckDB made embedded analytical processing viable on a laptop. And the industry consensus shifted from "best of breed" to "fewer tools, less glue." Headwater is built for this moment.

**Key numbers:**
- First discovery report: minutes (catalog-connected) to under an hour (raw OLTP source, 50+ tables)
- Full model review and refinement: days (depends on data engineer's review pace)
- Cost: $0 (open-source, Apache 2.0)
- LLM cost per analysis run: ~$2-5 (optional; no-LLM mode is first-class)
- Target: data professionals in lean teams (1-10 data people, 5-200 source tables)
- Deployment: single `docker-compose up` command

---

## 2. Problem Statement & Market Opportunity

### 2.1 The Three Problem Clusters

Headwater targets three interconnected problems that steal time from data professionals. Solving them together creates compound value that solving any one alone cannot.

#### Problem Cluster 1: The Maintenance Trap and the Trust Gap

Data professionals spend more than half their time keeping things running rather than building new capabilities. Documentation rots because maintaining it is manual, tedious, and unrewarded. When a team member leaves or rotates, the next person spends weeks reverse-engineering what exists. AI-powered tools promise to help -- but data engineers have learned to be skeptical. LLM-generated outputs hallucinate. "Auto-magic" demos break on real data. The result: data professionals want reliable, transparent tools that prove their value, not another promise.

| Evidence | Source |
|----------|--------|
| 50%+ of data engineering time spent on maintenance, not building | Ascend.io DataAware Survey |
| Bus factor is existential -- one departure breaks production | MindCTO, CloseLoop |
| Data teams report increasing skepticism toward AI-generated outputs after hallucination incidents | Industry surveys 2025-2026 |

**What Headwater does**: Handles the mechanical documentation and discovery work that nobody has time for -- but does so transparently and with measurable accountability. Every discovery run generates contextual documentation. Not boilerplate -- semantic documentation that explains what a table represents, why a column exists, how a metric is calculated, and what upstream systems feed it. When a data engineer modifies or rejects an auto-generated proposal, Headwater records the change, the reason, and updates its own accuracy metrics. Over weeks and months, the data team can see: "Headwater's column descriptions were accepted without edit 78% of the time. Quality contract false-positive rate is 6%." This is how trust is built -- through tracked, verifiable performance, not through claims.

#### Problem Cluster 2: The Trust Deficit

67% of organizations do not trust their data. Quality incidents are rising. Business users find problems before data teams 74% of the time. Data engineers end up firefighting quality issues reactively instead of building proactive guardrails -- because writing and maintaining quality rules manually across dozens of tables is unsustainable for a lean team.

| Evidence | Source |
|----------|--------|
| Quality issue resolution time up 166% | Monte Carlo State of Data Quality |
| 74% of issues found by business users first | Monte Carlo State of Data Quality |
| Self-service analytics below 20% adoption | Observable, Yellowfin |

**What Headwater does**: Generates data quality contract drafts from statistical profiling, giving data engineers a starting point instead of a blank page. Contracts enter observation mode first -- Headwater tracks violations without enforcing, so the data engineer can validate that rules match reality before turning them on. This turns weeks of manual rule-writing into hours of review and refinement.

#### Problem Cluster 3: The Ad-Hoc Tax

Data teams become help desks. Business users cannot self-serve because documentation is sparse, terminology is inconsistent, and nobody knows which table to query. Every "quick question" from a stakeholder interrupts the data engineer's deep work. The irony: the data professional built the pipeline but has no time to improve it because they are busy answering questions about it.

| Evidence | Source |
|----------|--------|
| 5 out of 100 intended users actually engage with BI tools | Observable |
| Year 1 actual costs exceed projections by 60% | Capella Solutions TCO Study |
| A "free" Airflow installation costs $400K/yr in engineering time | Capella Solutions TCO Study |

**What Headwater does**: Auto-generated documentation and semantic understanding make data self-describing. Stakeholders can look up what tables exist, what columns mean, and how metrics are calculated -- without interrupting the data team. The AI assistant (Phase 4) uses full metadata context to answer questions in natural language, deflecting routine queries away from the data engineer's inbox.

#### The Compound Problem

These three clusters reinforce each other. No time for documentation leads to no trust. No trust leads to low adoption. Low adoption means more ad-hoc requests, which means even less time for documentation. Headwater breaks this cycle by handling the mechanical documentation and quality monitoring that data professionals recognize as important but never have time to do properly.

### 2.2 Market Sizing

**Primary users**: Data engineers, analytics engineers, and data analysts in lean teams (1-10 data professionals) at companies with 10-2,000 employees and 5-200 structured data tables.

**Why lean teams**: These professionals are stretched across discovery, modeling, documentation, quality, and ad-hoc support. They have the skills to build and maintain a data platform, but not the bandwidth. Headwater is not a substitute for their expertise -- it is a force multiplier that handles the mechanical work so they can apply their judgment where it matters most.

| Maturity Stage | Who Uses Headwater | How They Connect | How They Use It |
|-------|-------------|-----------|---------------|
| Stage 1: Raw Queries | Backend engineer wearing a data hat | Direct source connection (Postgres, MySQL) | Bootstraps discovery, documentation, and basic models from scratch. Gets to "data platform" without starting from zero. |
| Stage 2: Basic Reporting | Solo data engineer or analyst | Direct source + file-based (CSV, Parquet, JSON) | Accelerates documentation, automates quality baselines, generates staging boilerplate. Frees time for business logic and stakeholder work. |
| Stage 3: Data Platform | Data team of 3-10 | Existing catalog (Glue, Unity Catalog, Iceberg REST, Hive Metastore) | Audits existing pipelines. Fills documentation gaps. Generates quality contracts for tables that never got them. Onboarding tool for new team members. |

**Market size**: According to the U.S. Census Bureau, there are approximately 650,000 companies in the 10-499 employee range and ~120,000 in the 500-1,999 range. Conservatively, 25-35% have data professionals who would benefit from Headwater. That is 190,000-270,000 potential adopting organizations in the U.S. alone.

**Serviceable addressable market (Year 1)**: Of the 190K-270K TAM, approximately 5,000-15,000 organizations are reachable through developer community channels (Hacker News, dbt Slack, DuckDB Discord, data engineering subreddits, conference talks). These are teams where at least one person can run `docker-compose up` and has the authority to try a new tool.

**What these teams currently do**: Spend 50%+ of their time on maintenance tasks that Headwater can handle, or skip documentation and quality monitoring entirely because there is no bandwidth.

### 2.3 Timing Argument: Why Now

Three shifts converged in 2024-2026 that make Headwater feasible today:

1. **LLMs can infer semantic meaning from schema + data patterns.** Column names, data distributions, distinct value sets, and table relationships give LLMs enough signal to generate meaningful descriptions, detect entity types, and infer business domains. This was not possible with pre-LLM NLP.

2. **DuckDB made embedded analytical processing viable.** DuckDB processes millions of rows on a laptop. A data engineer can prototype, profile, and test locally without provisioning cloud infrastructure. This is not about avoiding cloud warehouses -- it is about shortening the feedback loop from discovery to insight.

3. **The consolidation moment is here.** Data professionals are fatigued by 10-15 tool stacks with constant integration overhead. The industry narrative shifted from "best of breed" to "fewer tools, less glue" (Nexla, Definite, Knowi surveys 2025-2026). Data teams want tools that reduce their tool surface area, not add to it.

---

## 3. Product Vision

### 3.1 Core Principle: Advisory, Not Autonomous

Headwater proposes. Humans approve. This is the foundational design principle, not a limitation.

The automation boundary is explicit and immovable:

| Layer | Automation Level | Examples | Rationale |
|-------|-----------------|----------|-----------|
| **Mechanical** | Fully automated | Schema discovery, statistical profiling, staging model generation, data quarantine for clear constraint violations, safe additive schema changes | 100% deterministic outcomes. No business context required. |
| **Semantic** | Advisory (auto-draft, human-apply) | Mart model proposals, data contract proposals, schema drift resolution, domain classification, relationship inference | Requires business context that only a human can validate. |
| **Strategic** | Human-only | Business metric definitions, access policies, data retention rules, compliance decisions | Organizational decisions that should never be delegated to software. |

There is no "auto-apply" mode for semantic-layer changes. Not as a paid feature. Not as a power-user toggle. The system drafts; the human decides. This is a safety boundary, not a convenience trade-off.

### 3.2 Four Core Capabilities

**Capability 1: Universal Discovery & Documentation**

Headwater connects to your data wherever it lives:

| Connection Type | Sources | Discovery Speed |
|----------------|---------|----------------|
| **Existing catalogs** | AWS Glue Catalog, Databricks Unity Catalog, Iceberg REST Catalog, Hive Metastore | Minutes. Metadata is pre-organized; Headwater enriches it with profiling and semantic analysis. |
| **OLTP databases** | Postgres, MySQL, SQL Server, SQLite | 5-15 minutes for <50 tables. 15-45 minutes for 50-200 tables. Depends on profiling depth and network latency. |
| **File-based sources** | CSV, Parquet, JSON/NDJSON, Iceberg tables on S3/GCS/ADLS | Minutes to tens of minutes depending on file count and size. |
| **Data warehouses** | Snowflake, BigQuery, DuckDB (Phase 5) | Minutes for metadata. Profiling time depends on warehouse query speed. |

For partly mature organizations with an existing catalog, Headwater reads the catalog metadata and enriches it -- adding statistical profiles, quality observations, relationship detection, and semantic descriptions that the catalog does not provide. For organizations without analytics infrastructure, Headwater connects directly to production sources and bootstraps the entire discovery from scratch.

Discovery produces: schema maps, column profiles, relationship graphs, domain classifications, quality observations, and contextual documentation. Subsequent runs detect changes and update descriptions, flagging what is new, what changed, and what disappeared.

**Capability 2: Quality Contracts**

Statistical profiling generates typed quality contracts: not-null expectations, value ranges, cardinality bounds, uniqueness constraints, freshness thresholds, and referential integrity checks. Contracts follow a lifecycle: **proposed** (Headwater suggests) -> **observing** (tracking violations without enforcing) -> **enforced** (violations trigger alerts) -> **amended** (user adjusts thresholds based on observed patterns). No contract skips the observation phase.

**Capability 3: Advisory Model Generation**

Headwater proposes SQL models in two tiers:

- **Staging models**: Mechanical transformations (renaming, type casting, deduplication, null handling). Auto-approved because they encode no business logic.
- **Mart models**: Business logic (revenue calculations, user segmentation, funnel metrics). Presented one at a time with the SQL, the assumptions made, and clarifying questions (e.g., "Is revenue recognized on invoice date or payment date?"). Never batch-approved.

**Capability 4: Confidence Tracking**

Every suggestion Headwater makes is tracked against the data professional's response:

| Metric | What It Measures | How It Builds Trust |
|--------|-----------------|-------------------|
| **Description acceptance rate** | % of auto-generated column/table descriptions accepted without edit | Shows whether Headwater "understands" your data. Starts low, improves as semantic locks accumulate. |
| **Quality contract precision** | % of contract alerts that are true issues (not false positives) | A 95% precision means the data engineer can trust alerts. A 60% precision means the thresholds need tuning. |
| **Model edit distance** | How much the data engineer modifies proposed mart models before approving | Low edit distance = Headwater's proposals are close to what the engineer would write. High = scaffolding is useful but needs significant rework. |
| **Drift detection lead time** | How far ahead of business-user complaints does Headwater catch schema/quality issues | Measures whether Headwater is actually preventing firefighting. |

These metrics are surfaced in the Headwater dashboard. Over weeks and months, the data team builds a quantitative picture of how much to trust Headwater's output -- and Headwater's suggestions improve as it learns from the corrections.

### 3.3 The Flywheel

```
Discovery generates metadata
       |
       v
Metadata enables better quality contracts
       |
       v
Quality contracts build trust in the data
       |
       v
Trust increases usage
       |
       v
Usage patterns feed back into metadata (what is queried, how often, by whom)
       |
       v
Richer metadata improves the next discovery cycle
       |
       +-----> FLYWHEEL
```

No existing tool creates this flywheel because no tool spans all three layers. dbt understands transformations but not usage. BI tools understand dashboards but not data quality. Catalogs understand metadata but not business questions. Headwater's value is the unified understanding across all layers.

### 3.4 Who Is Headwater For

Headwater is designed for data professionals across roles and experience levels. The value it provides changes with the role.

#### By Role

| Role | How Headwater Helps | Example |
|------|-------------------|---------|
| **Data Engineer (IC)** | Eliminates discovery, documentation, and quality-rule boilerplate. Detects schema drift before it breaks pipelines. Tracks change history for debugging. | "I ran Headwater against our 80-table Postgres. It found 6 referential integrity gaps I didn't know about and generated quality contracts I would have taken two weeks to write manually." |
| **Analytics Engineer** | Scaffolds staging and mart models with clarifying questions. Exports to dbt format. Maps upstream dependencies for impact analysis. | "I inherited a 150-table warehouse with no documentation. Headwater mapped the relationships and generated descriptions in an afternoon. I spent the week refining models instead of reverse-engineering." |
| **Data Analyst** | Self-serve data discovery. Understands what tables exist, what columns mean, and where quality issues lurk -- without interrupting the data engineer. | "I used to Slack the data team every time I needed a new metric. Now I search Headwater's documentation first. I still ask questions, but they're better questions." |
| **Data Platform Engineer / Lead** | Audits existing infrastructure. Identifies documentation gaps, undocumented relationships, and tables with no quality coverage. Onboarding tool for new hires. | "We connected Headwater to our Unity Catalog. It found 40% of tables had no descriptions and 12 cross-schema relationships that weren't in our lineage tool." |
| **Engineering Manager / Data Lead** | Monitors team's data estate health over time. Uses confidence metrics to evaluate tooling ROI. Decision log provides audit trail for compliance conversations. | "I show the Headwater confidence dashboard in our monthly review. Description acceptance rate went from 65% to 84% in three months -- the team trusts it now." |
| **Backend / Product Engineer** | Bootstraps basic analytics from a production database without becoming a full-time data engineer. Gets the foundation; adds business logic themselves. | "I needed revenue reporting for our Series A board deck. Headwater proposed a mart model with the right joins. I just had to answer its questions about how we recognize revenue." |

#### By Organization Size

| Scenario | Team | Tables | How Headwater Fits | Realistic Timeline |
|----------|------|--------|-------------------|-------------------|
| **Series A startup** | 1 engineer wearing a data hat | 20-40 | Bootstraps from zero. Generates first documentation, quality contracts, and staging models. | Day 1: discovery report. Week 1: reviewed and running models. |
| **Growth-stage company** | 2-3 data professionals | 50-150 | Fills documentation gaps, automates quality baselines, scaffolds new models. Complements existing dbt or manual SQL. | Day 1: discovery. Days 2-5: contract and model review. Week 2: operational. |
| **Mid-market with existing catalog** | 5-10 data team | 100-500 | Connects to Glue/Unity/Iceberg catalog. Enriches existing metadata with profiling and quality. Audit tool for coverage gaps. | Day 1: catalog-enriched discovery. Week 1-2: quality contract review. Ongoing: drift monitoring. |

### 3.5 What Headwater Is NOT

Being explicit about scope prevents overcommitment and builds credibility.

- **Not a replacement for data engineers.** Headwater is a tool in the data professional's kit, like dbt or Great Expectations. It handles mechanical work so the human can focus on judgment, business logic, and stakeholder collaboration. Every proposal requires human review. The data engineer's expertise is what makes the output valuable.
- **Not a data warehouse.** Headwater uses DuckDB as an embedded analytical engine. It does not replace Snowflake, BigQuery, or Databricks for teams that need them. It works alongside existing warehouses.
- **Not enterprise governance.** Headwater does not provide HIPAA/SOX/GDPR compliance frameworks, audit certification, or regulatory reporting. Use Collibra or Alation for that.
- **Not a BI tool.** Headwater does not build dashboards or visualizations. It produces the clean, documented, quality-monitored analytical layer that BI tools consume.
- **Not magic.** Auto-generated models are a starting point -- a first draft, not a finished product. The LLM will guess wrong sometimes. The review workflow exists because data professionals' domain knowledge and judgment cannot be automated. Headwater publishes its own accuracy metrics so you can see exactly how often it gets things right.
- **Not for real-time.** Headwater operates on batch and micro-batch cadences. Sub-second streaming analytics is out of scope.
- **Not prescriptive.** Headwater does not force a methodology or opinionated workflow. It integrates with your existing stack (exports to dbt, works with any warehouse) and adapts to how your team already works.

### 3.6 User Journey: Realistic Timelines

#### Scenario A: Partly Mature Org (existing Glue Catalog, 120 tables)

**Hour 1: Connect to catalog and discover**
```bash
docker-compose up
headwater discover --catalog glue --region us-east-1 --database analytics_prod
```
```
Connected to AWS Glue Catalog (us-east-1)
Reading catalog metadata... 120 tables across 4 databases
Enriching with statistical profiles... (sampling 10K rows per table via Athena)

Phase 1 complete (catalog metadata): 3 minutes
Phase 2 complete (statistical profiling): 28 minutes
Phase 3 complete (semantic analysis): 12 minutes

=== Discovery Report ===
Existing catalog coverage: 120 tables cataloged, 34 have descriptions (28%)
Headwater enrichment: +86 table descriptions, +1,840 column profiles
Detected 42 cross-table relationships (14 not in catalog)
Quality observations: 23 warnings, 8 info
```

**Value on Day 1**: The data engineer sees the full picture -- including the 72% of tables that had no descriptions in Glue, the 14 relationships the catalog missed, and 23 quality concerns nobody knew about. This is a map of where to focus, not a finished product.

**Days 2-5: Review quality contracts and models at your pace.** No rush. Headwater's proposals wait until you are ready.

#### Scenario B: No Analytics Infrastructure (production Postgres, 47 tables)

**Hour 1: Connect and discover**
```bash
docker-compose up
headwater discover --source postgres://prod-db:5432/myapp
```
```
Connecting to prod-db...
Discovered 47 tables across 3 schemas (public, analytics, billing)
Profiling data... (sampling 10K rows per table)

Phase 1 complete (schema discovery): 1 minute
Phase 2 complete (statistical profiling): 8 minutes
Phase 3 complete (semantic analysis): 6 minutes

=== Discovery Report ===
Detected Domains:
  - Users & Accounts (12 tables, ~2.3M rows)
  - Orders & Payments (8 tables, ~890K rows)
  - Products & Inventory (6 tables, ~45K rows)
  - Event Tracking (4 tables, ~12M rows)
  - Internal/System (17 tables -- excluded from modeling)

Detected Relationships:
  - users.id -> orders.user_id (1:many, 94% referential integrity)
  - orders.id -> payments.order_id (1:many, 99.8% integrity)
  - [14 more relationships detected]

Data Quality Observations:
  - orders.discount_amount: 23% null (expected for non-discounted orders?)
  - users.phone: 67% null (optional field?)
  - payments.amount: 3 negative values found (refunds or errors?)

Proposed Models: 12 staging (auto-approved), 5 mart (awaiting review)
168 quality contracts generated (observation mode)
```

**Value on Day 1**: The data engineer gets a complete map of the data estate with quality flags and relationship detection. This is the starting point for their real work: deciding what to model, what to fix, and what to prioritize.

**Days 2-5**: The data engineer reviews mart models at their own pace. Each model is a conversation, not a deadline.

#### Both Scenarios: Week 2 -- Review and Refine

The data engineer opens the review UI. Headwater presents each proposed mart model individually -- not as a finished product, but as a conversation starter:

```
=== Proposed Model: mart_revenue ===

Description:
  Monthly revenue by product category and customer segment.

Assumptions Made:
  1. Revenue = payment amount (not order amount)
  2. Refunds are negative payments
  3. Customer segment comes from users.plan_type

Questions for You:
  - Is revenue recognized on invoice creation or payment date?
  - Should cancelled orders with partial payments be included?
  - Is users.plan_type the correct segmentation field?

[Approve] [Edit] [Reject] [Skip for now]
```

**Month 1: The Data Engineer Gets Proactive Alerts Instead of Reactive Firefighting**

Schema drift detection is active. When the source database adds a column, renames a field, or changes a type, Headwater detects it, drafts a resolution, and alerts the data engineer before business users notice:

```
Schema Change Detected: orders table
  - Column added: orders.shipping_tier (varchar, 12% null)
  - Impact: affects staging_orders, mart_revenue

Proposed Resolution:
  - Add shipping_tier to staging_orders (auto-approved, additive change)
  - No mart changes proposed (shipping_tier not referenced in existing marts)
  - Quality contract proposed: shipping_tier NOT NULL rate > 85%

[Review Resolution]
```

---

## 4. Architecture

### 4.1 High-Level Architecture

```
+------------------+     +------------------+     +------------------+
|  Data Sources    |     |  Headwater Core  |     |  Outputs         |
|                  |     |                  |     |                  |
|  - Postgres      | --> |  Discovery       | --> |  Discovery Report|
|  - MySQL         |     |  Engine          |     |  Documentation   |
|  - SQLite        |     |    |             |     |  Quality Alerts  |
|  - CSV/Parquet   |     |    v             |     |  SQL Models      |
|  - Iceberg       |     |  Profiler        |     |  Data Contracts  |
|                  |     |  (Polars/DuckDB) |     |  Decision Log    |
+------------------+     |    |             |     +------------------+
                          |    v             |
                          |  Semantic        |            +----------+
                          |  Analyzer        |            |          |
                          |  (LLM plugin)    |            | Review   |
                          |    |             |  <-------> | UI       |
                          |    v             |            | (Next.js)|
                          |  Model           |            |          |
                          |  Generator       |            +----------+
                          |    |             |
                          |    v             |
                          |  Quality         |
                          |  Engine          |
                          |    |             |
                          |    v             |
                          |  +------------+  |
                          |  | Metadata   |  |
                          |  | Store      |  |
                          |  | (SQLite /  |  |
                          |  |  Postgres) |  |
                          |  +------------+  |
                          |  +------------+  |
                          |  | Analytical |  |
                          |  | Engine     |  |
                          |  | (DuckDB)   |  |
                          |  +------------+  |
                          +------------------+
```

### 4.2 Arrow-Native Data Flow

Every component in Headwater's data path speaks Apache Arrow natively. Data flows from source to analytics without serialization overhead:

```
Source DB --> Connector (Arrow RecordBatch)
                |
                v  (zero-copy)
         Polars DataFrame (profiling, simple aggregations)
                |
                v  (zero-copy via Arrow)
         DuckDB (complex queries, model materialization)
                |
                v
         Analytical tables (DuckDB file or external warehouse)
```

**Why this matters**: Serialization (converting data between formats) is the hidden tax in most data tools. A typical pipeline that moves data from Postgres through Python (Pandas) to a warehouse serializes the data 4-6 times. Headwater serializes once (at the source connector) and operates on Arrow memory throughout. This makes profiling a 10M-row table on a laptop practical rather than theoretical.

### 4.3 Metadata Architecture

The metadata store is the heart of Headwater. It records everything the platform has learned: table structures, column profiles, relationships, semantic classifications, quality contracts, model definitions, change history, and decision log entries.

**POC**: SQLite. Handles the light read/write concurrency of a single-user demo without the single-writer contention that DuckDB imposes. Zero configuration.

**Phase 1+**: Postgres. Any managed instance ($5-15/month on Railway, Neon, or Supabase). Required when multiple processes (background profiler, web UI, CLI) access metadata concurrently.

**DuckDB is never used for metadata.** DuckDB's single-writer lock means that a background pipeline writing analytical data would block the web UI from updating a contract or recording a decision. This architectural split is non-negotiable.

### 4.4 LLM Integration

LLM is a **plugin**, not a dependency. Headwater has three operating tiers:

| Tier | LLM | Capabilities | Cost |
|------|-----|-------------|------|
| **Tier 1: No LLM** | None | Schema discovery, statistical profiling, heuristic descriptions (inferred from column names + data patterns), rule-based contracts, staging model generation | $0 |
| **Tier 2: Local LLM** | Ollama (Llama, Mistral, etc.) | Better descriptions, basic domain classification, simple relationship inference | $0 (hardware cost only) |
| **Tier 3: Cloud LLM** | Claude API (Anthropic) | Best semantic analysis, nuanced relationship inference, mart model proposals with clarifying questions, natural language assistant | ~$2-5 per full discovery run |

**Tier 1 is not a degraded fallback.** It is a genuinely useful experience. A data engineer can run Headwater with zero LLM interaction and still get: complete schema maps, statistical profiles, relationship detection via foreign key and naming heuristics, auto-generated quality contracts, and staging models. The LLM adds richness; it does not provide the foundation. Many data engineers may prefer Tier 1 for security-sensitive environments or simply because they trust their own judgment over LLM suggestions.

#### LLM Context Hardening

LLM prompts never receive raw sample rows. Instead, the profiler sends:

- **Top-N distinct values** (excluding nulls, empty strings, and test data patterns)
- **Statistical signatures**: mean, median, standard deviation, distribution shape, cardinality
- **Column name and type**
- **Table context**: other column names in the table, detected relationships

This prevents two failure modes:
1. **Hallucination on garbage data**: Raw rows frequently contain NULLs, test emails, or placeholder values that mislead the LLM.
2. **PII leakage**: Distinct value aggregates are far less likely to contain personally identifiable information than raw rows.

#### Semantic Locking

Once a column or domain is classified (either by the LLM or by a human edit), that classification is **locked**. Subsequent discovery runs include the locked classification in the LLM prompt as ground truth. The LLM cannot reclassify without an explicit user-triggered "re-analyze" action.

This prevents stochastic semantic drift -- the problem where Monday's discovery says "Billing domain" and Friday's says "Operations domain" for the same tables, breaking downstream queries and reports that depend on consistent domain mapping.

### 4.5 Defensive Architecture

Headwater is designed to fail safely. Three mechanisms prevent cascading damage:

#### Circuit Breakers

| Trigger | Action |
|---------|--------|
| Incoming row volume shifts by >200% compared to baseline | Halt ingestion. Alert user. Do not profile or model anomalous data. |
| Average string column size bloats beyond 10x baseline | Halt ingestion. Possible garbage dump or encoding change. |
| More than 50% of quality contracts fail simultaneously | Freeze downstream model execution. Alert user. Likely a source-side incident, not individual data issues. |

#### Drift Budget

If a table exhibits schema drift more than 3 times in a 24-hour window, the drift detector disengages for that table and flags it for manual investigation. This prevents infinite detect-fix-detect loops caused by flapping upstream systems (e.g., a buggy ELT script toggling a column type between INT and STRING).

#### Tiered Approval

Review fatigue is a real risk. When Headwater presents 15 mart models for approval, time-starved teams will click "Approve All" and blame Headwater when an LLM-generated model produces wrong numbers.

Headwater's approval tiers prevent this:

| Artifact | Approval Mode | Rationale |
|----------|--------------|-----------|
| Staging models | Auto-approved | Mechanical transformations. No business logic. Safe. |
| Quality contracts | Observation mode first | Track violations silently for 1-2 weeks before enforcing. Builds confidence that rules match reality. |
| Mart models | Individual review with questions | Presented one at a time. Each model includes the SQL, the assumptions, and clarifying questions. No batch approval. |
| Schema drift resolutions | Individual review with diff view | Shows exactly what changed, what is affected, and the proposed fix. |

### 4.6 Deployment Models

**Demo / Local (single machine)**
```bash
docker-compose up
# 2 containers: Headwater core (FastAPI + embedded DuckDB) + Next.js UI
# Metadata in SQLite (embedded)
# No cloud accounts. No API keys (unless LLM Tier 3 desired).
```

**Production (small team)**
```bash
docker-compose -f docker-compose.prod.yml up
# 3 containers: Headwater core + Next.js UI + Postgres
# Metadata in Postgres
# DuckDB for analytical data (or connect to external warehouse)
```

**Kubernetes**
```bash
helm install headwater ./charts/headwater
# Helm chart with sensible defaults
# Requires: Postgres instance, persistent volume for DuckDB
# Optional: LLM API key, external warehouse credentials
```

Headwater does not ship cloud-specific deployment templates (Terraform, CloudFormation, Pulumi). The core team maintains the Docker image and Helm chart. Community-contributed deployment modules for specific cloud providers are welcome as separate repositories.

### 4.7 Plugin Architecture

Headwater uses Python `entry_points` for plugin discovery. Seven plugin types:

| Plugin Type | Purpose | Examples |
|-------------|---------|----------|
| **Source Connector** | Read from data sources | `headwater-connector-postgres`, `headwater-connector-mysql`, `headwater-connector-iceberg` |
| **Warehouse Backend** | Write analytical data | `headwater-warehouse-duckdb`, `headwater-warehouse-snowflake` |
| **LLM Provider** | Semantic analysis | `headwater-llm-anthropic`, `headwater-llm-ollama`, `headwater-llm-openai` |
| **Notifier** | Alert delivery | `headwater-notify-slack`, `headwater-notify-email` |
| **Model Template** | Industry-specific model patterns | `headwater-template-saas`, `headwater-template-ecommerce` |
| **Exporter** | Output to other tools | `headwater-export-dbt`, `headwater-export-sqlmesh` |
| **Cloud Deployer** | Deployment automation | Community-maintained: `headwater-deploy-aws`, etc. |

Plugins are installed via pip:
```bash
pip install headwater-connector-snowflake
# Headwater auto-discovers the plugin at next startup
```

Scaffold a new plugin:
```bash
headwater plugin scaffold --type connector --name bigquery
# Generates: skeleton code, tests, pyproject.toml, README
```

### 4.8 Privacy, Security & Data Protection

Data professionals evaluating AI-powered tools have a legitimate concern: where does my data go, and how do I know it is not leaking sensitive information? Headwater addresses this with defense-in-depth -- multiple independent layers of protection, any one of which is sufficient to prevent data leakage.

#### Three Operating Modes

| Mode | Network Calls | Data Exposure | Best For |
|------|--------------|--------------|----------|
| **Air-Gapped** | None. Zero network calls. | No data leaves the machine, ever. | Classified environments, regulated industries (healthcare, finance), organizations with strict data residency requirements. All core features work: discovery, profiling, heuristic descriptions, quality contracts, staging models. |
| **Local LLM** | None external. Ollama runs on the same machine or local network. | Data stays within the network boundary. LLM inference happens on hardware you control. | Organizations that want semantic analysis without any cloud dependency. Moderate hardware required (16GB+ RAM for 7B parameter models). |
| **Cloud LLM** | Stateless API calls to Anthropic (Claude). No training, no data storage on provider side. | Only sanitized statistical summaries and schema metadata leave the machine. Never raw rows. Never PII. | Organizations comfortable with cloud APIs under strict data controls. Best semantic analysis quality. |

#### Smart Techniques for Data Protection

**1. Schema-Only Mode (default for initial discovery)**

Before any data is profiled, Headwater runs a schema-only pass: column names, data types, constraints, foreign keys, and table relationships. This metadata alone -- with zero data values -- is sufficient for:
- Relationship detection via naming heuristics and FK constraints
- Domain classification (a table with `user_id`, `email`, `created_at` columns is clearly a Users domain)
- Staging model generation (type casting, renaming, deduplication logic)

The data engineer can review the schema-only discovery report before opting into profiling and LLM analysis.

**2. Statistical Summaries Instead of Raw Data**

When LLM analysis is enabled, Headwater never sends raw rows. Instead, the profiler computes and sends:
- **Top-N distinct non-null values** (N=20, configurable) with frequencies
- **Statistical signatures**: count, null rate, cardinality, min/max, mean, median, standard deviation, percentiles, distribution shape
- **Format patterns**: "90% match email pattern", "85% match UUID v4"
- **Column name, data type, and constraints**

These aggregates are far less likely to contain PII than raw rows. A column with 50,000 email addresses becomes: `{type: "varchar", cardinality: 48200, null_rate: 0.02, format: "email_pattern", top_values: ["example@domain.com patterns"]}`.

**3. PII Detection and Exclusion**

Before any data leaves the machine, the profiler scans for PII patterns:

| PII Type | Detection Method | Action |
|----------|-----------------|--------|
| Email addresses | Regex pattern matching | Flag + exclude from LLM context |
| Phone numbers | Regex + length heuristics | Flag + exclude |
| SSN / Tax IDs | Regex (9-digit patterns) | Flag + exclude |
| Credit card numbers | Luhn algorithm | Flag + exclude |
| Personal names | Column name heuristics + NER | Flag + exclude |
| IP addresses | Regex (IPv4, IPv6) | Flag + exclude |
| Free-text with embedded PII | Configurable regex + dictionary | Flag + exclude or redact |

PII-flagged columns are excluded from LLM context entirely. Statistical aggregates (null rate, cardinality, type distribution) are still computed locally and available in the discovery report.

**4. Prompt Sandboxing and Response Validation**

LLM responses are not applied blindly:
- Every LLM-generated description is validated against the metadata schema (must reference real columns, real tables)
- Generated SQL models are parsed and validated before being proposed (must be valid SQL, must reference only known tables/columns)
- Domain classifications are checked for consistency with existing locked classifications
- If an LLM response fails validation, it is discarded and the heuristic fallback is used

**5. Complete Audit Trail**

Every LLM interaction is logged locally:
- The exact prompt sent (so the data engineer can see exactly what data was shared)
- The response received
- Whether the response was applied, edited, or rejected
- Timestamp, LLM provider, model version

This audit trail is queryable: "Show me every prompt sent to Claude in the last 30 days" or "Show me what data from the billing schema was included in any LLM call."

**6. Data Residency**

All data stays in the user's infrastructure:
- Metadata store (SQLite/Postgres): runs on the user's machine or their Postgres instance
- Analytical data (DuckDB): local file on the user's machine
- LLM calls (when enabled): stateless API requests. Anthropic's API does not train on user data and does not store prompts beyond the request lifecycle (per their data policy). No data is retained on the provider side.

**Configuration via `headwater.yaml`**:
```yaml
privacy:
  mode: air_gapped          # air_gapped | local_llm | cloud_llm
  pii_detection: true
  pii_action: exclude        # exclude | redact | flag_only
  schema_only_first: true    # Run schema-only pass before profiling
  llm_data_sharing: stats    # stats | schema_only | none
  audit_log: true            # Log all LLM interactions
  sensitive_schemas:
    - billing
    - auth
    - users
  never_send_columns:
    - "*.ssn"
    - "*.credit_card"
    - "users.email"
```

---

## 5. Competitive Positioning

### 5.1 Feature Comparison

| Capability | Headwater | Atlan | Secoda | Monte Carlo | dbt + Great Expectations | Manual |
|-----------|-----------|-------|--------|-------------|--------------------------|--------|
| **Auto-discovery** | Yes (minutes) | Yes (hours to configure) | Yes | No (monitors, not discovers) | No | No |
| **Auto-documentation** | Yes (continuous) | AI-assisted (manual trigger) | AI-assisted | No | No | Manual |
| **Auto-quality contracts** | Yes (profile -> propose -> observe -> enforce) | No | No | ML anomaly detection | Manual rule authoring | Manual |
| **Advisory model generation** | Yes (staging + marts with review) | No | No | No | dbt Copilot (Enterprise only, requires existing project) | Manual |
| **Schema drift detection** | Yes (with impact analysis + proposed fix) | Limited | No | Yes | No | Manual |
| **Institutional memory** | Yes (decision log, change reasons) | Tags and wiki (manual) | AI-generated docs | No | No | Tribal knowledge |
| **Time to first value** | <10 minutes | Days to weeks | Hours | Days | Days (setup + rule authoring) | Weeks |
| **Best for** | Lean data teams (1-10 people) | Enterprise data teams (20+) | Growing data teams (5-20) | Enterprise with existing stack | Teams with dbt expertise | Any |
| **Cost** | $0 (OSS) | $25K-$100K+/yr | ~$400+/mo | $25K-$50K+/yr | $0 (OSS) but significant setup time | Fully manual effort |
| **Deployment** | Docker (minutes) | SaaS only | SaaS only | SaaS only | Self-hosted (complex) | N/A |
| **LLM dependency** | Optional (3 tiers) | Required for AI features | Required for AI features | N/A | N/A | N/A |

### 5.2 Honest Gaps

Credibility requires acknowledging what competitors do better:

- **Atlan** has mature enterprise governance, SSO/RBAC, compliance frameworks, and a large customer base. For companies that need SOC 2 compliance and role-based access control today, Atlan is the better choice.
- **Monte Carlo** has years of production-tested ML anomaly detection and deep integrations with every major warehouse. For companies that already have a data stack and need observability, Monte Carlo is more mature.
- **dbt** has the largest community in data transformation. The ecosystem of packages, the documentation, and the community support are unmatched. Headwater's model generation does not replace the dbt ecosystem; it generates models that can be exported to dbt format.
- **Secoda** is the closest in philosophy (AI-powered catalog for growing teams) and has a head start in market presence.

### 5.3 The Real Differentiators

No single product combines these four capabilities for data professionals in lean teams:

1. **Auto-generated, continuously maintained documentation from live data observation.** Catalogs are passive registries that rot because nobody has time to maintain them. Headwater generates documentation from schema + profiling + semantic analysis and keeps it current automatically. The data engineer reviews and refines instead of writing from scratch.

2. **Auto-generated quality contracts with an observation-first lifecycle.** Nobody auto-generates typed quality rules from statistical profiling, then runs them in observation mode before enforcement. Headwater gives data engineers a starting point for quality -- not a blank YAML file to fill in manually.

3. **Advisory model generation with structured human review.** No product takes a raw database and proposes staging + mart models with accompanying clarifying questions. The data engineer brings domain knowledge; Headwater brings the scaffolding.

4. **Zero-friction, zero-cost entry.** Enterprise tools start at $25K/year and take weeks to configure. Headwater is $0, runs with one command, and delivers value in the first 10 minutes. A data engineer can try it on their laptop during a lunch break and decide if it fits their workflow.

The moat is not any single capability. It is the integrated advisory workflow that respects the data professional's expertise while eliminating the mechanical toil that burns them out.

### 5.4 Defensibility

The honest answer: Headwater's technical moat is thin. Any well-funded team could replicate individual features (auto-discovery, LLM descriptions, quality contracts) in months. The defensibility is not in any single capability:

- **First-mover execution in an underserved segment.** Enterprise vendors (Atlan, Monte Carlo, Collibra) have no incentive to serve the sub-$25K market. Their sales motions, pricing, and feature sets are calibrated for large enterprises. Headwater's value is in the integrated workflow at a price point they will not match.
- **Compounding community contributions.** Connectors, model templates, and industry-specific patterns contributed by the community create a library that a closed-source competitor cannot replicate. Each contribution makes Headwater more useful for the next adopter.
- **Accumulated trust data.** Over time, Headwater's confidence tracking builds a dataset of what works (descriptions, contracts, model patterns) for specific data shapes and domains. This accumulated understanding -- which suggestions get accepted, which get edited, which get rejected -- is a form of institutional intelligence that cannot be easily copied.

### 5.5 Competitors to Watch

- **Secoda**: Closest in philosophy. If they add model generation and quality contracts, the differentiation narrows. Defense: execution speed, OSS positioning, and deeper integration with data engineering workflows.
- **Databricks Genie / Snowflake Cortex Analyst**: Deep AI integration with their respective platforms. For data teams already on those platforms, these are natural choices. Defense: Headwater is platform-agnostic and works for teams not locked into a single vendor.
- **dbt Copilot / dbt Analyst Agent**: Model generation and semantic layer within the dbt ecosystem. Requires Enterprise pricing and an existing dbt project. Defense: Headwater works as a complementary tool -- it can bootstrap the initial discovery and export to dbt format, giving data engineers a head start on their dbt project.

---

## 6. The Semantic Layer Question

The data industry is converging on the idea that a **semantic layer** -- a consistent mapping of business terms to SQL metric definitions -- is essential for trustworthy analytics and AI-powered data interaction. dbt Semantic Layer, Cube.dev, AtScale, and LookML all approach this differently. With the rise of AI agents and ML models that consume data, the semantic layer is increasingly positioned as the interface between human intent and data queries.

Headwater takes a deliberate, neutral position on this.

### 6.1 The Case For a Semantic Layer

| Argument | Why It Matters |
|----------|---------------|
| **Metric consistency** | "Revenue" means one thing, defined once, queried the same way everywhere. Eliminates the "three different MRR numbers" problem. |
| **AI/ML readiness** | LLM-based assistants (Databricks Genie, Snowflake Cortex Analyst, custom NL-to-SQL) need consistent metric definitions to generate accurate queries. Without a semantic layer, every AI query is a guess. |
| **Self-service enablement** | Business users can query by business term ("revenue by region") without knowing which tables or joins are involved. |
| **Governance** | Metric definitions are versioned, auditable, and centrally managed. Changes propagate everywhere. |

### 6.2 The Case Against (or for caution)

| Argument | Why It Matters |
|----------|---------------|
| **Added complexity** | A semantic layer is another abstraction to build, maintain, debug, and explain. For a team of 1-3, this can increase the maintenance burden rather than reduce it. |
| **Premature for early-stage teams** | If you have 10 tables and 3 metrics, a semantic layer is over-engineering. A well-documented mart model serves the same purpose with less overhead. |
| **Competing standards** | dbt Metrics, Cube.dev, AtScale, LookML, custom -- the industry has not converged on a single standard. Betting on the wrong one creates migration debt. |
| **AI can work without it** | With enough metadata context (table descriptions, column semantics, quality flags, relationship maps), an AI assistant can generate accurate SQL without a formal semantic layer. The metadata IS the semantic context. |

### 6.3 Headwater's Position: Semantic Metadata, Not a Semantic Runtime

Headwater generates rich semantic metadata as part of its core workflow:
- **Business term -> column mapping**: "MRR" maps to `mart_revenue.net_revenue` filtered by `revenue_month`
- **Metric definitions**: SQL expressions with descriptions, assumptions, and change history
- **Domain classifications**: Tables grouped by business domain with consistent terminology
- **Relationship semantics**: Not just "users.id -> orders.user_id" but "A user places many orders"

This metadata serves as a **lightweight semantic registry** -- the essential ingredient for any semantic layer, without forcing a specific runtime.

**How this works with AI/ML:**
- Headwater's metadata store provides the context that AI assistants need to generate accurate SQL. When a user asks "What was revenue last quarter?", the assistant has: the metric definition, the source tables, the assumptions, and the known caveats.
- This approach is runtime-agnostic. The metadata works whether the AI assistant is Headwater's own (Phase 4), Databricks Genie, or a custom LLM integration.

**How this works with existing semantic layers:**
- Headwater exports to dbt Semantic Layer format (metrics YAML)
- Headwater exports to Cube.dev schema format
- Headwater's metadata API can be consumed by any semantic layer tool

**What Headwater does NOT do:**
- Headwater does not run a semantic layer query engine. It does not intercept or rewrite SQL at query time.
- Headwater does not mandate a semantic layer. For teams where well-documented mart models are sufficient, that is a valid and complete workflow.

**The principle:** Generate the semantic metadata that makes any downstream tool -- BI, AI assistant, semantic layer engine -- work better. Do not lock users into Headwater's own runtime. Let the data team choose the right level of abstraction for their maturity.

---

## 7. Technology Stack


### 7.1 Core Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| **Language** | Python 3.11+ | Ecosystem: Anthropic SDK, PyIceberg, Polars, FastAPI, Pydantic. Python is the orchestrator; heavy computation is delegated to Rust/C++ engines (Polars, DuckDB). |
| **Package Manager** | `uv` | 10-100x faster than pip. Deterministic installs. |
| **Data Processing** | Polars | Arrow-native, lazy evaluation, 5-10x faster than Pandas. Used for profiling and simple aggregations. |
| **Analytical Engine** | DuckDB | Embedded OLAP, Arrow-native, single file deployment. Used for complex queries and model materialization. |
| **Metadata Store** | SQLite (POC) / Postgres (Phase 1+) | SQLite handles light concurrency for demo. Postgres for multi-process production. |
| **API** | FastAPI | Async, Pydantic v2 validation, WebSocket support, auto-generated OpenAPI spec. |
| **CLI** | Typer | Type-hint driven, auto-generated help, integrates with FastAPI's Pydantic models. |
| **LLM** | Anthropic SDK (plugin) | Claude for semantic analysis. Prompt caching reduces cost. Swappable via plugin interface. |
| **SQL Templating** | Jinja2 | Industry standard. Compatible with dbt export. |
| **Validation** | Pydantic v2 | Config validation, API schemas, shared models between CLI/API/core. |
| **UI** | Next.js + TailwindCSS | Rich interactive review UI: SQL diffs, DAG visualization, approval workflows. No throwaway Streamlit rewrite. |
| **Containerization** | Docker + docker-compose | Single-command deployment. Helm chart for Kubernetes. |
| **Testing** | pytest + testcontainers | Integration tests against real databases, not mocks. |
| **CI** | GitHub Actions | Standard OSS CI/CD. |
| **Linting** | Ruff | Fast, replaces flake8 + isort + black. |
| **Type Checking** | pyright | Strict type checking for core modules. |

### 7.2 What We Do NOT Use (And Why)

| Technology | Why Not |
|-----------|---------|
| Pandas | Replaced by Polars. Arrow-native, faster, lower memory. |
| SQLite for analytical data | DuckDB is purpose-built for OLAP. SQLite is row-oriented. |
| DuckDB for metadata | Single-writer lock causes contention between background processes and UI. |
| LanceDB | Cut. DuckDB full-text search sufficient for Phase 1-4. Re-evaluate if semantic search becomes critical. |
| Streamlit | Requires rewrite to production UI. Start with Next.js to avoid throwaway work. |
| dbt/SQLMesh in core | Added as export targets, not dependencies. Headwater generates SQL directly; users can export to dbt format. |
| Airflow/Dagster in core | APScheduler sufficient for Phase 1-3. Embedded, no separate infra. |
| Terraform/CloudFormation | Massive maintenance burden. Docker + Helm chart. Community handles cloud specifics. |
| Local LLMs in core | Supported via Ollama plugin. Not bundled (large download, hardware requirements). |

### 7.3 Streaming Readiness

Headwater does not implement streaming in Phase 1-5. However, the architecture is streaming-compatible by design:

- **Arrow RecordBatch**: The native data unit throughout the pipeline. Streaming systems (Kafka Arrow, Redis Streams) produce RecordBatches natively.
- **Polars lazy evaluation**: Processes data in chunks. Adding a streaming source means pointing Polars at a different iterator.
- **Incremental profiling**: Quality contracts track baselines over time. Switching from batch to incremental updates is an optimization, not an architecture change.
- **Event-driven quality**: Circuit breakers already operate on per-batch signals. Streaming is a faster batch cadence.

When streaming is needed (Phase 6+), it requires adding a message bus consumer, not rearchitecting the pipeline.

---

## 8. Roadmap & Timeline

### 8.1 Phase Overview

| Phase | Name | Duration | Team | Goal |
|-------|------|----------|------|------|
| **POC** | Prove It Works | 6 weeks | 2 people | Discovery + profiling + documentation + model generation on synthetic data. Prove the 10-minute-to-first-value claim. |
| **Phase 1** | Harden & Connect | 4 weeks | 1 + AI agents | Add real-world connectors (Postgres, MySQL, CSV, Parquet). Postgres metadata. CI/CD. Test coverage. |
| **Phase 2** | See & Interact | 5 weeks | 1 + AI agents | Next.js review UI. API hardening. Schema drift detection. |
| **Phase 3** | Operate | 6 weeks | 1 + AI agents | Scheduler (APScheduler). Drift response workflows. Notifications. Nessie integration for branching. |
| **Phase 4** | Understand | 8 weeks | 1 + AI agents | AI assistant. Semantic layer auto-generation. NL-to-SQL. |
| **Phase 5** | Scale | 8 weeks | 1-2 + AI agents | Cloud warehouse backends (Snowflake, BigQuery). Auth (OAuth2/OIDC). Production hardening. |

### 8.2 Phase Details & Go/No-Go Criteria

#### POC (Weeks 1-6)

**Scope**: Synthetic environmental health dataset (bundled, no download required). Yelp Open Dataset as optional power demo.

**Deliverables**:
- Data loading (JSON/NDJSON/Parquet -> DuckDB)
- Schema discovery (introspection + relationship detection)
- Statistical profiling via Polars (nulls, cardinality, distribution, distinct values)
- LLM semantic analysis (descriptions, domain classification, relationship enrichment)
- Staging model generation (auto-approved)
- Mart model proposal (with clarifying questions)
- Quality contract generation (observation mode)
- CLI: `headwater demo`, `headwater discover`, `headwater generate`, `headwater run`
- Basic Next.js UI: discovery browser, profile viewer, model reviewer
- Docker setup (2 containers)

**Technical Go/No-Go**:
- [ ] Discovery report generated in <5 minutes on synthetic dataset (25 tables)
- [ ] At least 3 auto-generated mart models that make semantic sense to a domain expert
- [ ] Quality contracts generated for >80% of columns
- [ ] UI successfully renders discovery report and model review workflow
- [ ] Demo can run end-to-end from `docker-compose up` to approved models in <30 minutes

**Adoption Go/No-Go** (validated with 3-5 design partners):
- [ ] A data engineer completes the full flow (discover -> review -> approve) without hand-holding
- [ ] Description acceptance rate >60% on first run (before any tuning)
- [ ] At least 2 design partners say "I would use this on a real project" (not just "interesting demo")

#### Phase 1: Harden & Connect (Weeks 7-10)

**Deliverables**:
- Postgres, MySQL, SQLite, CSV, Parquet, Iceberg connectors
- Error handling and edge cases (empty tables, >1000 columns, unicode, JSON blobs)
- Postgres metadata backend (migration from SQLite)
- 80%+ test coverage on core modules
- CI/CD (GitHub Actions: lint, type-check, test, Docker build)
- CLI polish, documentation

**Go/No-Go**:
- [ ] Successfully discovers and profiles a real-world Postgres database (not synthetic)
- [ ] Metadata survives concurrent CLI + UI access (Postgres backend)
- [ ] All tests pass in CI, including integration tests against Postgres via testcontainers
- [ ] Iceberg metadata-only discovery completes in <30 seconds for a 100-table catalog

#### Phase 2: See & Interact (Weeks 11-15)

**Deliverables**:
- Full Next.js review UI: model diff viewer, quality contract manager, relationship graph, decision log viewer
- API hardening: rate limiting, error responses, pagination, OpenAPI client generation
- Schema drift detection (polling-based, configurable interval)
- Drift impact analysis (which downstream models and contracts are affected)
- WebSocket for live progress updates during discovery

**Go/No-Go**:
- [ ] A data professional can navigate the UI and approve/reject models without reading external documentation
- [ ] Schema drift detected and surfaced within 1 polling cycle
- [ ] Impact analysis correctly identifies all downstream dependencies of a changed column

#### Phase 3: Operate (Weeks 16-21)

**Deliverables**:
- APScheduler for periodic discovery, profiling, and contract evaluation
- Drift response workflow: detect -> draft resolution -> alert -> human review -> apply
- Notification plugins: Slack, email
- Nessie integration: branching for proposed changes, merge on approval
- Export: dbt project generation, SQLMesh project generation

**Go/No-Go**:
- [ ] Scheduled discovery runs complete without manual intervention for 2 weeks
- [ ] Drift resolution workflow: end-to-end from detection to applied fix
- [ ] Export produces a valid, runnable dbt project from Headwater's generated models

#### Phase 4: Understand (Weeks 22-29)

**Deliverables**:
- AI assistant with full metadata context (multi-turn chat)
- Semantic layer auto-generation (business term -> SQL metric mapping)
- NL-to-SQL with quality-aware query generation
- Root cause analysis: "Why did revenue drop 15% last week?"

**Go/No-Go**:
- [ ] AI assistant correctly answers "How is MRR calculated?" with SQL, business context, and caveats
- [ ] NL-to-SQL produces correct results for 70%+ of common analytical questions
- [ ] Semantic layer covers >80% of mart model metrics

#### Phase 5: Scale (Weeks 30-37)

**Deliverables**:
- Snowflake and BigQuery warehouse backends
- OAuth2/OIDC authentication, role-based access control
- Celery + Redis for background task processing (replaces APScheduler)
- Observability: structured logging, metrics endpoint, health checks
- Security hardening: credential encryption, network policies, audit logging

**Go/No-Go**:
- [ ] Full workflow (discover -> model -> execute) against Snowflake and BigQuery
- [ ] Multi-user access with role separation (viewer, editor, admin)
- [ ] Zero credentials stored in plaintext in any log or metadata record

### 8.3 Schedule Contingency

The "1 person + AI agents" model for Phase 1-4 is ambitious. If velocity is lower than planned:

| If This Happens | Then We Do This |
|----------------|----------------|
| Phase 1 takes 8 weeks instead of 4 | Defer Iceberg connector to Phase 2. Ship with Postgres, MySQL, CSV, Parquet only. |
| Phase 2 UI takes longer than expected | Ship CLI-only for Phase 2. Defer full Next.js UI to Phase 3. Data engineers are comfortable with CLI. |
| Phase 4 (AI assistant, NL-to-SQL) proves too ambitious for 1 person | Descope to "semantic search over metadata" (simpler). Defer full NL-to-SQL to Phase 5 or community contribution. |
| AI agent productivity is lower than 60-70% boilerplate coverage | Hire a part-time contractor for connector development (most parallelizable work). |

The critical path is Phase 1-2 (discovery, connectors, quality contracts, review UI). Phase 3-5 features are valuable but not required for a useful, launchable product. If Phase 1-2 delivers well, the product is already useful enough to build a community.

### 8.4 Adoption Metrics (Tracked from POC onward)

Beyond technical Go/No-Go, these adoption metrics determine whether Headwater is actually useful:

| Metric | Target (Month 3) | Target (Month 6) | How Measured |
|--------|-----------------|-----------------|-------------|
| **Design partner retention** | 3 of 5 still using weekly | 5 of 10 still using weekly | Active discovery runs per workspace |
| **Description acceptance rate** | >60% | >75% | Approved without edit / total generated |
| **Quality contract precision** | >80% | >90% | True alerts / total alerts |
| **Model edit distance** | <40% lines changed | <30% lines changed | Diff between proposed and approved SQL |
| **Time to first value** | <30 minutes (catalog) / <1 hour (OLTP) | Same | Measured in design partner sessions |
| **GitHub stars** | 500 | 2,000 | GitHub API |
| **Monthly active users (MAU)** | 50 | 300 | Opt-in anonymous telemetry |

---

## 9. Go-to-Market Strategy

### 9.1 Distribution Channels

| Channel | Audience | Timing |
|---------|----------|--------|
| **Hacker News launch** | Technical founders, senior engineers | POC launch (Week 6) |
| **dbt Slack community** | Analytics engineers, dbt users | Phase 1 (dbt export feature) |
| **DuckDB Discord / ecosystem** | DuckDB adopters, embedded analytics users | POC launch |
| **Data engineering subreddits** (r/dataengineering) | Data engineers at all levels | Ongoing |
| **Conference talks** (Data Council, dbt Coalesce, local meetups) | Practitioners, leads | Phase 2+ |
| **Technical blog posts** | SEO-driven discovery | Ongoing from Phase 1 |

### 9.2 Content Strategy

Publish practical, non-promotional content that demonstrates Headwater's value through real scenarios:
- "What Headwater found in a 60-table Postgres database" (concrete discovery report walkthrough)
- "Auto-generated quality contracts vs. hand-written: a comparison" (honest accuracy assessment)
- "Connecting Headwater to your Glue Catalog: a 15-minute guide"
- "Headwater's confidence metrics after 3 months: what we learned"

Each post should include Headwater's accuracy metrics for the scenario -- reinforcing the trust-through-transparency positioning.

### 9.3 Early Adopter Program

Before public launch, recruit 5-10 design partners:
- Data engineers at startups or growth-stage companies with 20-100 tables
- Must be willing to run Headwater on real (not synthetic) data
- Must provide structured feedback (discovery accuracy, model quality, workflow friction)
- Their feedback shapes Phase 1-2 priorities and provides launch testimonials

### 9.4 Community Building

- **Discord server** for users, contributors, and design partners
- **GitHub Discussions** for feature requests, architecture Q&A
- **Monthly "Headwater in the wild" report**: anonymized aggregate of confidence metrics from opt-in users (e.g., "across 50 deployments, average description acceptance rate is 76%")

---

## 10. Investment & Budget

### 10.1 Team

| Phase | People | AI Agent Usage | Duration |
|-------|--------|---------------|----------|
| POC | 2 (builder + Ralph) | Moderate (code generation, testing) | 6 weeks |
| Phase 1-4 | 1 + AI agents | Heavy (AI agents handle boilerplate, tests, documentation) | 23 weeks |
| Phase 5+ | 1-2 + AI agents | Heavy | 8+ weeks |

The "1 person + AI agents" model is viable because:
- The architecture is modular (plugin system, clear interfaces)
- AI coding agents handle 60-70% of boilerplate (connectors, API endpoints, UI components, tests)
- The human focuses on architecture decisions, review workflow design, and quality judgment

### 10.2 Infrastructure Costs

| Item | POC | Phase 1-3 | Phase 4-5 |
|------|-----|-----------|-----------|
| Development machines | Existing laptops | Existing laptops | Existing laptops |
| CI/CD | GitHub Actions (free tier) | GitHub Actions (free tier) | GitHub Actions ($4/mo) |
| Postgres | N/A (SQLite) | Neon/Railway free tier or $5/mo | $15-25/mo |
| LLM API (Claude) | ~$20-50 total (development) | ~$30-50/mo (testing) | ~$50-100/mo (testing + assistant dev) |
| Cloud testing (Snowflake/BQ) | N/A | N/A | ~$50-100/mo (dev accounts) |
| Domain + hosting (docs site) | $15/yr | $15/yr | $15/yr |
| **Total monthly** | **~$5-10** | **~$40-60** | **~$120-230** |

### 10.3 LLM Cost Model for Users

End users running Headwater with Claude API:

| Operation | Approximate Cost | Frequency |
|-----------|-----------------|-----------|
| Full discovery (50 tables) | $2-5 | On initial setup, then weekly/monthly |
| Incremental discovery (changed tables only) | $0.10-0.50 | Daily or on-demand |
| Mart model proposal (per model) | $0.05-0.15 | On demand |
| AI assistant query | $0.01-0.05 | Per question |

A typical 30-person startup with 35 Postgres tables would spend approximately $5-15/month on LLM costs if running weekly discovery. With prompt caching (Anthropic's feature), repeated analysis of unchanged tables approaches zero marginal cost.

### 10.4 Sustainability Model

| Tier | Model | Pricing (Target) | Available |
|------|-------|---------|-----------|
| **Core OSS** | Apache 2.0, free forever. All features in this RFP. | $0 | Phase 1 (public launch) |
| **Headwater Cloud** | Managed hosting: Postgres metadata, built-in LLM (Claude), SSO, team collaboration, scheduled discovery. No Docker required. | $49-149/month per workspace (based on table count: <50 / 50-200 / 200+) | Phase 5+ (~Month 12-14) |
| **Enterprise** | Headwater Cloud + RBAC, audit export, custom SLAs, dedicated support, custom connectors. | $500-2,000/month (annual contract) | Phase 5+ (~Month 14-18) |

**Path to revenue**: The OSS core builds the community and the brand (Months 1-12). Headwater Cloud launches when Phase 5 delivers cloud backends and auth (Month 12-14). The conversion funnel: data engineer tries OSS on laptop -> team adopts -> wants managed infrastructure without Docker -> Headwater Cloud.

**Break-even estimate**: At $99/month average, 100 paying workspaces = $9,900/month recurring revenue. Target: 100 paying customers within 6 months of Headwater Cloud launch.

**What stays free**: Everything in the OSS core -- discovery, profiling, documentation, quality contracts, model generation, drift detection, the review UI, the CLI. Headwater Cloud adds convenience (managed infrastructure, SSO, scheduling) and team features (RBAC, shared workspaces), not core functionality.

---

## 11. Risk Assessment

### 11.1 Technical Risks

| Risk | Likelihood | Impact | Mitigation | Status |
|------|-----------|--------|------------|--------|
| **DuckDB single-writer contention** | High (if not mitigated) | Critical -- UI blocks when pipeline writes | Metadata in SQLite/Postgres from day one. DuckDB strictly for analytical data. | Mitigated by architecture split |
| **LLM hallucination on dirty data** | Medium | High -- wrong descriptions erode trust | Top-N distinct values + statistical signatures instead of raw rows. Semantic locking prevents drift. | Mitigated by design |
| **Polars OOM on large tables** | Medium | Medium -- profiling crashes on >100M rows | Polars for profiling (simple aggregations only). DuckDB for complex queries. Defensive streaming-mode check before execution. | Mitigated by design |
| **Auto-generated model quality** | Medium | High -- wrong business logic in marts | Tiered approval: staging auto, marts individual review with questions. No batch approve. | Mitigated by workflow |
| **Review fatigue** | Medium | Medium -- users approve without reading | Observation mode for contracts. One-at-a-time mart review. Progressive disclosure. | Mitigated by UX design |
| **PyIceberg small-file explosion** | Low | Medium -- slow discovery on unmaintained Iceberg tables | Timeout mechanism. User warning about table maintenance. | Mitigated by documentation |
| **Semantic locking creates stale definitions** | Low | Low -- old descriptions persist after schema changes | Drift detection flags locked definitions for re-review when schema changes. | Mitigated by design |

### 11.2 Market Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| **Databricks/Snowflake build equivalent AI features** | High | Medium | Target companies NOT on those platforms. The DuckDB/Postgres segment is underserved by these vendors. |
| **Secoda adds model generation + quality contracts** | Medium | High | Execution speed and OSS positioning. Secoda is SaaS-only and starts at ~$400/mo. |
| **dbt Copilot becomes free/open** | Low | Medium | Headwater works without an existing dbt project. Different entry point (greenfield vs. existing stack). |
| **LLM costs increase significantly** | Low | Medium | No-LLM mode is first-class. Core functionality works at $0. |

### 11.3 Adoption Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| **Time-to-first-value exceeds 10 minutes** | Medium | High | Bundled synthetic data. Single docker-compose command. Demo mode requires zero configuration. |
| **Users don't complete model review** | Medium | Medium | Progressive onboarding: start with discovery report (instant value), then contracts (observation mode), then models. |
| **Plugin ecosystem doesn't develop** | Medium | Low (for core product) | Core ships with 5 connectors and 1 LLM provider. Plugins are additive, not required. |
| **Solo maintainer burnout** | Medium | High | AI agents reduce workload. Modular architecture allows community contributions. Focus on Phase 1-3 before expanding. |

---

## 12. OSS Governance & Community

### 12.1 License

**Apache 2.0** -- the same license as Airflow, Spark, DuckDB, and Iceberg. Chosen because:
- Permissive: companies can use Headwater internally without legal friction
- Patent grant: protects contributors and users
- Commercial-friendly: allows cloud managed version without relicensing
- Community standard: familiar to the data engineering OSS community

### 12.2 Repository Structure

```
headwater/                    # Monorepo (core + bundled connectors)
  headwater/                  # Python package
    core/                     # Models, config, metadata store
    connectors/               # Bundled: postgres, mysql, sqlite, csv, parquet, iceberg
    profiler/                 # Polars-based profiling engine
    analyzer/                 # LLM semantic analyzer
    generator/                # Model + contract generator
    executor/                 # DuckDB query executor
    quality/                  # Quality engine
    api/                      # FastAPI endpoints
    cli/                      # Typer CLI
  ui/                         # Next.js frontend
  charts/                     # Helm chart
  tests/                      # pytest + testcontainers
  docs/                       # Documentation site
```

Community-contributed plugins live in separate repositories:
```
headwater-connector-bigquery/
headwater-llm-openai/
headwater-notify-slack/
headwater-template-ecommerce/
headwater-deploy-aws/
```

### 12.3 Contribution Paths

Ordered by ease of entry:

1. **Connectors** (easiest): Well-defined interface (`BaseConnector`), clear test patterns, immediate value. "Can you connect to MongoDB? Here is the interface, here are the Postgres and MySQL connectors as examples."
2. **Model Templates**: Industry-specific model patterns (SaaS metrics, e-commerce funnels, healthcare measures). Domain expertise more valuable than code expertise.
3. **Bug fixes and improvements**: Standard OSS contribution flow.
4. **Documentation**: Tutorials, guides, translations.
5. **Core features** (hardest): Requires understanding the full architecture. Guided by maintainer-approved issues.

### 12.4 Governance

**Phase 1 (POC through Phase 3)**: Benevolent dictator model. Core maintainers make all architectural decisions. Fast iteration, consistent vision.

**Phase 2 (Phase 4+)**: Transition to maintainer group. 3-5 maintainers with merge access. RFC process for architectural changes. Community voting on roadmap priorities.

---

## 13. Appendices

### Appendix A: Detailed Architecture Diagram

```
+================================================================+
|                    HEADWATER CORE (Python)                       |
|                                                                  |
|  +------------------+  +-------------------+  +--------------+   |
|  | Discovery Engine |  | Quality Engine    |  | Decision Log |   |
|  |                  |  |                   |  |              |   |
|  | - Schema crawl   |  | - Contract gen    |  | - Change     |   |
|  | - Relationship   |  | - Contract eval   |  |   history    |   |
|  |   detection      |  | - Anomaly detect  |  | - Reasons    |   |
|  | - Domain         |  | - Freshness track |  | - User edits |   |
|  |   classification |  | - Circuit breaker |  | - Approvals  |   |
|  +--------+---------+  +--------+----------+  +------+-------+   |
|           |                     |                     |           |
|  +--------v---------+  +-------v----------+          |           |
|  | Profiler         |  | Drift Detector   |          |           |
|  | (Polars: simple) |  | - Schema diff    |          |           |
|  | (DuckDB: complex)|  | - Impact analysis|          |           |
|  +--------+---------+  | - Drift budget   |          |           |
|           |             +-------+----------+          |           |
|           |                     |                     |           |
|  +--------v---------+  +-------v----------+          |           |
|  | Semantic Analyzer |  | Model Generator  |          |           |
|  | (LLM Plugin)      |  | - Staging (auto) |          |           |
|  | - Descriptions    |  | - Marts (review) |          |           |
|  | - Entities        |  | - SQL + Jinja2   |          |           |
|  | - Semantic lock   |  | - Questions      |          |           |
|  +------------------+  +------------------+          |           |
|           |                     |                     |           |
|           +----------+----------+----------+----------+           |
|                      |                     |                      |
|              +-------v--------+    +-------v--------+            |
|              | Metadata Store |    | Analytical     |            |
|              | (SQLite/       |    | Engine         |            |
|              |  Postgres)     |    | (DuckDB)       |            |
|              +----------------+    +----------------+            |
+================================================================+
         |                                         |
   +-----v------+                            +-----v------+
   | FastAPI    |                            | Next.js UI |
   | - REST     |<-------------------------->| - Review   |
   | - WebSocket|                            | - Browse   |
   | - OpenAPI  |                            | - Monitor  |
   +-----+------+                            +------------+
         |
   +-----v------+
   | Typer CLI  |
   | - discover |
   | - generate |
   | - run      |
   | - review   |
   +------------+
```

### Appendix B: Example Discovery Report Output

```
$ headwater discover --source postgres://prod:5432/myapp

  Headwater v1.0 -- Discovery Report
  ===================================

  Source: postgres://prod:5432/myapp
  Scanned: 47 tables, 342 columns
  Duration: 3m 42s
  LLM Tier: Cloud (Claude)

  DOMAINS DETECTED
  ----------------
  [Users & Accounts]  12 tables  |  2.3M total rows
    users, accounts, subscriptions, roles, permissions,
    user_preferences, teams, team_members, invitations,
    api_keys, sessions, audit_log

  [Orders & Payments]  8 tables  |  890K total rows
    orders, order_items, payments, refunds, discounts,
    shipping, invoices, tax_records

  [Products]  6 tables  |  45K total rows
    products, categories, inventory, price_history,
    product_images, product_tags

  [Events]  4 tables  |  12M total rows
    events, event_properties, page_views, feature_flags

  [System/Internal]  17 tables  |  excluded from modeling
    django_*, pg_*, schema_migrations, ...

  RELATIONSHIPS (18 detected)
  ---------------------------
  users.id          --> orders.user_id         1:many  94% integrity
  users.id          --> subscriptions.user_id  1:many  100% integrity
  orders.id         --> payments.order_id      1:many  99.8% integrity
  orders.id         --> order_items.order_id   1:many  100% integrity
  order_items.product_id --> products.id       many:1  100% integrity
  ... (13 more)

  QUALITY OBSERVATIONS
  --------------------
  [WARNING] orders.discount_amount     23% null (expected for non-discounted?)
  [WARNING] users.phone                67% null (optional field?)
  [WARNING] payments.amount            3 negative values (refunds or errors?)
  [INFO]    events.properties          JSON column, 340 distinct keys
  [INFO]    products.description       42 rows with identical text "TBD"
  [INFO]    subscriptions.cancelled_at 78% null (expected for active subs)

  PROPOSED MODELS
  ---------------
  Staging (12 models, auto-approved):
    stg_users, stg_orders, stg_payments, stg_products, ...

  Marts (5 models, awaiting review):
    mart_revenue, mart_user_segments, mart_product_performance,
    mart_subscription_metrics, mart_funnel

  QUALITY CONTRACTS (proposed, observation mode)
  -----------------------------------------------
  168 contracts generated across 30 tables
    87 not-null expectations
    34 value range bounds
    22 uniqueness constraints
    15 referential integrity checks
    10 freshness thresholds

  Next: headwater review   (open review UI)
        headwater run      (materialize approved models)
```

### Appendix C: Example Auto-Generated Model with Review Questions

```sql
-- ============================================================
-- Model: mart_revenue
-- Generated by: Headwater v1.0
-- Generated at: 2026-04-15 10:32:00 UTC
-- Status: AWAITING REVIEW
-- ============================================================
--
-- Description:
--   Monthly revenue aggregation by product category and
--   customer segment. Includes gross revenue, discounts,
--   refunds, and net revenue.
--
-- Source tables:
--   stg_orders, stg_payments, stg_products, stg_users
--
-- ASSUMPTIONS (please verify):
--   1. Revenue = sum of payment amounts (not order amounts)
--   2. Refunds are identified by negative payment amounts
--   3. Customer segment derived from users.plan_type
--   4. Month boundaries use payment.created_at, not order.created_at
--
-- QUESTIONS FOR REVIEWER:
--   Q1: Is revenue recognized on invoice creation or payment date?
--       (Currently using payment date)
--   Q2: Should cancelled orders with partial payments be included?
--       (Currently included)
--   Q3: Is users.plan_type the correct field for customer segmentation?
--       (Alternatives found: users.tier, subscriptions.plan_name)
--   Q4: Should discounts be subtracted from gross or tracked separately?
--       (Currently tracked separately)
-- ============================================================

WITH payments_enriched AS (
    SELECT
        p.id AS payment_id,
        p.order_id,
        p.amount AS payment_amount,
        p.created_at AS payment_date,
        CASE WHEN p.amount < 0 THEN 'refund' ELSE 'payment' END AS payment_type,
        o.user_id,
        o.discount_amount
    FROM stg_payments p
    JOIN stg_orders o ON p.order_id = o.id
),

order_products AS (
    SELECT
        oi.order_id,
        pr.category AS product_category
    FROM stg_order_items oi
    JOIN stg_products pr ON oi.product_id = pr.id
),

final AS (
    SELECT
        DATE_TRUNC('month', pe.payment_date) AS revenue_month,
        u.plan_type AS customer_segment,
        op.product_category,
        SUM(CASE WHEN pe.payment_type = 'payment' THEN pe.payment_amount ELSE 0 END) AS gross_revenue,
        SUM(CASE WHEN pe.payment_type = 'refund' THEN ABS(pe.payment_amount) ELSE 0 END) AS refunds,
        SUM(COALESCE(pe.discount_amount, 0)) AS discounts,
        SUM(pe.payment_amount) AS net_revenue,
        COUNT(DISTINCT pe.order_id) AS order_count,
        COUNT(DISTINCT pe.user_id) AS customer_count
    FROM payments_enriched pe
    JOIN stg_users u ON pe.user_id = u.id
    LEFT JOIN order_products op ON pe.order_id = op.order_id
    GROUP BY 1, 2, 3
)

SELECT * FROM final
```

### Appendix D: Privacy & PII Handling (Quick Reference)

> For the full privacy architecture including operating modes (Air-Gapped, Local LLM, Cloud LLM), smart techniques, and data residency guarantees, see **Section 4.8**.

**PII Detection Methods** (applied automatically during profiling):

| PII Type | Detection Method |
|----------|-----------------|
| Email addresses | Regex pattern matching |
| Phone numbers | Regex + length heuristics |
| SSN / Tax IDs | Regex (9-digit patterns with dashes) |
| Credit card numbers | Luhn algorithm validation |
| Names (first/last) | Column name heuristics + dictionary check |
| IP addresses | Regex (IPv4, IPv6) |
| Geographic coordinates | Column name + value range (-90 to 90, -180 to 180) |

**Response to detected PII**:

1. Column flagged as `pii_detected: true` in metadata
2. PII type recorded (email, phone, SSN, etc.)
3. Column excluded from LLM context (never sent to any external API)
4. Statistical aggregates still computed (null rate, cardinality, format consistency)
5. User alerted in discovery report: "PII detected in users.email -- excluded from LLM analysis"
6. Full audit trail logged: what was detected, what was excluded, what was sent

**Configuration** via `headwater.yaml`:

```yaml
privacy:
  mode: air_gapped          # air_gapped | local_llm | cloud_llm
  pii_detection: true
  pii_action: exclude        # exclude | redact | flag_only
  schema_only_first: true    # send only schema info on first pass
  llm_data_sharing: stats    # stats | schema_only | none
  audit_log: true
  sensitive_schemas:
    - billing
    - auth
    - users
  never_send_columns:
    - "*.ssn"
    - "*.credit_card"
    - "users.email"
  overrides:
    orders.billing_city: safe_for_llm
```

### Appendix E: Plugin Development Guide

**Scaffold a new connector plugin:**

```bash
headwater plugin scaffold --type connector --name bigquery
```

Generates:

```
headwater-connector-bigquery/
  headwater_connector_bigquery/
    __init__.py
    connector.py       # Implements BaseConnector
    config.py          # Pydantic config model
  tests/
    test_connector.py  # Integration test template
  pyproject.toml       # Package metadata + entry_point
  README.md
```

**BaseConnector interface:**

```python
from abc import ABC, abstractmethod
from headwater.core.models import TableInfo, ColumnInfo

class BaseConnector(ABC):
    """Interface for all Headwater source connectors."""

    @abstractmethod
    def connect(self, config: dict) -> None:
        """Establish connection to the data source."""
        ...

    @abstractmethod
    def list_tables(self) -> list[TableInfo]:
        """Return metadata for all discoverable tables."""
        ...

    @abstractmethod
    def get_columns(self, table: str) -> list[ColumnInfo]:
        """Return column metadata for a specific table."""
        ...

    @abstractmethod
    def sample_values(self, table: str, column: str, n: int = 100) -> list:
        """Return top-N distinct non-null values for a column."""
        ...

    @abstractmethod
    def row_count(self, table: str) -> int:
        """Return approximate or exact row count."""
        ...

    @abstractmethod
    def to_arrow(self, table: str, limit: int | None = None) -> "pa.Table":
        """Return table data as an Arrow Table."""
        ...
```

**Register the plugin** in `pyproject.toml`:

```toml
[project.entry-points."headwater.connectors"]
bigquery = "headwater_connector_bigquery.connector:BigQueryConnector"
```

After `pip install headwater-connector-bigquery`, Headwater auto-discovers the plugin at startup and makes it available via:

```bash
headwater discover --source bigquery://project-id/dataset
```

---

*This document is a living proposal. Feedback, questions, and contributions are welcome.*

*Apache 2.0 License. Built with Polars, DuckDB, and the Anthropic Claude API.*
