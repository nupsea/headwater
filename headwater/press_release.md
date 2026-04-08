# Headwater Launches: The Open-Source Advisory Companion for Data Engineers

**Auto-discovery, documentation, quality contracts, and model scaffolding -- so data professionals can focus on the work that actually requires their expertise.**

---

**April 2026**

Data engineers are caught in a paradox. AI-powered tooling promises to automate everything, yet over half of a data engineer's time still goes to maintenance -- keeping pipelines running, chasing schema changes, writing documentation that falls out of date immediately, and answering ad-hoc questions from stakeholders. The meaningful work -- data modeling, business logic, architecture decisions, stakeholder collaboration -- gets squeezed into whatever time is left.

The problem is not a lack of tools. It is a lack of tools that data professionals actually trust. The modern data stack delivered a 10-15 tool maintenance surface that takes months to set up and years to maintain. Data quality issues are rising: resolution time is up 166%, and 74% of quality problems are still discovered by business users rather than by the systems designed to catch them. Meanwhile, AI-powered data tools ask for broad access and return confident-sounding but unverifiable results. Data engineers have seen the hallucinations. They have tried the "magic" demos that break on real data.

Today, **Headwater** launches as an open-source advisory companion for data professionals. It handles the mechanical groundwork -- discovery, profiling, documentation, quality baselines, staging model boilerplate -- so that data engineers, analytics engineers, and data analysts can reclaim their time for the work that actually requires human judgment.

Point Headwater at any data source -- an existing catalog (AWS Glue, Databricks Unity Catalog, Iceberg REST), a production database (Postgres, MySQL), or flat files -- and within minutes receive a complete discovery report: every table mapped, every column profiled, every relationship detected, every quality concern flagged, and documentation generated. Headwater then proposes analytical models and data quality contracts for your review. You bring the domain knowledge. Headwater brings the scaffolding.

Headwater is advisory by design. It proposes. You decide. Staging models that involve no business logic are auto-approved. Mart models that encode business definitions -- how you calculate revenue, what counts as an active user -- are presented one at a time with the SQL, the assumptions, and clarifying questions. Quality contracts enter observation mode first, tracking violations silently so you can validate the rules before turning them on. There is no "auto-apply everything" button. Your expertise is what makes the output trustworthy.

Critically, Headwater **tracks its own accuracy over time**. Description acceptance rate, quality contract false-positive rate, model edit distance, drift detection lead time -- all measured and surfaced. Instead of asking you to trust it on faith, Headwater gives you quantitative evidence. A tool that publishes its own accuracy metrics earns trust; a tool that hides behind "AI-powered" deserves skepticism.

> "We built Headwater because we have been the overloaded data engineer. You know the feeling: you built the pipeline, you maintain the pipeline, you document the pipeline, you answer every question about the pipeline, and somehow you are also supposed to be building the next thing. Headwater does not replace data engineers -- it takes the mechanical drudgery off their plate. The discovery, the profiling, the documentation, the quality baselines, the staging boilerplate -- that is Headwater's job. The business logic, the modeling decisions, the stakeholder conversations -- that is yours. And Headwater shows you exactly how accurate its suggestions are, so you can decide how much to trust it."

---

## How It Works

**1. Connect** -- Run `docker-compose up` and point Headwater at your data source. Connect to an existing catalog for metadata-enriched discovery, or directly to a database. No cloud accounts required. No API keys needed for core functionality.

```bash
docker-compose up

# Connect to a catalog (mature orgs):
headwater discover --catalog glue --region us-east-1 --database analytics_prod

# Or connect directly to a database:
headwater discover --source postgres://your-db:5432/production
```

**2. Discover** -- Headwater crawls your data source, profiles every column, detects relationships between tables, classifies business domains, and generates documentation. A catalog-connected source completes in minutes. A 50-table OLTP database completes in 5-15 minutes. The discovery report surfaces quality issues, missing relationships, and data patterns that often surprise even the engineers who built the database.

```
Discovered 47 tables across 3 schemas
Detected 5 business domains, 18 relationships
Flagged 12 quality observations
Proposed 12 staging models, 5 mart models
Generated 168 quality contracts (observation mode)
Confidence baseline established -- accuracy tracking begins
```

**3. Review & Refine** -- Open the review UI in your browser. Staging models are auto-approved (mechanical transformations, no judgment required). Each proposed mart model is presented individually with the SQL, the assumptions made, and questions Headwater could not answer on its own. You approve, edit, or reject based on your domain knowledge. Headwater materializes approved models and begins monitoring for quality issues and schema changes -- alerting you before stakeholders notice problems. Every acceptance, edit, and rejection feeds the confidence metrics.

---

## Who Is Headwater For

**The data engineer** maintaining 80 tables, 12 dashboards, and everyone's ad-hoc requests. Headwater handles discovery, documentation, and quality baselines so they can focus on modeling and architecture instead of firefighting. "I ran Headwater against our Postgres. It found 6 referential integrity gaps I didn't know about and generated quality contracts I would have taken two weeks to write manually."

**The analytics engineer** inheriting a pipeline they did not build. Headwater maps the existing data estate in minutes -- relationships, quality issues, domain boundaries -- providing the context that would otherwise take weeks to reverse-engineer. Exports to dbt format so they can start building immediately.

**The data analyst** who asks better questions when they have better context. Headwater's auto-generated documentation means they can discover what tables exist, what columns mean, and where quality issues lurk without interrupting the data engineer.

**The data platform engineer** auditing an existing infrastructure. Connects Headwater to a Glue or Unity Catalog to find documentation gaps, undocumented cross-schema relationships, and tables with no quality coverage.

**The engineering manager** who uses Headwater's confidence dashboard in monthly reviews -- tracking how suggestion accuracy improves over time, monitoring data estate health, and making tooling ROI decisions based on data rather than anecdotes.

**The backend engineer wearing a data hat** who needs to stand up basic reporting without becoming a full-time data engineer. Headwater scaffolds the foundation; they add the business logic. "I needed revenue reporting for our Series A board deck. Headwater proposed a mart model with the right joins. I just had to answer its questions about how we recognize revenue."

---

## What Headwater Is NOT

Headwater is explicit about its scope. It is **not a replacement for data engineers** -- it is a tool in their kit, like dbt or Great Expectations. It is **not a data warehouse**, a BI tool, or an enterprise governance platform. It is **not magic** -- auto-generated models are a starting point, not a finished product, and Headwater publishes its own accuracy metrics so you know exactly how often it gets things right. It is **not for real-time** streaming analytics. It does not force a methodology or opinionated workflow -- it integrates with your existing stack and adapts to how your team already works.

---

## Technical Highlights

**Arrow-native architecture.** Data flows from source to analytics as Apache Arrow memory throughout. Polars handles profiling. DuckDB handles complex queries and model materialization. Zero serialization overhead between components. This makes profiling a 10-million-row table on a laptop practical, not theoretical.

**LLM-optional with three operating modes.** Headwater operates in three tiers: **Air-Gapped** (no network calls, heuristic descriptions, statistical profiling, rule-based contracts -- fully functional at $0), **Local LLM** via Ollama (better descriptions, domain classification -- data never leaves your network), or **Cloud LLM** via Claude API (rich semantic analysis, mart model proposals, ~$2-5 per full discovery run). The LLM enhances; it does not provide the foundation.

**Privacy-first with defense-in-depth.** Six independent layers protect your data: schema-only mode runs before any profiling; statistical summaries replace raw data in all LLM context (never raw rows); PII is automatically detected and excluded; prompts are sandboxed and responses validated; every LLM interaction is logged in a queryable audit trail; all data stays in your infrastructure. LLM prompts never receive raw data rows -- only statistical aggregates and schema metadata.

**Semantic metadata generator, not a semantic layer runtime.** Headwater auto-generates business-context metadata (descriptions, domain classifications, relationships) and exports it to existing semantic layer tools (dbt metrics, Cube.dev, Looker LookML). It meets your stack where it is rather than forcing adoption of another runtime.

**Works with your existing stack.** Connects to data catalogs (AWS Glue, Databricks Unity, Iceberg REST) for metadata-enriched discovery, or directly to OLTP databases (Postgres, MySQL). Exports to dbt project format, generates SQLMesh-compatible models, and connects to external warehouses (Snowflake, BigQuery). It does not force you to abandon your tools -- it gives you a head start within them.

**Plugin architecture.** Seven plugin types (source connectors, warehouse backends, LLM providers, notifiers, model templates, exporters, cloud deployers) via Python entry_points. Install a connector with `pip install headwater-connector-snowflake`. Scaffold a new plugin with `headwater plugin scaffold`.

**Defensive by design.** Circuit breakers halt ingestion on anomalous volume shifts. A drift budget prevents infinite detect-fix-detect loops. Tiered approval prevents review fatigue. Quality contracts observe before they enforce. Confidence metrics track accuracy over time. Every artifact is reviewable, editable, revertible, and explainable.

---

> "I inherited a Postgres database with 60 tables, zero documentation, and the person who built it had left three months before I joined. My first week was supposed to be productive. Instead, I spent it writing SELECT * queries and trying to guess what columns meant from their names. I ran Headwater on day two. In 20 minutes I had a complete map -- relationships, quality flags, domain classifications. It found a referential integrity gap between orders and payments that nobody knew about. More importantly, it generated quality contracts that caught a silent data drop the following week, before the CEO noticed the dashboard numbers were off. Three months in, Headwater's description acceptance rate is at 82% -- I trust its documentation now and only edit the edge cases. Headwater did not do my job -- it gave me the map so I could do my job from day one instead of day thirty."
>
> -- Data Engineer, 45-person SaaS company (first month on the job)

---

## Pricing & Availability

Headwater is open-source under the Apache 2.0 license and available today on GitHub. It runs on any machine with Docker.

The core platform is and will always be free. This includes: schema discovery, statistical profiling, relationship detection, auto-documentation, quality contract generation, staging model generation, mart model proposals, schema drift detection, confidence tracking, and the full review UI.

An optional cloud LLM integration (for enhanced semantic analysis and AI-powered documentation) uses the Anthropic Claude API at approximately $2-5 per full discovery run. Local LLM support via Ollama is free. Air-gapped mode with no network calls is a first-class experience -- not a degraded fallback.

A managed cloud version with built-in Postgres, LLM integration, SSO, and team collaboration is planned for 2027.

---

## Get Started

10 minutes from zero to your first discovery report:

```bash
git clone https://github.com/headwater-data/headwater
cd headwater
docker-compose up

# Try the built-in demo (no database required):
headwater demo

# Or connect your own database:
headwater discover --source postgres://localhost:5432/mydb

# Or connect your catalog:
headwater discover --catalog glue --region us-east-1 --database analytics_prod
```

**GitHub**: github.com/headwater-data/headwater
**Documentation**: docs.headwater.dev
**Community**: discord.gg/headwater

---

*Headwater. Where your data story begins.*

*Built with Polars, DuckDB, and the Anthropic Claude API. Apache 2.0 License.*
