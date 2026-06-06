# GFL Commercial — Route Profitability: Specification

Spec-driven companion to `design.svg`. **Part 1 (Specify)** states the problem,
the deliverables, and the metric that defines the answer. **Part 2 (Plan)** records
the architecture and the engineering decisions behind it, with reasoning.

Stack: **PySpark + Delta Lake**, medallion architecture, star-schema serving layer.
Implementation is tracked task-by-task in [`tasks.md`](./tasks.md).

---

# Part 1 — Specify

## 1.1 Problem

GFL Commercial runs ~120 collection routes across 6 regions / 8 BUs / 29 areas,
covering 2022–2024 (12,000 route-day records). Leadership wants to know which routes
are **underperforming** and why — without being fooled by the structural differences
between what routes collect.

## 1.2 Deliverables

- **Part 1 — Pipeline:** a medallion (Bronze→Silver→Gold) PySpark + Delta pipeline that
  ingests the raw CSV and produces a clean, queryable star schema.
- **Part 2 — Analysis:** a per-route verdict (`route_scorecard`) identifying
  underperformers, plus an analysis notebook that evidences the findings.
- **Docs:** this spec, the architecture/ERD diagram (`design.svg`), and a README that
  makes the whole thing clone-and-run.

## 1.3 The metric — what "underperforming" means

A flat margin threshold is indefensible here: route economics are **structurally**
different by what the route collects. Median margin runs ~35.6% for General Waste vs
~76.5% for Cardboard — a ~40-point gap that reflects the material, not how well the
route is operated.

So underperformance is judged **relative to a peer cohort** and at the **route level**
(the unit you can actually act on), not the route-day level.

- **Cohort** = `primary_waste_stream × primary_customer_segment`.
- A route is assessed on **persistence** — how consistently it sits below its cohort.
- **Two tiers**, because operations triages them differently:
  - **Tier 1 — Loss-making:** negative median gross profit / high loss-day rate → re-price or cut.
  - **Tier 2 — Margin leak:** profitable but chronically below cohort peers → optimise (routing, sequencing, disposal).
- **Absolute floor:** any route-day with negative gross profit is flagged regardless of
  cohort. Losing money is never "fine relative to peers."

Why margin % **and** profit-per-stop together: margin alone is fooled by a high-revenue
bleeder or a tiny high-margin route — the exact trap leadership named. Pairing efficiency
(margin %) with unit economics scaled to work done (profit/stop) avoids it.

Data check backing this: underperformance is **concentrated** (22 of 120 routes are below
cohort on >70% of their days) and **structural, not episodic** (only ~3% of loss days have
an incident; maintenance cost on loss days is normal).

## 1.4 Acceptance (what "done" means)

The pipeline runs end-to-end from the committed CSV; Gold tables reproduce the headline
figures above (cohort medians, the ~22/120 concentration, the ~3% incident rate); re-runs
are idempotent. Each task in [`tasks.md`](./tasks.md) carries its own check.

---

# Part 2 — Plan

## 2.1 Pipeline — medallion layers

**Bronze — immutable landing.** Raw CSV → Delta with a hand-written `StructType`
(enforced, not inferred) so a malformed file fails loudly. Adds ingestion metadata
(`_ingested_at`, `_source_file`). No transforms — Bronze is the audit trail.

**Silver — one trustworthy row per route-day.** Dedup on `route_date_key` (PK), enforce
types, quarantine bad rows, handle division-by-zero (e.g. `completed_stops = 0` → metric
null + flagged, not a crash). **Recomputes** `net_revenue`, `gross_profit`,
`gross_margin_pct` from the cost components and keeps the source columns as `*_src` plus a
reconciliation flag — self-contained lineage; a future bad file surfaces instead of
flowing through. Adds derived metrics: `profit_per_stop`, `profit_per_km`,
`cost_per_tonne`, `completion_rate`, and `cohort_key`.

**Gold — three tables.**
- `fact_route_day` — atomic grain, the foundation.
- `fact_route_month` — aggregate of the above for BI speed; partition by `region`,
  OPTIMIZE + ZORDER(`bu`, `area`).
- `route_scorecard` — one row per route with the Tier 1/2 verdict (the Part 2 answer).

**Delta feature (primary): MERGE on `route_date_key`** — idempotent re-runs; re-processing
a day upserts instead of duplicating. Paired with **OPTIMIZE + ZORDER** to back the
partitioning/slicing story.

## 2.2 Dimensional model — star schema

- **Star, not snowflake.** Geography (`region/bu/area`) is flattened onto `dim_route`.
  Verified the hierarchy is strict and stable (every area → one BU → one region; no route
  ever moves), so denormalising carries no anomaly risk and gives BI tools fewer joins.
- **Natural keys, not surrogates.** Stable surrogate-key generation in distributed Spark
  is awkward (`monotonically_increasing_id` isn't stable across runs); MERGE on business
  keys gives clean idempotent upserts. `dim_date` uses a smart `yyyymmdd` key.
- **Dimensions:** `dim_date`, `dim_route`. **Facts:** `fact_route_day` (atomic) →
  `fact_route_month` (aggregate); `route_scorecard` (derived).

### 2.3 SCD policy — Type 1 now, when to go Type 2

Chosen **Type 1 (overwrite)** because the hierarchy is verified stable across 2022–2024
(0 routes change area/BU/region). Building versioned history for attributes that never move
would be cargo-cult.

**Go to Type 2 when** any of these become true:
- a `route_id` is reassigned to a different `area`, `bu`, or `region` over time;
- org restructure / M&A re-maps the geography hierarchy;
- the business needs point-in-time accuracy ("what BU did this route belong to *then*").

Delta's MERGE makes the Type 1 → Type 2 upgrade a small change (add effective-dating +
current-flag logic), not a rebuild.
