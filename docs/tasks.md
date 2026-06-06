# GFL Commercial — Build Tasks

Ordered, checkable units. We implement one at a time, tick it, and commit it.
Spec: [`spec.md`](./spec.md) · Diagram: `design.svg`.

Legend: `[ ]` todo · `[~]` in progress · `[x]` done

---

## Task 0 — Land data + scaffold
- [x] `data/gfl_commercial_routes.csv` committed (12,000 rows + header)
- [x] `.gitignore` excludes the regenerable lakehouse + caches
- [x] `docs/spec.md` (SDD) and `docs/tasks.md` in place
- [x] `src/` config module: Spark+Delta session builder, path constants
- **Acceptance:** ✅ `pip install -r requirements.txt` clean; `python -m src.config`
  starts a Spark+Delta session (auto-resolves JAVA_HOME) and reads the CSV at 12,000 rows.

## Task 1 — Bronze (immutable landing)
- [x] Hand-written `StructType` for all 39 source columns (enforced, not inferred)
- [x] CSV → Delta at `data/lakehouse/bronze/route_day` (overwrite — immutable reload)
- [x] Ingestion metadata: `_ingested_at`, `_source_file`
- [x] No transforms
- **Acceptance:** ✅ Bronze table = 12,000 rows, 41 cols (39 + 2 metadata); FAILFAST +
  enforced schema raises `BadRecordException` on a bad type at write (verified) rather
  than nulling; both metadata columns present and populated.

## Task 2 — Silver (one trustworthy row per route-day)
- [x] Dedup on `route_date_key` (PK, latest ingest wins); quarantine table for rejects
- [x] Type enforcement (from Bronze schema) + null-PK validation
- [x] Division-by-zero guards (`completed_stops`/`total_stops`/`distance`/`tonnes` → null + `metric_null_flag`)
- [x] Recompute `total_cost`, `net_revenue`, `gross_profit`, `gross_margin_pct` from cost
      components; keep source as `*_src` + `recon_flag`
- [x] Derived: `profit_per_stop`, `profit_per_km`, `cost_per_tonne`, `completion_rate`, `cohort_key`
- **Acceptance:** ✅ exactly 1 row per `route_date_key` (12,000 = distinct PKs); recompute
  matches source **100%** (0 recon mismatches — all 4 formulas verified across 12,000 rows);
  guards/dedup/quarantine are defensive (this file is clean: 0 quarantined, 0 null-metric).
  Idempotent MERGE verified (re-run → Delta `MERGE`, count stays 12,000). 21 cohorts.

## Task 3 — Gold dimensions + DDL
- [x] `dim_route` — flattened geography, `cohort_key`; MERGE on `route_id`
- [x] `dim_date` — smart `yyyymmdd` key, year/quarter/month/day_of_week; MERGE on `date_key`
- [x] Assert strict hierarchy (each route_id → 1 region/bu/area/cohort across all years)
- **Acceptance:** ✅ `dim_route` = 120 routes (unique PK); strict-hierarchy assertion
  passed (0 violators — proves SCD Type 1 is valid); `dim_date` = 1,012 days (every date
  present in the data) spanning 2022-01-01 .. 2024-12-31.

## Task 4 — Gold facts
- [ ] `fact_route_day` — atomic grain, FKs to `dim_date`/`dim_route`; MERGE on `route_date_key`
- [ ] `fact_route_month` — rolled-up sums + volume-weighted margin; partition by `region`;
      OPTIMIZE + ZORDER(`bu`, `area`)
- **Acceptance:** `fact_route_day` = 12,000; **re-running the pipeline leaves row counts
  unchanged** (idempotent MERGE); month-level sums tie back to day-level sums.

## Task 5 — route_scorecard (the Part 2 verdict)
- [ ] Per-route: `median_margin_pct`, `pct_days_below_peer`, `loss_day_rate`,
      `cohort_median_margin`
- [ ] Tier assignment: 1 Loss-making / 2 Margin leak / OK
- **Acceptance:** ~22 of 120 routes flagged below cohort on >70% of days; every route gets a
  tier; Tier 1 routes have negative median profit / high loss-day rate.

## Task 6 — Analysis notebook
- [ ] Cohort margin gap, concentration of underperformance, structural-vs-episodic evidence
- **Acceptance:** reproduces 35.6% General Waste / 76.5% Cardboard cohort medians and the
  ~3% loss-days-with-incident figure from the committed data.

## Task 7 — README
- [ ] Run instructions, design summary, how to reproduce, results overview
- **Acceptance:** a fresh clone → documented single command runs the full pipeline end to
  end and produces the Gold tables.
