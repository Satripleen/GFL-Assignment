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
- [ ] Dedup on `route_date_key` (PK); quarantine table for rejects
- [ ] Type enforcement + null/range validation
- [ ] Division-by-zero guards (`completed_stops = 0`, `total_tonnes = 0`, etc. → null + flag)
- [ ] Recompute `net_revenue`, `gross_profit`, `gross_margin_pct` from cost components;
      keep source as `*_src` + `recon_flag`
- [ ] Derived: `profit_per_stop`, `profit_per_km`, `cost_per_tonne`, `completion_rate`, `cohort_key`
- **Acceptance:** exactly 1 row per `route_date_key`; recomputed `gross_profit` reconciles
  to `*_src` within tolerance for the vast majority of rows; zero-denominator rows produce
  null metrics (no crash) and are flagged.

## Task 3 — Gold dimensions + DDL
- [ ] `dim_route` — flattened geography, `cohort_key`; MERGE on `route_id`
- [ ] `dim_date` — smart `yyyymmdd` key, year/quarter/month/day_of_week
- [ ] Assert strict hierarchy (each area → 1 BU → 1 region; no route moves)
- **Acceptance:** `dim_route` = 120 routes; hierarchy assertion passes (0 violations);
  `dim_date` covers every date in 2022–2024 present in the data.

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
