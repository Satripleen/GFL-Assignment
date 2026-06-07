-- =============================================================================
-- GFL Commercial — Route Profitability: Gold star-schema DDL (Delta Lake)
-- =============================================================================
-- Part 3 deliverable. These statements mirror exactly the path-based Delta
-- tables the pipeline builds under data/lakehouse/gold/ (see lib/config.py).
--
-- The pipeline itself creates these tables programmatically via DataFrameWriter
-- + MERGE (idempotent upserts) rather than running this DDL — it is provided as
-- the explicit schema contract for the dimensional model, and so the tables can
-- be (re)created in a metastore-backed environment (e.g. Databricks/Unity
-- Catalog) if desired.
--
-- ERD: docs/design.png (also docs/design.svg).
--
-- How Delta Lake influenced these design choices
-- ----------------------------------------------------------------------------
--  * Natural keys, not surrogates. Stable surrogate-key generation in
--    distributed Spark is awkward; Delta MERGE on the business key gives clean
--    idempotent upserts, so the PKs below are natural keys (route_id,
--    date_key = yyyymmdd, route_date_key).
--  * Star, not snowflake. The region/bu/area hierarchy is verified strict and
--    stable, so it is flattened onto dim_route (SCD Type 1) — fewer joins for BI.
--  * fact_route_month is PARTITIONED BY (region) and ZORDERed by (bu, area) to
--    back efficient slicing by region/BU/area (see OPTIMIZE at the end).
--  * Delta is schema-on-write: the column types here are enforced, not inferred.
-- =============================================================================


-- =============================================================================
-- DIMENSIONS
-- =============================================================================

-- dim_date — conformed calendar; smart yyyymmdd integer key (PK).
CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INT     NOT NULL COMMENT 'PK — yyyymmdd integer (e.g. 20240115)',
    date         DATE    NOT NULL,
    year         INT     NOT NULL,
    quarter      STRING  NOT NULL COMMENT 'Q1..Q4',
    month        INT     NOT NULL,
    day_of_week  STRING  NOT NULL
)
USING DELTA
LOCATION 'data/lakehouse/gold/dim_date'
COMMENT 'Date dimension — one row per calendar date in the 2022–2024 window.';


-- dim_route — route dimension with the geography hierarchy flattened on
-- (SCD Type 1; hierarchy verified stable). route_id is the PK.
CREATE TABLE IF NOT EXISTS dim_route (
    route_id                  STRING NOT NULL COMMENT 'PK — natural key',
    region                    STRING NOT NULL,
    bu                        STRING NOT NULL COMMENT 'Business unit',
    area                      STRING NOT NULL,
    primary_waste_stream      STRING NOT NULL COMMENT 'General Waste | Recycling | Organics | Cardboard',
    primary_customer_segment  STRING NOT NULL,
    cohort_key                STRING NOT NULL COMMENT 'waste_stream | customer_segment — peer-cohort grouping'
)
USING DELTA
LOCATION 'data/lakehouse/gold/dim_route'
COMMENT 'Route dimension — one row per route; geography flattened (SCD Type 1).';


-- =============================================================================
-- FACTS
-- =============================================================================

-- fact_route_day — atomic grain: one row per route per day.
-- FKs: date_key -> dim_date.date_key, route_id -> dim_route.route_id.
CREATE TABLE IF NOT EXISTS fact_route_day (
    route_date_key          STRING NOT NULL COMMENT 'PK / business key — one row per route per day',
    date_key                INT    NOT NULL COMMENT 'FK -> dim_date.date_key',
    route_id                STRING NOT NULL COMMENT 'FK -> dim_route.route_id',
    -- crew & fleet
    num_drivers             INT,
    num_trucks              INT,
    -- stops
    total_stops             INT,
    completed_stops         INT,
    missed_stops            INT,
    -- operations
    total_distance_km       DOUBLE,
    total_fuel_used_litres  DOUBLE,
    total_labour_hours      DOUBLE,
    total_yards             DOUBLE,
    total_tonnes            DOUBLE,
    -- revenue & cost components
    avg_revenue_per_stop    DOUBLE,
    total_revenue           DOUBLE,
    disposal_cost           DOUBLE,
    fuel_cost               DOUBLE,
    labour_cost             DOUBLE,
    maintenance_cost        DOUBLE,
    admin_cost              DOUBLE,
    total_cost              DOUBLE COMMENT 'Recomputed from components in Silver',
    net_revenue             DOUBLE COMMENT 'Recomputed: total_revenue - disposal_cost',
    gross_profit            DOUBLE COMMENT 'Recomputed: total_revenue - total_cost',
    gross_margin_pct        DOUBLE COMMENT 'Recomputed: gross_profit*100 / total_revenue (guarded)',
    -- schedule adherence
    scheduled_hours         DOUBLE,
    actual_hours            DOUBLE,
    delay_minutes           INT,
    -- derived unit economics (Silver)
    profit_per_stop         DOUBLE,
    profit_per_km           DOUBLE,
    cost_per_tonne          DOUBLE,
    completion_rate         DOUBLE,
    -- flags
    on_time_flag            INT,
    incident_flag           INT,
    incident_type           STRING,
    metric_null_flag        BOOLEAN COMMENT 'A guarded denominator was 0/null',
    recon_flag              BOOLEAN COMMENT 'Recomputed measure disagreed with source *_src'
)
USING DELTA
LOCATION 'data/lakehouse/gold/fact_route_day'
COMMENT 'Atomic fact — one row per route-day; FKs to dim_date and dim_route.';


-- fact_route_month — aggregate (route x month) for BI speed.
-- Carries region/bu/area so it can be partitioned by region and ZORDERed by
-- (bu, area). Additive measures are sums; gross_margin_pct is revenue-weighted.
CREATE TABLE IF NOT EXISTS fact_route_month (
    route_id          STRING NOT NULL COMMENT 'FK -> dim_route.route_id',
    region            STRING NOT NULL COMMENT 'Partition column',
    bu                STRING NOT NULL,
    area              STRING NOT NULL,
    year              INT    NOT NULL,
    month             INT    NOT NULL,
    days_active       BIGINT COMMENT 'Count of route-days in the month',
    loss_days         BIGINT COMMENT 'Route-days with gross_profit < 0',
    total_stops       BIGINT,
    completed_stops   BIGINT,
    missed_stops      BIGINT,
    total_distance_km DOUBLE,
    total_fuel_used_litres DOUBLE,
    total_labour_hours DOUBLE,
    total_yards       DOUBLE,
    total_tonnes      DOUBLE,
    total_revenue     DOUBLE,
    disposal_cost     DOUBLE,
    fuel_cost         DOUBLE,
    labour_cost       DOUBLE,
    maintenance_cost  DOUBLE,
    admin_cost        DOUBLE,
    total_cost        DOUBLE,
    net_revenue       DOUBLE,
    gross_profit      DOUBLE,
    gross_margin_pct  DOUBLE COMMENT 'Revenue-weighted: sum(gross_profit)*100 / sum(total_revenue)',
    month_key         INT    COMMENT 'yyyymm integer'
)
USING DELTA
PARTITIONED BY (region)
LOCATION 'data/lakehouse/gold/fact_route_month'
COMMENT 'Aggregate fact — route x month; partitioned by region, ZORDER (bu, area).';


-- =============================================================================
-- DERIVED / SERVING
-- =============================================================================

-- route_scorecard — the Part 2 verdict: one row per route with a Tier rating.
CREATE TABLE IF NOT EXISTS route_scorecard (
    route_id                       STRING NOT NULL COMMENT 'PK -> dim_route.route_id',
    cohort_key                     STRING,
    n_days                         BIGINT,
    median_margin_pct              DOUBLE,
    median_profit_per_stop         DOUBLE,
    median_gross_profit            DOUBLE,
    loss_day_rate                  DOUBLE COMMENT 'Fraction of days with gross_profit < 0',
    pct_days_below_peer            DOUBLE COMMENT 'Fraction of days below cohort median margin',
    cohort_median_margin           DOUBLE,
    cohort_median_profit_per_stop  DOUBLE,
    below_peer_flag                BOOLEAN COMMENT 'pct_days_below_peer > 0.70',
    tier                           STRING  COMMENT 'Tier 1 - Loss-making | Tier 2 - Margin leak | OK',
    tier_code                      INT     COMMENT '1 | 2 | 0'
)
USING DELTA
LOCATION 'data/lakehouse/gold/route_scorecard'
COMMENT 'Per-route profitability verdict (Part 2 answer).';


-- =============================================================================
-- Delta maintenance — compaction + data-skipping to back region/BU/area slicing
-- (the pipeline runs this after writing fact_route_month).
-- =============================================================================
-- OPTIMIZE fact_route_month ZORDER BY (bu, area);
