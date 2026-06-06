"""Gold facts — fact_route_day (atomic) and fact_route_month (aggregate).

fact_route_day is the atomic grain: one row per route_date_key, carrying FKs to
dim_date / dim_route plus the measures. Dimensional attributes (region, cohort,
etc.) deliberately live in the dimensions, not the fact.

fact_route_month rolls the day fact up to route x month for BI speed: additive
sums + a revenue-weighted margin (sum(gross_profit)/sum(total_revenue)), carrying
region/bu/area so it can be partitioned by region and ZORDERed by (bu, area).

Both are idempotent: fact_route_day via MERGE on route_date_key; fact_route_month
via a partitioned full recompute (overwrite).

    .venv/bin/python -m src.gold_facts
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lib import config

log = config.get_logger(__name__)

# Measures carried at the atomic grain (everything that isn't a dim attribute).
FACT_MEASURES = [
    "num_drivers", "num_trucks",
    "total_stops", "completed_stops", "missed_stops",
    "total_distance_km", "total_fuel_used_litres", "total_labour_hours",
    "total_yards", "total_tonnes",
    "avg_revenue_per_stop", "total_revenue",
    "disposal_cost", "fuel_cost", "labour_cost", "maintenance_cost", "admin_cost",
    "total_cost", "net_revenue", "gross_profit", "gross_margin_pct",
    "scheduled_hours", "actual_hours", "delay_minutes",
    "profit_per_stop", "profit_per_km", "cost_per_tonne", "completion_rate",
    "on_time_flag", "incident_flag", "incident_type",
    "metric_null_flag", "recon_flag",
]

# Additive measures rolled up by sum at the month grain.
SUM_MEASURES = [
    "total_stops", "completed_stops", "missed_stops",
    "total_distance_km", "total_fuel_used_litres", "total_labour_hours",
    "total_yards", "total_tonnes", "total_revenue",
    "disposal_cost", "fuel_cost", "labour_cost", "maintenance_cost", "admin_cost",
    "total_cost", "net_revenue", "gross_profit",
]


def build_fact_route_day(silver: DataFrame) -> DataFrame:
    date_key = (
        F.year("date") * 10000 + F.month("date") * 100 + F.dayofmonth("date")
    )
    return silver.select(
        "route_date_key",
        date_key.alias("date_key"),   # FK -> dim_date
        "route_id",                   # FK -> dim_route
        *FACT_MEASURES,
    )


def build_fact_route_month(fact_day: DataFrame, dim_route: DataFrame) -> DataFrame:
    enriched = (
        fact_day.withColumn("year", (F.col("date_key") / 10000).cast("int"))
        .withColumn("month", ((F.col("date_key") / 100) % 100).cast("int"))
        .join(dim_route.select("route_id", "region", "bu", "area"), "route_id")
    )
    agg = enriched.groupBy("route_id", "region", "bu", "area", "year", "month").agg(
        F.count(F.lit(1)).alias("days_active"),
        F.sum((F.col("gross_profit") < 0).cast("int")).alias("loss_days"),
        *[F.sum(c).alias(c) for c in SUM_MEASURES],
    )
    # Revenue-weighted margin — the correct way to aggregate a ratio.
    return agg.withColumn(
        "gross_margin_pct",
        F.when(F.col("total_revenue") == 0, None).otherwise(
            F.col("gross_profit") * 100 / F.col("total_revenue")
        ),
    ).withColumn("month_key", F.col("year") * 100 + F.col("month"))


if __name__ == "__main__":
    spark = config.get_spark("gold-facts")
    spark.sparkContext.setLogLevel("ERROR")

    silver = spark.read.format("delta").load(str(config.SILVER_ROUTE_DAY))
    dim_route = spark.read.format("delta").load(str(config.DIM_ROUTE))

    fact_day = build_fact_route_day(silver)
    config.upsert_delta(spark, fact_day, config.FACT_ROUTE_DAY, key_cols=["route_date_key"])

    fact_day_tbl = spark.read.format("delta").load(str(config.FACT_ROUTE_DAY))
    fact_month = build_fact_route_month(fact_day_tbl, dim_route)
    (
        fact_month.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("region")
        .save(str(config.FACT_ROUTE_MONTH))
    )
    # Delta OPTIMIZE + ZORDER to back the slicing/partitioning story.
    spark.sql(f"OPTIMIZE delta.`{config.FACT_ROUTE_MONTH}` ZORDER BY (bu, area)")

    # --- Acceptance checks ---------------------------------------------------
    fd = spark.read.format("delta").load(str(config.FACT_ROUTE_DAY))
    fm = spark.read.format("delta").load(str(config.FACT_ROUTE_MONTH))
    nd = fd.count()
    log.info("fact_route_day rows   = %s  (expected %s)", f"{nd:,}", f"{config.EXPECTED_SOURCE_ROWS:,}")
    log.info("fact_route_month rows = %s", f"{fm.count():,}")
    assert nd == config.EXPECTED_SOURCE_ROWS, "atomic grain broken"

    # Month sums must tie back to day sums (additive measures).
    for c in ["total_revenue", "gross_profit", "total_cost", "completed_stops"]:
        day_total = fd.agg(F.sum(c)).first()[0]
        month_total = fm.agg(F.sum(c)).first()[0]
        diff = abs(float(day_total) - float(month_total))
        log.info(f"  tie {c:16s}: day={day_total:,.2f}  month={month_total:,.2f}  diff={diff:.4f}")
        assert diff < 0.5, f"{c} does not tie between day and month"
    log.info("OK — Gold facts built.")
    spark.stop()
