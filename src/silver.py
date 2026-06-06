"""Silver layer — one trustworthy row per route-day.

Reads Bronze and produces a single clean row per `route_date_key`:
  * dedup on the PK (latest ingest wins); reject null-PK / duplicate rows to a
    quarantine table instead of dropping them silently;
  * recompute net_revenue / gross_profit / gross_margin_pct (and total_cost)
    from the cost components, keeping the source values as *_src plus a
    `recon_flag` so a future bad file surfaces instead of flowing through;
  * guard every division (completed_stops, distance, tonnes, total_stops) so a
    zero denominator yields a null metric + `metric_null_flag`, never a crash;
  * add derived metrics and the `cohort_key`.

On this dataset the file is already clean, so the dedup / quarantine / guards /
recon are defensive (0 rejects, 0 mismatches) — they protect future loads.

    .venv/bin/python -m src.silver
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src import config

log = config.get_logger(__name__)

RECON_TOLERANCE = 0.02  # cents-level float tolerance


def _safe_div(num, den):
    """num/den, but null when the denominator is 0 or null (no div-by-zero)."""
    return F.when((den.isNull()) | (den == 0), None).otherwise(num / den)


def build_silver(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    bronze = spark.read.format("delta").load(str(config.BRONZE_ROUTE_DAY))

    # --- Quarantine: null PKs, and all-but-one of any duplicate PK group ------
    w = Window.partitionBy("route_date_key").orderBy(F.col("_ingested_at").desc())
    ranked = bronze.withColumn("_rn", F.row_number().over(w))
    null_pk = bronze.filter(F.col("route_date_key").isNull())
    dup_losers = ranked.filter((F.col("_rn") > 1)).drop("_rn")
    quarantine = (
        null_pk.withColumn("_reject_reason", F.lit("null_pk"))
        .unionByName(dup_losers.withColumn("_reject_reason", F.lit("duplicate_pk")))
    )

    clean = ranked.filter(
        (F.col("_rn") == 1) & F.col("route_date_key").isNotNull()
    ).drop("_rn")

    # --- Recompute from cost components; keep source as *_src ----------------
    total_cost_calc = (
        F.col("disposal_cost")
        + F.col("fuel_cost")
        + F.col("labour_cost")
        + F.col("maintenance_cost")
        + F.col("admin_cost")
    )
    s = (
        clean.withColumnRenamed("total_cost", "total_cost_src")
        .withColumnRenamed("net_revenue", "net_revenue_src")
        .withColumnRenamed("gross_profit", "gross_profit_src")
        .withColumnRenamed("gross_margin_pct", "gross_margin_pct_src")
        .withColumn("total_cost", total_cost_calc)
    )
    s = (
        s.withColumn("net_revenue", F.col("total_revenue") - F.col("disposal_cost"))
        .withColumn("gross_profit", F.col("total_revenue") - F.col("total_cost"))
        .withColumn(
            "gross_margin_pct",
            _safe_div(F.col("gross_profit") * 100, F.col("total_revenue")),
        )
    )

    # Reconciliation flag: recomputed vs source beyond tolerance
    recon = (
        (F.abs(F.col("total_cost") - F.col("total_cost_src")) > RECON_TOLERANCE)
        | (F.abs(F.col("net_revenue") - F.col("net_revenue_src")) > RECON_TOLERANCE)
        | (F.abs(F.col("gross_profit") - F.col("gross_profit_src")) > RECON_TOLERANCE)
    )
    s = s.withColumn("recon_flag", recon)

    # --- Guarded derived metrics --------------------------------------------
    s = (
        s.withColumn("profit_per_stop", _safe_div(F.col("gross_profit"), F.col("completed_stops")))
        .withColumn("profit_per_km", _safe_div(F.col("gross_profit"), F.col("total_distance_km")))
        .withColumn("cost_per_tonne", _safe_div(F.col("total_cost"), F.col("total_tonnes")))
        .withColumn("completion_rate", _safe_div(F.col("completed_stops"), F.col("total_stops")))
    )
    # Flag any row where a guarded denominator was zero/null (metric went null)
    metric_null = (
        (F.col("completed_stops").isNull() | (F.col("completed_stops") == 0))
        | (F.col("total_distance_km").isNull() | (F.col("total_distance_km") == 0))
        | (F.col("total_tonnes").isNull() | (F.col("total_tonnes") == 0))
        | (F.col("total_stops").isNull() | (F.col("total_stops") == 0))
    )
    s = s.withColumn("metric_null_flag", metric_null)

    # --- Cohort key ----------------------------------------------------------
    s = s.withColumn(
        "cohort_key",
        F.concat_ws(" | ", F.col("primary_waste_stream"), F.col("primary_customer_segment")),
    )

    return s, quarantine


if __name__ == "__main__":
    spark = config.get_spark("silver")
    spark.sparkContext.setLogLevel("ERROR")

    silver, quarantine = build_silver(spark)
    config.upsert_delta(spark, silver, config.SILVER_ROUTE_DAY, key_cols=["route_date_key"])
    quarantine.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(str(config.SILVER_QUARANTINE))

    out = spark.read.format("delta").load(str(config.SILVER_ROUTE_DAY))
    n = out.count()
    distinct_pk = out.select("route_date_key").distinct().count()
    q = spark.read.format("delta").load(str(config.SILVER_QUARANTINE)).count()
    recon_bad = out.filter(F.col("recon_flag")).count()
    null_metric = out.filter(F.col("metric_null_flag")).count()
    cohorts = out.select("cohort_key").distinct().count()

    log.info("Silver rows           = %s  (1 per route_date_key: %s)", f"{n:,}", f"{distinct_pk:,}")
    log.info("quarantined           = %d", q)
    log.info("recon mismatches      = %d", recon_bad)
    log.info("null-metric rows      = %d", null_metric)
    log.info("distinct cohorts      = %d", cohorts)
    assert n == distinct_pk == config.EXPECTED_SOURCE_ROWS, "PK grain broken"
    assert recon_bad == 0, "recompute disagrees with source beyond tolerance"
    log.info("OK — Silver built.")
    spark.stop()
