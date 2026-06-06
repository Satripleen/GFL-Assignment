"""End-to-end pipeline driver — runs every layer in order against one Spark session.

    .venv/bin/python -m src.pipeline

Bronze -> Silver -> Gold dims -> Gold facts -> route_scorecard. Idempotent: safe
to re-run (MERGE upserts; counts stay stable). Each stage prints a one-line summary.
"""
from __future__ import annotations

import time

from src import config
from src import bronze, silver, gold_dims, gold_facts, scorecard

log = config.get_logger(__name__)


def _count(spark, path) -> int:
    return spark.read.format("delta").load(str(path)).count()


def run() -> None:
    t0 = time.time()
    spark = config.get_spark("pipeline")
    spark.sparkContext.setLogLevel("ERROR")

    # Bronze --------------------------------------------------------------
    bronze.build_bronze(spark)
    log.info("[bronze] route_day              rows=%6s", f"{_count(spark, config.BRONZE_ROUTE_DAY):,}")

    # Silver --------------------------------------------------------------
    sv, quarantine = silver.build_silver(spark)
    config.upsert_delta(spark, sv, config.SILVER_ROUTE_DAY, key_cols=["route_date_key"])
    (quarantine.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save(str(config.SILVER_QUARANTINE)))
    log.info("[silver] route_day              rows=%6s  quarantined=%d",
             f"{_count(spark, config.SILVER_ROUTE_DAY):,}", _count(spark, config.SILVER_QUARANTINE))

    # Gold dimensions -----------------------------------------------------
    sv_tbl = spark.read.format("delta").load(str(config.SILVER_ROUTE_DAY))
    gold_dims.assert_strict_hierarchy(sv_tbl)
    config.upsert_delta(spark, gold_dims.build_dim_route(sv_tbl), config.DIM_ROUTE, key_cols=["route_id"])
    config.upsert_delta(spark, gold_dims.build_dim_date(sv_tbl), config.DIM_DATE, key_cols=["date_key"])
    log.info("[gold]   dim_route               rows=%6s", f"{_count(spark, config.DIM_ROUTE):,}")
    log.info("[gold]   dim_date                rows=%6s", f"{_count(spark, config.DIM_DATE):,}")

    # Gold facts ----------------------------------------------------------
    fact_day = gold_facts.build_fact_route_day(sv_tbl)
    config.upsert_delta(spark, fact_day, config.FACT_ROUTE_DAY, key_cols=["route_date_key"])
    fact_day_tbl = spark.read.format("delta").load(str(config.FACT_ROUTE_DAY))
    dim_route = spark.read.format("delta").load(str(config.DIM_ROUTE))
    fact_month = gold_facts.build_fact_route_month(fact_day_tbl, dim_route)
    (fact_month.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").partitionBy("region").save(str(config.FACT_ROUTE_MONTH)))
    spark.sql(f"OPTIMIZE delta.`{config.FACT_ROUTE_MONTH}` ZORDER BY (bu, area)")
    log.info("[gold]   fact_route_day          rows=%6s", f"{_count(spark, config.FACT_ROUTE_DAY):,}")
    log.info("[gold]   fact_route_month        rows=%6s", f"{_count(spark, config.FACT_ROUTE_MONTH):,}")

    # Scorecard -----------------------------------------------------------
    config.upsert_delta(spark, scorecard.build_scorecard(spark), config.ROUTE_SCORECARD, key_cols=["route_id"])
    sc = spark.read.format("delta").load(str(config.ROUTE_SCORECARD))
    tiers = {r["tier"]: r["count"] for r in sc.groupBy("tier").count().collect()}
    log.info("[gold]   route_scorecard         rows=%6s  T1=%d T2=%d OK=%d",
             f"{sc.count():,}",
             tiers.get("Tier 1 - Loss-making", 0),
             tiers.get("Tier 2 - Margin leak", 0),
             tiers.get("OK", 0))

    log.info("Pipeline complete in %.0fs.", time.time() - t0)
    spark.stop()


if __name__ == "__main__":
    run()
