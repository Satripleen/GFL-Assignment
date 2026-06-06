"""End-to-end pipeline driver — runs every layer in order against one Spark session.

    .venv/bin/python -m src.pipeline

Bronze -> Silver -> Gold dims -> Gold facts -> route_scorecard. Idempotent: safe
to re-run (MERGE upserts; counts stay stable). Each stage prints a one-line summary.
"""
from __future__ import annotations

import time

from pyspark.sql import functions as F

from src import config
from src import bronze, silver, gold_dims, gold_facts, scorecard


def _count(spark, path) -> int:
    return spark.read.format("delta").load(str(path)).count()


def run() -> None:
    t0 = time.time()
    spark = config.get_spark("pipeline")
    spark.sparkContext.setLogLevel("ERROR")

    # Bronze --------------------------------------------------------------
    bronze.build_bronze(spark)
    print(f"[bronze] route_day              rows={_count(spark, config.BRONZE_ROUTE_DAY):>6,}")

    # Silver --------------------------------------------------------------
    sv, quarantine = silver.build_silver(spark)
    config.upsert_delta(spark, sv, config.SILVER_ROUTE_DAY, key_cols=["route_date_key"])
    (quarantine.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save(str(config.SILVER_QUARANTINE)))
    print(f"[silver] route_day              rows={_count(spark, config.SILVER_ROUTE_DAY):>6,}"
          f"  quarantined={_count(spark, config.SILVER_QUARANTINE)}")

    # Gold dimensions -----------------------------------------------------
    sv_tbl = spark.read.format("delta").load(str(config.SILVER_ROUTE_DAY))
    gold_dims.assert_strict_hierarchy(sv_tbl)
    config.upsert_delta(spark, gold_dims.build_dim_route(sv_tbl), config.DIM_ROUTE, key_cols=["route_id"])
    config.upsert_delta(spark, gold_dims.build_dim_date(sv_tbl), config.DIM_DATE, key_cols=["date_key"])
    print(f"[gold]   dim_route               rows={_count(spark, config.DIM_ROUTE):>6,}")
    print(f"[gold]   dim_date                rows={_count(spark, config.DIM_DATE):>6,}")

    # Gold facts ----------------------------------------------------------
    fact_day = gold_facts.build_fact_route_day(sv_tbl)
    config.upsert_delta(spark, fact_day, config.FACT_ROUTE_DAY, key_cols=["route_date_key"])
    fact_day_tbl = spark.read.format("delta").load(str(config.FACT_ROUTE_DAY))
    dim_route = spark.read.format("delta").load(str(config.DIM_ROUTE))
    fact_month = gold_facts.build_fact_route_month(fact_day_tbl, dim_route)
    (fact_month.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").partitionBy("region").save(str(config.FACT_ROUTE_MONTH)))
    spark.sql(f"OPTIMIZE delta.`{config.FACT_ROUTE_MONTH}` ZORDER BY (bu, area)")
    print(f"[gold]   fact_route_day          rows={_count(spark, config.FACT_ROUTE_DAY):>6,}")
    print(f"[gold]   fact_route_month        rows={_count(spark, config.FACT_ROUTE_MONTH):>6,}")

    # Scorecard -----------------------------------------------------------
    config.upsert_delta(spark, scorecard.build_scorecard(spark), config.ROUTE_SCORECARD, key_cols=["route_id"])
    sc = spark.read.format("delta").load(str(config.ROUTE_SCORECARD))
    tiers = {r["tier"]: r["count"] for r in sc.groupBy("tier").count().collect()}
    print(f"[gold]   route_scorecard         rows={sc.count():>6,}  "
          f"T1={tiers.get('Tier 1 - Loss-making', 0)} "
          f"T2={tiers.get('Tier 2 - Margin leak', 0)} "
          f"OK={tiers.get('OK', 0)}")

    print(f"\nPipeline complete in {time.time() - t0:.0f}s.")
    spark.stop()


if __name__ == "__main__":
    run()
