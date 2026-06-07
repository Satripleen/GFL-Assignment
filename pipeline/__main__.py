"""End-to-end pipeline driver — runs every layer in order against one Spark session.

    .venv/bin/python -m pipeline

Bronze -> Silver -> Gold dims -> Gold facts -> route_scorecard. Idempotent: safe
to re-run (MERGE upserts; counts stay stable). Each stage logs a one-line summary.
"""
from __future__ import annotations

import time

from lib import config, scorecard
from src import bronze, silver, gold_dims, gold_facts

log = config.get_logger(__name__)


def _count(spark, path) -> int:
    return spark.read.format("delta").load(str(path)).count()


def run() -> None:
    t0 = time.time()
    spark = config.get_spark("pipeline")
    spark.sparkContext.setLogLevel("ERROR")

    # Each layer owns its own build+persist in `<module>.run(spark)`; the pipeline
    # just sequences them and logs a one-line summary from the written tables.

    # Bronze --------------------------------------------------------------
    bronze.run(spark)
    log.info("[bronze] route_day              rows=%6s", f"{_count(spark, config.BRONZE_ROUTE_DAY):,}")

    # Silver --------------------------------------------------------------
    silver.run(spark)
    log.info("[silver] route_day              rows=%6s  quarantined=%d",
             f"{_count(spark, config.SILVER_ROUTE_DAY):,}", _count(spark, config.SILVER_QUARANTINE))

    # Gold dimensions -----------------------------------------------------
    gold_dims.run(spark)
    log.info("[gold]   dim_route               rows=%6s", f"{_count(spark, config.DIM_ROUTE):,}")
    log.info("[gold]   dim_date                rows=%6s", f"{_count(spark, config.DIM_DATE):,}")

    # Gold facts ----------------------------------------------------------
    gold_facts.run(spark)
    log.info("[gold]   fact_route_day          rows=%6s", f"{_count(spark, config.FACT_ROUTE_DAY):,}")
    log.info("[gold]   fact_route_month        rows=%6s", f"{_count(spark, config.FACT_ROUTE_MONTH):,}")

    # Scorecard -----------------------------------------------------------
    scorecard.run(spark)
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
