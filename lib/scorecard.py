"""route_scorecard — the Part 2 verdict: one row per route, with a Tier 1/2 rating.

Underperformance is cohort-relative and persistence-based (spec 1.3). The exact
knobs below were pinned by reproducing the spec's published figure: "22 of 120
routes below cohort on >70% of their days".

  * Cohort        = primary_waste_stream x primary_customer_segment (on dim_route).
  * Below-peer    = a route-day whose gross_margin_pct is below its cohort's median.
  * Persistence   = a route flagged when below-peer on > BELOW_PEER_THRESHOLD of days.
  * Tier 1 (Loss-making) : median gross_profit < 0 OR loss_day_rate > LOSS_DAY_RATE_THRESHOLD.
  * Tier 2 (Margin leak) : persistently below peers but not loss-making.
  * OK            : neither.

profit_per_stop (per route and per cohort) is carried alongside the verdict for
inspection (spec 1.3) — a sanity read on the unit economics behind each margin.
Note it is *not* wired into the tier rule itself: the classification is currently
margin/profit-driven (median gross profit, loss-day rate, below-cohort persistence).

    .venv/bin/python -m lib.scorecard
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lib import config

log = config.get_logger(__name__)

BELOW_PEER_THRESHOLD = 0.70
LOSS_DAY_RATE_THRESHOLD = 0.50

# Expected figures for the *committed* dataset (data/gfl_commercial_routes.csv),
# matching the spec's published numbers. These are a sanity check against that one
# file, NOT invariants — a refreshed data drop will legitimately change them, so
# the standalone smoke test warns on a mismatch instead of crashing.
COMMITTED_DATASET_BASELINE = {"routes": 120, "below_peer": 22, "tier1": 4, "tier2": 18}


def build_scorecard(spark: SparkSession) -> DataFrame:
    fd = spark.read.format("delta").load(str(config.FACT_ROUTE_DAY))
    dr = spark.read.format("delta").load(str(config.DIM_ROUTE))
    df = fd.join(dr.select("route_id", "cohort_key"), "route_id")

    # Cohort-level medians (the peer benchmark).
    cohort = df.groupBy("cohort_key").agg(
        F.percentile_approx("gross_margin_pct", 0.5).alias("cohort_median_margin"),
        F.percentile_approx("profit_per_stop", 0.5).alias("cohort_median_profit_per_stop"),
    )

    flagged = df.join(cohort, "cohort_key").withColumn(
        "below_peer_day",
        (F.col("gross_margin_pct") < F.col("cohort_median_margin")).cast("int"),
    )

    per_route = flagged.groupBy("route_id", "cohort_key").agg(
        F.count(F.lit(1)).alias("n_days"),
        F.percentile_approx("gross_margin_pct", 0.5).alias("median_margin_pct"),
        F.percentile_approx("profit_per_stop", 0.5).alias("median_profit_per_stop"),
        F.percentile_approx("gross_profit", 0.5).alias("median_gross_profit"),
        F.avg((F.col("gross_profit") < 0).cast("int")).alias("loss_day_rate"),
        F.avg("below_peer_day").alias("pct_days_below_peer"),
        F.first("cohort_median_margin").alias("cohort_median_margin"),
        F.first("cohort_median_profit_per_stop").alias("cohort_median_profit_per_stop"),
    )

    is_loss_making = (F.col("median_gross_profit") < 0) | (
        F.col("loss_day_rate") > LOSS_DAY_RATE_THRESHOLD
    )
    is_below_peer = F.col("pct_days_below_peer") > BELOW_PEER_THRESHOLD

    return (
        per_route.withColumn("below_peer_flag", is_below_peer)
        .withColumn(
            "tier",
            F.when(is_loss_making, F.lit("Tier 1 - Loss-making"))
            .when(is_below_peer, F.lit("Tier 2 - Margin leak"))
            .otherwise(F.lit("OK")),
        )
        .withColumn(
            "tier_code",
            F.when(is_loss_making, F.lit(1))
            .when(is_below_peer, F.lit(2))
            .otherwise(F.lit(0)),
        )
    )


def run(spark: SparkSession) -> None:
    """Build and persist route_scorecard via MERGE (the step the pipeline calls)."""
    config.upsert_delta(spark, build_scorecard(spark), config.ROUTE_SCORECARD, key_cols=["route_id"])


if __name__ == "__main__":
    spark = config.get_spark("scorecard")
    spark.sparkContext.setLogLevel("ERROR")

    run(spark)

    out = spark.read.format("delta").load(str(config.ROUTE_SCORECARD))
    dim_route = spark.read.format("delta").load(str(config.DIM_ROUTE))
    n = out.count()
    counts = {r["tier"]: r["count"] for r in out.groupBy("tier").count().collect()}
    below = out.filter(F.col("below_peer_flag")).count()
    log.info("route_scorecard rows = %d", n)
    log.info("below-peer (>70%% of days) = %d", below)
    for t in ["Tier 1 - Loss-making", "Tier 2 - Margin leak", "OK"]:
        log.info("  %-24s: %d", t, counts.get(t, 0))

    # --- Structural invariants — true for any dataset, so these stay hard ----
    n_routes = dim_route.select("route_id").distinct().count()
    assert n == n_routes, f"scorecard must have one row per route ({n} vs {n_routes})"
    assert out.filter(F.col("tier").isNull()).count() == 0, "every route needs a tier"

    # --- Committed-dataset baseline — warn (don't crash) on a data drop ------
    actual = {
        "routes": n,
        "below_peer": below,
        "tier1": counts.get("Tier 1 - Loss-making", 0),
        "tier2": counts.get("Tier 2 - Margin leak", 0),
    }
    if actual == COMMITTED_DATASET_BASELINE:
        log.info("OK — matches the committed-dataset baseline %s", COMMITTED_DATASET_BASELINE)
    else:
        log.warning(
            "scorecard figures differ from the committed-dataset baseline: expected %s, "
            "got %s — expected if the source data changed.",
            COMMITTED_DATASET_BASELINE, actual,
        )
    log.info("OK — route_scorecard built.")
    spark.stop()
