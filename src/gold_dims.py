"""Gold dimensions — dim_route and dim_date (star schema, natural keys, SCD Type 1).

dim_route flattens the geography hierarchy (region/bu/area) onto the route, which
is only safe because the hierarchy is strict and stable. We *assert* that here —
each route_id maps to exactly one (region, bu, area, waste_stream, segment) across
all of 2022-2024 — so the Type 1 / flattened design is verified, not assumed.

dim_date is a conformed calendar with a smart yyyymmdd integer key.

Both are written with MERGE on the natural key (Type 1 overwrite -> upgrade to
Type 2 is then a small change, not a rebuild).

    .venv/bin/python -m src.gold_dims
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lib import config

log = config.get_logger(__name__)

ROUTE_ATTRS = [
    "region",
    "bu",
    "area",
    "primary_waste_stream",
    "primary_customer_segment",
    "cohort_key",
]


def assert_strict_hierarchy(silver: DataFrame) -> int:
    """Fail loudly if any route_id ever has more than one attribute combination
    (i.e. a route moved area/BU/region or changed cohort). Returns route count."""
    per_route = silver.groupBy("route_id").agg(
        *[F.countDistinct(c).alias(c) for c in ROUTE_ATTRS]
    )
    violators = per_route.filter(
        " OR ".join(f"{c} > 1" for c in ROUTE_ATTRS)
    )
    n_bad = violators.count()
    if n_bad:
        violators.show(truncate=False)
        raise AssertionError(
            f"{n_bad} route(s) have a non-stable hierarchy — SCD Type 1 invalid; "
            f"see spec 2.3 for the Type 2 upgrade trigger."
        )
    return per_route.count()


def build_dim_route(silver: DataFrame) -> DataFrame:
    return silver.select("route_id", *ROUTE_ATTRS).dropDuplicates(["route_id"])


def build_dim_date(silver: DataFrame) -> DataFrame:
    return (
        silver.select("date")
        .distinct()
        .withColumn(
            "date_key",
            F.year("date") * 10000 + F.month("date") * 100 + F.dayofmonth("date"),
        )
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("date")))
        .withColumn("month", F.month("date"))
        .withColumn("day_of_week", F.date_format("date", "EEEE"))
        .select("date_key", "date", "year", "quarter", "month", "day_of_week")
    )


if __name__ == "__main__":
    spark = config.get_spark("gold-dims")
    spark.sparkContext.setLogLevel("ERROR")
    silver = spark.read.format("delta").load(str(config.SILVER_ROUTE_DAY))

    n_routes = assert_strict_hierarchy(silver)
    log.info("strict-hierarchy assertion PASSED — %d routes, 0 violators", n_routes)

    dim_route = build_dim_route(silver)
    dim_date = build_dim_date(silver)
    config.upsert_delta(spark, dim_route, config.DIM_ROUTE, key_cols=["route_id"])
    config.upsert_delta(spark, dim_date, config.DIM_DATE, key_cols=["date_key"])

    r = spark.read.format("delta").load(str(config.DIM_ROUTE))
    d = spark.read.format("delta").load(str(config.DIM_DATE))
    nr, nd = r.count(), d.count()
    span = d.agg(F.min("date").alias("lo"), F.max("date").alias("hi")).first()
    log.info("dim_route rows = %d  (distinct route_id = %d)", nr, r.select("route_id").distinct().count())
    log.info("dim_date  rows = %d  span %s .. %s", nd, span["lo"], span["hi"])
    assert nr == r.select("route_id").distinct().count(), "dim_route PK not unique"
    assert nd == d.select("date_key").distinct().count(), "dim_date PK not unique"
    log.info("OK — Gold dimensions built.")
    spark.stop()
