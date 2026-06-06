"""Bronze layer — immutable raw landing.

Raw CSV -> Delta with a hand-written, enforced schema (not inferred) so a
malformed file fails loudly rather than silently nulling. No transforms: Bronze
is the audit trail. Adds ingestion metadata only.

    .venv/bin/python -m src.bronze
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src import config

log = config.get_logger(__name__)

# Hand-written schema for all 39 source columns — enforced, never inferred.
SOURCE_SCHEMA = StructType(
    [
        StructField("route_date_key", StringType(), False),
        StructField("date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("quarter", StringType(), False),
        StructField("day_of_week", StringType(), False),
        StructField("region", StringType(), False),
        StructField("bu", StringType(), False),
        StructField("area", StringType(), False),
        StructField("route_id", StringType(), False),
        StructField("primary_waste_stream", StringType(), False),
        StructField("primary_customer_segment", StringType(), False),
        StructField("num_drivers", IntegerType(), True),
        StructField("num_trucks", IntegerType(), True),
        StructField("total_stops", IntegerType(), True),
        StructField("completed_stops", IntegerType(), True),
        StructField("missed_stops", IntegerType(), True),
        StructField("total_distance_km", DoubleType(), True),
        StructField("total_fuel_used_litres", DoubleType(), True),
        StructField("total_labour_hours", DoubleType(), True),
        StructField("total_yards", DoubleType(), True),
        StructField("total_tonnes", DoubleType(), True),
        StructField("avg_revenue_per_stop", DoubleType(), True),
        StructField("total_revenue", DoubleType(), True),
        StructField("disposal_cost", DoubleType(), True),
        StructField("fuel_cost", DoubleType(), True),
        StructField("labour_cost", DoubleType(), True),
        StructField("maintenance_cost", DoubleType(), True),
        StructField("admin_cost", DoubleType(), True),
        StructField("total_cost", DoubleType(), True),
        StructField("net_revenue", DoubleType(), True),
        StructField("gross_profit", DoubleType(), True),
        StructField("gross_margin_pct", DoubleType(), True),
        StructField("scheduled_hours", DoubleType(), True),
        StructField("actual_hours", DoubleType(), True),
        StructField("delay_minutes", IntegerType(), True),
        StructField("on_time_flag", IntegerType(), True),
        StructField("incident_flag", IntegerType(), True),
        StructField("incident_type", StringType(), True),
    ]
)


def read_source(spark: SparkSession, path: str) -> DataFrame:
    """Read the raw CSV under the enforced schema in FAILFAST mode — a row that
    does not conform to the declared types raises instead of being nulled."""
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(SOURCE_SCHEMA)
        .csv(path)
    )


def build_bronze(spark: SparkSession) -> DataFrame:
    """Land the source CSV into the Bronze Delta table with ingestion metadata."""
    df = read_source(spark, str(config.SOURCE_CSV))
    bronze = df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.input_file_name()
    )
    (
        bronze.write.format("delta")
        .mode("overwrite")  # immutable full reload of the source file each run
        .option("overwriteSchema", "true")
        .save(str(config.BRONZE_ROUTE_DAY))
    )
    return bronze


if __name__ == "__main__":
    spark = config.get_spark("bronze")
    build_bronze(spark)

    bronze = spark.read.format("delta").load(str(config.BRONZE_ROUTE_DAY))
    n = bronze.count()
    log.info("Bronze rows = %s  (expected %s)", f"{n:,}", f"{config.EXPECTED_SOURCE_ROWS:,}")
    assert n == config.EXPECTED_SOURCE_ROWS, f"row count mismatch: {n}"

    meta = [c for c in ("_ingested_at", "_source_file") if c in bronze.columns]
    log.info("ingestion metadata present: %s", meta)
    assert len(meta) == 2, "missing ingestion metadata"
    assert bronze.filter(F.col("_source_file").isNull()).count() == 0

    log.info("columns = %d (39 source + 2 metadata)", len(bronze.columns))
    assert len(bronze.columns) == 41
    log.info("OK — Bronze landed.")
    spark.stop()
