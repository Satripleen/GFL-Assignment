"""Shared configuration: filesystem paths and the Spark + Delta session builder.

Everything downstream (Bronze/Silver/Gold) imports from here so paths and the
session are defined once. Run this module directly as a smoke test:

    .venv/bin/python -m src.config
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# --- Paths -----------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
SOURCE_CSV = DATA_DIR / "gfl_commercial_routes.csv"

LAKEHOUSE = DATA_DIR / "lakehouse"
BRONZE = LAKEHOUSE / "bronze"
SILVER = LAKEHOUSE / "silver"
GOLD = LAKEHOUSE / "gold"

# Delta table locations (path-based tables — no external metastore needed)
BRONZE_ROUTE_DAY = BRONZE / "route_day"
SILVER_ROUTE_DAY = SILVER / "route_day"
SILVER_QUARANTINE = SILVER / "route_day_quarantine"
DIM_DATE = GOLD / "dim_date"
DIM_ROUTE = GOLD / "dim_route"
FACT_ROUTE_DAY = GOLD / "fact_route_day"
FACT_ROUTE_MONTH = GOLD / "fact_route_month"
ROUTE_SCORECARD = GOLD / "route_scorecard"

EXPECTED_SOURCE_ROWS = 12_000


# --- Logging ---------------------------------------------------------------
def get_logger(name: str = "gfl") -> logging.Logger:
    """Timestamped stdout logger, lazily configured so re-imports never stack
    duplicate handlers. Honours the LOG_LEVEL env var (default INFO)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s", "%H:%M:%S")
        )
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
        logger.propagate = False
    return logger


# --- Java ------------------------------------------------------------------
def _ensure_java_home() -> None:
    """Point JAVA_HOME at the Homebrew openjdk@17 if it isn't already set, so the
    pipeline runs without the caller having to export it first. Falls back
    silently to whatever `java` is on PATH."""
    if os.environ.get("JAVA_HOME"):
        return
    try:
        prefix = subprocess.check_output(
            ["brew", "--prefix", "openjdk@17"], text=True, stderr=subprocess.DEVNULL
        ).strip()
        candidate = Path(prefix) / "libexec/openjdk.jdk/Contents/Home"
        if candidate.exists():
            os.environ["JAVA_HOME"] = str(candidate)
    except Exception:
        pass


# --- Spark -----------------------------------------------------------------
def get_spark(app_name: str = "gfl-route-profitability") -> SparkSession:
    """Build a local Spark session with the Delta Lake extension enabled."""
    _ensure_java_home()
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        # Small dataset (12k rows) — keep shuffle partitions low so it stays snappy.
        .config("spark.sql.shuffle.partitions", "8")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def upsert_delta(
    spark: SparkSession,
    df: DataFrame,
    path,
    key_cols: list[str],
    partition_by: list[str] | None = None,
) -> None:
    """Idempotent MERGE on the business key(s) — the primary Delta feature.

    Re-processing the same rows upserts in place instead of duplicating, so
    re-runs leave row counts unchanged. Creates the table on first write."""
    path = str(path)
    if DeltaTable.isDeltaTable(spark, path):
        cond = " AND ".join(f"t.{k} = s.{k}" for k in key_cols)
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        writer = df.write.format("delta")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.mode("overwrite").save(path)


if __name__ == "__main__":
    # Task 0 acceptance check: session starts and reads the CSV at 12,000 rows.
    log = get_logger("config")
    spark = get_spark("config-smoke-test")
    log.info("JAVA_HOME = %s", os.environ.get("JAVA_HOME"))
    log.info("Spark %s  |  Delta extension enabled", spark.version)
    rows = spark.read.option("header", True).csv(str(SOURCE_CSV)).count()
    log.info("source CSV rows = %s  (expected %s)", f"{rows:,}", f"{EXPECTED_SOURCE_ROWS:,}")
    assert rows == EXPECTED_SOURCE_ROWS, f"row count mismatch: {rows}"
    log.info("OK — environment is good.")
    spark.stop()
