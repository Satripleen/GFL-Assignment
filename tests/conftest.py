"""Shared pytest fixtures for the GFL pipeline test suite.

A single Spark+Delta session is built once per test session (Spark startup is the
slow part, ~5s) and reused. Synthetic-row helpers let each test construct exactly
the rows it needs against the real Bronze schema, so the tests don't depend on the
committed 12k-row CSV.
"""
from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from lib import config
from src.bronze import SOURCE_SCHEMA

# Bronze adds these two metadata columns on top of the 39 source columns.
# Build a NEW StructType — StructType.add() mutates in place, which would
# corrupt the imported SOURCE_SCHEMA for every other test.
BRONZE_SCHEMA = StructType(
    list(SOURCE_SCHEMA.fields)
    + [
        StructField("_ingested_at", TimestampType(), True),
        StructField("_source_file", StringType(), True),
    ]
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """One Delta-enabled local Spark session shared across the whole test run."""
    spark = config.get_spark("gfl-tests")
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# --- Synthetic row helpers -------------------------------------------------
# A baseline, internally-consistent route-day. Tests override only the fields
# they care about via make_source_row(**overrides).
_BASELINE: dict = dict(
    route_date_key="RDK-0000001",
    date=date(2023, 1, 2),
    year=2023,
    month=1,
    quarter="Q1",
    day_of_week="Monday",
    region="Atlantic",
    bu="Atlantic BU",
    area="Moncton",
    route_id="RTE-0001",
    primary_waste_stream="Recycling",
    primary_customer_segment="Office & Commercial",
    num_drivers=1,
    num_trucks=1,
    total_stops=100,
    completed_stops=100,
    missed_stops=0,
    total_distance_km=200.0,
    total_fuel_used_litres=100.0,
    total_labour_hours=15.0,
    total_yards=220.0,
    total_tonnes=20.0,
    avg_revenue_per_stop=50.0,
    total_revenue=5000.0,
    disposal_cost=500.0,
    fuel_cost=400.0,
    labour_cost=600.0,
    maintenance_cost=100.0,
    admin_cost=200.0,
    # cost/profit columns below are the *source* values; Silver recomputes them.
    total_cost=1800.0,            # 500+400+600+100+200
    net_revenue=4500.0,           # 5000-500
    gross_profit=3200.0,          # 5000-1800
    gross_margin_pct=64.0,        # 3200/5000*100
    scheduled_hours=15.0,
    actual_hours=15.0,
    delay_minutes=0,
    on_time_flag=1,
    incident_flag=0,
    incident_type=None,
)

_SOURCE_FIELDS = [f.name for f in SOURCE_SCHEMA.fields]


def make_source_row(**overrides) -> tuple:
    """Build one source-schema row (a tuple in SOURCE_SCHEMA field order).

    Pass any column as a keyword to override the consistent baseline, e.g.
    make_source_row(route_id="RTE-0002", total_revenue=0.0).
    """
    unknown = set(overrides) - set(_SOURCE_FIELDS)
    assert not unknown, f"unknown column(s): {unknown}"
    row = {**_BASELINE, **overrides}
    return tuple(row[name] for name in _SOURCE_FIELDS)


@pytest.fixture
def make_source():
    return make_source_row
