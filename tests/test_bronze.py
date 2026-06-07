"""Tests for the Bronze layer — enforced schema, FAILFAST, ingestion metadata."""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from lib import config
from src import bronze


def test_source_schema_has_39_columns():
    """The hand-written schema must cover exactly the 39 source columns."""
    assert len(bronze.SOURCE_SCHEMA.fields) == 39


def test_key_columns_are_non_nullable():
    """Identity/grain columns are declared NOT NULL in the enforced schema."""
    non_nullable = {f.name for f in bronze.SOURCE_SCHEMA.fields if not f.nullable}
    assert {"route_date_key", "date", "route_id", "region"} <= non_nullable


def test_read_source_reads_typed_csv(spark):
    """The committed CSV loads under the enforced schema at the expected count
    and with correctly-typed (not all-string) columns."""
    df = bronze.read_source(spark, str(config.SOURCE_CSV))
    assert df.count() == config.EXPECTED_SOURCE_ROWS
    dtypes = dict(df.dtypes)
    assert dtypes["date"] == "date"
    assert dtypes["total_revenue"] == "double"
    assert dtypes["total_stops"] == "int"


def test_read_source_failfast_rejects_malformed_row(spark, tmp_path):
    """A row whose typed column can't parse must raise, not silently null."""
    bad = tmp_path / "bad.csv"
    bad.write_text(
        "route_date_key,date,year,month,quarter,day_of_week,region,bu,area,route_id,"
        "primary_waste_stream,primary_customer_segment,num_drivers,num_trucks,total_stops,"
        "completed_stops,missed_stops,total_distance_km,total_fuel_used_litres,total_labour_hours,"
        "total_yards,total_tonnes,avg_revenue_per_stop,total_revenue,disposal_cost,fuel_cost,"
        "labour_cost,maintenance_cost,admin_cost,total_cost,net_revenue,gross_profit,"
        "gross_margin_pct,scheduled_hours,actual_hours,delay_minutes,on_time_flag,incident_flag,incident_type\n"
        # num_drivers = "NOT_AN_INT" -> FAILFAST should raise on the action below
        "RDK-1,2023-01-01,2023,1,Q1,Sunday,Atlantic,Atlantic BU,Moncton,RTE-0001,"
        "Recycling,Office & Commercial,NOT_AN_INT,1,100,100,0,200.0,100.0,15.0,"
        "220.0,20.0,50.0,5000.0,500.0,400.0,600.0,100.0,200.0,1800.0,4500.0,3200.0,"
        "64.0,15.0,15.0,0,1,0,\n"
    )
    with pytest.raises(Exception):
        bronze.read_source(spark, str(bad)).collect()


def test_build_bronze_adds_metadata_and_preserves_count(spark, tmp_path, monkeypatch):
    """build_bronze writes 41 columns (39 + 2 metadata) and never nulls the
    source-file metadata."""
    target = tmp_path / "bronze_route_day"
    monkeypatch.setattr(config, "BRONZE_ROUTE_DAY", target)

    out = bronze.build_bronze(spark)
    assert {"_ingested_at", "_source_file"} <= set(out.columns)
    assert len(out.columns) == 41

    landed = spark.read.format("delta").load(str(target))
    assert landed.count() == config.EXPECTED_SOURCE_ROWS
    assert landed.filter(F.col("_source_file").isNull()).count() == 0
