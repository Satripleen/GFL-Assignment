"""Tests for the Silver layer — safe division, dedup/quarantine, recompute &
reconciliation, guarded metrics, and the cohort key.

Silver reads the Bronze Delta table, so each test writes a small synthetic Bronze
table to a temp path and points config.BRONZE_ROUTE_DAY at it via monkeypatch.
"""
from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType

from lib import config
from src import silver
from tests.conftest import BRONZE_SCHEMA, make_source_row


# A fully-nullable copy of the Bronze schema, needed to land a null-PK row (the
# enforced schema declares route_date_key NOT NULL).
_NULLABLE_BRONZE_SCHEMA = StructType(
    [StructField(f.name, f.dataType, True) for f in BRONZE_SCHEMA.fields]
)


def _write_bronze(spark, tmp_path, monkeypatch, rows, schema=BRONZE_SCHEMA):
    """rows: list of (source_row_tuple, ingested_at_datetime). Writes them as a
    Bronze Delta table and repoints config.BRONZE_ROUTE_DAY at it."""
    full = [src + (ts, "file://test.csv") for src, ts in rows]
    df = spark.createDataFrame(full, schema=schema)
    target = tmp_path / "bronze"
    df.write.format("delta").mode("overwrite").save(str(target))
    monkeypatch.setattr(config, "BRONZE_ROUTE_DAY", target)
    return target


# --- _safe_div -------------------------------------------------------------
@pytest.mark.parametrize(
    "num,den,expected",
    [(10.0, 2.0, 5.0), (10.0, 0.0, None), (10.0, None, None), (0.0, 5.0, 0.0)],
)
def test_safe_div_guards_zero_and_null(spark, num, den, expected):
    schema = StructType(
        [StructField("n", DoubleType(), True), StructField("d", DoubleType(), True)]
    )
    df = spark.createDataFrame([(num, den)], schema)
    out = df.select(silver._safe_div(F.col("n"), F.col("d")).alias("r")).first()["r"]
    assert out == expected


# --- recompute & reconciliation -------------------------------------------
def test_recompute_overrides_source_and_keeps_src_copy(spark, tmp_path, monkeypatch):
    """Silver recomputes cost/profit/margin from components and preserves the
    original source values under *_src. A consistent source -> recon_flag False."""
    ts = datetime(2023, 1, 1, 12, 0, 0)
    _write_bronze(spark, tmp_path, monkeypatch, [(make_source_row(), ts)])

    s, _ = silver.build_silver(spark)
    row = s.first()
    assert row["total_cost"] == 1800.0          # 500+400+600+100+200
    assert row["net_revenue"] == 4500.0          # 5000 - 500 disposal
    assert row["gross_profit"] == 3200.0         # 5000 - 1800
    assert row["gross_margin_pct"] == pytest.approx(64.0)
    assert row["total_cost_src"] == 1800.0       # source preserved
    assert row["recon_flag"] is False


def test_recon_flag_trips_when_source_disagrees(spark, tmp_path, monkeypatch):
    """A source total_cost that doesn't match the components must set recon_flag,
    but the *recomputed* value is what flows downstream."""
    ts = datetime(2023, 1, 1, 12, 0, 0)
    bad = make_source_row(total_cost=9999.0, gross_profit=1.0, net_revenue=1.0)
    _write_bronze(spark, tmp_path, monkeypatch, [(bad, ts)])

    s, _ = silver.build_silver(spark)
    row = s.first()
    assert row["recon_flag"] is True
    assert row["total_cost"] == 1800.0           # trusted recompute, not 9999


# --- dedup & quarantine ----------------------------------------------------
def test_duplicate_pk_keeps_latest_and_quarantines_loser(spark, tmp_path, monkeypatch):
    """Two rows share a PK; the latest _ingested_at wins, the older is quarantined
    with reason 'duplicate_pk'."""
    old = (make_source_row(total_revenue=1000.0), datetime(2023, 1, 1, 8, 0, 0))
    new = (make_source_row(total_revenue=5000.0), datetime(2023, 1, 1, 9, 0, 0))
    _write_bronze(spark, tmp_path, monkeypatch, [old, new])

    s, q = silver.build_silver(spark)
    assert s.count() == 1
    assert s.first()["total_revenue"] == 5000.0   # latest ingest won
    assert q.count() == 1
    assert q.first()["_reject_reason"] == "duplicate_pk"


def test_null_pk_is_quarantined(spark, tmp_path, monkeypatch):
    good = (make_source_row(route_date_key="RDK-0000001"), datetime(2023, 1, 1, 9, 0))
    nullpk = (make_source_row(route_date_key=None), datetime(2023, 1, 1, 9, 0))
    _write_bronze(spark, tmp_path, monkeypatch, [good, nullpk],
                  schema=_NULLABLE_BRONZE_SCHEMA)

    s, q = silver.build_silver(spark)
    assert s.count() == 1
    assert s.first()["route_date_key"] == "RDK-0000001"
    assert q.filter(F.col("_reject_reason") == "null_pk").count() == 1


# --- guarded metrics & flags ----------------------------------------------
def test_zero_denominator_nulls_metric_and_sets_flag(spark, tmp_path, monkeypatch):
    """A zero denominator (e.g. completed_stops=0) yields a null metric and trips
    metric_null_flag instead of raising."""
    ts = datetime(2023, 1, 1, 12, 0, 0)
    row = make_source_row(completed_stops=0)
    _write_bronze(spark, tmp_path, monkeypatch, [(row, ts)])

    s, _ = silver.build_silver(spark)
    out = s.first()
    assert out["profit_per_stop"] is None
    assert out["metric_null_flag"] is True


def test_cohort_key_is_waste_stream_and_segment(spark, tmp_path, monkeypatch):
    ts = datetime(2023, 1, 1, 12, 0, 0)
    row = make_source_row(
        primary_waste_stream="Organics", primary_customer_segment="Office & Commercial"
    )
    _write_bronze(spark, tmp_path, monkeypatch, [(row, ts)])

    s, _ = silver.build_silver(spark)
    assert s.first()["cohort_key"] == "Organics | Office & Commercial"
