"""Tests for Gold facts — atomic grain & FKs, and the month rollup.

To get a realistic Silver DataFrame (all measure columns present) the helper
writes synthetic Bronze and runs the real Silver build, then exercises the fact
builders on top of it.
"""
from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from lib import config
from src import gold_facts, silver
from tests.conftest import BRONZE_SCHEMA, make_source_row


def _build_silver(spark, tmp_path, monkeypatch, source_rows):
    """source_rows: list of source-row tuples. Returns the Silver DataFrame."""
    ts = datetime(2023, 1, 1, 12, 0, 0)
    full = [src + (ts, "file://test.csv") for src in source_rows]
    df = spark.createDataFrame(full, schema=BRONZE_SCHEMA)
    target = tmp_path / "bronze"
    df.write.format("delta").mode("overwrite").save(str(target))
    monkeypatch.setattr(config, "BRONZE_ROUTE_DAY", target)
    s, _ = silver.build_silver(spark)
    return s


# --- fact_route_day --------------------------------------------------------
def test_fact_day_grain_and_foreign_keys(spark, tmp_path, monkeypatch):
    """One row per route_date_key, carrying date_key and route_id FKs."""
    rows = [
        make_source_row(route_date_key="RDK-1", route_id="RTE-0001", date=date(2023, 1, 2)),
        make_source_row(route_date_key="RDK-2", route_id="RTE-0001", date=date(2023, 1, 3)),
    ]
    s = _build_silver(spark, tmp_path, monkeypatch, rows)
    fact = gold_facts.build_fact_route_day(s)

    assert fact.count() == 2
    assert {"date_key", "route_id", "route_date_key"} <= set(fact.columns)
    keys = {r["date_key"] for r in fact.collect()}
    assert keys == {20230102, 20230103}
    # Dimensional attributes live in the dims, not the fact.
    assert "region" not in fact.columns
    assert "cohort_key" not in fact.columns


# --- fact_route_month ------------------------------------------------------
def test_fact_month_sums_tie_back_to_day(spark, tmp_path, monkeypatch):
    """Additive measures summed at month grain equal the day-grain totals."""
    rows = [
        make_source_row(route_date_key="RDK-1", route_id="RTE-0001",
                        date=date(2023, 1, 2), total_revenue=5000.0),
        make_source_row(route_date_key="RDK-2", route_id="RTE-0001",
                        date=date(2023, 1, 3), total_revenue=3000.0),
    ]
    s = _build_silver(spark, tmp_path, monkeypatch, rows)
    fact_day = gold_facts.build_fact_route_day(s)
    dim_route = s.select("route_id", "region", "bu", "area").dropDuplicates(["route_id"])

    fm = gold_facts.build_fact_route_month(fact_day, dim_route)

    assert fm.count() == 1                      # one route x one month
    row = fm.first()
    assert row["days_active"] == 2
    assert row["total_revenue"] == pytest.approx(8000.0)
    assert row["month_key"] == 202301


def test_fact_month_margin_is_revenue_weighted(spark, tmp_path, monkeypatch):
    """The month margin is sum(gross_profit)/sum(revenue)*100 — a weighted ratio,
    not an average of daily percentages."""
    # Day A: rev 1000, gp 100 (10%).  Day B: rev 9000, gp 4500 (50%).
    # Simple mean of pct = 30%; revenue-weighted = 4600/10000 = 46%.
    rows = [
        make_source_row(route_date_key="RDK-1", route_id="RTE-0001", date=date(2023, 1, 2),
                        total_revenue=1000.0, disposal_cost=0.0, fuel_cost=0.0,
                        labour_cost=900.0, maintenance_cost=0.0, admin_cost=0.0),
        make_source_row(route_date_key="RDK-2", route_id="RTE-0001", date=date(2023, 1, 3),
                        total_revenue=9000.0, disposal_cost=0.0, fuel_cost=0.0,
                        labour_cost=4500.0, maintenance_cost=0.0, admin_cost=0.0),
    ]
    s = _build_silver(spark, tmp_path, monkeypatch, rows)
    fact_day = gold_facts.build_fact_route_day(s)
    dim_route = s.select("route_id", "region", "bu", "area").dropDuplicates(["route_id"])

    fm = gold_facts.build_fact_route_month(fact_day, dim_route).first()
    assert fm["gross_margin_pct"] == pytest.approx(46.0)


def test_fact_month_counts_loss_days(spark, tmp_path, monkeypatch):
    """loss_days counts route-days where recomputed gross_profit < 0."""
    rows = [
        make_source_row(route_date_key="RDK-1", route_id="RTE-0001", date=date(2023, 1, 2),
                        total_revenue=1000.0, disposal_cost=0.0, fuel_cost=0.0,
                        labour_cost=5000.0, maintenance_cost=0.0, admin_cost=0.0),  # gp<0
        make_source_row(route_date_key="RDK-2", route_id="RTE-0001", date=date(2023, 1, 3),
                        total_revenue=9000.0, disposal_cost=0.0, fuel_cost=0.0,
                        labour_cost=100.0, maintenance_cost=0.0, admin_cost=0.0),   # gp>0
    ]
    s = _build_silver(spark, tmp_path, monkeypatch, rows)
    fact_day = gold_facts.build_fact_route_day(s)
    dim_route = s.select("route_id", "region", "bu", "area").dropDuplicates(["route_id"])

    fm = gold_facts.build_fact_route_month(fact_day, dim_route).first()
    assert fm["loss_days"] == 1
