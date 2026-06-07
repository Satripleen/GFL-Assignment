"""Tests for the Part 2 verdict — cohort-relative, persistence-based tiering.

build_scorecard reads fact_route_day and dim_route from Delta, so each test
writes small synthetic versions of both and repoints the config paths.
"""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from lib import config, scorecard


def _write_fact_and_dim(spark, tmp_path, monkeypatch, fact_rows, dim_rows):
    fd = spark.createDataFrame(
        fact_rows, ["route_id", "gross_margin_pct", "profit_per_stop", "gross_profit"]
    )
    dr = spark.createDataFrame(dim_rows, ["route_id", "cohort_key"])
    fact_path = tmp_path / "fact_route_day"
    dim_path = tmp_path / "dim_route"
    fd.write.format("delta").mode("overwrite").save(str(fact_path))
    dr.write.format("delta").mode("overwrite").save(str(dim_path))
    monkeypatch.setattr(config, "FACT_ROUTE_DAY", fact_path)
    monkeypatch.setattr(config, "DIM_ROUTE", dim_path)


def _scenario(spark, tmp_path, monkeypatch):
    """One cohort 'C' with five routes (3 days each):
      A — loss-making (gross_profit < 0)         -> Tier 1
      B — profitable but below cohort median     -> Tier 2
      C/D/E — at the cohort top (above median)   -> OK
    Cohort median margin works out to 80, so B (margin 20) is below peer on
    100% of its days while C/D/E (margin 80) are not.
    """
    fact_rows, dim_rows = [], []
    spec = {
        "A": dict(margin=5.0, gp=-100.0),
        "B": dict(margin=20.0, gp=1000.0),
        "C": dict(margin=80.0, gp=5000.0),
        "D": dict(margin=80.0, gp=5000.0),
        "E": dict(margin=80.0, gp=5000.0),
    }
    for rid, v in spec.items():
        dim_rows.append((rid, "C"))
        for _ in range(3):
            fact_rows.append((rid, v["margin"], v["gp"] / 100, v["gp"]))
    _write_fact_and_dim(spark, tmp_path, monkeypatch, fact_rows, dim_rows)


def test_one_row_per_route_with_a_tier(spark, tmp_path, monkeypatch):
    _scenario(spark, tmp_path, monkeypatch)
    sc = scorecard.build_scorecard(spark)
    assert sc.count() == 5
    assert sc.filter(F.col("tier").isNull()).count() == 0


def test_tier_classification(spark, tmp_path, monkeypatch):
    _scenario(spark, tmp_path, monkeypatch)
    tiers = {r["route_id"]: r["tier"] for r in scorecard.build_scorecard(spark).collect()}
    assert tiers["A"] == "Tier 1 - Loss-making"
    assert tiers["B"] == "Tier 2 - Margin leak"
    assert tiers["C"] == "OK"
    assert tiers["D"] == "OK"
    assert tiers["E"] == "OK"


def test_tier_codes_match_labels(spark, tmp_path, monkeypatch):
    _scenario(spark, tmp_path, monkeypatch)
    rows = {r["route_id"]: r for r in scorecard.build_scorecard(spark).collect()}
    assert rows["A"]["tier_code"] == 1
    assert rows["B"]["tier_code"] == 2
    assert rows["C"]["tier_code"] == 0


def test_cohort_median_is_the_peer_benchmark(spark, tmp_path, monkeypatch):
    """The benchmark each route is judged against is its cohort's median margin."""
    _scenario(spark, tmp_path, monkeypatch)
    row = scorecard.build_scorecard(spark).filter(F.col("route_id") == "B").first()
    assert row["cohort_median_margin"] == pytest.approx(80.0)
    assert row["pct_days_below_peer"] == pytest.approx(1.0)
    assert row["below_peer_flag"] is True


def test_below_peer_flag_off_for_top_routes(spark, tmp_path, monkeypatch):
    _scenario(spark, tmp_path, monkeypatch)
    row = scorecard.build_scorecard(spark).filter(F.col("route_id") == "C").first()
    assert row["pct_days_below_peer"] == pytest.approx(0.0)
    assert row["below_peer_flag"] is False
