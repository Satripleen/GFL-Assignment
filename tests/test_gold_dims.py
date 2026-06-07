"""Tests for Gold dimensions — strict-hierarchy assertion, dim_route, dim_date."""
from __future__ import annotations

from datetime import date

import pytest

from src import gold_dims

# Minimal Silver-shaped schema carrying just what the dim builders read.
_SILVER_COLS = [
    "route_id", "region", "bu", "area",
    "primary_waste_stream", "primary_customer_segment", "cohort_key", "date",
]


def _silver_row(route_id, region="Atlantic", bu="Atlantic BU", area="Moncton",
                waste="Recycling", seg="Office & Commercial", d=date(2023, 1, 2)):
    cohort = f"{waste} | {seg}"
    return (route_id, region, bu, area, waste, seg, cohort, d)


def _silver_df(spark, rows):
    return spark.createDataFrame(rows, _SILVER_COLS)


# --- assert_strict_hierarchy ----------------------------------------------
def test_strict_hierarchy_passes_for_stable_routes(spark):
    """Each route maps to one attribute combo across days -> assertion returns
    the route count."""
    rows = [
        _silver_row("RTE-0001", d=date(2023, 1, 1)),
        _silver_row("RTE-0001", d=date(2023, 1, 2)),   # same attrs, different day
        _silver_row("RTE-0002", region="Prairies", bu="Prairies BU", area="Regina"),
    ]
    assert gold_dims.assert_strict_hierarchy(_silver_df(spark, rows)) == 2


def test_strict_hierarchy_raises_when_route_moves_area(spark):
    """A route_id that appears under two areas violates the SCD-1 assumption."""
    rows = [
        _silver_row("RTE-0001", area="Moncton"),
        _silver_row("RTE-0001", area="Halifax"),   # same route, different area
    ]
    with pytest.raises(AssertionError, match="non-stable hierarchy"):
        gold_dims.assert_strict_hierarchy(_silver_df(spark, rows))


# --- build_dim_route -------------------------------------------------------
def test_dim_route_one_row_per_route(spark):
    rows = [
        _silver_row("RTE-0001", d=date(2023, 1, 1)),
        _silver_row("RTE-0001", d=date(2023, 1, 2)),
        _silver_row("RTE-0002"),
    ]
    dim = gold_dims.build_dim_route(_silver_df(spark, rows))
    assert dim.count() == 2
    assert dim.select("route_id").distinct().count() == 2
    assert "cohort_key" in dim.columns


# --- build_dim_date --------------------------------------------------------
def test_dim_date_key_and_attributes(spark):
    rows = [_silver_row("RTE-0001", d=date(2023, 3, 15))]
    dim = gold_dims.build_dim_date(_silver_df(spark, rows))
    r = dim.first()
    assert r["date_key"] == 20230315          # yyyymmdd integer key
    assert r["year"] == 2023
    assert r["month"] == 3
    assert r["quarter"] == "Q1"
    assert r["day_of_week"] == "Wednesday"


def test_dim_date_is_deduplicated(spark):
    """Many route-days on the same calendar date collapse to one dim_date row."""
    rows = [
        _silver_row("RTE-0001", d=date(2023, 3, 15)),
        _silver_row("RTE-0002", d=date(2023, 3, 15)),
        _silver_row("RTE-0003", d=date(2023, 3, 16)),
    ]
    dim = gold_dims.build_dim_date(_silver_df(spark, rows))
    assert dim.count() == 2
    assert dim.select("date_key").distinct().count() == 2
