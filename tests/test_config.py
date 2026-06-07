"""Tests for lib/config — logging, paths, and the idempotent MERGE helper."""
from __future__ import annotations

from lib import config


def test_get_logger_does_not_stack_handlers():
    """Re-importing / re-fetching the logger must not duplicate handlers."""
    a = config.get_logger("gfl-test-logger")
    n_first = len(a.handlers)
    b = config.get_logger("gfl-test-logger")
    assert a is b
    assert len(b.handlers) == n_first == 1
    assert b.propagate is False


def test_paths_are_under_project_root():
    """All Delta locations hang off the lakehouse dir under the data dir."""
    assert config.SOURCE_CSV.parent == config.DATA_DIR
    assert config.DATA_DIR.parent == config.PROJECT_ROOT
    for tbl in (
        config.BRONZE_ROUTE_DAY,
        config.SILVER_ROUTE_DAY,
        config.DIM_DATE,
        config.DIM_ROUTE,
        config.FACT_ROUTE_DAY,
        config.FACT_ROUTE_MONTH,
        config.ROUTE_SCORECARD,
    ):
        assert str(config.LAKEHOUSE) in str(tbl)


def test_upsert_delta_creates_then_merges_idempotently(spark, tmp_path):
    """First call creates the table; re-MERGEing the same keys leaves the row
    count unchanged (the idempotency guarantee), and updates land in place."""
    path = tmp_path / "merge_target"
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])

    config.upsert_delta(spark, df, path, key_cols=["id"])
    assert spark.read.format("delta").load(str(path)).count() == 2

    # Re-run with one updated row + one new row.
    df2 = spark.createDataFrame([(2, "B_updated"), (3, "c")], ["id", "val"])
    config.upsert_delta(spark, df2, path, key_cols=["id"])

    out = {r["id"]: r["val"] for r in spark.read.format("delta").load(str(path)).collect()}
    assert out == {1: "a", 2: "B_updated", 3: "c"}  # upserted, not duplicated


def test_upsert_delta_partitioned_write(spark, tmp_path):
    path = tmp_path / "partitioned"
    df = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "region"])
    config.upsert_delta(spark, df, path, key_cols=["id"], partition_by=["region"])
    assert spark.read.format("delta").load(str(path)).count() == 2
