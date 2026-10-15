from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pytest
from pyspark.sql.types import Row

from pysail.testing.containers.celeborn import celeborn_endpoint_overrides
from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

if TYPE_CHECKING:
    from collections.abc import Generator

    from pysail.testing.containers.celeborn import MasterService, WorkerService

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only"),
]


def _celeborn_envs(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
    *,
    adaptive: bool,
) -> dict[str, str]:
    endpoint_overrides = celeborn_endpoint_overrides(celeborn_workers)
    return {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "celeborn",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS": (
            f'["{celeborn_master.host}:{celeborn_master.port}"]'
        ),
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__ENDPOINT_OVERRIDES": endpoint_overrides,
        "SAIL_CLUSTER__ADAPTIVE__ENABLED": "true" if adaptive else "false",
        # Hash shuffles start over-partitioned so that coalescing has an effect.
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "16",
        # Small sizes so that coalescing and skew splitting apply to test-sized data.
        "SAIL_CLUSTER__ADAPTIVE__COALESCE__TARGET_SIZE": "65536",
        "SAIL_CLUSTER__ADAPTIVE__COALESCE__MIN_PARTITION_SIZE": "1024",
        "SAIL_CLUSTER__ADAPTIVE__SKEW__THRESHOLD": "65536",
        "SAIL_CLUSTER__ADAPTIVE__SKEW__FACTOR": "2.0",
        # Retry failed tasks so that failure injection can be observed.
        "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "3",
        # Small batches, so that a task pushes output to Celeborn before an injected failure.
        "SAIL_EXECUTION__BATCH_SIZE": "1024",
    }


@pytest.fixture(scope="module")
def remote(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
) -> Generator[str, None, None]:
    """Run Spark Connect with a Celeborn shuffle backend and adaptive execution."""
    envs = _celeborn_envs(celeborn_master, celeborn_workers, adaptive=True)
    with spark_connect_server(envs=envs) as server:
        yield server.remote


@pytest.fixture(scope="module")
def static_remote(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
) -> Generator[str, None, None]:
    """Run Spark Connect with a Celeborn shuffle backend without adaptive execution."""
    envs = _celeborn_envs(celeborn_master, celeborn_workers, adaptive=False)
    with spark_connect_server(envs=envs) as server:
        yield server.remote


def _queries(spark):
    left = spark.createDataFrame([Row(k=i % 37, v=i) for i in range(2000)])
    right = spark.createDataFrame([Row(k=i, name=f"n{i}") for i in range(30)])
    return {
        "aggregate": left.groupBy("k").agg(F.count("*").alias("c"), F.sum("v").alias("s")).orderBy("k"),
        "join": left.join(right, "k").groupBy("name").agg(F.sum("v").alias("s")).orderBy("name"),
        "left_join": left.join(right, "k", "left").groupBy("k").agg(F.count("name").alias("c")).orderBy("k"),
        "empty_join": left.join(right.where(F.col("k") < 0), "k").groupBy("k").count().orderBy("k"),
        "distinct": left.select((F.col("v") % 101).alias("m")).distinct().orderBy("m"),
    }


def test_adaptive_results_match_static_results(remote, static_remote):
    with spark_session_factory(remote) as adaptive_sessions, spark_session_factory(static_remote) as static_sessions:
        adaptive = _queries(adaptive_sessions.create())
        static = _queries(static_sessions.create())
        for name, df in adaptive.items():
            assert df.collect() == static[name].collect(), name


def _poll(spark, query: str, timeout: float = 10.0):
    """System events are recorded asynchronously, so poll until rows appear."""
    deadline = time.monotonic() + timeout
    while not (rows := spark.sql(query).collect()) and time.monotonic() < deadline:
        time.sleep(0.2)
    return rows


def test_coalesced_stages_are_recorded(spark):
    rows = spark.createDataFrame([Row(k=i % 7, v=i) for i in range(100)]).groupBy("k").count().orderBy("k").collect()
    assert [row.k for row in rows] == list(range(7))

    replanned = _poll(
        spark,
        "SELECT CAST(partitions AS BIGINT) AS partitions, "
        "CAST(initial_partitions AS BIGINT) AS initial_partitions, adaptive_rules "
        "FROM system.execution.stages WHERE replanned AND initial_partitions IS NOT NULL",
    )
    assert replanned, "no stage was re-planned"
    assert any(row.partitions < row.initial_partitions for row in replanned)
    assert any("coalesce" in row.adaptive_rules for row in replanned)


def test_committed_statistics_are_recorded(spark):
    spark.range(1000).repartition(8, "id").groupBy((F.col("id") % 3).alias("m")).count().collect()
    rows = _poll(
        spark,
        "SELECT CAST(rows AS BIGINT) AS rows, CAST(logical_bytes AS BIGINT) AS logical_bytes "
        "FROM system.execution.stages WHERE rows IS NOT NULL",
    )
    assert rows
    assert all(row.logical_bytes >= 0 for row in rows)


def test_user_chosen_partition_count_is_kept(spark):
    partitions = spark.range(1000).repartition(12, "id").select(F.spark_partition_id().alias("p")).distinct().count()
    assert partitions == 12


def _skewed_join(spark):
    # 70% of the fact rows have key 0, so its hash bucket is far larger than the others.
    # Both sides are larger than the broadcast threshold, so the join is partitioned.
    fact = spark.range(300_000).select(
        F.when(F.col("id") % 10 < 7, F.lit(0)).otherwise(F.col("id") % 200_000).alias("k"),
        F.col("id").alias("v"),
    )
    dim = spark.range(200_000).select(F.col("id").alias("k"), (F.col("id") % 17).alias("g"))
    return fact.join(dim, "k").groupBy("g").agg(F.count("*").alias("c"), F.sum("v").alias("s")).orderBy("g")


def test_skewed_join_is_split_with_map_range_reads(remote, static_remote):
    with spark_session_factory(remote) as adaptive_sessions, spark_session_factory(static_remote) as static_sessions:
        adaptive = adaptive_sessions.create()
        static = static_sessions.create()
        expected = _skewed_join(static).collect()
        assert sum(row.c for row in expected) == 300_000  # noqa: PLR2004
        assert _skewed_join(adaptive).collect() == expected

        rules = _poll(
            adaptive,
            "SELECT adaptive_rules FROM system.execution.stages WHERE array_contains(adaptive_rules, 'skew_split')",
        )
        assert rules, "no stage was split for skew"


def _fail_once(marker: str):
    """Returns a UDF that fails the first time it sees id 150000, after earlier
    batches of the task have already been pushed to Celeborn."""

    @F.udf("long")
    def fail_once(value):
        import os  # noqa: PLC0415

        if value == 150_000 and not os.path.exists(marker):  # noqa: PLR2004
            with open(marker, "w") as f:  # noqa: PTH123
                f.write("failed")
            msg = "injected failure"
            raise RuntimeError(msg)
        return value

    return fail_once


@pytest.mark.parametrize("adaptive", [True, False])
def test_retried_producer_output_is_read_once(remote, static_remote, tmp_path, adaptive):
    marker = str(tmp_path / f"failed-{adaptive}")
    with spark_session_factory(remote if adaptive else static_remote) as sessions:
        spark = sessions.create()
        rows = (
            spark.range(0, 200_000, 1, 2)
            .select(_fail_once(marker)("id").alias("id"))
            # Shuffle the rows as they are produced, so that the failing attempt pushes
            # output to Celeborn before it fails.
            .repartition(8, "id")
            .groupBy((F.col("id") % 100).alias("m"))
            .agg(F.count("*").alias("c"), F.sum("id").alias("s"))
            .collect()
        )
    import os  # noqa: PLC0415

    assert os.path.exists(marker), "the failure was not injected"
    # A failed attempt pushed partial output before failing. Reading it in addition to
    # the retried attempt would duplicate rows.
    assert sum(row.c for row in rows) == 200_000  # noqa: PLR2004
    assert sum(row.s for row in rows) == sum(range(200_000))
