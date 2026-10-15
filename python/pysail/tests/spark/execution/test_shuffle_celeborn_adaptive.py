from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pytest
from pyspark.sql.types import Row

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
    endpoint_overrides = "[{}]".format(
        ", ".join(
            f'{{ internal = "{hostname}:{port}", external = "{worker.host}:{mapped_port}" }}'
            for hostname, worker in celeborn_workers.items()
            for port, mapped_port in [
                (12000, worker.rpc_port),
                (12001, worker.push_port),
                (12002, worker.fetch_port),
                (12003, worker.replicate_port),
            ]
        )
    )
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
        "SELECT partitions, initial_partitions, adaptive_rules "
        "FROM system.execution.stages WHERE replanned AND initial_partitions IS NOT NULL",
    )
    assert replanned, "no stage was re-planned"
    assert any(row.partitions < row.initial_partitions for row in replanned)
    assert any("coalesce" in row.adaptive_rules for row in replanned)


def test_committed_statistics_are_recorded(spark):
    spark.range(1000).repartition(8, "id").groupBy((F.col("id") % 3).alias("m")).count().collect()
    rows = _poll(spark, "SELECT rows, logical_bytes FROM system.execution.stages WHERE rows IS NOT NULL")
    assert rows
    assert all(row.logical_bytes >= 0 for row in rows)


def test_user_chosen_partition_count_is_kept(spark):
    partitions = spark.range(1000).repartition(12, "id").select(F.spark_partition_id().alias("p")).distinct().count()
    assert partitions == 12
