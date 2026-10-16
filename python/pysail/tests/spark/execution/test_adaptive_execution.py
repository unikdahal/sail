from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pyspark.sql.functions as F  # noqa: N812
import pytest

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


def _poll(df, timeout: float = 10.0):
    """System events are recorded asynchronously, so poll until rows appear."""
    deadline = time.monotonic() + timeout
    while not (rows := df.collect()) and time.monotonic() < deadline:
        time.sleep(0.2)
    return rows


def test_committed_statistics_are_recorded(spark):
    spark.range(1000).repartition(8, "id").groupBy((F.col("id") % 3).alias("m")).count().collect()
    rows = _poll(
        spark.sql(
            "SELECT session_id, CAST(rows AS BIGINT) AS rows, CAST(logical_bytes AS BIGINT) AS logical_bytes "
            "FROM system.execution.stages WHERE rows IS NOT NULL"
        ).where(F.col("session_id") == spark.session_id)
    )
    assert rows
    assert all(row.logical_bytes >= 0 for row in rows)


def test_statistics_are_not_collected_without_adaptive_execution(static_remote):
    with spark_session_factory(static_remote) as sessions:
        spark = sessions.create()
        spark.range(1000).repartition(8, "id").groupBy((F.col("id") % 3).alias("m")).count().collect()
        # System events are recorded asynchronously, so allow time for any to arrive.
        time.sleep(2)
        # Both servers of this module share the system store, so look at this session only.
        rows = (
            spark.sql("SELECT session_id FROM system.execution.stages WHERE rows IS NOT NULL")
            .where(F.col("session_id") == spark.session_id)
            .collect()
        )
        assert not rows, "statistics were collected without adaptive execution"
