#!/usr/bin/env python
"""Benchmark adaptive query execution on a remote shuffle service (currently Celeborn).

The script starts a Sail server in local-cluster mode with adaptive execution off,
then another with it on, runs the same workloads on both, checks that the results
are identical, and reports wall time together with what the scheduler did, as
recorded in the `system.execution` tables.

    python scripts/adaptive/benchmark.py --master 127.0.0.1:12097 --output results.json

The Celeborn workers must advertise addresses reachable from this process, as the
local cluster of `scripts/celeborn/local-cluster.sh` does.

The process exits with code 1 if any workload returns different results.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F  # noqa: N812

from pysail.testing.spark.session import spark_connect_server, spark_session_factory

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from pyspark.sql import DataFrame, SparkSession


@dataclass
class Measurement:
    """What one workload did in one mode."""

    wall_seconds: list[float] = field(default_factory=list)
    jobs: int = 0
    stages: int = 0
    tasks: int = 0
    task_attempts: int = 0
    replanned_stages: int = 0
    initial_partitions: int = 0
    partitions: int = 0
    rules: list[str] = field(default_factory=list)
    task_seconds: float = 0.0
    slowest_task_seconds: float = 0.0

    @property
    def median_wall_seconds(self) -> float:
        return statistics.median(self.wall_seconds) if self.wall_seconds else 0.0


def over_partitioned_aggregate(spark: SparkSession, scale: int) -> DataFrame:
    """A low-cardinality aggregate whose shuffle has far more partitions than data."""
    return (
        spark.range(2_000_000 * scale)
        .groupBy((F.col("id") % 1000).alias("k"))
        .agg(F.count("*").alias("c"), F.sum("id").alias("s"))
    )


def skewed_join(spark: SparkSession, scale: int) -> DataFrame:
    """A partitioned join where 60% of the fact rows share one key."""
    fact = spark.range(4_000_000 * scale).select(
        F.when(F.col("id") % 10 < 6, F.lit(0)).otherwise(F.col("id") % 200_000).alias("k"),  # noqa: PLR2004
        F.col("id").alias("v"),
    )
    # The dimension is too large to broadcast, so the join is partitioned.
    dim = spark.range(200_000).select(F.col("id").alias("k"), (F.col("id") % 50).alias("g"))
    return fact.join(dim, "k").groupBy("g").agg(F.count("*").alias("c"), F.sum("v").alias("s"))


def swapped_join(spark: SparkSession, scale: int) -> DataFrame:
    """A partitioned join whose planned build side is the larger input."""
    large = spark.range(3_000_000 * scale).select((F.col("id") % 300_000).alias("k"), F.col("id").alias("v"))
    small = spark.range(300_000).select(F.col("id").alias("k"), (F.col("id") % 20).alias("g"))
    return large.join(small, "k").groupBy("g").agg(F.count("*").alias("c"), F.sum("v").alias("s"))


def empty_join(spark: SparkSession, scale: int) -> DataFrame:
    """A join with a side that turns out to be empty at run time."""
    base = spark.range(2_000_000 * scale).select((F.col("id") % 1000).alias("k"), F.col("id").alias("v"))
    other = spark.range(1_000_000).select((F.col("id") % 1000).alias("k"), F.col("id").alias("w"))
    return base.join(other.where(F.col("w") < 0), "k").groupBy("k").count()


WORKLOADS: dict[str, Callable[[SparkSession, int], DataFrame]] = {
    "over_partitioned_aggregate": over_partitioned_aggregate,
    "skewed_join": skewed_join,
    "swapped_join": swapped_join,
    "empty_join": empty_join,
}


def _max_job_id(spark: SparkSession) -> int:
    row = (
        spark.sql("SELECT session_id, CAST(job_id AS BIGINT) AS job_id FROM system.execution.jobs")
        .where(F.col("session_id") == spark.session_id)
        .agg(F.max("job_id").alias("m"))
        .collect()[0]
    )
    return -1 if row.m is None else row.m


def _settled(df: DataFrame, timeout: float = 15.0) -> list[Any]:
    """System events are recorded asynchronously, so wait until the rows stop changing."""
    deadline = time.monotonic() + timeout
    previous = None
    while True:
        rows = df.collect()
        if (rows and rows == previous) or time.monotonic() > deadline:
            return rows
        previous = rows
        time.sleep(0.5)


def _record(spark: SparkSession, measurement: Measurement, first: int, last: int) -> None:
    def scoped(df: DataFrame) -> DataFrame:
        return df.where(
            (F.col("session_id") == spark.session_id) & (F.col("job_id") > first) & (F.col("job_id") <= last)
        )

    stages = _settled(
        scoped(
            spark.sql(
                "SELECT session_id, CAST(job_id AS BIGINT) AS job_id, CAST(stage AS BIGINT) AS stage, "
                "CAST(partitions AS BIGINT) AS partitions, "
                "CAST(initial_partitions AS BIGINT) AS initial_partitions, replanned, adaptive_rules "
                "FROM system.execution.stages"
            )
        ).orderBy("job_id", "stage")
    )
    tasks = _settled(
        scoped(
            spark.sql(
                "SELECT session_id, CAST(job_id AS BIGINT) AS job_id, CAST(stage AS BIGINT) AS stage, "
                "CAST(partition AS BIGINT) AS partition, CAST(attempt AS BIGINT) AS attempt, "
                "CAST(unix_micros(stopped_at) - unix_micros(created_at) AS DOUBLE) / 1e6 AS seconds "
                "FROM system.execution.tasks"
            )
        ).orderBy("job_id", "stage", "partition", "attempt")
    )
    measurement.jobs = len({row.job_id for row in stages})
    measurement.stages = len(stages)
    measurement.partitions = sum(row.partitions or 0 for row in stages)
    measurement.initial_partitions = sum((row.initial_partitions or row.partitions or 0) for row in stages)
    measurement.replanned_stages = sum(1 for row in stages if row.replanned)
    measurement.rules = sorted({rule for row in stages for rule in (row.adaptive_rules or [])})
    measurement.tasks = len({(row.job_id, row.stage, row.partition) for row in tasks})
    measurement.task_attempts = len(tasks)
    seconds = [row.seconds for row in tasks if row.seconds is not None]
    measurement.task_seconds = sum(seconds)
    measurement.slowest_task_seconds = max(seconds, default=0.0)


def _envs(args: argparse.Namespace, *, adaptive: bool) -> dict[str, str]:
    return {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "celeborn",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS": json.dumps([args.master]),
        "SAIL_CLUSTER__ADAPTIVE__ENABLED": "true" if adaptive else "false",
        "SAIL_CLUSTER__ADAPTIVE__COALESCE__TARGET_SIZE": str(args.target_size),
        "SAIL_CLUSTER__ADAPTIVE__SKEW__THRESHOLD": str(args.skew_threshold),
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": str(args.parallelism),
    }


def _run_mode(args: argparse.Namespace, *, adaptive: bool) -> dict[str, tuple[list[Any], Measurement]]:
    results = {}
    with (
        spark_connect_server(envs=_envs(args, adaptive=adaptive)) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        spark = sessions.create()
        for name, workload in WORKLOADS.items():
            print(f"[adaptive={'on' if adaptive else 'off'}] {name}", flush=True)  # noqa: T201
            measurement = Measurement()
            # The first run is measured from the system tables and also warms up.
            first = _max_job_id(spark)
            rows = sorted(tuple(row) for row in workload(spark, args.scale).collect())
            _record(spark, measurement, first, _max_job_id(spark))
            for _ in range(args.repeat):
                start = time.perf_counter()
                workload(spark, args.scale).collect()
                measurement.wall_seconds.append(time.perf_counter() - start)
            results[name] = (rows, measurement)
    return results


def _print_table(report: dict[str, Any]) -> None:
    columns = [
        "workload",
        "adaptive",
        "parity",
        "median wall s",
        "tasks",
        "partitions (initial)",
        "replanned",
        "rules",
        "task s",
        "slowest task s",
    ]
    print("\n| " + " | ".join(columns) + " |")  # noqa: T201
    print("|" + "---|" * len(columns))  # noqa: T201
    for name, entry in report.items():
        for mode in ("off", "on"):
            m = entry[mode]
            print(  # noqa: T201
                f"| {name} | {mode} | {entry['parity']} | {m['median_wall_seconds']:.2f} | {m['tasks']} "
                f"| {m['partitions']} ({m['initial_partitions']}) | {m['replanned_stages']} "
                f"| {', '.join(m['rules']) or '-'} | {m['task_seconds']:.1f} | {m['slowest_task_seconds']:.2f} |"
            )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--master", required=True, help="the Celeborn master endpoint as HOST:PORT")
    parser.add_argument("--parallelism", type=int, default=200, help="the default shuffle partition count")
    parser.add_argument("--scale", type=int, default=1, help="a multiplier for the workload sizes")
    parser.add_argument("--repeat", type=int, default=3, help="the timed runs per workload")
    parser.add_argument("--target-size", type=int, default=4 << 20, help="the coalesce target size in bytes")
    parser.add_argument("--skew-threshold", type=int, default=4 << 20, help="the skew threshold in bytes")
    parser.add_argument("--output", help="a path to write the results as JSON")
    args = parser.parse_args(argv)

    off = _run_mode(args, adaptive=False)
    on = _run_mode(args, adaptive=True)
    report = {}
    for name in WORKLOADS:
        parity = off[name][0] == on[name][0]
        if not parity:
            print(f"error: results of {name} differ with adaptive execution", file=sys.stderr)  # noqa: T201
        report[name] = {
            "parity": parity,
            "off": {**asdict(off[name][1]), "median_wall_seconds": off[name][1].median_wall_seconds},
            "on": {**asdict(on[name][1]), "median_wall_seconds": on[name][1].median_wall_seconds},
        }
    _print_table(report)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump({"arguments": vars(args), "workloads": report}, f, indent=2)
    return 0 if all(entry["parity"] for entry in report.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
