"""Benchmark Sail adaptive query execution (AQE) on vs off over the Celeborn shuffle backend.

This is a standalone script (it is not collected by pytest). It starts two Sail servers in
``local-cluster`` mode sequentially, one with AQE disabled and one with AQE enabled, runs a
set of workloads on each, checks that results are identical, and reports measurements
collected from the ``system.execution.stages`` and ``system.execution.tasks`` tables.

Usage::

    python -m pysail.tests.spark.execution.benchmark_celeborn_aqe \\
        --master HOST:PORT [--parallelism 64] [--scale 1] [--repeat 3] \\
        [--skew-threshold-bytes 1048576] [--target-size-bytes 1048576] \\
        [--output results.json]

``--master`` is the Celeborn master endpoint. The Celeborn workers are expected to advertise
addresses that are reachable from this process, so no endpoint overrides are configured.

Workloads (row counts are multiplied by ``--scale``):

* ``over_partitioned_aggregate``: a low-cardinality aggregate over many shuffle partitions.
* ``skewed_join``: a partitioned join where ~60% of fact rows share one key.
* ``join_decisions``: a join with a large build side and a join with an empty side.

The process exits with code 1 if any workload returns different results with AQE on and off.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F

from pysail.testing.spark.session import spark_connect_server, spark_session_factory

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pyspark.sql import DataFrame, SparkSession

Rows = list[tuple[Any, ...]]

SETTLE_TIMEOUT_SECONDS = 15.0
SETTLE_INTERVAL_SECONDS = 0.3


@dataclass
class Measurement:
    """Measurements for one workload under one mode."""

    wall_seconds_median: float = 0.0
    wall_seconds_all: list[float] = field(default_factory=list)
    jobs: int = 0
    stages: int = 0
    tasks: int = 0
    partitions: int | None = None
    replanned_stages: int | None = None
    adaptive_rules: list[str] = field(default_factory=list)
    max_task_seconds: float | None = None
    median_task_seconds: float | None = None
    task_seconds: float | None = None
    stage_columns: list[str] = field(default_factory=list)
    task_columns: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Workloads
# ---------------------------------------------------------------------------


def _normalize(rows: Sequence[Any]) -> Rows:
    return sorted((tuple(row) for row in rows), key=repr)


def _disable_broadcast(spark: SparkSession) -> None:
    try:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    except Exception as e:  # noqa: BLE001
        print(f"warning: cannot disable broadcast joins: {e}", file=sys.stderr)


def over_partitioned_aggregate(spark: SparkSession, scale: int) -> Rows:
    df = (
        spark.range(2_000_000 * scale)
        .groupBy((F.col("id") % 1000).alias("k"))
        .agg(F.count("*").alias("c"), F.sum("id").alias("s"))
    )
    return _normalize(df.collect())


def _skewed_fact(spark: SparkSession, scale: int) -> DataFrame:
    key = F.when(F.col("id") % 10 < 6, F.lit(0)).otherwise(F.col("id") % 10000)
    return spark.range(4_000_000 * scale).select(key.alias("k"), F.col("id").alias("v"))


def skewed_join(spark: SparkSession, scale: int) -> Rows:
    _disable_broadcast(spark)
    fact = _skewed_fact(spark, scale)
    dim = spark.range(10_000).select(F.col("id").alias("k"), (F.col("id") % 50).alias("g"))
    df = fact.join(dim, "k").groupBy("g").agg(F.count("*").alias("c"), F.sum("v").alias("s"))
    return _normalize(df.collect())


def join_decisions(spark: SparkSession, scale: int) -> Rows:
    _disable_broadcast(spark)
    # (a) large build (left) side with a small, still-shuffled right side.
    left = spark.range(2_000_000 * scale).select((F.col("id") % 5000).alias("k"), F.col("id").alias("v"))
    right = spark.range(5000).select(F.col("id").alias("k"), (F.col("id") % 20).alias("g"))
    large_left = left.join(right, "k").groupBy("g").agg(F.count("*").alias("c"), F.sum("v").alias("s"))
    # (b) inner join with an empty side.
    base = spark.range(1_000_000 * scale).select((F.col("id") % 1000).alias("k"), F.col("id").alias("v"))
    empty = base.where(F.lit(False))
    other = spark.range(1000).select(F.col("id").alias("k"), F.col("id").alias("w"))
    empty_join = base.join(empty.join(other, "k").select("k"), "k").groupBy("k").count()
    rows_a = [("large_left", *row) for row in _normalize(large_left.collect())]
    rows_b = [("empty_join", *row) for row in _normalize(empty_join.collect())]
    return rows_a + rows_b


WORKLOADS: dict[str, Any] = {
    "over_partitioned_aggregate": over_partitioned_aggregate,
    "skewed_join": skewed_join,
    "join_decisions": join_decisions,
}


# ---------------------------------------------------------------------------
# System tables
# ---------------------------------------------------------------------------


def _query(spark: SparkSession, table: str) -> tuple[list[str], list[dict[str, Any]]]:
    df = spark.sql(f"SELECT * FROM system.execution.{table}")
    return list(df.columns), [row.asDict() for row in df.collect()]


def _max_job_id(spark: SparkSession) -> int:
    try:
        rows = spark.sql("SELECT MAX(job_id) AS m FROM system.execution.stages").collect()
        value = rows[0]["m"] if rows else None
        return int(value) if value is not None else -1
    except Exception as e:  # noqa: BLE001
        print(f"warning: cannot read max job_id: {e}", file=sys.stderr)
        return -1


def _job_rows(rows: list[dict[str, Any]], lo: int, hi: int) -> list[dict[str, Any]]:
    return [r for r in rows if r.get("job_id") is not None and lo < int(r["job_id"]) <= hi]


def _settled_rows(
    spark: SparkSession, lo: int, hi: int
) -> tuple[list[str], list[dict[str, Any]], list[str], list[dict]]:
    """System events are recorded asynchronously, so wait until the row counts stop changing."""
    deadline = time.monotonic() + SETTLE_TIMEOUT_SECONDS
    previous = (-1, -1)
    while True:
        stage_cols, stages = _query(spark, "stages")
        try:
            task_cols, tasks = _query(spark, "tasks")
        except Exception as e:  # noqa: BLE001
            print(f"warning: cannot read tasks table: {e}", file=sys.stderr)
            task_cols, tasks = [], []
        stages, tasks = _job_rows(stages, lo, hi), _job_rows(tasks, lo, hi)
        counts = (len(stages), len(tasks))
        if (counts == previous and counts[0] > 0) or time.monotonic() > deadline:
            return stage_cols, stages, task_cols, tasks
        previous = counts
        time.sleep(SETTLE_INTERVAL_SECONDS)


def _seconds(start: Any, stop: Any) -> float | None:
    if not isinstance(start, datetime) or not isinstance(stop, datetime):
        return None
    return max((stop - start).total_seconds(), 0.0)


def _summarize(
    measurement: Measurement,
    stage_cols: list[str],
    stages: list[dict[str, Any]],
    task_cols: list[str],
    tasks: list[dict[str, Any]],
) -> None:
    measurement.stage_columns, measurement.task_columns = stage_cols, task_cols
    measurement.jobs = len({r["job_id"] for r in stages} | {r["job_id"] for r in tasks})
    measurement.stages = len({(r["job_id"], r.get("stage")) for r in stages})
    measurement.tasks = len({(r["job_id"], r.get("stage"), r.get("partition"), r.get("attempt")) for r in tasks})
    if "partitions" in stage_cols:
        measurement.partitions = sum(int(r["partitions"] or 0) for r in stages)
    if "replanned" in stage_cols:
        measurement.replanned_stages = sum(1 for r in stages if r.get("replanned"))
    if "adaptive_rules" in stage_cols:
        rules: set[str] = set()
        for r in stages:
            value = r.get("adaptive_rules")
            if isinstance(value, str):
                rules.update(part.strip() for part in value.replace(";", ",").split(",") if part.strip())
            elif value:
                rules.update(str(v) for v in value)
        measurement.adaptive_rules = sorted(rules)
    durations = [d for r in tasks if (d := _seconds(r.get("created_at"), r.get("stopped_at"))) is not None]
    if durations:
        measurement.max_task_seconds = max(durations)
        measurement.median_task_seconds = statistics.median(durations)
        measurement.task_seconds = sum(durations)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _envs(args: argparse.Namespace, *, adaptive: bool) -> dict[str, str]:
    return {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "celeborn",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS": json.dumps([args.master]),
        "SAIL_CLUSTER__ADAPTIVE__ENABLED": "true" if adaptive else "false",
        "SAIL_CLUSTER__ADAPTIVE__SKEW__THRESHOLD": str(args.skew_threshold_bytes),
        "SAIL_CLUSTER__ADAPTIVE__COALESCE__TARGET_SIZE": str(args.target_size_bytes),
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": str(args.parallelism),
    }


def _run_mode(args: argparse.Namespace, *, adaptive: bool) -> dict[str, tuple[Rows, Measurement]]:
    results: dict[str, tuple[Rows, Measurement]] = {}
    with (
        spark_connect_server(envs=_envs(args, adaptive=adaptive)) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        spark = sessions.create()
        try:
            spark.conf.set("spark.sql.shuffle.partitions", str(args.parallelism))
        except Exception as e:  # noqa: BLE001
            print(f"warning: cannot set spark.sql.shuffle.partitions: {e}", file=sys.stderr)
        for name, workload in WORKLOADS.items():
            print(f"[aqe={'on' if adaptive else 'off'}] {name} ...", flush=True)
            before = _max_job_id(spark)
            rows = workload(spark, args.scale)  # warm-up; also the run the job metrics are attributed to
            after = _max_job_id(spark)
            measurement = Measurement()
            _summarize(measurement, *_settled_rows(spark, before, after))
            for _ in range(args.repeat):
                start = time.perf_counter()
                workload(spark, args.scale)
                measurement.wall_seconds_all.append(time.perf_counter() - start)
            measurement.wall_seconds_median = statistics.median(measurement.wall_seconds_all)
            results[name] = (rows, measurement)
    return results


def _fmt(value: float | None, digits: int = 3) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def _print_table(report: dict[str, dict[str, Any]]) -> None:
    header = (
        "| workload | mode | parity | wall s (median) | tasks | partitions | replanned | rules "
        "| max task s | median task s | task-seconds |"
    )
    print()
    print(header)
    print("|" + "|".join("---" for _ in range(header.count("|") - 1)) + "|")
    for name, entry in report.items():
        for mode in ("aqe_off", "aqe_on"):
            m = entry[mode]
            print(
                f"| {name} | {mode} | {entry['parity']} | {_fmt(m['wall_seconds_median'])} | {m['tasks']} "
                f"| {m['partitions'] if m['partitions'] is not None else '-'} "
                f"| {m['replanned_stages'] if m['replanned_stages'] is not None else '-'} "
                f"| {', '.join(m['adaptive_rules']) or '-'} | {_fmt(m['max_task_seconds'])} "
                f"| {_fmt(m['median_task_seconds'])} | {_fmt(m['task_seconds'])} |"
            )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--master", required=True, help="Celeborn master endpoint, as HOST:PORT")
    p.add_argument("--parallelism", type=int, default=64, help="default parallelism / shuffle partitions")
    p.add_argument("--scale", type=int, default=1, help="multiplier for workload row counts")
    p.add_argument("--repeat", type=int, default=3, help="timed runs per workload (median is reported)")
    p.add_argument("--skew-threshold-bytes", type=int, default=1048576, help="AQE skew threshold")
    p.add_argument("--target-size-bytes", type=int, default=1048576, help="AQE coalesce target partition size")
    p.add_argument("--output", help="write full results as JSON to this path")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    off = _run_mode(args, adaptive=False)
    on = _run_mode(args, adaptive=True)

    report: dict[str, dict[str, Any]] = {}
    ok = True
    for name in WORKLOADS:
        rows_off, m_off = off[name]
        rows_on, m_on = on[name]
        parity = rows_off == rows_on
        if not parity:
            ok = False
            print(
                f"ERROR: result mismatch for workload {name!r} between AQE off and on "
                f"({len(rows_off)} vs {len(rows_on)} rows)",
                file=sys.stderr,
            )
        report[name] = {
            "parity": parity,
            "result_rows": len(rows_off),
            "aqe_off": asdict(m_off),
            "aqe_on": asdict(m_on),
        }

    _print_table(report)
    if args.output:
        payload = {"args": vars(args), "workloads": report}
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        print(f"\nwrote {args.output}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
