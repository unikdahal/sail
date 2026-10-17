# ruff: noqa: INP001, S101

from __future__ import annotations

import importlib.util
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location("aqe_benchmark", Path(__file__).with_name("benchmark.py"))
benchmark = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = benchmark
spec.loader.exec_module(benchmark)


class Frame:
    def __init__(self, rows):
        self.rows = rows

    def where(self, predicate):
        return Frame([row for row in self.rows if predicate(row)])

    def orderBy(self, *columns):  # noqa: N802
        return Frame(sorted(self.rows, key=lambda row: tuple(getattr(row, name) for name in columns)))

    def collect(self):
        return self.rows


class Column:
    __hash__ = None

    def __init__(self, name):
        self.name = name

    def __eq__(self, value):
        return lambda row: getattr(row, self.name) == value


def stage(session_id, job_id=1, partitions=2):
    return SimpleNamespace(
        session_id=session_id,
        job_id=job_id,
        stage=0,
        partitions=partitions,
        initial_partitions=8,
        replanned=True,
        adaptive_rules=["coalesce"],
    )


def task(session_id, job_id=1, attempt=0, seconds=1.5):
    return SimpleNamespace(session_id=session_id, job_id=job_id, stage=0, partition=0, attempt=attempt, seconds=seconds)


def test_measurement_excludes_other_sessions_and_keeps_retry_attempts(monkeypatch):
    stages = [stage("workload"), stage("observer", partitions=100), stage("earlier-workload", partitions=200)]
    tasks = [task("workload"), task("workload", attempt=1, seconds=2.5), task("observer"), task("earlier-workload")]
    observer = SimpleNamespace(sql=lambda query: Frame(stages if "execution.stages" in query else tasks))
    monkeypatch.setattr(benchmark, "F", SimpleNamespace(col=Column))
    monkeypatch.setattr(benchmark, "_settled", lambda df: df.collect())
    result = benchmark.Measurement()
    benchmark._record(observer, "workload", result)
    assert (result.jobs, result.stages, result.tasks, result.task_attempts) == (1, 1, 1, 2)
    assert (result.partitions, result.initial_partitions, result.rules) == (2, 8, ["coalesce"])
    assert (result.task_seconds, result.slowest_task_seconds) == (4, 2.5)


def test_run_mode_does_not_count_observer_or_other_workload_jobs(monkeypatch):
    stages, tasks = [], []
    created = []

    class Session:
        def __init__(self):
            self.session_id = f"session-{len(created)}"
            self.next_job = 0
            created.append(self)

        def add_job(self):
            self.next_job += 1
            stages.append(stage(self.session_id, self.next_job))
            tasks.append(task(self.session_id, self.next_job))

        def sql(self, query):
            self.add_job()
            return Frame(stages if "execution.stages" in query else tasks)

    @contextmanager
    def server(**_kwargs):
        yield SimpleNamespace(remote="fake")

    @contextmanager
    def factory(_remote):
        yield SimpleNamespace(create=Session)

    def workload(spark, scale):
        spark.add_job()
        return Frame([(scale, 10), (scale, 5)])

    monkeypatch.setattr(benchmark, "spark_connect_server", server)
    monkeypatch.setattr(benchmark, "spark_session_factory", factory)
    monkeypatch.setattr(benchmark, "WORKLOADS", {"first": workload, "second": workload})
    monkeypatch.setattr(benchmark, "F", SimpleNamespace(col=Column))
    monkeypatch.setattr(benchmark, "_settled", lambda df: df.collect())
    args = SimpleNamespace(master="fake:1", target_size=4, skew_threshold=4, parallelism=8, scale=2, repeat=2)
    result = benchmark._run_mode(args, adaptive=True)
    assert len(created) == len(benchmark.WORKLOADS) + 1
    for rows, measurement in result.values():
        assert rows == [(2, 5), (2, 10)]
        assert (measurement.jobs, measurement.stages, measurement.tasks, measurement.partitions) == (1, 1, 1, 2)
        assert len(measurement.wall_seconds) == args.repeat
