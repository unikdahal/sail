"""Worker commit failures: a commit is retried as is, and a failed commit never loses rows silently.

Each worker commits a shuffle once and then stops storing pushes for it, so the
lifecycle manager must never select other map attempts after any worker has
committed. A failed worker commit is retried with the same request; if it keeps
failing, the shuffle is lost and the query fails.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from threading import Event
from typing import TYPE_CHECKING

import pytest

from pysail.testing.containers.celeborn import (
    CelebornFrame,
    CelebornMessageType,
    celeborn_rpc_endpoint_overrides,
    celeborn_rpc_message,
    celeborn_with_rpc_payload,
)
from pysail.testing.spark.session import spark_session_factory
from pysail.testing.utils.proxy import Forward, FrameReceived, FrameRule, Replace, RuleApplied

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from pyspark.sql import SparkSession

    from pysail.testing.containers.celeborn import MasterService, WorkerService
    from pysail.testing.utils.proxy import EndpointProxy

_COMMIT_FILES = 30
_COMMIT_FILES_RESPONSE = 31
_MOCK_FAILURE_STATUS = b"\x08\x31"  # PbCommitFilesResponse.status = 49
_ROWS = 256
_SERVER_SCRIPT = """
import sys
from pathlib import Path
from pysail.spark import SparkConnectServer

server = SparkConnectServer("127.0.0.1", 0)
server.start(background=True)
try:
    address = Path(sys.argv[1])
    pending = address.with_suffix(".pending")
    pending.write_text(f"sc://127.0.0.1:{server.listening_address[1]}")
    pending.replace(address)
    sys.stdin.read()
finally:
    server.stop()
"""


@pytest.fixture
def commit_remote(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
    celeborn_rpc_proxies: dict[str, EndpointProxy],
    tmp_path: Path,
) -> Generator[tuple[str, Path], None, None]:
    # A subprocess gives this test its own logger even when another test has
    # already initialized native telemetry. Keep the log as a pytest artifact.
    env = os.environ | {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "celeborn",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS": (
            f'["{celeborn_master.host}:{celeborn_master.port}"]'
        ),
        "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__ENDPOINT_OVERRIDES": celeborn_rpc_endpoint_overrides(
            celeborn_workers, celeborn_rpc_proxies
        ),
        "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "3",
        "RUST_LOG": "warn,sail_execution::driver::job_scheduler::core=debug",
    }
    address = tmp_path / "server-address"
    log = tmp_path / "driver.log"
    with log.open("w") as stderr:
        process = subprocess.Popen(
            [sys.executable, "-c", _SERVER_SCRIPT, str(address)],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=stderr,
            env=env,
            text=True,
        )
        try:
            deadline = time.monotonic() + 30
            while not address.exists():
                assert process.poll() is None, log.read_text()
                assert time.monotonic() < deadline, f"server startup timed out: {log.read_text()}"
                time.sleep(0.05)
            yield address.read_text(), log
        finally:
            try:
                process.communicate(input="", timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def _query(spark: SparkSession) -> list[int]:
    # One mapper makes the selected attempt unambiguous. Two output partitions
    # put files on both workers, so one worker can commit while the other fails.
    return sorted(row.id for row in spark.range(0, _ROWS, 1, 1).repartition(2, "id").collect())


def _is_commit(event: FrameReceived[CelebornFrame]) -> bool:
    return (
        event.direction == "client_to_server"
        and event.frame.message_type == CelebornMessageType.RPC_REQUEST
        and celeborn_rpc_message(event.frame)[0] == _COMMIT_FILES
    )


def _commit_responses(events: list[object]) -> list[bytes]:
    return [
        message[1]
        for event in events
        if isinstance(event, FrameReceived)
        and event.direction == "server_to_client"
        and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
        and (message := celeborn_rpc_message(event.frame))[0] == _COMMIT_FILES_RESPONSE
    ]


def _commit_requests(events: list[object]) -> list[bytes]:
    # Proxy observations retain the original request, before mockFailure is set.
    # Compare protobuf payloads: transport request IDs change on each retry.
    return [
        celeborn_rpc_message(event.frame)[1]
        for event in events
        if isinstance(event, FrameReceived) and _is_commit(event)
    ]


def _commits(log: Path) -> list[tuple[str, str, str, str]]:
    return re.findall(
        r"job (\S+) stage (\d+) shuffle commit: ShuffleCommit "
        r"\{ attempts: \[(.*?)\], succeeded: (false|true) \}",
        log.read_text(),
    )


def _fail_commit(event: FrameReceived[CelebornFrame]) -> Replace[CelebornFrame]:
    _, payload = celeborn_rpc_message(event.frame)
    # Celeborn 0.6.3 PbCommitFiles.mockFailure is bool field 7. Its worker rejects
    # the request before changing any commit state and returns status 49.
    # Appending the field also updates the RPC envelope's length fields.
    return Replace(celeborn_with_rpc_payload(event.frame, payload + b"\x38\x01"))


def _run_with_commit_fault(
    remote: str,
    proxies: dict[str, EndpointProxy],
    worker: int,
    count: int | None,
) -> tuple[list[int], list[int] | Exception, dict[str, list[object]]]:
    """Run the query, then run it again while CommitFiles requests to one worker fail."""
    with spark_session_factory(remote) as sessions:
        spark = sessions.create()
        expected = _query(spark)
        offsets = {name: len(proxy.events.snapshot()) for name, proxy in proxies.items()}
        failing = sorted(proxies)[worker]
        healthy = next(name for name in proxies if name != failing)
        committed = Event()

        def is_successful_commit(event: FrameReceived[CelebornFrame]) -> bool:
            # Java's protobuf writer emits status (field 1) first and omits zero.
            return (
                event.direction == "server_to_client"
                and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
                and (message := celeborn_rpc_message(event.frame))[0] == _COMMIT_FILES_RESPONSE
                and (not message[1].startswith(b"\x08") or message[1].startswith(b"\x08\x00"))
            )

        def observe_commit(_: FrameReceived[CelebornFrame]) -> Forward:
            committed.set()
            return Forward()

        def fail_after_commit(event: FrameReceived[CelebornFrame]) -> Replace[CelebornFrame]:
            # Worker commits run concurrently. Hold this request until the other
            # worker has committed, reproducing the original partial-commit bug
            # regardless of worker iteration order or CI scheduling.
            if not committed.wait(timeout=10):
                msg = "the other worker did not finish its commit before fault injection"
                raise ValueError(msg)
            return _fail_commit(event)

        proxies[healthy].rules.add(
            FrameRule(CelebornFrame, action=observe_commit, condition=is_successful_commit, count=1)
        )
        proxies[failing].rules.add(
            FrameRule(CelebornFrame, action=fail_after_commit, condition=_is_commit, count=count)
        )
        try:
            actual: list[int] | Exception = _query(spark)
        except Exception as error:  # noqa: BLE001
            actual = error
    events = {name: proxy.events.snapshot()[offsets[name] :] for name, proxy in proxies.items()}
    assert committed.is_set(), "the healthy worker never completed its commit"
    healthy_response = next(
        event for event in events[healthy] if isinstance(event, FrameReceived) and is_successful_commit(event)
    )
    injected = [
        event for event in events[failing] if isinstance(event, RuleApplied) and event.decision_name == "Replace"
    ]
    assert injected, "no CommitFiles fault was injected"
    assert all(event.timestamp > healthy_response.timestamp for event in injected)
    return expected, actual, events


@pytest.mark.timeout(120)
@pytest.mark.parametrize("worker", [0, 1], ids=["worker-1", "worker-2"])
def test_failed_worker_commit_is_retried_without_losing_rows(
    commit_remote: tuple[str, Path],
    celeborn_rpc_proxies: dict[str, EndpointProxy],
    worker: int,
) -> None:
    remote, log = commit_remote
    expected, actual, events = _run_with_commit_fault(remote, celeborn_rpc_proxies, worker, count=1)
    assert expected == list(range(_ROWS))

    failing = sorted(celeborn_rpc_proxies)[worker]
    for name, worker_events in events.items():
        requests = _commit_requests(worker_events)
        responses = _commit_responses(worker_events)
        statuses = [response.startswith(_MOCK_FAILURE_STATUS) for response in responses]
        if name == failing:
            # The worker rejected the first request and accepted the identical retry.
            assert statuses == [True, False], log.read_text()
            assert len(requests) == len(statuses)
            assert requests[0] == requests[1], "the retry changed the CommitFiles request"
        else:
            # The other worker committed once, so its commit was final.
            assert statuses == [False], log.read_text()
            assert len(requests) == 1

    commits = _commits(log)
    # The observer reports the final outcome once per query; the retry itself is
    # visible in the driver warning and in the worker RPCs above.
    assert [commit[2:] for commit in commits] == [("0", "true"), ("0", "true")], log.read_text()
    assert len({commit[:2] for commit in commits}) == len(commits), log.read_text()
    retries = re.findall(r"retrying commit of shuffle \d+ on worker (\S+) \(1/\d+\)", log.read_text())
    assert retries == [failing.split(":")[0]], log.read_text()
    assert not isinstance(actual, Exception), str(actual)
    assert actual == expected, f"the shuffle returned {len(actual)} of {len(expected)} rows"


@pytest.mark.timeout(120)
def test_persistently_failing_commit_fails_the_query(
    commit_remote: tuple[str, Path],
    celeborn_rpc_proxies: dict[str, EndpointProxy],
) -> None:
    remote, log = commit_remote
    _, actual, events = _run_with_commit_fault(remote, celeborn_rpc_proxies, 0, count=None)

    assert isinstance(actual, Exception), f"the query returned {len(actual)} rows from a lost shuffle"
    assert "lost" in str(actual), str(actual)
    failing = sorted(celeborn_rpc_proxies)[0]
    responses = _commit_responses(events[failing])
    requests = _commit_requests(events[failing])
    # The first request and every retry were rejected.
    assert len(responses) > 1
    assert all(response.startswith(_MOCK_FAILURE_STATUS) for response in responses)
    assert len(requests) == len(responses)
    assert all(request == requests[0] for request in requests), "a retry changed the CommitFiles request"
    commits = _commits(log)
    assert [commit[2:] for commit in commits] == [("0", "true"), ("0", "false")], log.read_text()
