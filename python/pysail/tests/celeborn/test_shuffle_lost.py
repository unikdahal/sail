"""A lost global commit also invalidates direct client partition reads."""

from __future__ import annotations

from threading import Event
from typing import TYPE_CHECKING

import pytest

from pysail import _native
from pysail.testing.containers.celeborn import (
    CelebornFrame,
    CelebornMessageType,
    celeborn_rpc_message,
    celeborn_with_rpc_payload,
)
from pysail.testing.utils.proxy import Forward, FrameReceived, FrameRule, Replace, RuleApplied

if TYPE_CHECKING:
    from pysail.testing.containers.celeborn import MasterService, WorkerService
    from pysail.testing.utils.proxy import EndpointProxy

ShuffleClient = _native._celeborn.ShuffleClient  # noqa: SLF001
LifecycleManager = _native._celeborn.LifecycleManager  # noqa: SLF001
StaticEndpointResolver = _native._celeborn.StaticEndpointResolver  # noqa: SLF001
_COMMIT_FILES = 30
_COMMIT_FILES_RESPONSE = 31


@pytest.mark.timeout(60)
def test_direct_reader_rejects_healthy_partition_after_global_commit_failure(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
    celeborn_rpc_proxies: dict[str, EndpointProxy],
) -> None:
    resolver = StaticEndpointResolver(
        {
            (name.split(":")[0], port): (host, mapped_port)
            for name, worker in celeborn_workers.items()
            for port, host, mapped_port in zip(
                worker.internal_ports,
                (celeborn_rpc_proxies[name].host, worker.host, worker.host, worker.host),
                (celeborn_rpc_proxies[name].port, worker.push_port, worker.fetch_port, worker.replicate_port),
                strict=True,
            )
        }
    )
    committed = Event()

    def is_successful_commit(event: FrameReceived[CelebornFrame]) -> bool:
        return (
            event.direction == "server_to_client"
            and event.frame.message_type == CelebornMessageType.RPC_RESPONSE
            and (message := celeborn_rpc_message(event.frame))[0] == _COMMIT_FILES_RESPONSE
            and (not message[1].startswith(b"\x08") or message[1].startswith(b"\x08\x00"))
        )

    def observe_commit(_: FrameReceived[CelebornFrame]) -> Forward:
        committed.set()
        return Forward()

    def commits_partition_one(event: FrameReceived[CelebornFrame]) -> bool:
        return (
            event.direction == "client_to_server"
            and event.frame.message_type == CelebornMessageType.RPC_REQUEST
            and (message := celeborn_rpc_message(event.frame))[0] == _COMMIT_FILES
            # PbCommitFiles.primaryIds is string field 3. Initial partition
            # location 1 has unique ID "1-0" (partition 1, epoch 0).
            and b"\x1a\x031-0" in message[1]
        )

    def fail_after_healthy_commit(event: FrameReceived[CelebornFrame]) -> Replace[CelebornFrame]:
        if not committed.wait(timeout=10):
            msg = "partition 0's worker did not commit before fault injection"
            raise ValueError(msg)
        _, payload = celeborn_rpc_message(event.frame)
        # Every retry fails before changing this worker's commit state.
        # PbCommitFiles.mockFailure is bool field 7 in Celeborn 0.6.3.
        return Replace(celeborn_with_rpc_payload(event.frame, payload + b"\x38\x01"))

    with (
        LifecycleManager(
            [f"{celeborn_master.host}:{celeborn_master.port}"],
            "sail-celeborn-lost-read-integration",
            endpoint_resolver=resolver,
        ) as manager,
        ShuffleClient(manager) as client,
    ):
        # These two reduce partitions are allocated to different workers.
        assert len(client.register_shuffle(1, [0, 1], False, 2)) == 2  # noqa: PLR2004
        client.push_data(1, 0, 0, 0, b"healthy partition")
        client.push_data(1, 1, 0, 0, b"failed partition")
        for proxy in celeborn_rpc_proxies.values():
            proxy.rules.add(FrameRule(CelebornFrame, action=observe_commit, condition=is_successful_commit, count=1))
            proxy.rules.add(
                FrameRule(CelebornFrame, action=fail_after_healthy_commit, condition=commits_partition_one, count=None)
            )
        with pytest.raises(RuntimeError, match="is lost"):
            client.mapper_end(1, 0, 0, 1)
        assert committed.is_set(), "the healthy worker never completed its commit"
        assert any(
            isinstance(event, RuleApplied) and event.decision_name == "Replace"
            for proxy in celeborn_rpc_proxies.values()
            for event in proxy.events.snapshot()
        ), "no commit failure was injected"

        # Partition 0 is physically readable, but its global shuffle commit has
        # failed. This API has no engine task-definition guard to reject it.
        with pytest.raises(RuntimeError, match="is lost"):
            list(client.read_partition_stream(1, 0))
