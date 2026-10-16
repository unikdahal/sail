"""Integration tests for Celeborn partition reads: committed attempts, duplicate
batches and map-range reads against a real Celeborn worker."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pysail import _native

if TYPE_CHECKING:
    from collections.abc import Generator

    from pysail.testing.containers.celeborn import MasterService, WorkerService

ShuffleClient = _native._celeborn.ShuffleClient  # noqa: SLF001
LifecycleManager = _native._celeborn.LifecycleManager  # noqa: SLF001


@pytest.fixture(scope="module")
def lifecycle_manager(
    celeborn_master: MasterService,
    celeborn_workers: dict[str, WorkerService],
    celeborn_endpoint_resolver: object,
) -> Generator[LifecycleManager, None, None]:
    assert celeborn_workers
    with LifecycleManager(
        [f"{celeborn_master.host}:{celeborn_master.port}"],
        "sail-celeborn-read-integration",
        endpoint_resolver=celeborn_endpoint_resolver,
    ) as manager:
        yield manager


@pytest.fixture(scope="module")
def shuffle_client(lifecycle_manager: LifecycleManager) -> Generator[ShuffleClient, None, None]:
    with ShuffleClient(lifecycle_manager) as client:
        yield client


def _read(client: ShuffleClient, shuffle_id: int, **kwargs: object) -> list[bytes]:
    return list(client.read_partition_stream(shuffle_id, 0, **kwargs))


def test_reads_skip_batches_of_uncommitted_attempts(
    shuffle_client: ShuffleClient, lifecycle_manager: LifecycleManager
) -> None:
    shuffle_client.register_shuffle(11, [0], False, 1)
    # Attempt 0 of map 0 pushes partial output and fails without `mapper_end`.
    shuffle_client.push_data(11, 0, 0, 0, b"partial")
    # Attempt 1 of map 0 is retried and committed.
    shuffle_client.push_data(11, 0, 0, 1, b"retried")
    shuffle_client.mapper_end(11, 0, 1, 1)
    assert _read(shuffle_client, 11, map_attempts=[1]) == [b"retried"]
    # The worker stores the batches of every attempt, so a reader that does not know
    # the committed attempts would read the partial output too.
    assert sorted(_read(shuffle_client, 11)) == [b"partial", b"retried"]
    lifecycle_manager.unregister_shuffle(11)


def test_map_range_reads_return_only_the_mappers_in_range(
    shuffle_client: ShuffleClient, lifecycle_manager: LifecycleManager
) -> None:
    shuffle_client.register_shuffle(12, [0], False, 1)
    mappers = 4
    # Interleave the batches of the mappers so that the worker must sort the file.
    for batch in range(3):
        for map_id in range(mappers):
            shuffle_client.push_data(12, 0, map_id, 0, f"m{map_id}b{batch}".encode())
    for map_id in range(mappers):
        shuffle_client.mapper_end(12, map_id, 0, mappers)
    attempts = [0] * mappers
    everything = _read(shuffle_client, 12, map_attempts=attempts)
    assert sorted(everything) == sorted(f"m{m}b{b}".encode() for m in range(mappers) for b in range(3))
    for start, end in [(0, 1), (1, 3), (3, 4), (0, 4)]:
        rows = _read(shuffle_client, 12, map_range=(start, end), map_attempts=attempts)
        assert sorted(rows) == sorted(f"m{m}b{b}".encode() for m in range(start, end) for b in range(3)), (
            start,
            end,
        )
        # Batches of one mapper keep their order.
        for m in range(start, end):
            assert [r for r in rows if r.startswith(f"m{m}b".encode())] == [f"m{m}b{b}".encode() for b in range(3)]
    # The ranges of a split cover the partition exactly once.
    split = [r for start, end in [(0, 1), (1, 3), (3, 4)] for r in _read(shuffle_client, 12, map_range=(start, end))]
    assert sorted(split) == sorted(everything)
    lifecycle_manager.unregister_shuffle(12)
