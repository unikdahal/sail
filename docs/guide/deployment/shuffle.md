---
title: Shuffle Backends
rank: 3
---

# Shuffle Backends

Sail supports various shuffle backends for distributed data processing.

The shuffle backend can be configured via the `cluster.shuffle_backend.*` options. The examples on this page show how to set these configuration options using environment variables.

## Arrow Flight

The default shuffle backend is `flight`, which uses the Arrow Flight gRPC protocol for data exchange between tasks. This is equivalent to setting the following environment variable explicitly.

```bash
export SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE=flight
```

The `flight` shuffle backend implies _pipelined shuffle_, which means that shuffle data is streamed directly from map tasks to reduce tasks without intermediate storage. This is recommended for most use cases because it does not incur shuffle data persistence overhead. However, pipelined shuffle requires sufficient resources to run all tasks simultaneously. It also means that the entire job must be retried if any task fails, which may be less desirable for large-scale jobs and environments with high failure rates.

## Storage

To improve the resilience of shuffle-heavy, large-scale jobs, you can use the `storage` shuffle backend. This implies _blocking shuffle_, where map tasks write shuffle data to a storage backend before reduce tasks are launched to read it. This allows a job to have multiple task regions, so stages in different regions can be retried independently. You also need fewer workers to run the job because not all tasks must run simultaneously.

```bash
export SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE=storage
export SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH="s3://sail/shuffle"
```

## Apache Celeborn

[Apache Celeborn](https://celeborn.apache.org/) is a distributed shuffle service that provides high-performance shuffle data management. Sail supports Celeborn as a shuffle backend, which also implies _blocking shuffle_.

```bash
export SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE=celeborn
export SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS='["127.0.0.1:12097"]'
```

You can also specify multiple master endpoints in the array if your Celeborn cluster supports high availability.

Sail implements a native Celeborn client that communicates with the Celeborn master and workers.
The master advertises worker endpoints when the shuffle is registered. Worker hosts must be on the same network as Sail so that Sail workers can connect to Celeborn workers to read and write shuffle data.

### Adaptive Query Execution

With the `celeborn` shuffle backend, Sail can re-plan a stage just before it first runs, using the committed sizes of the shuffle data it reads. This experimental feature is disabled by default and can be enabled with the following environment variable.

```bash
export SAIL_CLUSTER__ADAPTIVE__ENABLED=true
```

When adaptive query execution is enabled, Sail applies the following rules to each stage whose inputs come from completed Celeborn shuffles.

- Small hash shuffle partitions are coalesced into fewer partitions of about `cluster.adaptive.coalesce.target_size` compressed bytes, so fewer tasks and task slots are needed. A coalesced partition never exceeds `cluster.adaptive.coalesce.max_logical_size` uncompressed bytes. Partition counts that you chose explicitly, for example with `repartition(n, ...)`, are kept.
- A join or grouped aggregation whose result must be empty because an input has no rows is replaced with an empty result.
- A partitioned hash join swaps its inputs when its build side is certainly larger than its probe side, and becomes a sort-merge join when a build partition certainly exceeds `cluster.adaptive.join.max_build_partition_size` bytes. Coalescing keeps every build partition of a hash join within the same limit.
- A skewed partition of a join input is split into several partitions, each reading the output of a subset of the map tasks with a Celeborn map-range read, and each paired with the matching partition of the other input. Only the join sides whose results do not depend on seeing the whole other side are split, so full outer joins are never split.

Sizes are known only for the shuffle data itself. A rule that depends on the size of an operator's output, such as a join input behind a filter or a nested join, is applied only when the shuffle sizes bound that output. Otherwise the stage keeps its static plan for that rule.

Each decision is made once and is kept when tasks are retried. If a rule cannot be applied safely, the stage runs as originally planned. `EXPLAIN` shows the initial job graph, and the `system.execution.stages` table shows the committed statistics of each stage and the stages that were re-planned. Statistics are collected only when adaptive query execution is enabled.

You can refer to the [Configuration](/reference/configuration/) reference page for advanced options for shuffle backends and adaptive query execution.
