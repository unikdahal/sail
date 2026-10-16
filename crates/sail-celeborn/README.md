# Celeborn DFS partition reads

The native client reads HDFS and S3-compatible (S3, S3A, S3N, OSS) partition
files through `object_store`. Local-disk and memory partitions continue to use
worker chunk fetches. The shuffle reader API is unchanged.

## Configuration

`ShuffleClientOptions::with_dfs_read_options` configures a `DfsReadOptions` value.
`WorkerClientOptions` exposes the same builder for direct worker clients.
Defaults use the native HDFS client's Hadoop configuration and the S3 builder's
AWS environment/credential providers. For an S3-compatible service, for example:

```rust
use sail_celeborn::dfs::DfsReadOptions;

let mut dfs = DfsReadOptions::default();
dfs.s3_options.insert("region".into(), "us-east-1".into());
dfs.s3_options.insert("endpoint".into(), "http://minio:9000".into());
dfs.s3_options.insert("allow_http".into(), "true".into());
// Supply credentials through the AWS environment or the S3 builder options.
// Pass `dfs` to the shuffle or worker client options builder.
```

`hdfs_options` supplies Hadoop configuration overrides. `object_stores` can
register an existing `Arc<dyn ObjectStore>` by `scheme://authority`, such as
`s3://bucket` or `hdfs://namenode:8020`. Object paths are relative to that store's
root. Registered stores take precedence over built-in configuration; they also
allow custom backends. OSS requires its S3 compatibility API with AWS signature
V4 enabled, the signing region, a bucket-qualified endpoint, and
`virtual_hosted_style_request=true`.
`read_chunk_size` bounds individual data requests (8 MiB by default); a batch can
span requests.

## Wire protocol and file layout

Celeborn 0.6.x normally responds to DFS `OpenStream` with zero chunks and no
path. For such responses the client requests direct-read metadata with
`readLocalShuffle=true`. The worker returns its actual chosen data path; this
also handles whole reads after a previous map-range read sorted the partition.
The first request for a nonempty local partition still uses chunk fetches.
The extra worker stream registrations are closed on completion, failure, or
consumer cancellation.

For an original path `P`, full reads use `P` and `P.index`. The index contains a
big-endian signed 32-bit offset count followed by big-endian signed 64-bit chunk
offsets. Map-range reads wait for the worker to sort, then use `P.sorted` and
`P.index`. The sorted index contains map records in arbitrary order: a
big-endian signed 32-bit map ID and block count, followed by that many pairs of
big-endian signed 64-bit byte offsets and lengths. The client selects blocks in
`[startMapIndex, endMapIndex)` and reads them in file order. It rejects malformed
indexes, out-of-file regions, and truncated batches.
For unsorted reads, offsets supplied by the worker take precedence over the
index: another reader can replace `P.index` while sorting, although the active
stream still pins the original data file.

Batch headers remain little-endian. Both storage paths feed the same shared
filter, which selects committed mapper attempts, enforces map ranges, deduplicates
`(mapId, attemptId, batchId)` across split locations, and decompresses payloads.

Format references (Celeborn 0.6.3):

- [DfsPartitionReader](https://github.com/apache/celeborn/blob/v0.6.3/client/src/main/java/org/apache/celeborn/client/read/DfsPartitionReader.java)
- [ShuffleBlockInfoUtils](https://github.com/apache/celeborn/blob/v0.6.3/common/src/main/java/org/apache/celeborn/common/util/ShuffleBlockInfoUtils.java)
- [PartitionFilesSorter](https://github.com/apache/celeborn/blob/v0.6.3/worker/src/main/java/org/apache/celeborn/service/deploy/worker/storage/PartitionFilesSorter.java)
- [TierWriter](https://github.com/apache/celeborn/blob/v0.6.3/worker/src/main/scala/org/apache/celeborn/service/deploy/worker/storage/TierWriter.scala)
- [FetchHandler](https://github.com/apache/celeborn/blob/v0.6.3/worker/src/main/scala/org/apache/celeborn/service/deploy/worker/FetchHandler.scala)

## Tests

`cargo test -p sail-celeborn` runs in-memory object-store fixtures in the Celeborn
file layout and scripted TCP workers. These tests need no external service.
They cover index selection, read boundaries, committed-attempt filtering,
deduplication, metadata discovery, and the local chunk-fetch path.
