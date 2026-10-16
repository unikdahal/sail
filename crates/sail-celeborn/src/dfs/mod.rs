//! Direct reads of Celeborn's distributed-storage partition files.

mod options;

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
pub use options::DfsReadOptions;

use crate::common::BatchHeader;
use crate::error::{CelebornError, CelebornResult};
use crate::worker::take_batch;

/// The worker has already finished any requested sort before returning `data_path`.
pub(crate) async fn read_partition(
    data_path: String,
    map_range: Option<Range<i32>>,
    options: DfsReadOptions,
    chunk_offsets: Vec<i64>,
) -> CelebornResult<BoxStream<'static, CelebornResult<(BatchHeader, Bytes)>>> {
    let (store, path) = options.resolve(&data_path)?;
    let chunk_offsets = (!chunk_offsets.is_empty()).then_some(chunk_offsets);
    read_partition_from_store(
        store,
        path,
        map_range,
        options.read_chunk_size,
        chunk_offsets,
    )
    .await
}

async fn read_partition_from_store(
    store: Arc<dyn ObjectStore>,
    path: Path,
    map_range: Option<Range<i32>>,
    read_chunk_size: u64,
    chunk_offsets: Option<Vec<i64>>,
) -> CelebornResult<BoxStream<'static, CelebornResult<(BatchHeader, Bytes)>>> {
    if read_chunk_size == 0 {
        return Err(CelebornError::InvalidArgument(
            "DFS read chunk size must be positive".to_string(),
        ));
    }
    if map_range
        .as_ref()
        .is_some_and(|range| range.start < 0 || range.end < range.start)
    {
        return Err(CelebornError::InvalidArgument(
            "invalid DFS map range".to_string(),
        ));
    }

    // Sorting replaces P.index with the map/block index and writes P.sorted. A
    // subsequent full-file read can therefore also receive the sorted data path.
    let sorted_base = path.as_ref().strip_suffix(".sorted");
    let index_path = Path::from(format!(
        "{}.index",
        sorted_base.unwrap_or_else(|| path.as_ref())
    ));
    let data_size = store.head(&path).await?.size;
    let regions = if sorted_base.is_some() {
        let index = store.get(&index_path).await?.bytes().await?;
        sorted_regions(index, map_range.as_ref(), data_size)?
    } else if let Some(chunk_offsets) = chunk_offsets {
        // OpenStream pins the original file while another reader may sort it
        // and replace P.index. Use its matching boundaries rather than that
        // potentially replaced index whenever the worker supplies them.
        unsorted_offset_regions(
            chunk_offsets.into_iter().map(|offset| {
                u64::try_from(offset).map_err(|_| invalid_index("negative DFS chunk offset"))
            }),
            data_size,
        )?
    } else {
        // A worker must return the sorted path for a map-range read. Reading an
        // unsorted file is still correct because the shared batch filter applies
        // the range after decoding each batch.
        let index = store.get(&index_path).await?.bytes().await?;
        unsorted_regions(index, data_size)?
    };
    let regions = if map_range.as_ref().is_some_and(Range::is_empty) {
        Vec::new()
    } else {
        merge_adjacent(regions)
    };

    let state = ReadState {
        store,
        path,
        regions: regions.into_iter(),
        current: None,
        buffered: BytesMut::new(),
        read_chunk_size,
    };
    Ok(Box::pin(stream::try_unfold(
        state,
        |mut state| async move {
            loop {
                if let Some(batch) = take_batch(&mut state.buffered)? {
                    return Ok(Some((batch, state)));
                }
                let Some(current) = state.current.as_mut() else {
                    state.current = state.regions.next();
                    if state.current.is_none() {
                        return Ok(None);
                    }
                    continue;
                };
                if current.is_empty() {
                    if !state.buffered.is_empty() {
                        return Err(invalid_index("truncated batch data in DFS region"));
                    }
                    state.current = None;
                    continue;
                }
                let end = current
                    .start
                    .saturating_add(state.read_chunk_size)
                    .min(current.end);
                let requested = current.start..end;
                let data = state
                    .store
                    .get_range(&state.path, requested.clone())
                    .await?;
                if u64::try_from(data.len()).ok() != Some(requested.end - requested.start) {
                    return Err(invalid_index("truncated DFS range response"));
                }
                current.start = end;
                state.buffered.extend_from_slice(&data);
            }
        },
    )))
}

struct ReadState {
    store: Arc<dyn ObjectStore>,
    path: Path,
    regions: std::vec::IntoIter<Range<u64>>,
    current: Option<Range<u64>>,
    buffered: BytesMut,
    read_chunk_size: u64,
}

// Celeborn 0.6.x TierWriter writes a big-endian i32 count followed by that
// many big-endian i64 chunk boundaries, including the initial zero and EOF.
fn unsorted_regions(mut index: Bytes, data_size: u64) -> CelebornResult<Vec<Range<u64>>> {
    let count = read_count(&mut index)?;
    if count == 0 || count != index.len() / 8 || !index.len().is_multiple_of(8) {
        return Err(invalid_index("invalid DFS chunk-offset count"));
    }
    unsorted_offset_regions((0..count).map(|_| read_offset(&mut index)), data_size)
}

fn unsorted_offset_regions(
    offsets: impl IntoIterator<Item = CelebornResult<u64>>,
    data_size: u64,
) -> CelebornResult<Vec<Range<u64>>> {
    let mut offsets = offsets.into_iter();
    let mut regions = Vec::new();
    let mut previous = offsets
        .next()
        .transpose()?
        .ok_or_else(|| invalid_index("missing DFS chunk offsets"))?;
    if previous != 0 {
        return Err(invalid_index("DFS chunk offsets must start at zero"));
    }
    for offset in offsets {
        let offset = offset?;
        if offset <= previous || offset > data_size {
            return Err(invalid_index(
                "invalid DFS chunk-offset order or file bounds",
            ));
        }
        regions.push(previous..offset);
        previous = offset;
    }
    if previous != data_size {
        return Err(invalid_index(
            "DFS chunk offsets do not end at the data file size",
        ));
    }
    Ok(regions)
}

// PartitionFilesSorter writes unordered map records: i32 mapId, i32 blockCount,
// then blockCount pairs of i64 file offset and i64 byte length (all big-endian).
fn sorted_regions(
    mut index: Bytes,
    map_range: Option<&Range<i32>>,
    data_size: u64,
) -> CelebornResult<Vec<Range<u64>>> {
    let mut maps = HashSet::new();
    let mut blocks = Vec::new();
    while !index.is_empty() {
        let map_id = read_i32(&mut index)?;
        if map_id < 0 || !maps.insert(map_id) {
            return Err(invalid_index("invalid or duplicate DFS index map ID"));
        }
        let count = read_count(&mut index)?;
        if count > index.len() / 16 {
            return Err(invalid_index("truncated DFS block index"));
        }
        let selected = map_range.is_none_or(|range| range.contains(&map_id));
        let mut previous_end = 0;
        for _ in 0..count {
            let offset = read_offset(&mut index)?;
            let length = read_offset(&mut index)?;
            let end = offset
                .checked_add(length)
                .ok_or_else(|| invalid_index("DFS block offset overflow"))?;
            if length == 0 || offset < previous_end || end > data_size {
                return Err(invalid_index(
                    "invalid DFS block order, length or file bounds",
                ));
            }
            blocks.push((offset..end, selected));
            previous_end = end;
        }
    }
    blocks.sort_unstable_by_key(|(range, _)| range.start);
    let mut previous_end = 0;
    let mut regions = Vec::new();
    for (range, selected) in blocks {
        if range.start != previous_end {
            return Err(invalid_index("overlapping or missing DFS index blocks"));
        }
        previous_end = range.end;
        if selected {
            regions.push(range);
        }
    }
    if previous_end != data_size {
        return Err(invalid_index("DFS index blocks do not cover the data file"));
    }
    Ok(regions)
}

fn merge_adjacent(regions: Vec<Range<u64>>) -> Vec<Range<u64>> {
    let mut merged: Vec<Range<u64>> = Vec::new();
    for region in regions {
        if let Some(previous) = merged.last_mut()
            && previous.end == region.start
        {
            previous.end = region.end;
            continue;
        }
        merged.push(region);
    }
    merged
}

fn read_i32(index: &mut Bytes) -> CelebornResult<i32> {
    if index.remaining() < 4 {
        return Err(invalid_index("truncated DFS index integer"));
    }
    Ok(index.get_i32())
}

fn read_count(index: &mut Bytes) -> CelebornResult<usize> {
    usize::try_from(read_i32(index)?).map_err(|_| invalid_index("negative DFS index count"))
}

fn read_offset(index: &mut Bytes) -> CelebornResult<u64> {
    if index.remaining() < 8 {
        return Err(invalid_index("truncated DFS index offset"));
    }
    u64::try_from(index.get_i64()).map_err(|_| invalid_index("negative DFS index offset or length"))
}

fn invalid_index(message: &str) -> CelebornError {
    CelebornError::Protocol(message.to_string())
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use futures::{StreamExt, TryStreamExt};
    use object_store::memory::InMemory;

    use super::*;
    use crate::common::{CompressionCodec, PartitionReadOptions};
    use crate::worker::BatchFilter;

    fn batch(map_id: i32, attempt_id: i32, batch_id: i32, payload: &[u8]) -> Vec<u8> {
        let mut bytes = BatchHeader {
            map_id,
            attempt_id,
            batch_id,
            length: i32::try_from(payload.len()).unwrap(),
        }
        .encode();
        bytes.extend_from_slice(payload);
        bytes
    }

    fn unsorted_index(offsets: &[i64]) -> Bytes {
        let mut index = i32::try_from(offsets.len()).unwrap().to_be_bytes().to_vec();
        for offset in offsets {
            index.extend_from_slice(&offset.to_be_bytes());
        }
        Bytes::from(index)
    }

    fn sorted_index(records: &[(i32, &[(i64, i64)])]) -> Bytes {
        let mut index = Vec::new();
        for (map_id, blocks) in records {
            index.extend_from_slice(&map_id.to_be_bytes());
            index.extend_from_slice(&i32::try_from(blocks.len()).unwrap().to_be_bytes());
            for (offset, length) in *blocks {
                index.extend_from_slice(&offset.to_be_bytes());
                index.extend_from_slice(&length.to_be_bytes());
            }
        }
        Bytes::from(index)
    }

    async fn fixture(path: &str, data: Vec<u8>, index: Bytes) -> Arc<dyn ObjectStore> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&Path::from(path), Bytes::from(data).into())
            .await
            .unwrap();
        let index_path = format!("{}.index", path.strip_suffix(".sorted").unwrap_or(path));
        store
            .put(&Path::from(index_path), index.into())
            .await
            .unwrap();
        store
    }

    async fn collect(
        store: Arc<dyn ObjectStore>,
        path: &str,
        map_range: Option<Range<i32>>,
        read_chunk_size: u64,
    ) -> CelebornResult<Vec<(BatchHeader, Bytes)>> {
        read_partition_from_store(store, Path::from(path), map_range, read_chunk_size, None)
            .await?
            .try_collect()
            .await
    }

    #[tokio::test]
    async fn reads_unsorted_batches_across_index_and_request_boundaries() {
        let first = batch(2, 1, 4, b"first payload");
        let second = batch(0, 0, 0, b"second payload");
        let data = [first, second].concat();
        let size = i64::try_from(data.len()).unwrap();
        // Even a header that crosses the index's chunk boundary must decode.
        let store = fixture("shuffle/partition", data, unsorted_index(&[0, 7, size])).await;
        let batches = collect(store, "shuffle/partition", None, 1).await.unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].0.map_id, 2);
        assert_eq!(batches[0].0.attempt_id, 1);
        assert_eq!(batches[0].0.batch_id, 4);
        assert_eq!(batches[0].1.as_ref(), b"first payload");
        assert_eq!(batches[1].0.map_id, 0);
        assert_eq!(batches[1].1.as_ref(), b"second payload");
    }

    #[tokio::test]
    async fn reads_sorted_ranges_with_unordered_records_multiple_blocks_and_gaps() {
        let first = batch(1, 0, 0, b"first");
        let second = batch(1, 0, 1, b"second");
        let third = batch(2, 1, 0, b"third");
        let fourth = batch(4, 0, 0, b"fourth");
        let gap_a = batch(3, 0, 0, b"unselected A");
        let gap_b = batch(0, 0, 0, b"unselected B");
        let first_len = i64::try_from(first.len()).unwrap();
        let second_len = i64::try_from(second.len()).unwrap();
        let third_len = i64::try_from(third.len()).unwrap();
        let fourth_len = i64::try_from(fourth.len()).unwrap();
        let gap_a_len = i64::try_from(gap_a.len()).unwrap();
        let gap_b_len = i64::try_from(gap_b.len()).unwrap();
        let gap_a_offset = first_len + second_len;
        let third_offset = gap_a_offset + gap_a_len;
        let gap_b_offset = third_offset + third_len;
        let fourth_offset = gap_b_offset + gap_b_len;
        let data = [first, second, gap_a, third, gap_b, fourth].concat();
        let index = sorted_index(&[
            (4, &[(fourth_offset, fourth_len)]),
            (2, &[(third_offset, third_len)]),
            (1, &[(0, first_len), (first_len, second_len)]),
            (3, &[(gap_a_offset, gap_a_len)]),
            (0, &[(gap_b_offset, gap_b_len)]),
        ]);
        let store = fixture("shuffle/partition.sorted", data, index).await;

        let batches = collect(store.clone(), "shuffle/partition.sorted", Some(1..3), 3)
            .await
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|(header, _)| header.map_id)
                .collect::<Vec<_>>(),
            vec![1, 1, 2]
        );
        assert_eq!(batches[2].1.as_ref(), b"third");

        // The worker may retain the sorted file for subsequent full-file reads.
        let batches = collect(store.clone(), "shuffle/partition.sorted", None, 7)
            .await
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|(header, _)| header.map_id)
                .collect::<Vec<_>>(),
            vec![1, 1, 3, 2, 0, 4]
        );
        for range in [2..2, 5..9] {
            assert!(
                collect(store.clone(), "shuffle/partition.sorted", Some(range), 7)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn reads_empty_partition_files() {
        for (path, index) in [
            ("empty", unsorted_index(&[0])),
            ("empty.sorted", Bytes::new()),
        ] {
            let store = fixture(path, Vec::new(), index).await;
            assert!(collect(store, path, None, 8).await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn resolves_configured_object_store_and_decoded_path() {
        let store = fixture(
            "shuffle/a b",
            batch(0, 0, 0, b"value"),
            unsorted_index(&[0, 21]),
        )
        .await;
        let options = DfsReadOptions {
            object_stores: std::collections::HashMap::from([("memory://bucket".into(), store)]),
            read_chunk_size: 2,
            ..Default::default()
        };
        let batches: Vec<_> = read_partition(
            "memory://bucket/shuffle/a%20b".into(),
            None,
            options,
            Vec::new(),
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.as_ref(), b"value");
    }

    #[tokio::test]
    async fn worker_offsets_remain_valid_when_a_concurrent_sort_replaces_the_index() {
        let first = batch(2, 0, 0, b"A");
        let second = batch(0, 0, 0, b"B");
        let store = fixture(
            "partition",
            [first, second].concat(),
            // P.index now describes P.sorted while the open stream pins P.
            sorted_index(&[(0, &[(0, 17)]), (2, &[(17, 17)])]),
        )
        .await;
        let options = DfsReadOptions {
            object_stores: std::collections::HashMap::from([(
                "memory://bucket".into(),
                store.clone(),
            )]),
            read_chunk_size: 3,
            ..Default::default()
        };
        for remove_index in [false, true] {
            if remove_index {
                store.delete(&Path::from("partition.index")).await.unwrap();
            }
            let batches: Vec<_> = read_partition(
                "memory://bucket/partition".into(),
                None,
                options.clone(),
                vec![0, 17, 34],
            )
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
            assert_eq!(
                batches
                    .iter()
                    .map(|(header, _)| header.map_id)
                    .collect::<Vec<_>>(),
                vec![2, 0]
            );
            assert_eq!(batches[0].1.as_ref(), b"A");
            assert_eq!(batches[1].1.as_ref(), b"B");
        }

        for offsets in [
            vec![],
            vec![-1, 34],
            vec![0, 17, 16, 34],
            vec![0, 33],
            vec![0, 35],
        ] {
            assert!(matches!(
                read_partition_from_store(
                    store.clone(),
                    Path::from("partition"),
                    None,
                    3,
                    Some(offsets),
                )
                .await,
                Err(CelebornError::Protocol(_))
            ));
        }
    }

    #[tokio::test]
    async fn filters_committed_attempts_ranges_and_duplicates_across_dfs_files() {
        let stale = batch(0, 0, 0, b"stale");
        let committed = batch(0, 1, 0, b"committed");
        let other = batch(1, 0, 0, b"other");
        let outside = batch(2, 0, 0, b"outside");
        let data = [stale, committed.clone(), committed.clone(), other, outside].concat();
        let size = i64::try_from(data.len()).unwrap();
        let first = fixture("first", data, unsorted_index(&[0, size])).await;
        let size = i64::try_from(committed.len()).unwrap();
        let second = fixture("second", committed, unsorted_index(&[0, size])).await;
        let options = PartitionReadOptions {
            map_range: Some(0..2),
            map_attempts: Some(Arc::from([1, 0, 0])),
        };
        let mut filter = BatchFilter::new(&options, CompressionCodec::None);
        let mut payloads = Vec::new();
        for (store, path) in [(first, "first"), (second, "second")] {
            for (header, payload) in collect(store, path, options.map_range.clone(), 5)
                .await
                .unwrap()
            {
                if let Some(payload) = filter.accept(header, payload).unwrap() {
                    payloads.push(payload);
                }
            }
        }
        assert_eq!(
            payloads,
            vec![
                Bytes::from_static(b"committed"),
                Bytes::from_static(b"other")
            ]
        );
    }

    #[tokio::test]
    async fn data_reads_are_lazy_and_object_store_errors_propagate() {
        let store = fixture(
            "partition",
            batch(0, 0, 0, b"value"),
            unsorted_index(&[0, 21]),
        )
        .await;
        let mut stream =
            read_partition_from_store(store.clone(), Path::from("partition"), None, 2, None)
                .await
                .unwrap();
        store.delete(&Path::from("partition")).await.unwrap();
        assert!(matches!(
            stream.next().await.unwrap(),
            Err(CelebornError::ObjectStore(_))
        ));
        assert!(stream.next().await.is_none());
        assert!(matches!(
            collect(store, "missing", None, 8).await,
            Err(CelebornError::ObjectStore(_))
        ));
    }

    #[tokio::test]
    async fn rejects_truncated_batches_and_negative_payload_lengths() {
        let complete = batch(0, 0, 0, b"payload");
        let mut negative_length = BatchHeader {
            map_id: 0,
            attempt_id: 0,
            batch_id: 0,
            length: -1,
        }
        .encode();
        negative_length.push(0);
        for data in [
            complete[..7].to_vec(),
            complete[..20].to_vec(),
            negative_length,
        ] {
            let size = i64::try_from(data.len()).unwrap();
            let store = fixture("partition", data, unsorted_index(&[0, size])).await;
            assert!(matches!(
                collect(store, "partition", None, 3).await,
                Err(CelebornError::Protocol(_))
            ));
        }

        // Separate indexed regions cannot complete one another's partial batch.
        let data = [complete[..7].to_vec(), vec![0; 4], complete].concat();
        let store = fixture(
            "partition.sorted",
            data,
            sorted_index(&[(0, &[(0, 7), (11, 23)]), (1, &[(7, 4)])]),
        )
        .await;
        assert!(matches!(
            collect(store, "partition.sorted", Some(0..1), 3).await,
            Err(CelebornError::Protocol(_))
        ));
    }

    #[test]
    fn rejects_malformed_unsorted_indices() {
        let mut trailing = unsorted_index(&[0, 12]).to_vec();
        trailing.push(0);
        for index in [
            Bytes::new(),
            Bytes::from_static(&[0, 0, 0]),
            Bytes::copy_from_slice(&(-1_i32).to_be_bytes()),
            unsorted_index(&[]),
            unsorted_index(&[1, 12]),
            unsorted_index(&[0, -1]),
            unsorted_index(&[0, 8, 7, 12]),
            unsorted_index(&[0, 8, 8, 12]),
            unsorted_index(&[0, 13]),
            unsorted_index(&[0, 11]),
            unsorted_index(&[0]),
            Bytes::from(trailing),
        ] {
            assert!(matches!(
                unsorted_regions(index, 12),
                Err(CelebornError::Protocol(_))
            ));
        }
        let mut truncated = unsorted_index(&[0, 12]).to_vec();
        truncated.pop();
        assert!(unsorted_regions(Bytes::from(truncated), 12).is_err());
    }

    #[test]
    fn rejects_malformed_sorted_indices_including_unselected_blocks() {
        let mut negative_count = 0_i32.to_be_bytes().to_vec();
        negative_count.extend_from_slice(&(-1_i32).to_be_bytes());
        let mut huge_count = 0_i32.to_be_bytes().to_vec();
        huge_count.extend_from_slice(&i32::MAX.to_be_bytes());
        for index in [
            Bytes::from_static(&[0]),
            Bytes::from(negative_count),
            Bytes::from(huge_count),
            sorted_index(&[(-1, &[(0, 12)])]),
            sorted_index(&[(0, &[(0, 6)]), (0, &[(6, 6)])]),
            sorted_index(&[(0, &[(-1, 12)])]),
            sorted_index(&[(0, &[(0, -1)])]),
            sorted_index(&[(0, &[(0, 0)])]),
            sorted_index(&[(0, &[(0, 13)])]),
            sorted_index(&[(0, &[(6, 6), (0, 6)])]),
            sorted_index(&[(0, &[(0, 8), (6, 6)])]),
            sorted_index(&[(0, &[(0, 8)]), (1, &[(6, 6)])]),
            sorted_index(&[(0, &[(1, 11)])]),
            sorted_index(&[(0, &[(0, 5), (6, 6)])]),
            sorted_index(&[(0, &[(0, 11)])]),
            sorted_index(&[(0, &[])]),
            Bytes::new(),
            sorted_index(&[(0, &[(i64::MAX, i64::MAX)])]),
        ] {
            assert!(matches!(
                sorted_regions(index, Some(&(9..10)), 12),
                Err(CelebornError::Protocol(_))
            ));
        }
        let mut truncated = sorted_index(&[(0, &[(0, 12)])]).to_vec();
        truncated.pop();
        assert!(sorted_regions(Bytes::from(truncated), None, 12).is_err());
    }

    #[tokio::test]
    async fn rejects_invalid_read_options_and_missing_index() {
        let store = fixture(
            "partition",
            batch(0, 0, 0, b"value"),
            unsorted_index(&[0, 21]),
        )
        .await;
        for (range, chunk_size) in [
            (None, 0),
            (Some(-1..2), 8),
            (Some(Range { start: 3, end: 2 }), 8),
        ] {
            assert!(matches!(
                collect(store.clone(), "partition", range, chunk_size).await,
                Err(CelebornError::InvalidArgument(_))
            ));
        }
        store.delete(&Path::from("partition.index")).await.unwrap();
        assert!(matches!(
            collect(store, "partition", None, 8).await,
            Err(CelebornError::ObjectStore(_))
        ));
    }
}
