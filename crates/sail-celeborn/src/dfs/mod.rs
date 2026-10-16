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
