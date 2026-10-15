//! Committed shuffle output statistics.
//!
//! Shuffle sinks count what they accept and publish the counts only after the
//! backend has committed the output. The counts travel from the worker to the
//! driver with the task's `Succeeded` status, and the driver keeps them in a
//! compact form until every consumer of the shuffle has made its adaptive
//! decision.
//!
//! Every metric has a fixed unit:
//!   - `rows`: rows accepted by the sink.
//!   - `physical_bytes`: bytes the backend stores for the channel after its own
//!     compression. This is unknown (never zero) when the backend cannot tell.
//!   - `logical_bytes`: uncompressed Arrow data the accepted batches reference,
//!     counting slices and view buffers by the bytes they actually reference.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};

/// The committed output statistics of one task attempt, one entry per output channel.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskOutputStatistics {
    pub rows: Vec<u64>,
    /// `None` when the backend cannot tell the stored size of the channels.
    pub physical_bytes: Option<Vec<u64>>,
    pub logical_bytes: Vec<u64>,
}

impl TaskOutputStatistics {
    pub fn channels(&self) -> usize {
        self.rows.len()
    }

    pub fn validate(&self) -> Result<()> {
        let channels = self.rows.len();
        if self.logical_bytes.len() != channels
            || self
                .physical_bytes
                .as_ref()
                .is_some_and(|x| x.len() != channels)
        {
            return Err(DataFusionError::Internal(format!(
                "inconsistent task output statistics: {} rows, {:?} physical bytes, {} logical bytes",
                channels,
                self.physical_bytes.as_ref().map(|x| x.len()),
                self.logical_bytes.len()
            )));
        }
        Ok(())
    }

    /// The schema of the completion batch emitted by `ShuffleWriteExec`.
    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("rows", DataType::UInt64, false),
            Field::new("physical_bytes", DataType::UInt64, true),
            Field::new("logical_bytes", DataType::UInt64, false),
        ]))
    }

    /// Encodes the statistics as a completion batch with one row per channel.
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        self.validate()?;
        let physical = match &self.physical_bytes {
            Some(x) => UInt64Array::from(x.clone()),
            None => UInt64Array::new_null(self.rows.len()),
        };
        Ok(RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(UInt64Array::from(self.rows.clone())),
                Arc::new(physical),
                Arc::new(UInt64Array::from(self.logical_bytes.clone())),
            ],
        )?)
    }

    /// Decodes a completion batch. Returns `None` for a batch without rows,
    /// which a writer emits when it has no committed output to describe.
    pub fn try_from_record_batch(batch: &RecordBatch) -> Result<Option<Self>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        if batch.schema().fields() != Self::schema().fields() {
            return Err(DataFusionError::Internal(format!(
                "unexpected task output statistics schema: {:?}",
                batch.schema()
            )));
        }
        let column = |i: usize| -> Result<&UInt64Array> {
            batch
                .column(i)
                .as_primitive_opt()
                .ok_or_else(|| DataFusionError::Internal("invalid statistics column".to_string()))
        };
        let rows = column(0)?;
        let physical = column(1)?;
        let logical = column(2)?;
        // A partially known physical size is treated as unknown.
        let physical_bytes = (physical.null_count() == 0).then(|| physical.values().to_vec());
        Ok(Some(Self {
            rows: rows.values().to_vec(),
            physical_bytes,
            logical_bytes: logical.values().to_vec(),
        }))
    }
}

impl From<TaskOutputStatistics> for crate::driver::r#gen::TaskOutputStatistics {
    fn from(value: TaskOutputStatistics) -> Self {
        Self {
            rows: value.rows,
            physical_bytes: value.physical_bytes.unwrap_or_default(),
            logical_bytes: value.logical_bytes,
        }
    }
}

impl TryFrom<crate::driver::r#gen::TaskOutputStatistics> for TaskOutputStatistics {
    type Error = DataFusionError;

    fn try_from(value: crate::driver::r#gen::TaskOutputStatistics) -> Result<Self> {
        // An empty list means unknown, except for a task without output channels.
        let physical_bytes = if value.physical_bytes.is_empty() && !value.rows.is_empty() {
            None
        } else {
            Some(value.physical_bytes)
        };
        let statistics = Self {
            rows: value.rows,
            physical_bytes,
            logical_bytes: value.logical_bytes,
        };
        statistics.validate()?;
        Ok(statistics)
    }
}

/// Counts what one channel sink accepts.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ChannelCounter {
    pub rows: u64,
    pub logical_bytes: u64,
}

impl ChannelCounter {
    pub fn add(&mut self, batch: &RecordBatch) -> Result<()> {
        self.rows = self.rows.saturating_add(batch.num_rows() as u64);
        self.logical_bytes = self
            .logical_bytes
            .saturating_add(logical_batch_size(batch)?);
        Ok(())
    }
}

/// Returns the uncompressed size of the Arrow data that the batch references.
///
/// Unlike [`RecordBatch::get_array_memory_size`], this counts sliced arrays and
/// shared buffers only by the bytes they reference, so splitting a batch into
/// slices leaves the total unchanged. View arrays count their views plus the
/// bytes each out-of-line view references. Dictionary values are counted in
/// full for every batch, since every batch carries the dictionary.
pub fn logical_batch_size(batch: &RecordBatch) -> Result<u64> {
    batch.columns().iter().try_fold(0u64, |acc, array| {
        Ok(acc.saturating_add(logical_array_size(array)?))
    })
}

fn logical_array_size(array: &ArrayRef) -> Result<u64> {
    let len = array.len();
    let nulls = if array.nulls().is_some() {
        len.div_ceil(8) as u64
    } else {
        0
    };
    let size = match array.data_type() {
        DataType::Utf8View | DataType::BinaryView => {
            let views = match array.data_type() {
                DataType::Utf8View => array.as_string_view().views(),
                _ => array.as_binary_view().views(),
            };
            let referenced = views
                .iter()
                .enumerate()
                .filter(|(i, _)| array.is_valid(*i))
                .map(|(_, view)| {
                    let length = *view as u32;
                    // Views up to 12 bytes store the value inline.
                    if length > 12 { length as u64 } else { 0 }
                })
                .sum::<u64>();
            ((len * size_of::<u128>()) as u64).saturating_add(referenced)
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            let start = offsets[0] as usize;
            let end = offsets[len] as usize;
            ((len + 1) * size_of::<i32>()) as u64
                + logical_array_size(&list.values().slice(start, end - start))?
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.value_offsets();
            let start = offsets[0] as usize;
            let end = offsets[len] as usize;
            ((len + 1) * size_of::<i64>()) as u64
                + logical_array_size(&list.values().slice(start, end - start))?
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            let start = offsets[0] as usize;
            let end = offsets[len] as usize;
            let entries: ArrayRef = Arc::new(map.entries().slice(start, end - start));
            ((len + 1) * size_of::<i32>()) as u64 + logical_array_size(&entries)?
        }
        DataType::FixedSizeList(_, size) => {
            let list = array.as_fixed_size_list();
            let size = *size as usize;
            let offset = list.offset() * size;
            logical_array_size(&list.values().slice(offset, len * size))?
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .try_fold(0u64, |acc, child| {
                Ok::<_, DataFusionError>(acc.saturating_add(logical_array_size(child)?))
            })?,
        DataType::Dictionary(key, _) => {
            let width = key.primitive_width().unwrap_or(size_of::<u64>());
            ((len * width) as u64)
                .saturating_add(logical_array_size(array.as_any_dictionary().values())?)
        }
        _ => {
            // Primitive, boolean and offset-based variable-width arrays are sized by
            // their slice. Other layouts fall back to Arrow's slice size, which counts
            // the referenced range of every buffer and already includes the null bitmap.
            return Ok(array.to_data().get_slice_memory_size()? as u64);
        }
    };
    Ok(size.saturating_add(nulls))
}

/// Encodes a size in one byte with 10% precision, as Spark does for map output sizes.
/// Zero stays exact.
pub(crate) fn compress_size(size: u64) -> u8 {
    const LOG_BASE: f64 = 1.1;
    if size == 0 {
        0
    } else if size <= 1 {
        1
    } else {
        ((size as f64).ln() / LOG_BASE.ln()).ceil().min(255.0) as u8
    }
}

pub(crate) fn decompress_size(size: u8) -> u64 {
    const LOG_BASE: f64 = 1.1;
    if size == 0 {
        0
    } else {
        LOG_BASE.powi(size as i32) as u64
    }
}

/// Statistics of one committed attempt as retained by the driver.
/// Totals are exact, while per-channel values use the one-byte log encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactOutputStatistics {
    pub total_rows: u64,
    pub total_physical_bytes: Option<u64>,
    pub total_logical_bytes: u64,
    rows: Box<[u8]>,
    physical_bytes: Option<Box<[u8]>>,
    logical_bytes: Box<[u8]>,
}

impl CompactOutputStatistics {
    pub fn new(statistics: &TaskOutputStatistics) -> Self {
        let compress = |x: &[u64]| x.iter().map(|v| compress_size(*v)).collect::<Box<[u8]>>();
        Self {
            total_rows: statistics.rows.iter().fold(0, |a, b| a.saturating_add(*b)),
            total_physical_bytes: statistics
                .physical_bytes
                .as_ref()
                .map(|x| x.iter().fold(0u64, |a, b| a.saturating_add(*b))),
            total_logical_bytes: statistics
                .logical_bytes
                .iter()
                .fold(0, |a, b| a.saturating_add(*b)),
            rows: compress(&statistics.rows),
            physical_bytes: statistics.physical_bytes.as_deref().map(compress),
            logical_bytes: compress(&statistics.logical_bytes),
        }
    }

    pub fn channels(&self) -> usize {
        self.rows.len()
    }

    /// The approximate number of bytes the driver retains for these statistics.
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self.rows.len()
            + self.logical_bytes.len()
            + self.physical_bytes.as_ref().map(|x| x.len()).unwrap_or(0)
    }

    pub fn rows(&self, channel: usize) -> u64 {
        // A channel with a nonzero total but rounding to zero cannot happen, since
        // only zero is encoded as zero.
        decompress_size(self.rows[channel])
    }

    pub fn physical_bytes(&self, channel: usize) -> Option<u64> {
        self.physical_bytes
            .as_ref()
            .map(|x| decompress_size(x[channel]))
    }

    pub fn logical_bytes(&self, channel: usize) -> u64 {
        decompress_size(self.logical_bytes[channel])
    }
}

/// Per-bucket totals of a shuffle, folded from the committed attempt of every
/// producer partition. Bucket `b` is channel `b` of every producer partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffleStatistics {
    /// Exact total rows across all buckets.
    pub total_rows: u64,
    pub total_logical_bytes: u64,
    pub total_physical_bytes: Option<u64>,
    /// Approximate rows per bucket. A bucket is zero only if it is exactly empty.
    pub rows: Vec<u64>,
    pub physical_bytes: Option<Vec<u64>>,
    pub logical_bytes: Vec<u64>,
}

impl ShuffleStatistics {
    /// Folds the statistics of the committed attempts of all producer partitions.
    pub fn fold<'a>(
        channels: usize,
        attempts: impl IntoIterator<Item = &'a CompactOutputStatistics>,
    ) -> Result<Self> {
        let mut result = Self {
            total_rows: 0,
            total_logical_bytes: 0,
            total_physical_bytes: Some(0),
            rows: vec![0; channels],
            physical_bytes: Some(vec![0; channels]),
            logical_bytes: vec![0; channels],
        };
        for attempt in attempts {
            if attempt.channels() != channels {
                return Err(DataFusionError::Internal(format!(
                    "expected statistics for {channels} channels, got {}",
                    attempt.channels()
                )));
            }
            result.total_rows = result.total_rows.saturating_add(attempt.total_rows);
            result.total_logical_bytes = result
                .total_logical_bytes
                .saturating_add(attempt.total_logical_bytes);
            result.total_physical_bytes = result
                .total_physical_bytes
                .zip(attempt.total_physical_bytes)
                .map(|(a, b)| a.saturating_add(b));
            for c in 0..channels {
                result.rows[c] = result.rows[c].saturating_add(attempt.rows(c));
                result.logical_bytes[c] =
                    result.logical_bytes[c].saturating_add(attempt.logical_bytes(c));
            }
            match (&mut result.physical_bytes, attempt.physical_bytes.is_some()) {
                (Some(physical), true) => {
                    for (c, value) in physical.iter_mut().enumerate() {
                        *value = value.saturating_add(attempt.physical_bytes(c).unwrap_or(0));
                    }
                }
                (physical, _) => *physical = None,
            }
        }
        Ok(result)
    }

    /// The per-bucket size used for sizing decisions: physical bytes when known.
    pub fn sizing_bytes(&self) -> &[u64] {
        self.physical_bytes
            .as_deref()
            .unwrap_or(&self.logical_bytes)
    }

    pub fn max_and_median_bucket_bytes(&self) -> (u64, u64) {
        let mut sizes = self.sizing_bytes().to_vec();
        if sizes.is_empty() {
            return (0, 0);
        }
        sizes.sort_unstable();
        (sizes[sizes.len() - 1], sizes[sizes.len() / 2])
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{
        BinaryViewArray, Int32Array, ListArray, StringArray, StringViewArray, StructArray,
    };
    use datafusion::arrow::datatypes::Int32Type;

    use super::*;

    fn batch() -> RecordBatch {
        let ints = Arc::new(Int32Array::from_iter(
            (0..100).map(|x| (x % 7 != 0).then_some(x)),
        ));
        let strings = Arc::new(StringArray::from_iter_values(
            (0..100).map(|x| "x".repeat(x % 13)),
        ));
        let views = Arc::new(StringViewArray::from_iter_values(
            (0..100).map(|x| "long-string-value-".repeat(x % 3)),
        ));
        let binary_views = Arc::new(BinaryViewArray::from_iter_values(
            (0..100).map(|x| vec![1u8; x % 20]),
        ));
        let lists = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            (0..100).map(|x| Some((0..(x % 5)).map(Some).collect::<Vec<_>>())),
        ));
        let structs = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from_iter_values(0..100)) as ArrayRef,
        )]));
        RecordBatch::try_from_iter(vec![
            ("ints", ints as ArrayRef),
            ("strings", strings as ArrayRef),
            ("views", views as ArrayRef),
            ("binary_views", binary_views as ArrayRef),
            ("lists", lists as ArrayRef),
            ("structs", structs as ArrayRef),
        ])
        .unwrap()
    }

    #[test]
    fn logical_size_is_additive_under_slicing() {
        let batch = batch();
        let total = logical_batch_size(&batch).unwrap();
        assert!(total > 0);
        for split in [1, 17, 50, 99] {
            let left = logical_batch_size(&batch.slice(0, split)).unwrap();
            let right = logical_batch_size(&batch.slice(split, batch.num_rows() - split)).unwrap();
            // Offset buffers carry one extra boundary per array (strings and lists),
            // and null bitmaps round up to whole bytes, so a split may add a few bytes.
            assert!(left + right >= total, "split at {split}");
            assert!(
                left + right <= total + 2 * size_of::<i32>() as u64 + 2,
                "split at {split}"
            );
        }
    }

    #[test]
    fn logical_size_ignores_unreferenced_buffers() {
        let batch = batch();
        let slice = batch.slice(10, 5);
        assert!(logical_batch_size(&slice).unwrap() < logical_batch_size(&batch).unwrap() / 5);
        assert!(slice.get_array_memory_size() >= batch.get_array_memory_size() / 2);
    }

    #[test]
    fn statistics_round_trip_through_completion_batch() {
        for physical_bytes in [Some(vec![3, 0, 5]), None] {
            let statistics = TaskOutputStatistics {
                rows: vec![1, 0, 2],
                physical_bytes,
                logical_bytes: vec![10, 0, 20],
            };
            let batch = statistics.to_record_batch().unwrap();
            assert_eq!(
                TaskOutputStatistics::try_from_record_batch(&batch).unwrap(),
                Some(statistics)
            );
        }
        let empty = RecordBatch::new_empty(TaskOutputStatistics::schema());
        assert_eq!(
            TaskOutputStatistics::try_from_record_batch(&empty).unwrap(),
            None
        );
    }

    #[test]
    fn inconsistent_statistics_are_rejected() {
        let statistics = TaskOutputStatistics {
            rows: vec![1, 2],
            physical_bytes: Some(vec![1]),
            logical_bytes: vec![1, 2],
        };
        assert!(statistics.to_record_batch().is_err());
    }

    #[test]
    fn compressed_sizes_keep_zero_exact_and_stay_within_ten_percent() {
        assert_eq!(decompress_size(compress_size(0)), 0);
        assert_eq!(decompress_size(compress_size(1)), 1);
        for size in [2u64, 10, 1000, 123_456, 1 << 30, 7 << 32] {
            let approx = decompress_size(compress_size(size)) as f64;
            let error = (approx - size as f64).abs() / size as f64;
            assert!(error <= 0.1, "size {size} approx {approx}");
        }
        assert_eq!(compress_size(u64::MAX), 255);
    }

    #[test]
    fn fold_sums_buckets_and_propagates_unknown_physical_sizes() {
        let a = CompactOutputStatistics::new(&TaskOutputStatistics {
            rows: vec![0, 100],
            physical_bytes: Some(vec![0, 1000]),
            logical_bytes: vec![0, 4000],
        });
        let b = CompactOutputStatistics::new(&TaskOutputStatistics {
            rows: vec![5, 0],
            physical_bytes: Some(vec![50, 0]),
            logical_bytes: vec![200, 0],
        });
        let folded = ShuffleStatistics::fold(2, [&a, &b]).unwrap();
        assert_eq!(folded.total_rows, 105);
        assert_eq!(folded.total_physical_bytes, Some(1050));
        assert!(folded.rows[0] > 0);
        let physical = folded.physical_bytes.clone().unwrap();
        assert!((45..=55).contains(&physical[0]));
        assert!((900..=1100).contains(&physical[1]));

        let c = CompactOutputStatistics::new(&TaskOutputStatistics {
            rows: vec![1, 1],
            physical_bytes: None,
            logical_bytes: vec![1, 1],
        });
        let folded = ShuffleStatistics::fold(2, [&a, &c]).unwrap();
        assert_eq!(folded.physical_bytes, None);
        assert_eq!(folded.total_physical_bytes, None);
        assert_eq!(folded.sizing_bytes(), folded.logical_bytes.as_slice());
        assert!(ShuffleStatistics::fold(3, [&a]).is_err());
    }
}
