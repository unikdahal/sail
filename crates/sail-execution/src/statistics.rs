//! Committed shuffle output statistics.
//!
//! Statistics are collected only for blocking outputs that the driver will re-plan
//! from, as requested in the task definition. Shuffle sinks count what they accept
//! and publish the counts only after the backend has committed the output.
//!
//! The exact per-channel counts ([`ChannelCounts`]) never leave the worker. A task
//! reports [`TaskOutputStatistics`]: exact totals and per-channel sizes in a compact
//! encoding ([`ChannelSizes`]) of at most two bytes per channel, which the driver
//! retains as is until every consumer of the shuffle has made its adaptive decision.
//! Logical sizes use dense upper bounds for safe coalescing caps; physical sizes
//! may use sparse averages for advisory targets and skew detection.
//!
//! Every metric has a fixed unit:
//!   - `rows`: rows accepted by the sink, reported as a total only.
//!   - `physical_bytes`: bytes the backend stores for the channel after its own
//!     compression. This is unknown (never zero) when the backend cannot tell.
//!   - `logical_bytes`: uncompressed Arrow data the accepted batches reference,
//!     counting slices and view buffers by the bytes they actually reference.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};

use crate::driver::r#gen;

/// The exact counts of one task attempt, one entry per output channel.
/// They describe the committed output of the attempt on the worker.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChannelCounts {
    pub rows: Vec<u64>,
    /// `None` when the backend cannot tell the stored size of the channels.
    pub physical_bytes: Option<Vec<u64>>,
    pub logical_bytes: Vec<u64>,
}

impl ChannelCounts {
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
                "inconsistent channel counts: {} rows, {:?} physical bytes, {} logical bytes",
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

    /// Encodes the counts as a completion batch with one row per channel.
    /// The batch only travels from the shuffle writer to the task monitor.
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

    /// Decodes a completion batch. Returns `None` for a batch without rows, which a
    /// writer emits when it has no committed output to describe or does not count.
    pub fn try_from_record_batch(batch: &RecordBatch) -> Result<Option<Self>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        if batch.schema().fields() != Self::schema().fields() {
            return Err(DataFusionError::Internal(format!(
                "unexpected channel counts schema: {:?}",
                batch.schema()
            )));
        }
        let column = |i: usize| -> Result<&UInt64Array> {
            batch.column(i).as_primitive_opt().ok_or_else(|| {
                DataFusionError::Internal("invalid channel counts column".to_string())
            })
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

/// Counts what one channel sink accepts.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ChannelCounter {
    pub rows: u64,
    pub logical_bytes: u64,
}

impl ChannelCounter {
    /// Counting never fails a write, since statistics are advisory. If a layout
    /// cannot be sized, Arrow's buffer size is used as an upper bound.
    pub fn add(&mut self, batch: &RecordBatch) {
        self.rows = self.rows.saturating_add(batch.num_rows() as u64);
        let size =
            logical_batch_size(batch).unwrap_or_else(|_| batch.get_array_memory_size() as u64);
        self.logical_bytes = self.logical_bytes.saturating_add(size);
    }
}

/// Returns the uncompressed size of the Arrow data that the batch references.
///
/// Unlike [`RecordBatch::get_array_memory_size`], this counts sliced arrays only
/// by the bytes they reference. Splitting a batch can add offset boundaries and
/// bitmap rounding, and repeats the full dictionary values for every batch.
/// Consequently these sizes are not invariant under operators that rebatch data.
/// View arrays count their views plus the bytes each out-of-line view references.
pub fn logical_batch_size(batch: &RecordBatch) -> Result<u64> {
    batch.columns().iter().try_fold(0u64, |acc, array| {
        Ok(acc.saturating_add(logical_array_size(array)?))
    })
}

fn logical_array_size(array: &ArrayRef) -> Result<u64> {
    let len = array.len();
    if len == 0 {
        // An empty offset-based array may have an empty offsets buffer.
        return Ok(0);
    }
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

/// The base of the logarithmic size code. A decoded size is an upper bound no
/// more than about 1% larger than the encoded size, and every `u64` has a code,
/// so large sizes never saturate.
const SIZE_CODE_BASE: f64 = 1.01;

/// Encodes a size in two bytes: `0` for zero, otherwise `1 + ceil(log_1.01(size))`.
/// The largest code, for `u64::MAX`, is about 4,460.
pub(crate) fn encode_size(size: u64) -> u16 {
    if size == 0 {
        return 0;
    }
    let exponent = ((size as f64).ln() / SIZE_CODE_BASE.ln()).ceil();
    let mut code = (1.0 + exponent).min(u16::MAX as f64) as u16;
    // At large values, float conversion or logarithm rounding can round down
    // across an integer boundary. Logical sizes must remain upper bounds.
    while decode_size(code) < size {
        code += 1;
    }
    code
}

pub(crate) fn decode_size(code: u16) -> u64 {
    if code == 0 {
        0
    } else {
        // The float-to-integer conversion saturates.
        SIZE_CODE_BASE.powi(code as i32 - 1).round() as u64
    }
}

/// Above this many channels, a task reports sizes in the sparse form when it is
/// smaller than the dense form, as Spark's `HighlyCompressedMapStatus` does.
const DENSE_CHANNEL_LIMIT: usize = 2000;

/// A nonempty channel of a sparse encoding is recorded on its own when it is larger
/// than this factor times the average nonempty channel. Fewer than half of the
/// nonempty channels can exceed twice the average.
const HUGE_CHANNEL_FACTOR: u64 = 2;

/// Per-channel sizes of one task attempt.
///
///   - The dense form keeps a two-byte code per channel, within 1% of each size.
///   - The sparse form keeps which channels are empty, the sizes of channels much
///     larger than the average within 1%, and one average for the remaining channels.
///
/// An empty channel is exactly zero in both forms, and a nonempty channel is never
/// zero. The driver keeps this form as is, so its memory is at most two bytes per
/// channel and task, and often one bit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelSizes {
    Dense(Box<[u16]>),
    Sparse(SparseChannelSizes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseChannelSizes {
    channels: usize,
    /// A bitmap of the nonempty channels, least significant bit first.
    nonempty: Box<[u8]>,
    /// The size of every nonempty channel without its own entry.
    average: u64,
    /// The channels recorded on their own, in ascending order, with their size codes.
    huge: Box<[(u32, u16)]>,
}

impl ChannelSizes {
    fn encode_dense(sizes: &[u64]) -> Self {
        Self::Dense(sizes.iter().map(|&x| encode_size(x)).collect())
    }

    pub fn encode(sizes: &[u64]) -> Self {
        let dense = || Self::encode_dense(sizes);
        if sizes.len() <= DENSE_CHANNEL_LIMIT || u32::try_from(sizes.len()).is_err() {
            return dense();
        }
        // Sum before dividing in a wider type. Saturating at u64::MAX before
        // taking an average can substantially understate large channels.
        let (total, count) = sizes
            .iter()
            .filter(|&&x| x > 0)
            .fold((0u128, 0u128), |(t, c), &x| (t + u128::from(x), c + 1));
        let threshold = (total / count.max(1)) * u128::from(HUGE_CHANNEL_FACTOR);
        let mut nonempty = vec![0u8; sizes.len().div_ceil(8)];
        let mut huge = vec![];
        let (mut rest_total, mut rest_count) = (0u128, 0u128);
        for (channel, &size) in sizes.iter().enumerate() {
            if size == 0 {
                continue;
            }
            nonempty[channel / 8] |= 1 << (channel % 8);
            if u128::from(size) > threshold {
                // The channel count fits in `u32`, as checked above.
                huge.push((channel as u32, encode_size(size)));
            } else {
                rest_total += u128::from(size);
                rest_count += 1;
            }
        }
        let sparse = SparseChannelSizes {
            channels: sizes.len(),
            nonempty: nonempty.into(),
            // An average of u64 values also fits in u64.
            average: rest_total.div_ceil(rest_count.max(1)).max(1) as u64,
            huge: huge.into(),
        };
        if sparse.encoded_bytes() < sizes.len() * size_of::<u16>() {
            Self::Sparse(sparse)
        } else {
            dense()
        }
    }

    pub fn channels(&self) -> usize {
        match self {
            Self::Dense(codes) => codes.len(),
            Self::Sparse(sparse) => sparse.channels,
        }
    }

    /// Returns the approximate size of a channel, or zero for a channel out of range.
    pub fn get(&self, channel: usize) -> u64 {
        match self {
            Self::Dense(codes) => codes.get(channel).map_or(0, |&x| decode_size(x)),
            Self::Sparse(sparse) => {
                if !sparse.is_nonempty(channel) {
                    return 0;
                }
                match sparse
                    .huge
                    .binary_search_by_key(&channel, |&(c, _)| c as usize)
                {
                    Ok(i) => decode_size(sparse.huge[i].1),
                    Err(_) => sparse.average,
                }
            }
        }
    }

    /// Adds the approximate size of every channel to `totals`, which has one entry
    /// per channel.
    pub fn add_to(&self, totals: &mut [u64]) {
        match self {
            Self::Dense(codes) => {
                for (total, &code) in totals.iter_mut().zip(codes.iter()) {
                    *total = total.saturating_add(decode_size(code));
                }
            }
            Self::Sparse(sparse) => {
                let mut huge = sparse.huge.iter().peekable();
                for (channel, total) in totals.iter_mut().enumerate().take(sparse.channels) {
                    if !sparse.is_nonempty(channel) {
                        continue;
                    }
                    let size = match huge.next_if(|(c, _)| *c as usize == channel) {
                        Some(&(_, code)) => decode_size(code),
                        None => sparse.average,
                    };
                    *total = total.saturating_add(size);
                }
            }
        }
    }

    /// The approximate number of bytes the encoding occupies.
    pub fn encoded_bytes(&self) -> usize {
        match self {
            Self::Dense(codes) => codes.len() * size_of::<u16>(),
            Self::Sparse(sparse) => sparse.encoded_bytes(),
        }
    }
}

impl SparseChannelSizes {
    fn is_nonempty(&self, channel: usize) -> bool {
        channel < self.channels
            && self
                .nonempty
                .get(channel / 8)
                .is_some_and(|x| x & (1 << (channel % 8)) != 0)
    }

    fn encoded_bytes(&self) -> usize {
        self.nonempty.len() + size_of::<u64>() + self.huge.len() * size_of::<(u32, u16)>()
    }
}

impl From<ChannelSizes> for r#gen::ChannelSizes {
    fn from(value: ChannelSizes) -> Self {
        use r#gen::channel_sizes::Kind;
        let kind = match value {
            ChannelSizes::Dense(codes) => Kind::Dense(r#gen::DenseChannelSizes {
                codes: codes.iter().flat_map(|x| x.to_le_bytes()).collect(),
            }),
            ChannelSizes::Sparse(sparse) => Kind::Sparse(r#gen::SparseChannelSizes {
                channels: sparse.channels as u64,
                nonempty: sparse.nonempty.into_vec(),
                average: sparse.average,
                huge_channels: sparse.huge.iter().map(|&(c, _)| c).collect(),
                huge_codes: sparse
                    .huge
                    .iter()
                    .flat_map(|&(_, code)| code.to_le_bytes())
                    .collect(),
            }),
        };
        Self { kind: Some(kind) }
    }
}

impl TryFrom<r#gen::ChannelSizes> for ChannelSizes {
    type Error = DataFusionError;

    fn try_from(value: r#gen::ChannelSizes) -> Result<Self> {
        use r#gen::channel_sizes::Kind;
        let invalid =
            |reason: &str| DataFusionError::Internal(format!("invalid channel sizes: {reason}"));
        let codes = |bytes: &[u8]| -> Result<Vec<u16>> {
            let (codes, rest) = bytes.as_chunks::<{ size_of::<u16>() }>();
            if !rest.is_empty() {
                return Err(invalid("odd number of code bytes"));
            }
            Ok(codes.iter().map(|x| u16::from_le_bytes(*x)).collect())
        };
        match value.kind {
            Some(Kind::Dense(dense)) => Ok(Self::Dense(codes(&dense.codes)?.into())),
            Some(Kind::Sparse(sparse)) => {
                let channels =
                    usize::try_from(sparse.channels).map_err(|_| invalid("too many channels"))?;
                if sparse.nonempty.len() != channels.div_ceil(8) {
                    return Err(invalid("bitmap does not match the channels"));
                }
                if channels % 8 != 0
                    && sparse
                        .nonempty
                        .last()
                        .is_some_and(|&byte| byte >> (channels % 8) != 0)
                {
                    return Err(invalid("bitmap has bits beyond the channels"));
                }
                let huge_codes = codes(&sparse.huge_codes)?;
                if huge_codes.len() != sparse.huge_channels.len() {
                    return Err(invalid("huge channels and codes differ in length"));
                }
                let huge = sparse
                    .huge_channels
                    .into_iter()
                    .zip(huge_codes)
                    .collect::<Box<[_]>>();
                let result = SparseChannelSizes {
                    channels,
                    nonempty: sparse.nonempty.into(),
                    average: sparse.average,
                    huge,
                };
                let ordered = result.huge.windows(2).all(|x| x[0].0 < x[1].0);
                let consistent = result
                    .huge
                    .iter()
                    .all(|&(c, code)| code > 0 && result.is_nonempty(c as usize));
                if !ordered || !consistent || result.average == 0 {
                    return Err(invalid("inconsistent sparse sizes"));
                }
                Ok(Self::Sparse(result))
            }
            None => Err(invalid("missing kind")),
        }
    }
}

/// The committed output statistics of one task attempt, as reported to the driver
/// and retained there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskOutputStatistics {
    pub rows: u64,
    pub logical_bytes: u64,
    /// `None` when the backend cannot tell the stored size of the channels.
    pub physical_bytes: Option<u64>,
    /// New reports use dense size codes, upper bounds with about 1% precision.
    /// Legacy sparse reports are read conservatively using the exact task total
    /// for every nonempty channel, rather than their potentially low averages.
    pub channel_logical_bytes: ChannelSizes,
    pub channel_physical_bytes: Option<ChannelSizes>,
}

impl TaskOutputStatistics {
    pub fn new(counts: &ChannelCounts) -> Result<Self> {
        counts.validate()?;
        let sum = |x: &[u64]| x.iter().fold(0u64, |a, b| a.saturating_add(*b));
        Ok(Self {
            rows: sum(&counts.rows),
            logical_bytes: sum(&counts.logical_bytes),
            physical_bytes: counts.physical_bytes.as_deref().map(sum),
            channel_logical_bytes: ChannelSizes::encode_dense(&counts.logical_bytes),
            channel_physical_bytes: counts.physical_bytes.as_deref().map(ChannelSizes::encode),
        })
    }

    pub fn channels(&self) -> usize {
        self.channel_logical_bytes.channels()
    }

    fn validate(&self) -> Result<()> {
        if self
            .channel_physical_bytes
            .as_ref()
            .is_some_and(|x| x.channels() != self.channels())
            || self.channel_physical_bytes.is_some() != self.physical_bytes.is_some()
        {
            return Err(DataFusionError::Internal(
                "inconsistent task output statistics".to_string(),
            ));
        }
        Ok(())
    }

    /// The approximate number of bytes the driver retains for these statistics.
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self.channel_logical_bytes.encoded_bytes()
            + self
                .channel_physical_bytes
                .as_ref()
                .map_or(0, |x| x.encoded_bytes())
    }

    pub fn physical_bytes_of(&self, channel: usize) -> Option<u64> {
        self.channel_physical_bytes.as_ref().map(|x| x.get(channel))
    }

    pub fn logical_bytes_of(&self, channel: usize) -> u64 {
        match &self.channel_logical_bytes {
            ChannelSizes::Dense(_) => self.channel_logical_bytes.get(channel),
            // Older workers may report sparse logical averages. A nonempty
            // channel cannot exceed the exact task total, which gives a safe
            // conservative bound without trusting the average.
            ChannelSizes::Sparse(sparse) => {
                if sparse.is_nonempty(channel) {
                    self.logical_bytes
                } else {
                    0
                }
            }
        }
    }

    fn add_logical_bytes_to(&self, totals: &mut [u64]) {
        match &self.channel_logical_bytes {
            ChannelSizes::Dense(_) => self.channel_logical_bytes.add_to(totals),
            ChannelSizes::Sparse(_) => {
                for (channel, total) in totals.iter_mut().enumerate() {
                    *total = total.saturating_add(self.logical_bytes_of(channel));
                }
            }
        }
    }
}

impl From<TaskOutputStatistics> for r#gen::TaskOutputStatistics {
    fn from(value: TaskOutputStatistics) -> Self {
        Self {
            rows: value.rows,
            logical_bytes: value.logical_bytes,
            physical_bytes: value.physical_bytes,
            channel_logical_bytes: Some(value.channel_logical_bytes.into()),
            channel_physical_bytes: value.channel_physical_bytes.map(Into::into),
        }
    }
}

impl TryFrom<r#gen::TaskOutputStatistics> for TaskOutputStatistics {
    type Error = DataFusionError;

    fn try_from(value: r#gen::TaskOutputStatistics) -> Result<Self> {
        let statistics = Self {
            rows: value.rows,
            logical_bytes: value.logical_bytes,
            physical_bytes: value.physical_bytes,
            channel_logical_bytes: value
                .channel_logical_bytes
                .ok_or_else(|| {
                    DataFusionError::Internal("missing channel logical bytes".to_string())
                })?
                .try_into()?,
            channel_physical_bytes: value
                .channel_physical_bytes
                .map(TryInto::try_into)
                .transpose()?,
        };
        statistics.validate()?;
        Ok(statistics)
    }
}

/// Per-bucket totals of a shuffle, folded from the committed attempt of every
/// producer partition. Bucket `b` is channel `b` of every producer partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffleStatistics {
    /// Exact totals across all buckets.
    pub total_rows: u64,
    pub total_logical_bytes: u64,
    pub total_physical_bytes: Option<u64>,
    /// Upper bounds per bucket. A bucket is zero only if it is exactly empty.
    pub logical_bytes: Vec<u64>,
    /// Approximate sizes per bucket, used for advisory sizing and skew detection.
    pub physical_bytes: Option<Vec<u64>>,
}

impl ShuffleStatistics {
    /// Folds the statistics of the committed attempts of all producer partitions.
    pub fn fold<'a>(
        channels: usize,
        attempts: impl IntoIterator<Item = &'a TaskOutputStatistics>,
    ) -> Result<Self> {
        let mut result = Self {
            total_rows: 0,
            total_logical_bytes: 0,
            total_physical_bytes: Some(0),
            logical_bytes: vec![0; channels],
            physical_bytes: Some(vec![0; channels]),
        };
        for attempt in attempts {
            attempt.validate()?;
            if attempt.channels() != channels {
                return Err(DataFusionError::Internal(format!(
                    "expected statistics for {channels} channels, got {}",
                    attempt.channels()
                )));
            }
            result.total_rows = result.total_rows.saturating_add(attempt.rows);
            result.total_logical_bytes = result
                .total_logical_bytes
                .saturating_add(attempt.logical_bytes);
            result.total_physical_bytes = result
                .total_physical_bytes
                .zip(attempt.physical_bytes)
                .map(|(a, b)| a.saturating_add(b));
            attempt.add_logical_bytes_to(&mut result.logical_bytes);
            match (&mut result.physical_bytes, &attempt.channel_physical_bytes) {
                (Some(physical), Some(sizes)) => sizes.add_to(physical),
                (physical, _) => *physical = None,
            }
        }
        Ok(result)
    }

    /// The approximate number of bytes the driver retains for these statistics.
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + (self.logical_bytes.len() + self.physical_bytes.as_ref().map_or(0, |x| x.len()))
                * size_of::<u64>()
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
        BinaryViewArray, DictionaryArray, Int32Array, ListArray, StringArray, StringViewArray,
        StructArray,
    };
    use datafusion::arrow::datatypes::Int32Type;
    use proptest::prelude::*;

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
    fn logical_size_of_empty_arrays_is_zero() {
        use datafusion::arrow::array::ArrayData;
        use datafusion::arrow::buffer::Buffer;

        // An empty string array may come with an empty offsets buffer, e.g. after IPC decoding.
        let data = ArrayData::builder(DataType::Utf8)
            .len(0)
            .add_buffer(Buffer::from_vec(Vec::<i32>::new()))
            .add_buffer(Buffer::from_vec(Vec::<u8>::new()))
            .build()
            .unwrap();
        let array = datafusion::arrow::array::make_array(data);
        let batch = RecordBatch::try_from_iter(vec![("s", array)]).unwrap();
        assert_eq!(logical_batch_size(&batch).unwrap(), 0);
    }

    #[test]
    fn logical_size_ignores_unreferenced_buffers() {
        let batch = batch();
        let slice = batch.slice(10, 5);
        assert!(logical_batch_size(&slice).unwrap() < logical_batch_size(&batch).unwrap() / 5);
        assert!(slice.get_array_memory_size() >= batch.get_array_memory_size() / 2);
    }

    #[tokio::test]
    async fn sorting_can_increase_logical_size_by_repeating_dictionaries() {
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::TaskContext;
        use datafusion::execution::context::SessionConfig;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        use datafusion::physical_plan::sorts::sort::SortExec;

        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0; 100]),
            Arc::new(StringArray::from(vec!["x".repeat(10_000)])),
        )
        .unwrap();
        let input = RecordBatch::try_from_iter(vec![
            (
                "key",
                Arc::new(Int32Array::from_iter_values((0..100).rev())) as ArrayRef,
            ),
            ("value", Arc::new(dictionary) as ArrayRef),
        ])
        .unwrap();
        let input_size = logical_batch_size(&input).unwrap();
        let source =
            MemorySourceConfig::try_new_exec(&[vec![input.clone()]], input.schema(), None).unwrap();
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            Column::new("key", 0),
        ))])
        .unwrap();
        let sort = Arc::new(SortExec::new(ordering, source));
        let context = Arc::new(
            TaskContext::default().with_session_config(
                SessionConfig::new()
                    .with_batch_size(10)
                    .with_sort_in_place_threshold_bytes(usize::MAX),
            ),
        );
        let output = datafusion::physical_plan::collect(sort, context)
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 100);
        assert!(output.len() > 1);
        let output_size = output
            .iter()
            .map(|batch| logical_batch_size(batch).unwrap())
            .sum::<u64>();
        assert!(output_size > input_size * 5);
    }

    #[test]
    fn counts_round_trip_through_completion_batch() {
        for physical_bytes in [Some(vec![3, 0, 5]), None] {
            let counts = ChannelCounts {
                rows: vec![1, 0, 2],
                physical_bytes,
                logical_bytes: vec![10, 0, 20],
            };
            let batch = counts.to_record_batch().unwrap();
            assert_eq!(
                ChannelCounts::try_from_record_batch(&batch).unwrap(),
                Some(counts)
            );
        }
        let empty = RecordBatch::new_empty(ChannelCounts::schema());
        assert_eq!(ChannelCounts::try_from_record_batch(&empty).unwrap(), None);
    }

    #[test]
    fn inconsistent_counts_are_rejected() {
        let counts = ChannelCounts {
            rows: vec![1, 2],
            physical_bytes: Some(vec![1]),
            logical_bytes: vec![1, 2],
        };
        assert!(counts.to_record_batch().is_err());
        assert!(TaskOutputStatistics::new(&counts).is_err());
    }

    fn within_one_percent(approx: u64, size: u64) -> bool {
        approx.abs_diff(size) <= size / 100 + 1
    }

    #[test]
    fn size_codes_keep_zero_exact_and_never_saturate() {
        assert_eq!(decode_size(encode_size(0)), 0);
        assert_eq!(decode_size(encode_size(1)), 1);
        for size in [2u64, 10, 1000, 123_456, 1 << 30, 7 << 32, 1 << 50] {
            let approx = decode_size(encode_size(size));
            assert!(approx >= size, "{size} -> {approx}");
            assert!(within_one_percent(approx, size), "{size} -> {approx}");
        }
        assert!(encode_size(u64::MAX) < 4500);
        assert!(decode_size(encode_size(u64::MAX)) > u64::MAX / 2);
    }

    #[test]
    fn size_codes_are_upper_bounds_at_rounding_boundaries() {
        for code in 1..=encode_size(u64::MAX) {
            let boundary = decode_size(code);
            for size in [
                boundary.saturating_sub(1),
                boundary,
                boundary.saturating_add(1),
            ] {
                assert!(decode_size(encode_size(size)) >= size, "{size}");
            }
        }
    }

    proptest! {
        #[test]
        fn size_codes_are_within_one_percent(size in 1u64..u64::MAX) {
            let approx = decode_size(encode_size(size));
            prop_assert!(approx >= size, "{} -> {}", size, approx);
            prop_assert!(within_one_percent(approx, size), "{} -> {}", size, approx);
        }

        #[test]
        fn channel_sizes_preserve_emptiness_and_totals(
            sizes in prop::collection::vec(
                prop_oneof![Just(0u64), 1u64..1000, 1u64..(1 << 40)],
                0..5000,
            ),
        ) {
            let encoded = ChannelSizes::encode(&sizes);
            prop_assert_eq!(encoded.channels(), sizes.len());
            let proto: r#gen::ChannelSizes = encoded.clone().into();
            prop_assert_eq!(&ChannelSizes::try_from(proto).unwrap(), &encoded);
            prop_assert!(encoded.encoded_bytes() <= sizes.len() * 2 + 64);

            let mut totals = vec![0; sizes.len()];
            encoded.add_to(&mut totals);
            for (c, &size) in sizes.iter().enumerate() {
                prop_assert_eq!(totals[c], encoded.get(c));
                prop_assert_eq!(totals[c] == 0, size == 0);
                if matches!(encoded, ChannelSizes::Dense(_)) {
                    prop_assert!(within_one_percent(totals[c], size));
                }
            }
            // The sparse form keeps the total within the code precision plus the
            // rounding of the average.
            let total = sizes.iter().sum::<u64>();
            let approx = totals.iter().sum::<u64>();
            prop_assert!(approx.abs_diff(total) <= total / 100 + sizes.len() as u64);
        }
    }

    #[test]
    fn many_channels_use_the_sparse_form_with_accurate_large_channels() {
        let mut sizes = vec![0u64; 10_000];
        for (c, size) in sizes.iter_mut().enumerate().step_by(3) {
            *size = 100 + c as u64 % 7;
        }
        sizes[42] = 1 << 33;
        let encoded = ChannelSizes::encode(&sizes);
        assert!(matches!(encoded, ChannelSizes::Sparse(_)));
        assert!(encoded.encoded_bytes() < 10_000 / 8 + 64);
        assert!(within_one_percent(encoded.get(42), 1 << 33));
        assert_eq!(encoded.get(1), 0);
        assert!((100..=107).contains(&encoded.get(3)));
    }

    #[test]
    fn sparse_averages_do_not_saturate_before_dividing() {
        let channels = 4096;
        for size in [u64::MAX / 4096 + u64::MAX / 8192, u64::MAX] {
            let encoded = ChannelSizes::encode(&vec![size; channels]);
            assert!(matches!(encoded, ChannelSizes::Sparse(_)));
            assert_eq!(encoded.get(0), size);
            assert_eq!(encoded.get(channels - 1), size);
            assert_eq!(encoded.get(channels), 0);
        }
    }

    #[test]
    fn sparse_padding_channels_are_empty() {
        let sizes = vec![1; DENSE_CHANNEL_LIMIT + 1];
        let encoded = ChannelSizes::encode(&sizes);
        assert!(matches!(encoded, ChannelSizes::Sparse(_)));
        for channel in sizes.len()..sizes.len().div_ceil(8) * 8 {
            assert_eq!(encoded.get(channel), 0);
        }
    }

    #[test]
    fn invalid_sparse_sizes_are_rejected() {
        let proto = r#gen::ChannelSizes {
            kind: Some(r#gen::channel_sizes::Kind::Sparse(
                r#gen::SparseChannelSizes {
                    channels: 16,
                    nonempty: vec![1, 0],
                    average: 10,
                    huge_channels: vec![3],
                    huge_codes: 100u16.to_le_bytes().to_vec(),
                },
            )),
        };
        // Channel 3 is recorded as huge but marked empty.
        assert!(ChannelSizes::try_from(proto).is_err());
    }

    #[test]
    fn sparse_bitmap_padding_is_rejected() {
        let proto = r#gen::ChannelSizes {
            kind: Some(r#gen::channel_sizes::Kind::Sparse(
                r#gen::SparseChannelSizes {
                    channels: 9,
                    nonempty: vec![1, 2],
                    average: 10,
                    huge_channels: vec![],
                    huge_codes: vec![],
                },
            )),
        };
        assert!(ChannelSizes::try_from(proto).is_err());
    }

    #[test]
    fn fold_sums_buckets_and_propagates_unknown_physical_sizes() {
        let a = TaskOutputStatistics::new(&ChannelCounts {
            rows: vec![0, 100],
            physical_bytes: Some(vec![0, 1000]),
            logical_bytes: vec![0, 4000],
        })
        .unwrap();
        let b = TaskOutputStatistics::new(&ChannelCounts {
            rows: vec![5, 0],
            physical_bytes: Some(vec![50, 0]),
            logical_bytes: vec![200, 0],
        })
        .unwrap();
        let folded = ShuffleStatistics::fold(2, [&a, &b]).unwrap();
        assert_eq!(folded.total_rows, 105);
        assert_eq!(folded.total_physical_bytes, Some(1050));
        let physical = folded.physical_bytes.clone().unwrap();
        assert!(within_one_percent(physical[0], 50));
        assert!(within_one_percent(physical[1], 1000));

        let c = TaskOutputStatistics::new(&ChannelCounts {
            rows: vec![1, 1],
            physical_bytes: None,
            logical_bytes: vec![1, 1],
        })
        .unwrap();
        let folded = ShuffleStatistics::fold(2, [&a, &c]).unwrap();
        assert_eq!(folded.physical_bytes, None);
        assert_eq!(folded.total_physical_bytes, None);
        assert_eq!(folded.sizing_bytes(), folded.logical_bytes.as_slice());
        assert!(ShuffleStatistics::fold(3, [&a]).is_err());
    }

    #[test]
    fn fold_rejects_inconsistent_physical_sizes() {
        let mut statistics = TaskOutputStatistics::new(&ChannelCounts {
            rows: vec![1, 2],
            physical_bytes: Some(vec![1, 2]),
            logical_bytes: vec![10, 20],
        })
        .unwrap();
        statistics.channel_physical_bytes = Some(ChannelSizes::encode(&[1]));
        assert!(ShuffleStatistics::fold(2, [&statistics]).is_err());
        statistics.channel_physical_bytes = None;
        assert!(ShuffleStatistics::fold(2, [&statistics]).is_err());
    }

    #[test]
    fn statistics_round_trip_through_proto() {
        for physical_bytes in [Some(vec![3, 0, 5]), None] {
            let statistics = TaskOutputStatistics::new(&ChannelCounts {
                rows: vec![1, 0, 2],
                physical_bytes,
                logical_bytes: vec![10, 0, 20],
            })
            .unwrap();
            let proto: r#gen::TaskOutputStatistics = statistics.clone().into();
            assert_eq!(TaskOutputStatistics::try_from(proto).unwrap(), statistics);
        }
    }

    #[test]
    fn logical_channel_sizes_upper_bound_skewed_channels_and_legacy_sparse_reports() {
        let mut sizes = (0..6000)
            .map(|channel| if channel < 3000 { 1 } else { 1000 })
            .collect::<Vec<_>>();
        sizes[3] = 0;
        let statistics = TaskOutputStatistics::new(&ChannelCounts {
            rows: vec![1; sizes.len()],
            physical_bytes: Some(sizes.clone()),
            logical_bytes: sizes.clone(),
        })
        .unwrap();
        assert!(matches!(
            statistics.channel_logical_bytes,
            ChannelSizes::Dense(_)
        ));
        assert!(matches!(
            statistics.channel_physical_bytes,
            Some(ChannelSizes::Sparse(_))
        ));
        let mut legacy = statistics.clone();
        legacy.channel_logical_bytes = ChannelSizes::encode(&sizes);
        assert!(matches!(
            legacy.channel_logical_bytes,
            ChannelSizes::Sparse(_)
        ));
        for original in [statistics, legacy] {
            let proto: r#gen::TaskOutputStatistics = original.into();
            let decoded = TaskOutputStatistics::try_from(proto).unwrap();
            let folded = ShuffleStatistics::fold(sizes.len(), [&decoded]).unwrap();
            for (channel, &size) in sizes.iter().enumerate() {
                assert!(decoded.logical_bytes_of(channel) >= size);
                assert!(folded.logical_bytes[channel] >= size);
                assert_eq!(decoded.logical_bytes_of(channel) == 0, size == 0);
            }
        }
    }
}
