//! The exact-coverage contract of adaptive partition specs.
//!
//! A spec maps each partition of an input's own domain to the bucket slices it reads.
//! Inputs that an operator pairs partition by partition form a group, and their
//! specs must satisfy, together:
//!
//!   - **Segments.** Each partition reads, from every input of the group, the same
//!     bucket range: either a contiguous range of whole buckets, or a part of a
//!     single bucket. The ranges of consecutive partitions tile all buckets in order.
//!   - **Coalesced ranges.** A range of more than one bucket is read whole by exactly
//!     one partition, from every input.
//!   - **Split buckets.** For a single bucket read by several partitions, the distinct
//!     parts that each input reads tile that input's producers exactly once, and the
//!     partitions are exactly the cross product of the inputs' parts. A side that is
//!     not split is read whole by every partition of the bucket, which pairs each
//!     slice of the other side with the whole bucket once.
//!
//! Together these give exactly-once consumption: every row of every input bucket is
//! read once per pairing it takes part in, and every key meets every key of the
//! other inputs exactly once.

use std::collections::HashSet;
use std::ops::Range;

use crate::job_graph::{AdaptiveInput, AdaptiveProperty, BucketSlice, SubRange};

/// The shape of one input's input for a group: its spec, bucket count and producer count.
pub(crate) struct SpecInput<'a> {
    pub spec: &'a AdaptiveInput,
    pub buckets: usize,
    pub producers: usize,
}

/// What a partition reads from one input.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Entry {
    buckets: Range<usize>,
    /// The producer range of a split bucket, or `None` for whole buckets.
    part: Option<Range<usize>>,
}

fn entry(slices: &[BucketSlice], input: &SpecInput) -> Result<Entry, String> {
    let (first, last) = match (slices.first(), slices.last()) {
        (Some(first), Some(last)) => (first, last),
        _ => return Err("a partition reads nothing".to_string()),
    };
    let mut next = first.buckets.start;
    for slice in slices {
        if slice.buckets.is_empty() || slice.buckets.start != next {
            return Err(format!("non-contiguous bucket slices at {slice}"));
        }
        next = slice.buckets.end;
        if slice.buckets.end > input.buckets {
            return Err(format!(
                "slice {slice} is outside {} buckets",
                input.buckets
            ));
        }
    }
    match (slices, &first.part) {
        ([_], Some(SubRange::Producers(producers))) => {
            if first.buckets.len() != 1 {
                return Err(format!("a partial slice must cover one bucket: {first}"));
            }
            if producers.is_empty() || producers.end > input.producers {
                return Err(format!(
                    "invalid producer range in {first} for {} producers",
                    input.producers
                ));
            }
            Ok(Entry {
                buckets: first.buckets.clone(),
                part: Some(producers.clone()),
            })
        }
        _ if slices.iter().any(|slice| slice.part.is_some()) => {
            Err("a partial slice must be read alone".to_string())
        }
        _ => Ok(Entry {
            buckets: first.buckets.start..last.buckets.end,
            part: None,
        }),
    }
}

/// Checks the specs of a group of inputs. See the module documentation.
pub(crate) fn check_group(inputs: &[SpecInput]) -> Result<(), String> {
    let Some(first) = inputs.first() else {
        return Ok(());
    };
    let partitions = first.spec.partitions();
    let buckets = first.buckets;
    for input in inputs {
        if input.spec.partitions() != partitions {
            return Err(format!(
                "inputs have {} and {} partitions",
                partitions,
                input.spec.partitions()
            ));
        }
        if input.buckets != buckets {
            return Err("inputs have different bucket counts".to_string());
        }
    }
    // entries[p][i] is what partition p reads from input i.
    let entries = (0..partitions)
        .map(|p| {
            inputs
                .iter()
                .map(|input| entry(&input.spec.spec[p], input))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut next_bucket = 0;
    let mut p = 0;
    while p < partitions {
        let range = entries[p][0].buckets.clone();
        if range.start != next_bucket {
            return Err(format!(
                "buckets {next_bucket}..{} are not read in order",
                range.start
            ));
        }
        // The partitions of this segment are the consecutive partitions reading it.
        let mut end = p;
        while end < partitions && entries[end].iter().all(|e| e.buckets == range) {
            end += 1;
        }
        if end == p {
            return Err(format!(
                "partition {p} reads different buckets from its inputs"
            ));
        }
        let segment = &entries[p..end];
        if range.len() > 1 || segment.len() == 1 {
            if segment.len() != 1 || segment[0].iter().any(|e| e.part.is_some()) {
                return Err(format!("buckets {range:?} are read more than once"));
            }
        } else {
            check_split_bucket(segment, inputs)?;
        }
        next_bucket = range.end;
        p = end;
    }
    if next_bucket != buckets {
        return Err(format!("only {next_bucket} of {buckets} buckets are read"));
    }
    for input in inputs {
        check_property(input.spec, buckets)?;
    }
    Ok(())
}

fn check_split_bucket(segment: &[Vec<Entry>], inputs: &[SpecInput]) -> Result<(), String> {
    let mut combinations = 1usize;
    for (i, input) in inputs.iter().enumerate() {
        let mut parts = segment
            .iter()
            .map(|entries| entries[i].part.clone())
            .collect::<Vec<_>>();
        parts.sort_by_key(|part| part.as_ref().map(|x| (x.start, x.end)));
        parts.dedup();
        match parts.as_slice() {
            [None] => {}
            _ => {
                let mut next = 0;
                for part in &parts {
                    let Some(part) = part else {
                        return Err("a bucket is read both whole and in parts".to_string());
                    };
                    if part.start != next {
                        return Err(format!(
                            "producer parts of a split bucket do not tile: {parts:?}"
                        ));
                    }
                    next = part.end;
                }
                if next != input.producers {
                    return Err(format!(
                        "producer parts cover {next} of {} producers",
                        input.producers
                    ));
                }
            }
        }
        combinations = combinations.saturating_mul(parts.len());
    }
    let distinct = segment.iter().collect::<HashSet<_>>();
    if distinct.len() != segment.len() || segment.len() != combinations {
        return Err(format!(
            "a split bucket has {} partitions, expected each of {combinations} slice pairs once",
            segment.len()
        ));
    }
    Ok(())
}

/// A coalesced-hash property is valid only if every bucket is read whole by exactly
/// one partition, with boundaries that match the spec.
fn check_property(spec: &AdaptiveInput, buckets: usize) -> Result<(), String> {
    match &spec.property {
        AdaptiveProperty::Unknown => Ok(()),
        AdaptiveProperty::CoalescedHash { from, boundaries } => {
            let mut expected = vec![];
            for slices in &spec.spec {
                if slices.iter().any(|slice| slice.part.is_some()) {
                    return Err("a split input cannot be coalesced-hash partitioned".to_string());
                }
                expected.push(slices.first().map_or(0, |slice| slice.buckets.start));
            }
            expected.push(buckets);
            let increasing = expected.windows(2).all(|w| w[0] < w[1]);
            if *from != buckets || *boundaries != expected || !increasing {
                return Err("inconsistent coalesced-hash boundaries".to_string());
            }
            Ok(())
        }
    }
}

/// Splits a list of sizes into contiguous ranges of about `target` bytes, following
/// Spark's `ShufflePartitionsUtil.splitSizeListByTargetSize`: a range is closed when
/// the next size would exceed the target, and a range is merged into the previous one
/// if both together stay below 1.2 times the target or either is below 0.2 times it.
/// Returns the start index of each range.
pub(crate) fn split_by_target_size(sizes: &[u64], target: u64) -> Vec<usize> {
    const SMALL_PARTITION_FACTOR: f64 = 0.2;
    const MERGED_PARTITION_FACTOR: f64 = 1.2;
    let target_f = target as f64;
    let mut starts = vec![0];
    let mut current = 0u64;
    let mut last: Option<u64> = None;
    let mut try_merge = |starts: &mut Vec<usize>, current: u64| {
        let merge = last.is_some_and(|last| {
            ((current + last) as f64) < target_f * MERGED_PARTITION_FACTOR
                || (current as f64) < target_f * SMALL_PARTITION_FACTOR
                || (last as f64) < target_f * SMALL_PARTITION_FACTOR
        });
        if merge {
            starts.pop();
            last = last.map(|last| last + current);
        } else {
            last = Some(current);
        }
    };
    for (i, &size) in sizes.iter().enumerate() {
        if i > 0 && current.saturating_add(size) > target {
            try_merge(&mut starts, current);
            starts.push(i);
            current = size;
        } else {
            current = current.saturating_add(size);
        }
    }
    try_merge(&mut starts, current);
    if starts.is_empty() {
        starts.push(0);
    }
    starts
}

/// Converts range start indices into ranges ending at `len`.
pub(crate) fn ranges_from_starts(starts: &[usize], len: usize) -> Vec<Range<usize>> {
    starts
        .iter()
        .enumerate()
        .map(|(i, &start)| start..starts.get(i + 1).copied().unwrap_or(len))
        .collect()
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::driver::job_scheduler::adaptive::coalesce::{CoalesceParams, coalesce};

    fn coalesced(ranges: &[Range<usize>], buckets: usize) -> AdaptiveInput {
        AdaptiveInput {
            spec: ranges
                .iter()
                .map(|r| vec![BucketSlice::whole(r.clone())])
                .collect(),
            property: AdaptiveProperty::CoalescedHash {
                from: buckets,
                boundaries: ranges.iter().map(|r| r.start).chain([buckets]).collect(),
            },
        }
    }

    fn input(spec: &AdaptiveInput, buckets: usize, producers: usize) -> SpecInput<'_> {
        SpecInput {
            spec,
            buckets,
            producers,
        }
    }

    #[test]
    fn accepts_coalesced_ranges() {
        let spec = coalesced(&[0..3, 3..4, 4..8], 8);
        check_group(&[input(&spec, 8, 4), input(&spec, 8, 2)]).unwrap();
    }

    #[test]
    fn rejects_gaps_overlaps_and_unaligned_inputs() {
        let gap = coalesced(&[0..3, 4..8], 8);
        assert!(check_group(&[input(&gap, 8, 1)]).is_err());
        let short = coalesced(&[0..3, 3..7], 8);
        assert!(check_group(&[input(&short, 8, 1)]).is_err());
        let a = coalesced(&[0..4, 4..8], 8);
        let b = coalesced(&[0..2, 2..8], 8);
        assert!(check_group(&[input(&a, 8, 1), input(&b, 8, 1)]).is_err());
    }

    #[test]
    fn accepts_one_sided_and_two_sided_splits() {
        // Bucket 1 is split into three producer ranges on the left side only.
        let left = AdaptiveInput {
            spec: vec![
                vec![BucketSlice::whole(0..1)],
                vec![BucketSlice::producers(1, 0..2)],
                vec![BucketSlice::producers(1, 2..3)],
                vec![BucketSlice::producers(1, 3..4)],
                vec![BucketSlice::whole(2..4)],
            ],
            property: AdaptiveProperty::Unknown,
        };
        let right = AdaptiveInput {
            spec: vec![
                vec![BucketSlice::whole(0..1)],
                vec![BucketSlice::whole(1..2)],
                vec![BucketSlice::whole(1..2)],
                vec![BucketSlice::whole(1..2)],
                vec![BucketSlice::whole(2..4)],
            ],
            property: AdaptiveProperty::Unknown,
        };
        check_group(&[input(&left, 4, 4), input(&right, 4, 3)]).unwrap();

        // Both sides split: 2 x 2 pairs.
        let l = |r: Range<usize>| vec![BucketSlice::producers(0, r)];
        let left = AdaptiveInput {
            spec: vec![l(0..1), l(0..1), l(1..2), l(1..2)],
            property: AdaptiveProperty::Unknown,
        };
        let right = AdaptiveInput {
            spec: vec![l(0..3), l(3..5), l(0..3), l(3..5)],
            property: AdaptiveProperty::Unknown,
        };
        check_group(&[input(&left, 1, 2), input(&right, 1, 5)]).unwrap();
        // A missing pair loses rows.
        let left_missing = AdaptiveInput {
            spec: left.spec[..3].to_vec(),
            property: AdaptiveProperty::Unknown,
        };
        let right_missing = AdaptiveInput {
            spec: right.spec[..3].to_vec(),
            property: AdaptiveProperty::Unknown,
        };
        assert!(check_group(&[input(&left_missing, 1, 2), input(&right_missing, 1, 5)]).is_err());
    }

    #[test]
    fn rejects_split_with_hash_property_and_incomplete_parts() {
        let spec = AdaptiveInput {
            spec: vec![
                vec![BucketSlice::producers(0, 0..2)],
                vec![BucketSlice::producers(0, 2..4)],
            ],
            property: AdaptiveProperty::CoalescedHash {
                from: 1,
                boundaries: vec![0, 0, 1],
            },
        };
        assert!(check_group(&[input(&spec, 1, 4)]).is_err());
        let partial = AdaptiveInput {
            spec: vec![
                vec![BucketSlice::producers(0, 0..2)],
                vec![BucketSlice::producers(0, 2..3)],
            ],
            property: AdaptiveProperty::Unknown,
        };
        assert!(check_group(&[input(&partial, 1, 4)]).is_err());
    }

    #[test]
    fn split_by_target_size_follows_spark() {
        assert_eq!(split_by_target_size(&[100, 100, 100], 100), vec![0, 1, 2]);
        // The last small range merges into the previous one.
        assert_eq!(split_by_target_size(&[100, 100, 10], 100), vec![0, 1]);
        // Two ranges that together stay below 1.2x the target merge.
        assert_eq!(split_by_target_size(&[60, 50], 100), vec![0]);
        assert_eq!(split_by_target_size(&[], 100), vec![0]);
        assert_eq!(ranges_from_starts(&[0, 2], 5), vec![0..2, 2..5]);
    }

    /// Builds specs as the adaptive rules do: coalesce, then split single-bucket
    /// ranges on one or both sides.
    fn build_specs(
        sizes: &[u64],
        producers: [usize; 2],
        splits: &[(usize, [usize; 2])],
        target: u64,
    ) -> [AdaptiveInput; 2] {
        let params = CoalesceParams {
            target_size: target,
            min_partition_size: 1,
            min_partitions: 1,
            build_cap: None,
        };
        let ranges = coalesce(sizes, None, &params);
        let mut specs = [vec![], vec![]];
        for range in ranges {
            let split = splits
                .iter()
                .find(|(b, _)| range.len() == 1 && *b == range.start)
                .map(|(_, parts)| *parts);
            match split {
                None => {
                    for spec in &mut specs {
                        spec.push(vec![BucketSlice::whole(range.clone())]);
                    }
                }
                Some(parts) => {
                    let slices = |side: usize| -> Vec<BucketSlice> {
                        let n = parts[side].clamp(1, producers[side]);
                        if n == 1 {
                            return vec![BucketSlice::whole(range.clone())];
                        }
                        let starts = (0..n).map(|k| k * producers[side] / n).collect::<Vec<_>>();
                        ranges_from_starts(&starts, producers[side])
                            .into_iter()
                            .map(|r| BucketSlice::producers(range.start, r))
                            .collect()
                    };
                    let (left, right) = (slices(0), slices(1));
                    for l in &left {
                        for r in &right {
                            specs[0].push(vec![l.clone()]);
                            specs[1].push(vec![r.clone()]);
                        }
                    }
                }
            }
        }
        specs.map(|spec| AdaptiveInput {
            spec,
            property: AdaptiveProperty::Unknown,
        })
    }

    /// Counts how many times each (left row, right row) pair of each bucket is
    /// produced, where a row is identified by its producer.
    fn pair_counts(
        specs: &[AdaptiveInput; 2],
        buckets: usize,
        producers: [usize; 2],
    ) -> Vec<usize> {
        let mut counts = vec![0; buckets * producers[0] * producers[1]];
        for p in 0..specs[0].partitions() {
            let read = |side: usize| -> Vec<(usize, usize)> {
                let mut result = vec![];
                for slice in &specs[side].spec[p] {
                    for b in slice.buckets.clone() {
                        let range = match &slice.part {
                            Some(SubRange::Producers(r)) => r.clone(),
                            None => 0..producers[side],
                        };
                        result.extend(range.map(|m| (b, m)));
                    }
                }
                result
            };
            for (bl, ml) in read(0) {
                for (br, mr) in read(1) {
                    if bl == br {
                        counts[(bl * producers[0] + ml) * producers[1] + mr] += 1;
                    }
                }
            }
        }
        counts
    }

    proptest! {
        #[test]
        fn coalesced_specs_read_every_bucket_once(
            sizes in prop::collection::vec(0u64..1000, 1..64),
            target in 1u64..3000,
        ) {
            let params = CoalesceParams {
                target_size: target,
                min_partition_size: target / 4,
                min_partitions: 1,
                build_cap: None,
            };
            let ranges = coalesce(&sizes, None, &params);
            let spec = coalesced(&ranges, sizes.len());
            prop_assert!(check_group(&[input(&spec, sizes.len(), 3), input(&spec, sizes.len(), 5)]).is_ok());
        }

        #[test]
        fn valid_split_specs_pair_every_row_exactly_once(
            sizes in prop::collection::vec(0u64..1000, 1..16),
            producers in (1usize..6, 1usize..6),
            splits in prop::collection::vec((0usize..16, (1usize..4, 1usize..4)), 0..4),
        ) {
            let producers = [producers.0, producers.1];
            let splits = splits.into_iter().map(|(b, (l, r))| (b, [l, r])).collect::<Vec<_>>();
            let specs = build_specs(&sizes, producers, &splits, 1500);
            let buckets = sizes.len();
            prop_assert!(check_group(&[
                input(&specs[0], buckets, producers[0]),
                input(&specs[1], buckets, producers[1]),
            ]).is_ok());
            prop_assert!(pair_counts(&specs, buckets, producers).iter().all(|&c| c == 1));
        }

        #[test]
        fn accepted_specs_never_lose_or_duplicate_pairs(
            sizes in prop::collection::vec(0u64..1000, 1..8),
            producers in (1usize..4, 1usize..4),
            splits in prop::collection::vec((0usize..8, (1usize..4, 1usize..4)), 0..3),
            drop in prop::option::of(0usize..64),
            duplicate in prop::option::of(0usize..64),
        ) {
            // Corrupt a valid spec by dropping or duplicating a partition. Whenever the
            // contract still accepts the result, the pairing must still be exact.
            let producers = [producers.0, producers.1];
            let splits = splits.into_iter().map(|(b, (l, r))| (b, [l, r])).collect::<Vec<_>>();
            let mut specs = build_specs(&sizes, producers, &splits, 1500);
            let n = specs[0].partitions();
            if let Some(d) = duplicate.map(|d| d % n) {
                for spec in &mut specs {
                    let entry = spec.spec[d].clone();
                    spec.spec.insert(d, entry);
                }
            }
            if let Some(d) = drop.map(|d| d % specs[0].partitions()) {
                for spec in &mut specs {
                    spec.spec.remove(d);
                }
            }
            let buckets = sizes.len();
            let accepted = check_group(&[
                input(&specs[0], buckets, producers[0]),
                input(&specs[1], buckets, producers[1]),
            ]).is_ok();
            if accepted {
                prop_assert!(pair_counts(&specs, buckets, producers).iter().all(|&c| c == 1));
            }
        }

        #[test]
        fn split_ranges_cover_all_producers(
            sizes in prop::collection::vec(0u64..10_000, 1..200),
            target in 1u64..20_000,
        ) {
            let starts = split_by_target_size(&sizes, target);
            let ranges = ranges_from_starts(&starts, sizes.len());
            prop_assert_eq!(ranges.first().map(|r| r.start), Some(0));
            prop_assert_eq!(ranges.last().map(|r| r.end), Some(sizes.len()));
            prop_assert!(ranges.windows(2).all(|w| w[0].end == w[1].start && !w[0].is_empty()));
        }
    }
}
