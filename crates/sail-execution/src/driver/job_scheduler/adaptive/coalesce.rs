//! Hash partition coalescing.
//!
//! Contiguous hash buckets are packed greedily up to a target size, and ranges below
//! the minimum size are merged into a neighbour. This follows Spark's
//! `ShufflePartitionsUtil.coalescePartitions`, with two differences:
//!   - every bucket is read exactly once, including empty buckets, so the coverage of
//!     a spec can be validated without statistics;
//!   - hard caps bound other per-range sizes, such as the uncompressed bytes of the
//!     range or the build side of a hash join. A range of more than one bucket never
//!     exceeds a cap; a single bucket above a cap is left alone, since coalescing
//!     cannot make it smaller.
//!
//! Both passes are linear in the number of buckets.

use std::ops::Range;

#[derive(Debug, Clone, Copy)]
pub(crate) struct CoalesceParams {
    pub target_size: u64,
    pub min_partition_size: u64,
    pub min_partitions: usize,
}

/// A hard limit on a per-bucket size, summed over each coalesced range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Cap {
    pub sizes: Vec<u64>,
    pub limit: u64,
}

/// Returns the target size for the given total, as Spark does:
/// `max(min_partition_size, min(advisory, total / min_partitions))`.
pub(crate) fn target_size(total: u64, params: &CoalesceParams) -> u64 {
    let min_partitions = params.min_partitions.max(1) as u64;
    total
        .div_ceil(min_partitions)
        .min(params.target_size)
        .max(params.min_partition_size)
        .max(1)
}

/// A range under construction, with its size and its size under each cap.
#[derive(Debug, Clone)]
struct Run {
    range: Range<usize>,
    size: u64,
    capped: Vec<u64>,
}

impl Run {
    fn new(i: usize, sizes: &[u64], caps: &[Cap]) -> Self {
        Self {
            range: i..i + 1,
            size: sizes[i],
            capped: caps.iter().map(|cap| cap.sizes[i]).collect(),
        }
    }

    /// Whether the run and the next one may form one range.
    fn fits(&self, next: &Run, caps: &[Cap]) -> bool {
        self.capped
            .iter()
            .zip(&next.capped)
            .zip(caps)
            .all(|((a, b), cap)| a.saturating_add(*b) <= cap.limit)
    }

    fn absorb(&mut self, next: Run) {
        self.range.end = next.range.end;
        self.size = self.size.saturating_add(next.size);
        for (a, b) in self.capped.iter_mut().zip(next.capped) {
            *a = a.saturating_add(b);
        }
    }
}

/// Packs buckets into contiguous ranges.
///
/// `sizes` are the bucket sizes summed across the group's inputs, and every cap
/// has one size per bucket. The returned ranges cover `0..sizes.len()` in order
/// without gaps.
pub(crate) fn coalesce(sizes: &[u64], caps: &[Cap], params: &CoalesceParams) -> Vec<Range<usize>> {
    let n = sizes.len();
    if n == 0 || caps.iter().any(|cap| cap.sizes.len() != n) {
        return (0..n).map(|i| i..i + 1).collect();
    }
    let total = sizes.iter().fold(0u64, |a, b| a.saturating_add(*b));
    let target = target_size(total, params);

    // Greedy pass: extend the current range while it stays within the target and
    // the caps.
    let mut runs: Vec<Run> = vec![];
    for i in 0..n {
        let bucket = Run::new(i, sizes, caps);
        match runs.last_mut() {
            Some(run)
                if run.size.saturating_add(bucket.size) <= target && run.fits(&bucket, caps) =>
            {
                run.absorb(bucket)
            }
            _ => runs.push(bucket),
        }
    }

    // Merge pass: a range below the minimum size joins its smaller neighbour that
    // keeps the caps. A range that joins its right neighbour is examined again,
    // merged, with its own right neighbour.
    let mut merged: Vec<Run> = Vec::with_capacity(runs.len());
    let mut runs = runs.into_iter().peekable();
    let mut pending: Option<Run> = None;
    while let Some(mut run) = pending.take().or_else(|| runs.next()) {
        if run.size >= params.min_partition_size {
            merged.push(run);
            continue;
        }
        let left = merged
            .last()
            .filter(|left| left.fits(&run, caps))
            .map(|left| left.size);
        let right = runs
            .peek()
            .filter(|right| run.fits(right, caps))
            .map(|right| right.size);
        let to_right = match (left, right) {
            (Some(l), Some(r)) => r < l,
            (None, Some(_)) => true,
            _ => false,
        };
        if to_right {
            if let Some(next) = runs.next() {
                run.absorb(next);
            }
            pending = Some(run);
        } else if let (Some(_), Some(left)) = (left, merged.last_mut()) {
            left.absorb(run);
        } else {
            merged.push(run);
        }
    }
    merged.into_iter().map(|run| run.range).collect()
}

/// Returns whether every range of more than one bucket stays within every cap.
pub(crate) fn within_caps(ranges: &[Range<usize>], caps: &[Cap]) -> bool {
    caps.iter().all(|cap| {
        ranges.iter().all(|range| {
            range.len() <= 1
                || cap
                    .sizes
                    .get(range.clone())
                    .is_some_and(|x| x.iter().fold(0u64, |a, b| a.saturating_add(*b)) <= cap.limit)
        })
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn params(target: u64, min: u64) -> CoalesceParams {
        CoalesceParams {
            target_size: target,
            min_partition_size: min,
            min_partitions: 1,
        }
    }

    fn covers(ranges: &[Range<usize>], n: usize) -> bool {
        let mut next = 0;
        for range in ranges {
            if range.start != next || range.is_empty() {
                return false;
            }
            next = range.end;
        }
        next == n
    }

    #[test]
    fn keeps_two_partitions_where_the_skew_helper_would_merge() {
        // Spark's coalescing keeps [60, 50] as two partitions for target 100.
        assert_eq!(coalesce(&[60, 50], &[], &params(100, 1)), vec![0..1, 1..2]);
    }

    #[test]
    fn packs_contiguous_buckets_up_to_the_target() {
        let ranges = coalesce(&[10, 20, 30, 40, 50, 10, 10], &[], &params(60, 1));
        assert_eq!(ranges, vec![0..3, 3..4, 4..6, 6..7]);
    }

    #[test]
    fn merges_small_ranges_into_the_smaller_neighbour() {
        // The greedy pass yields [0..1] (100), [1..2] (5), [2..3] (100), [3..4] (30).
        let ranges = coalesce(&[100, 5, 100, 30], &[], &params(100, 20));
        assert_eq!(ranges, vec![0..2, 2..3, 3..4]);
        // A small range prefers its smaller right neighbour.
        let ranges = coalesce(&[100, 5, 50], &[], &params(100, 20));
        assert_eq!(ranges, vec![0..1, 1..3]);
    }

    #[test]
    fn small_ranges_carried_forward_keep_merging() {
        // Every greedy range is small, so they merge left to right.
        let ranges = coalesce(&[9, 9, 9, 9], &[], &params(10, 30));
        assert_eq!(ranges, vec![0..4]);
    }

    #[test]
    fn empty_buckets_are_still_covered() {
        let ranges = coalesce(&[0, 0, 0, 0], &[], &params(100, 1));
        assert_eq!(ranges, vec![0..4]);
    }

    #[test]
    fn min_partitions_lowers_the_target() {
        let p = CoalesceParams {
            min_partitions: 4,
            ..params(1000, 1)
        };
        assert_eq!(target_size(400, &p), 100);
        assert_eq!(coalesce(&[50; 8], &[], &p).len(), 4);
    }

    #[test]
    fn caps_bound_coalesced_ranges() {
        let cap = Cap {
            sizes: vec![10, 10, 10, 30, 1],
            limit: 25,
        };
        let ranges = coalesce(&[1; 5], std::slice::from_ref(&cap), &params(1000, 1));
        assert_eq!(ranges, vec![0..2, 2..3, 3..4, 4..5]);
        assert!(within_caps(&ranges, std::slice::from_ref(&cap)));
        // A single bucket above the cap is allowed.
        let single = Cap {
            sizes: vec![100],
            limit: 25,
        };
        assert!(within_caps(std::slice::from_ref(&(0..1)), &[single]));
        let pair = Cap {
            sizes: vec![10, 20],
            limit: 25,
        };
        assert!(!within_caps(std::slice::from_ref(&(0..2)), &[pair]));
    }

    #[test]
    fn small_ranges_do_not_merge_across_a_cap() {
        let cap = Cap {
            sizes: vec![20, 20, 20],
            limit: 30,
        };
        let ranges = coalesce(&[1, 1, 1], &[cap], &params(1000, 100));
        assert_eq!(ranges, vec![0..1, 1..2, 2..3]);
    }

    #[test]
    fn many_buckets_coalesce_in_linear_time() {
        let sizes = vec![1u64; 1_000_000];
        let ranges = coalesce(&sizes, &[], &params(10, 1_000));
        assert!(covers(&ranges, sizes.len()));
        assert_eq!(ranges.len(), 1000);
    }

    proptest! {
        #[test]
        fn ranges_cover_all_buckets_and_respect_caps(
            sizes in prop::collection::vec(0u64..200, 1..64),
            cap_sizes in prop::collection::vec(0u64..50, 64),
            target in 1u64..300,
            min in 0u64..50,
            min_partitions in 1usize..5,
            limit in prop::option::of(1u64..100),
        ) {
            let n = sizes.len();
            let caps = limit
                .map(|limit| Cap { sizes: cap_sizes[..n].to_vec(), limit })
                .into_iter()
                .collect::<Vec<_>>();
            let p = CoalesceParams { target_size: target, min_partition_size: min, min_partitions };
            let ranges = coalesce(&sizes, &caps, &p);
            prop_assert!(covers(&ranges, n), "{:?} {:?}", sizes, ranges);
            prop_assert!(within_caps(&ranges, &caps));
        }
    }
}
