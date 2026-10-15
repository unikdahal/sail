//! Hash partition coalescing.
//!
//! Contiguous hash buckets are packed greedily up to a target size, and ranges below
//! the minimum size are merged into a neighbour. This follows Spark's
//! `ShufflePartitionsUtil.coalescePartitions`, with two differences:
//!   - every bucket is read exactly once, including empty buckets, so the coverage of
//!     a spec can be validated without statistics;
//!   - an optional build cap bounds the build-side bytes of each coalesced range,
//!     so coalescing never undoes a hash join decision.

use std::ops::Range;

#[derive(Debug, Clone, Copy)]
pub(crate) struct CoalesceParams {
    pub target_size: u64,
    pub min_partition_size: u64,
    pub min_partitions: usize,
    /// The largest build-side size of a coalesced range of more than one bucket.
    pub build_cap: Option<u64>,
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

/// Packs buckets into contiguous ranges.
///
/// `sizes` are the bucket sizes summed across the group's inputs, and `build_sizes`
/// are the bucket sizes of the inputs that feed the build side of a partitioned hash
/// join. The returned ranges cover `0..sizes.len()` in order without gaps.
pub(crate) fn coalesce(
    sizes: &[u64],
    build_sizes: Option<&[u64]>,
    params: &CoalesceParams,
) -> Vec<Range<usize>> {
    let n = sizes.len();
    if n == 0 {
        return vec![];
    }
    let build = |i: usize| build_sizes.map_or(0, |x| x[i]);
    let exceeds_cap = |b: u64| params.build_cap.is_some_and(|cap| b > cap);
    let total = sizes.iter().fold(0u64, |a, b| a.saturating_add(*b));
    let target = target_size(total, params);

    let mut ranges: Vec<(Range<usize>, u64, u64)> = vec![];
    let mut start = 0;
    let mut size = 0u64;
    let mut build_size = 0u64;
    for (i, &bucket_size) in sizes.iter().enumerate() {
        let next_size = size.saturating_add(bucket_size);
        let next_build = build_size.saturating_add(build(i));
        if i > start && (next_size > target || exceeds_cap(next_build)) {
            ranges.push((start..i, size, build_size));
            start = i;
            size = bucket_size;
            build_size = build(i);
        } else {
            size = next_size;
            build_size = next_build;
        }
    }
    ranges.push((start..n, size, build_size));

    // Merge ranges below the minimum size into their smaller neighbour,
    // unless the merged range would exceed the build cap.
    loop {
        let candidate = ranges
            .iter()
            .enumerate()
            .filter(|(_, (_, size, _))| *size < params.min_partition_size)
            .filter_map(|(i, (_, size, build_size))| {
                let neighbours = [i.checked_sub(1), (i + 1 < ranges.len()).then_some(i + 1)];
                neighbours
                    .into_iter()
                    .flatten()
                    .filter(|&j| !exceeds_cap(build_size.saturating_add(ranges[j].2)))
                    .min_by_key(|&j| ranges[j].1)
                    .map(|j| (i, j, *size))
            })
            .min_by_key(|(_, _, size)| *size);
        let Some((i, j, _)) = candidate else {
            break;
        };
        let (a, b) = (i.min(j), i.max(j));
        let merged = (
            ranges[a].0.start..ranges[b].0.end,
            ranges[a].1.saturating_add(ranges[b].1),
            ranges[a].2.saturating_add(ranges[b].2),
        );
        ranges[a] = merged;
        ranges.remove(b);
    }
    ranges.into_iter().map(|(range, _, _)| range).collect()
}

/// Returns whether every range of more than one bucket stays within the build cap.
pub(crate) fn within_build_cap(
    ranges: &[Range<usize>],
    build_sizes: &[u64],
    build_cap: u64,
) -> bool {
    ranges.iter().all(|range| {
        range.len() <= 1
            || build_sizes[range.clone()]
                .iter()
                .fold(0u64, |a, b| a.saturating_add(*b))
                <= build_cap
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(target: u64, min: u64) -> CoalesceParams {
        CoalesceParams {
            target_size: target,
            min_partition_size: min,
            min_partitions: 1,
            build_cap: None,
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
        assert_eq!(coalesce(&[60, 50], None, &params(100, 1)), vec![0..1, 1..2]);
    }

    #[test]
    fn packs_contiguous_buckets_up_to_the_target() {
        let ranges = coalesce(&[10, 20, 30, 40, 50, 10, 10], None, &params(60, 1));
        assert_eq!(ranges, vec![0..3, 3..4, 4..6, 6..7]);
    }

    #[test]
    fn merges_small_ranges_into_the_smaller_neighbour() {
        // The greedy pass yields [0..1] (100), [1..2] (5), [2..3] (100), [3..4] (30).
        let ranges = coalesce(&[100, 5, 100, 30], None, &params(100, 20));
        assert_eq!(ranges, vec![0..2, 2..3, 3..4]);
    }

    #[test]
    fn empty_buckets_are_still_covered() {
        let ranges = coalesce(&[0, 0, 0, 0], None, &params(100, 1));
        assert_eq!(ranges, vec![0..4]);
    }

    #[test]
    fn min_partitions_lowers_the_target() {
        let p = CoalesceParams {
            min_partitions: 4,
            ..params(1000, 1)
        };
        assert_eq!(target_size(400, &p), 100);
        assert_eq!(coalesce(&[50; 8], None, &p).len(), 4);
    }

    #[test]
    fn build_cap_bounds_coalesced_ranges() {
        let p = CoalesceParams {
            build_cap: Some(25),
            ..params(1000, 1)
        };
        let build = [10, 10, 10, 30, 1];
        let ranges = coalesce(&[1; 5], Some(&build), &p);
        assert_eq!(ranges, vec![0..2, 2..3, 3..4, 4..5]);
        assert!(within_build_cap(&ranges, &build, 25));
        // A single bucket above the cap is allowed.
        let single = std::iter::once(0..1).collect::<Vec<_>>();
        assert!(within_build_cap(&single, &[100], 25));
        let pair = std::iter::once(0..2).collect::<Vec<_>>();
        assert!(!within_build_cap(&pair, &[10, 20], 25));
    }

    #[test]
    fn ranges_always_cover_all_buckets() {
        // A deterministic pseudo-random sweep over size matrices.
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..500 {
            let n = (next() % 64) as usize + 1;
            let sizes = (0..n).map(|_| next() % 200).collect::<Vec<_>>();
            let build = (0..n).map(|_| next() % 50).collect::<Vec<_>>();
            let p = CoalesceParams {
                target_size: next() % 300 + 1,
                min_partition_size: next() % 50,
                min_partitions: (next() % 4) as usize + 1,
                build_cap: (next() % 2 == 0).then(|| next() % 100 + 1),
            };
            let ranges = coalesce(&sizes, Some(&build), &p);
            assert!(covers(&ranges, n), "{sizes:?} {ranges:?}");
            if let Some(cap) = p.build_cap {
                assert!(within_build_cap(&ranges, &build, cap));
            }
        }
    }
}
