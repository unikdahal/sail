//! The exact-coverage contract of adaptive partition specs.
//!
//! A spec maps each partition of an input's own domain to the bucket slices it reads.
//! Inputs that an operator pairs partition by partition form a group, and their
//! specs must satisfy, together:
//!
//!   - **Segments.** Each partition reads, from every input of the group, the same
//!     contiguous range of whole buckets. The ranges of consecutive partitions tile
//!     all buckets in order.
//!   - **Once.** Each range is read by exactly one partition, from every input.
//!
//! Together these give exactly-once consumption: every row of every input bucket is
//! read once, and every key meets every key of the other inputs exactly once.

use std::ops::Range;

use crate::job_graph::{AdaptiveInput, AdaptiveProperty, BucketSlice};

/// The shape of one input for a group: its spec and bucket count.
pub(crate) struct SpecInput<'a> {
    pub spec: &'a AdaptiveInput,
    pub buckets: usize,
}

/// Returns the bucket range that a partition reads from one input.
fn entry(slices: &[BucketSlice], input: &SpecInput) -> Result<Range<usize>, String> {
    let Some(first) = slices.first() else {
        return Err("a partition reads nothing".to_string());
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
    Ok(first.buckets.start..next)
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
    let mut next_bucket = 0;
    for p in 0..partitions {
        let ranges = inputs
            .iter()
            .map(|input| entry(&input.spec.spec[p], input))
            .collect::<Result<Vec<_>, _>>()?;
        let range = ranges[0].clone();
        if ranges.iter().any(|r| *r != range) {
            return Err(format!(
                "partition {p} reads different buckets from its inputs"
            ));
        }
        if range.start != next_bucket {
            return Err(format!(
                "buckets {next_bucket}..{} are not read exactly once in order",
                range.start
            ));
        }
        next_bucket = range.end;
    }
    if next_bucket != buckets {
        return Err(format!("only {next_bucket} of {buckets} buckets are read"));
    }
    for input in inputs {
        check_property(input.spec, buckets)?;
    }
    Ok(())
}

/// A coalesced-hash property is valid only if its boundaries match the spec.
fn check_property(spec: &AdaptiveInput, buckets: usize) -> Result<(), String> {
    match &spec.property {
        AdaptiveProperty::CoalescedHash { from, boundaries } => {
            let mut expected = spec
                .spec
                .iter()
                .map(|slices| slices.first().map_or(0, |slice| slice.buckets.start))
                .collect::<Vec<_>>();
            expected.push(buckets);
            let increasing = expected.windows(2).all(|w| w[0] < w[1]);
            if *from != buckets || *boundaries != expected || !increasing {
                return Err("inconsistent coalesced-hash boundaries".to_string());
            }
            Ok(())
        }
    }
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

    fn input(spec: &AdaptiveInput, buckets: usize) -> SpecInput<'_> {
        SpecInput { spec, buckets }
    }

    #[test]
    fn accepts_coalesced_ranges() {
        let spec = coalesced(&[0..3, 3..4, 4..8], 8);
        check_group(&[input(&spec, 8), input(&spec, 8)]).unwrap();
    }

    #[test]
    fn rejects_gaps_overlaps_and_unaligned_inputs() {
        let gap = coalesced(&[0..3, 4..8], 8);
        assert!(check_group(&[input(&gap, 8)]).is_err());
        let short = coalesced(&[0..3, 3..7], 8);
        assert!(check_group(&[input(&short, 8)]).is_err());
        let overlap = AdaptiveInput {
            spec: vec![
                vec![BucketSlice::whole(0..4)],
                vec![BucketSlice::whole(3..8)],
            ],
            property: AdaptiveProperty::CoalescedHash {
                from: 8,
                boundaries: vec![0, 3, 8],
            },
        };
        assert!(check_group(&[input(&overlap, 8)]).is_err());
        let a = coalesced(&[0..4, 4..8], 8);
        let b = coalesced(&[0..2, 2..8], 8);
        assert!(check_group(&[input(&a, 8), input(&b, 8)]).is_err());
    }

    #[test]
    fn rejects_inconsistent_coalesced_hash_boundaries() {
        let mut spec = coalesced(&[0..4, 4..8], 8);
        spec.property = AdaptiveProperty::CoalescedHash {
            from: 8,
            boundaries: vec![0, 2, 8],
        };
        assert!(check_group(&[input(&spec, 8)]).is_err());
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
            };
            let ranges = coalesce(&sizes, &[], &params);
            let spec = coalesced(&ranges, sizes.len());
            prop_assert!(check_group(&[input(&spec, sizes.len()), input(&spec, sizes.len())]).is_ok());
        }

        #[test]
        fn accepted_specs_read_every_bucket_exactly_once(
            sizes in prop::collection::vec(0u64..1000, 1..32),
            drop in prop::option::of(0usize..64),
            duplicate in prop::option::of(0usize..64),
        ) {
            // Corrupt a valid spec by dropping or duplicating a partition. Whenever the
            // contract still accepts the result, every bucket is read exactly once.
            let ranges = coalesce(&sizes, &[], &CoalesceParams {
                target_size: 1500,
                min_partition_size: 1,
                min_partitions: 1,
            });
            let mut spec = coalesced(&ranges, sizes.len());
            if let Some(d) = duplicate.map(|d| d % spec.partitions()) {
                let entry = spec.spec[d].clone();
                spec.spec.insert(d, entry);
            }
            if let Some(d) = drop.map(|d| d % spec.partitions()) {
                spec.spec.remove(d);
            }
            if check_group(&[input(&spec, sizes.len())]).is_ok() {
                let mut reads = vec![0; sizes.len()];
                for slices in &spec.spec {
                    for slice in slices {
                        for b in slice.buckets.clone() {
                            reads[b] += 1;
                        }
                    }
                }
                prop_assert!(reads.iter().all(|&c| c == 1));
            }
        }
    }
}
