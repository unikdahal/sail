//! Size bounds of operator outputs from committed input statistics.
//!
//! The committed statistics describe stage inputs. They bound what an operator
//! above an input produces only through operators that never add rows or widen
//! them. A decision that relies on a size uses the bound its soundness needs:
//!
//!   - an upper bound proves that a size is small (a coalesced build range stays
//!     within the build cap);
//!   - an exact size proves that a size is large (a build side is larger than its
//!     probe side, or a build partition exceeds the build cap).
//!
//! These bounds describe the recorded input data. Read rebatching can repeat or
//! retain Arrow buffers, and hash table overhead is unmeasured, so byte caps do
//! not guarantee a bound on a task's actual memory usage.
//!
//! Any other operator (an aggregate, a join, a projection that computes values,
//! a rebatching sort, a window, an unnest) has no bound, and decisions that need
//! one are not made.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;

use crate::plan::StageInputExec;

/// How the bytes an operator produces relate to the logical bytes of a stage input,
/// partition by partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Bound {
    /// The operator produces exactly the rows and columns of the input.
    Exact,
    /// The operator produces at most the rows of the input, each with a subset of
    /// its columns.
    Upper,
}

impl Bound {
    fn and(self, other: Bound) -> Bound {
        match (self, other) {
            (Bound::Exact, Bound::Exact) => Bound::Exact,
            _ => Bound::Upper,
        }
    }
}

/// A stage input whose logical bytes bound what an operator produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InputBound {
    pub input: usize,
    pub bound: Bound,
}

/// Returns the stage input whose committed logical bytes bound the output of the
/// node in each partition, or `None` if no input does.
pub(crate) fn input_bound(node: &Arc<dyn ExecutionPlan>) -> Option<InputBound> {
    let mut node = node;
    let mut bound = Bound::Exact;
    loop {
        if let Some(placeholder) = node.downcast_ref::<StageInputExec<usize>>() {
            return Some(InputBound {
                input: *placeholder.input(),
                bound,
            });
        }
        bound = bound.and(operator_bound(node)?);
        match node.children().as_slice() {
            [child] => node = child,
            _ => return None,
        }
    }
}

/// Returns how a unary operator's output relates to its input, or `None` if the
/// output is not bounded by the input.
fn operator_bound(node: &Arc<dyn ExecutionPlan>) -> Option<Bound> {
    let input_columns = node.children().first()?.schema().fields().len();
    if node.is::<CooperativeExec>() {
        return Some(Bound::Exact);
    }
    if node.is::<SortExec>() {
        // Sorting can rebatch the rows. In particular, every output dictionary
        // batch carries the dictionary values, so the total logical bytes may
        // exceed the committed input even though the rows are unchanged.
        return None;
    }
    if node.is::<LocalLimitExec>() || node.is::<GlobalLimitExec>() {
        return Some(Bound::Upper);
    }
    if let Some(filter) = node.downcast_ref::<FilterExec>() {
        if let Some(projection) = filter.projection() {
            distinct_columns(projection.iter().copied())?;
        }
        return Some(Bound::Upper);
    }
    if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
        let indices = projection
            .expr()
            .iter()
            .map(|x| column_index(&x.expr))
            .collect::<Option<Vec<_>>>()?;
        distinct_columns(indices.iter().copied())?;
        // Reordering columns keeps the size; dropping columns reduces it.
        return Some(if indices.len() == input_columns {
            Bound::Exact
        } else {
            Bound::Upper
        });
    }
    None
}

fn column_index(expr: &Arc<dyn PhysicalExpr>) -> Option<usize> {
    expr.downcast_ref::<Column>().map(|x| x.index())
}

/// Returns `Some` if no column is selected twice. A repeated column would count
/// the same input bytes more than once.
fn distinct_columns(indices: impl Iterator<Item = usize>) -> Option<()> {
    let mut seen = HashSet::new();
    indices.into_iter().all(|i| seen.insert(i)).then_some(())
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, lit};
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::{ExecutionPlanProperties, Partitioning, PlanProperties};

    use super::*;

    fn input(i: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(4),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Arc::new(StageInputExec::new(i, Arc::new(properties)))
    }

    fn column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    fn project(
        child: Arc<dyn ExecutionPlan>,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        let exprs = exprs
            .into_iter()
            .enumerate()
            .map(|(i, e)| (e, format!("c{i}")))
            .collect::<Vec<_>>();
        Arc::new(ProjectionExec::try_new(exprs, child).unwrap())
    }

    #[test]
    fn exact_through_column_reordering() {
        let plan = project(input(3), vec![column("b", 1), column("a", 0)]);
        assert_eq!(
            input_bound(&plan),
            Some(InputBound {
                input: 3,
                bound: Bound::Exact
            })
        );
    }

    #[test]
    fn upper_through_filters_limits_and_dropped_columns() {
        let predicate = Arc::new(BinaryExpr::new(column("a", 0), Operator::Gt, lit(1i64)));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input(0)).unwrap());
        assert_eq!(input_bound(&filter).unwrap().bound, Bound::Upper);
        let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(input(0), 10));
        assert_eq!(input_bound(&limit).unwrap().bound, Bound::Upper);
        let dropped = project(input(0), vec![column("a", 0)]);
        assert_eq!(input_bound(&dropped).unwrap().bound, Bound::Upper);
    }

    #[test]
    fn no_bound_through_sort_rebatching() {
        use datafusion::arrow::compute::SortOptions;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};

        let ordering = LexOrdering::new([PhysicalSortExpr::new(
            column("a", 0),
            SortOptions::default(),
        )])
        .unwrap();
        for fetch in [None, Some(1)] {
            let sort: Arc<dyn ExecutionPlan> = Arc::new(
                SortExec::new(ordering.clone(), input(0))
                    .with_preserve_partitioning(true)
                    .with_fetch(fetch),
            );
            assert_eq!(input_bound(&sort), None);
        }
    }

    #[test]
    fn no_bound_through_widening_operators() {
        // A computed value can be wider than its inputs.
        let computed = project(
            input(0),
            vec![Arc::new(BinaryExpr::new(
                column("a", 0),
                Operator::Plus,
                lit(1i64),
            ))],
        );
        assert_eq!(input_bound(&computed), None);
        // A repeated column counts the same bytes twice.
        let repeated = project(input(0), vec![column("b", 1), column("b", 1)]);
        assert_eq!(input_bound(&repeated), None);
        // An aggregate can produce values wider than its input rows.
        let child = input(0);
        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::new_single(vec![(column("a", 0), "a".to_string())]),
                vec![],
                vec![],
                child.clone(),
                child.schema(),
            )
            .unwrap(),
        );
        assert_eq!(aggregate.output_partitioning().partition_count(), 4);
        assert_eq!(input_bound(&aggregate), None);
    }
}
