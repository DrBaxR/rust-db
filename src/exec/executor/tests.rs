use crate::{
    exec::{
        executor::Execute,
        expression::{
            arithmetic::{ArithmeticExpression, ArithmeticType},
            boolean::{BooleanExpression, BooleanType},
            constant::{const_int, ConstantExpression},
            value::{ColumnValueExpression, JoinSide},
            Expression,
        },
        plan::{
            filter::FilterNode, projection::ProjectionPlanNode, values::ValuesPlanNode, PlanNode,
        },
    },
    table::{
        schema::{Column, ColumnType, Schema},
        value::{ColumnValue, IntegerValue},
    },
};

use super::{
    filter::FilterExecutor, projection::ProjectionExecutor, values::ValuesExecutor, Executor,
};

/// Creates a values executor with tuples of 1 column of type int. The values in that columns are those
/// passed via the `values` parameter.
///
/// Returns the executor and the schema of the values.
///
/// Executor: `() -> (Integer)`
fn get_values_executor(values: Vec<i32>) -> (ValuesExecutor, Schema) {
    let schema = Schema::with_types(vec![ColumnType::Integer]);

    let values = vec![
        vec![const_int(1)],
        vec![const_int(2)],
        vec![const_int(3)],
        vec![const_int(4)],
        vec![const_int(5)],
        vec![const_int(6)],
    ];

    let values_plan = ValuesPlanNode {
        output_schema: schema.clone(),
        values,
    };

    (
        ValuesExecutor {
            plan: values_plan,
            cursor: 0,
        },
        schema,
    )
}

#[test]
fn values_executor() {
    let values = vec![1, 2, 3, 4, 5, 6];
    let (mut executor, schema) = get_values_executor(values.clone());

    let mut results = vec![];
    while let Some(batch) = executor.next() {
        results.push(batch.0);
    }

    let results: Vec<_> = results
        .iter()
        .map(|t| {
            if let ColumnValue::Integer(val) = t.get_value(&schema, 0) {
                val.value
            } else {
                panic!("Expected integer value")
            }
        })
        .collect();
    assert_eq!(results, values);
}

/// Creates a projection executor that doubles the value of the first column and returns it as the only
/// column in the output.
///
/// Executor: `(Integer) -> (Integer)`
fn get_projection_executor(
    child_pln: PlanNode,
    child_exec: Executor,
) -> (ProjectionExecutor, Schema) {
    let schema = Schema::with_types(vec![ColumnType::Integer]);

    let expressions = vec![Expression::Arithmetic(ArithmeticExpression {
        left: Box::new(Expression::ColumnValue(ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 0,
            return_type: Column::new(ColumnType::Integer),
        })),
        right: Box::new(Expression::Constant(ConstantExpression {
            value: ColumnValue::Integer(IntegerValue { value: 2 }),
        })),
        typ: ArithmeticType::Multiply,
    })];

    let projection_plan = ProjectionPlanNode {
        output_schema: schema.clone(),
        expressions,
        child: Box::new(child_pln),
    };

    (
        ProjectionExecutor {
            plan: projection_plan,
            child: Box::new(child_exec),
        },
        schema,
    )
}

#[test]
fn projection_executor() {
    let (values_executor, _) = get_values_executor(vec![1, 2, 3, 4, 5, 6]);
    let (mut executor, schema) = get_projection_executor(
        PlanNode::Values(values_executor.plan.clone()),
        Executor::Values(values_executor),
    );

    let mut results = vec![];
    while let Some(batch) = executor.next() {
        results.push(batch.0);
    }

    let results: Vec<_> = results
        .iter()
        .map(|t| {
            if let ColumnValue::Integer(val) = t.get_value(&schema, 0) {
                val.value
            } else {
                panic!("Expected integer value")
            }
        })
        .collect();
    assert_eq!(results, vec![2, 4, 6, 8, 10, 12]);
}

/// Creates a filter executor that filters out all rows where the first column is not even.
///
/// Executor: `(Integer) -> (Integer)`
fn get_filter_executor(child_pln: PlanNode, child_exec: Executor) -> (FilterExecutor, Schema) {
    let schema = Schema::with_types(vec![ColumnType::Integer]);

    // filter: col0 % 2 == 0
    let predicate = BooleanExpression {
        left: Box::new(Expression::Arithmetic(ArithmeticExpression {
            left: Box::new(Expression::ColumnValue(ColumnValueExpression {
                join_side: JoinSide::Left,
                col_index: 0,
                return_type: Column::new(ColumnType::Integer),
            })),
            right: Box::new(Expression::Constant(ConstantExpression {
                value: ColumnValue::Integer(IntegerValue { value: 2 }),
            })),
            typ: ArithmeticType::Mod,
        })),
        right: Box::new(Expression::Constant(ConstantExpression {
            value: ColumnValue::Integer(IntegerValue { value: 0 }),
        })),
        typ: BooleanType::EQ,
    };

    let filter_plan = FilterNode {
        output_schema: schema.clone(),
        predicate,
        child: Box::new(child_pln),
    };

    (
        FilterExecutor {
            plan: filter_plan,
            child: Box::new(child_exec),
        },
        schema,
    )
}

#[test]
fn filter_executor() {
    let (values_executor, _) = get_values_executor(vec![1, 2, 3, 4, 5, 6]);
    let (mut executor, schema) = get_filter_executor(
        PlanNode::Values(values_executor.plan.clone()),
        Executor::Values(values_executor),
    );

    let mut results = vec![];
    while let Some(batch) = executor.next() {
        results.push(batch.0);
    }

    let results: Vec<_> = results
        .iter()
        .map(|t| {
            if let ColumnValue::Integer(val) = t.get_value(&schema, 0) {
                val.value
            } else {
                panic!("Expected integer value")
            }
        })
        .collect();
    assert_eq!(results, vec![2, 4, 6]);
}
