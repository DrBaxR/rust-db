use std::sync::Arc;

use crate::catalog::Catalog;
use crate::exec::executor::insert::InsertExecutor;
use crate::exec::executor::ExecutorContext;
use crate::exec::plan::insert::InsertPlanNode;
use crate::exec::{
    executor::{
        filter::FilterExecutor, projection::ProjectionExecutor, seq_scan::SeqScanExecutor,
        values::ValuesExecutor, Executor,
    },
    expression::{
        arithmetic::{ArithmeticExpression, ArithmeticType},
        boolean::{BooleanExpression, BooleanType},
        constant::ConstantExpression,
        value::{ColumnValueExpression, JoinSide},
        Expression,
    },
    plan::{
        filter::FilterNode, projection::ProjectionPlanNode, seq_scan::SeqScanPlanNode,
        values::ValuesPlanNode, PlanNode,
    },
};
use crate::table::{
    schema::{Column, ColumnType, Schema},
    value::{ColumnValue, IntegerValue},
};

use crate::test_utils::{const_bool, const_decimal, const_int};

use super::util::create_table;

/// EXEC: () -> (int, bool, decimal)
pub fn values_executor() -> (ValuesExecutor, Schema) {
    let schema = Schema::with_types(vec![
        ColumnType::Integer,
        ColumnType::Boolean,
        ColumnType::Decimal,
    ]);

    let values = vec![
        vec![const_int(1), const_bool(true), const_decimal(10.1)],
        vec![const_int(2), const_bool(false), const_decimal(20.2)],
        vec![const_int(3), const_bool(true), const_decimal(30.3)],
        vec![const_int(4), const_bool(false), const_decimal(40.4)],
        vec![const_int(5), const_bool(false), const_decimal(50.5)],
        vec![const_int(6), const_bool(false), const_decimal(60.6)],
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

/// EXEC: (int, bool, decimal) -> (int, decimal)
pub fn projection_executor(
    child_pln: PlanNode,
    child_exec: Executor,
) -> (ProjectionExecutor, Schema) {
    let int_col = Column::new(ColumnType::Integer);
    let dec_col = Column::new(ColumnType::Decimal);

    let schema = Schema::with_types(vec![ColumnType::Integer, ColumnType::Decimal]);

    let expressions = vec![
        Expression::ColumnValue(ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 0,
            return_type: int_col,
        }),
        Expression::ColumnValue(ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 2,
            return_type: dec_col,
        }),
    ];

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

/// EXEC: (int, decimal) -> (int, decimal)
pub fn filter_executor(child_pln: PlanNode, child_exec: Executor) -> (FilterExecutor, Schema) {
    let schema = Schema::with_types(vec![ColumnType::Integer, ColumnType::Decimal]);

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

    // filter: true && true
    // let predicate = BooleanExpression {
    //     left: Box::new(Expression::Constant(ConstantExpression {
    //         value: ColumnValue::Boolean(BooleanValue { value: true }),
    //     })),
    //     right: Box::new(Expression::Constant(ConstantExpression {
    //         value: ColumnValue::Boolean(BooleanValue { value: true }),
    //     })),
    //     typ: BooleanType::And,
    // };

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

/// EXEC: () -> (int, bool, decimal)
/// SIDE: creates a table and inserts three tuples into it
pub fn seq_scan_executor() -> (SeqScanExecutor, Schema) {
    let (executor_context, schema, table_oid, table_name) = create_table();

    // create a sequential scan executor
    let plan = SeqScanPlanNode {
        output_schema: schema.clone(),
        table_oid,
        table_name,
        filter_expr: None,
    };

    (SeqScanExecutor::new(executor_context, plan), schema)
}

/// EXEC: (int, bool, decimal) -> (int)
/// SIDE: creates a table and inserts three tuples into it
pub fn insert_executor(
    child_pln: PlanNode,
    child_exec: Executor,
) -> (InsertExecutor, Schema, Arc<Catalog>) {
    let (executor_context, _, table_oid, table_name) = create_table();
    let plan = InsertPlanNode::new(table_oid, table_name, child_pln);
    let catalog = executor_context.catalog.clone();

    (
        InsertExecutor::new(executor_context, plan, child_exec),
        Schema::with_types(vec![ColumnType::Integer]),
        catalog,
    )
}
