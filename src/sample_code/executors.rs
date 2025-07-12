use std::sync::Arc;

use crate::catalog::{Catalog, OID};
use crate::exec::executor::delete::DeleteExecutor;
use crate::exec::executor::idx_scan::IdxScanExecutor;
use crate::exec::executor::insert::InsertExecutor;
use crate::exec::executor::update::UpdateExecutor;
use crate::exec::executor::ExecutorContext;
use crate::exec::plan::delete::DeletePlanNode;
use crate::exec::plan::idx_scan::IdxScanPlanNode;
use crate::exec::plan::insert::InsertPlanNode;
use crate::exec::plan::update::UpdatePlanNode;
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
use crate::sample_code::util::create_table_with_values;
use crate::table::{
    schema::{Column, ColumnType, Schema},
    value::{ColumnValue, IntegerValue},
};

use crate::test_utils::{column_with, const_bool, const_decimal, const_int, int_value};

use super::util::create_table;

/// EXEC: () -> (int, bool, decimal)
pub fn values_executor(values: Vec<i32>) -> (ValuesExecutor, Schema) {
    let schema = Schema::with_types(vec![
        ColumnType::Integer,
        ColumnType::Boolean,
        ColumnType::Decimal,
    ]);

    let values = values
        .into_iter()
        .map(|v| {
            vec![
                const_int(v),
                const_bool(v % 2 == 0),
                const_decimal(v as f64 * 10.1),
            ]
        })
        .collect::<Vec<_>>();

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

type TableContext = (ExecutorContext, Schema, OID, String);

pub enum TableConstructorType {
    WithoutTable(TableContext),
    WithTable(String),
}

/// EXEC: () -> (int, bool, decimal)
/// SIDE: creates a table and inserts three tuples into it
pub fn seq_scan_executor(c_type: TableConstructorType) -> (SeqScanExecutor, TableContext) {
    let (executor_context, schema, table_oid, table_name) = match c_type {
        TableConstructorType::WithoutTable((executor_context, schema, table_oid, table_name)) => {
            (executor_context, schema, table_oid, table_name)
        }
        TableConstructorType::WithTable(db_file) => create_table(db_file),
    };

    // create a sequential scan executor
    let plan = SeqScanPlanNode {
        output_schema: schema.clone(),
        table_oid,
        table_name: table_name.clone(),
        filter_expr: None,
    };

    (
        SeqScanExecutor::new(executor_context.clone(), plan),
        (executor_context, schema, table_oid, table_name),
    )
}

/// EXEC: (int, bool, decimal) -> (int)
/// SIDE: creates a table and inserts three tuples into it
pub fn insert_executor(
    db_file: String,
    child_pln: PlanNode,
    child_exec: Executor,
) -> (InsertExecutor, Schema, Arc<Catalog>) {
    let (executor_context, _, table_oid, table_name) = create_table(db_file);
    let plan = InsertPlanNode::new(table_oid, table_name, child_pln);
    let catalog = executor_context.catalog.clone();

    (
        InsertExecutor::new(executor_context, plan, child_exec),
        Schema::with_types(vec![ColumnType::Integer]),
        catalog,
    )
}

/// EXEC: (int, bool, decimal) -> (int)
/// SIDE: creates a table and inserts three tuples into it (only if context is `None`)
pub fn delete_executor(
    child_pln: PlanNode,
    child_exec: Executor,
    c_type: TableConstructorType,
) -> (DeleteExecutor, Schema) {
    let (executor_context, table_oid, table_name) = match c_type {
        TableConstructorType::WithoutTable((executor_context, _, table_oid, table_name)) => {
            (executor_context, table_oid, table_name)
        }
        TableConstructorType::WithTable(db_file) => {
            let (executor_context, _, table_oid, table_name) = create_table(db_file);
            (executor_context, table_oid, table_name)
        }
    };

    let plan = DeletePlanNode::new(table_oid, table_name, child_pln);
    (
        DeleteExecutor::new(executor_context, plan, child_exec),
        Schema::with_types(vec![ColumnType::Integer]),
    )
}

/// EXEC: (int, bool, decimal) -> (int)
/// SIDE: requires a table that will have all its tuples' first column set to `12`
pub fn update_executor(
    child_pln: PlanNode,
    child_exec: Executor,
    c_type: TableConstructorType,
) -> (UpdateExecutor, Schema) {
    let (executor_context, table_oid, table_name) = match c_type {
        TableConstructorType::WithoutTable((executor_context, _, table_oid, table_name)) => {
            (executor_context, table_oid, table_name)
        }
        TableConstructorType::WithTable(db_file) => {
            let (executor_context, _, table_oid, table_name) = create_table(db_file);
            (executor_context, table_oid, table_name)
        }
    };

    let expressions = vec![
        // Expression::Arithmetic(ArithmeticExpression {
        //     left: Box::new(Expression::Constant(ConstantExpression {
        //         value: int_value(100),
        //     })),
        //     right: Box::new(Expression::ColumnValue(ColumnValueExpression {
        //         join_side: JoinSide::Left,
        //         col_index: 0,
        //         return_type: Column::new(ColumnType::Integer),
        //     })),
        //     typ: ArithmeticType::Multiply,
        // }),
        const_int(12),
        column_with(1, ColumnType::Boolean),
        column_with(2, ColumnType::Decimal),
    ];
    let plan = UpdatePlanNode::new(table_oid, table_name, expressions, child_pln);

    (
        UpdateExecutor::new(executor_context, plan, child_exec),
        Schema::with_types(vec![ColumnType::Integer]),
    )
}

/// EXEC: () -> (int, bool, decimal)
/// SIDE: creates a table and inserts three tuples into it (all the same)
pub fn idx_scan_executor(c_type: TableConstructorType) -> (IdxScanExecutor, TableContext) {
    let (executor_context, schema, table_oid, table_name) = match c_type {
        TableConstructorType::WithoutTable((executor_context, schema, table_oid, table_name)) => {
            (executor_context, schema, table_oid, table_name)
        }
        TableConstructorType::WithTable(db_file) => {
            let (executor_context, schema, table_oid, table_name) =
                create_table_with_values(db_file, &vec![2, 2, 2]);
            (executor_context, schema, table_oid, table_name)
        }
    };

    let filter_expr = BooleanExpression {
        left: Box::new(column_with(0, ColumnType::Integer)),
        right: Box::new(const_int(2)),
        typ: BooleanType::EQ,
    };
    let plan = IdxScanPlanNode {
        output_schema: Schema::with_types(vec![
            ColumnType::Integer,
            ColumnType::Boolean,
            ColumnType::Decimal,
        ]),
        table_oid,
        table_name: table_name.clone(),
        filter_expr,
    };

    (
        IdxScanExecutor::new(executor_context.clone(), plan),
        (executor_context, schema, table_oid, table_name),
    )
}
