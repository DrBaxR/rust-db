use crate::catalog::Catalog;
use crate::disk::buffer_pool_manager::BufferPoolManager;
use crate::exec::{
    executor::{
        filter::FilterExecutor, projection::ProjectionExecutor, seq_scan::SeqScanExecutor,
        values::ValuesExecutor, Execute, Executor, ExecutorContext,
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
    page::TupleMeta,
    schema::{Column, ColumnType, Schema},
    tuple::Tuple,
    value::{BooleanValue, ColumnValue, DecimalValue, IntegerValue},
    TableHeap,
};
use std::sync::Arc;

use crate::test_utils::{const_bool, const_decimal, const_int};

// EXEC: () -> (int, bool, decimal)
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

// EXEC: (int, bool, decimal) -> (int, decimal)
pub fn projection_executor(child_pln: PlanNode, child_exec: Executor) -> (ProjectionExecutor, Schema) {
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

// EXEC: (int, decimal) -> (int, decimal)
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

pub fn populate_heap(table_heap: &mut TableHeap, schema: &Schema) {
    table_heap.insert_tuple(
        TupleMeta {
            ts: 0,
            is_deleted: false,
        },
        Tuple::new(
            vec![
                ColumnValue::Integer(IntegerValue { value: 1 }),
                ColumnValue::Boolean(BooleanValue { value: true }),
                ColumnValue::Decimal(DecimalValue { value: 10.1 }),
            ],
            schema,
        ),
    );
    table_heap.insert_tuple(
        TupleMeta {
            ts: 0,
            is_deleted: false,
        },
        Tuple::new(
            vec![
                ColumnValue::Integer(IntegerValue { value: 2 }),
                ColumnValue::Boolean(BooleanValue { value: false }),
                ColumnValue::Decimal(DecimalValue { value: 20.2 }),
            ],
            schema,
        ),
    );
    table_heap.insert_tuple(
        TupleMeta {
            ts: 0,
            is_deleted: false,
        },
        Tuple::new(
            vec![
                ColumnValue::Integer(IntegerValue { value: 3 }),
                ColumnValue::Boolean(BooleanValue { value: false }),
                ColumnValue::Decimal(DecimalValue { value: 30.3 }),
            ],
            schema,
        ),
    );
}

// EXEC: () -> (int, bood, decimal)
pub fn seq_scan_executor() -> (SeqScanExecutor, Schema) {
    let schema = Schema::with_types(vec![
        ColumnType::Integer,
        ColumnType::Boolean,
        ColumnType::Decimal,
    ]);

    // init executor context
    let bpm = Arc::new(BufferPoolManager::new("db/test.db".to_string(), 2, 2));
    let catalog = Arc::new(Catalog::new(bpm.clone()));
    let executor_context = ExecutorContext {
        catalog: catalog.clone(),
        bpm: bpm.clone(),
    };

    // create a table
    bpm.new_page(); // this is needed as table heaps assume page with PID 0 is not used
    let table_name = "test_table".to_string();
    let table_oid = executor_context
        .catalog
        .create_table(&table_name, schema.clone())
        .unwrap()
        .lock()
        .unwrap()
        .oid;

    // insert some tuples
    let table_info = executor_context
        .catalog
        .get_table_by_oid(table_oid)
        .unwrap();
    let mut table_info = table_info.lock().unwrap();

    populate_heap(&mut table_info.table, &schema);

    drop(table_info);

    // create a sequential scan executor
    let plan = SeqScanPlanNode {
        output_schema: schema.clone(),
        table_oid,
        table_name,
        filter_expr: None,
    };

    (SeqScanExecutor::new(executor_context, plan), schema)
}
