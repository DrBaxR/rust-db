use exec::{
    executor::{
        filter::FilterExecutor, projection::ProjectionExecutor, values::ValuesExecutor, Execute,
        Executor,
    },
    expression::{
        arithmetic::{ArithmeticExpression, ArithmeticType},
        boolean::{BooleanExpression, BooleanType},
        constant::ConstantExpression,
        value::{ColumnValueExpression, JoinSide},
        Expression,
    },
    plan::{filter::FilterNode, projection::ProjectionPlanNode, values::ValuesPlanNode, PlanNode},
};
use table::{
    schema::{Column, ColumnType, Schema},
    value::{ColumnValue, IntegerValue},
};
use test_utils::{const_bool, const_decimal, const_int};

// #[cfg(test)]
mod test_utils;

mod b_tree;
mod config;
mod disk;
mod exec;
mod index;
mod parser;
mod table;
mod catalog;

fn values_executor() -> (ValuesExecutor, Schema) {
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

fn projection_executor(child_pln: PlanNode, child_exec: Executor) -> (ProjectionExecutor, Schema) {
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

fn filter_executor(child_pln: PlanNode, child_exec: Executor) -> (FilterExecutor, Schema) {
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

fn main() {
    let (values_executor, _) = values_executor();
    let (projection_executor, schema) = projection_executor(
        PlanNode::Values(values_executor.plan.clone()),
        Executor::Values(values_executor),
    );
    let (mut filter_executor, _) = filter_executor(
        PlanNode::Projection(projection_executor.plan.clone()),
        Executor::Projection(projection_executor),
    );
    println!("{}", filter_executor.to_string(0));

    while let Some((tuple, _)) = filter_executor.next() {
        println!("{}", tuple.to_string(&schema));
    }
}
