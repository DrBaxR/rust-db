use exec::{
    executor::{projection::ProjectionExecutor, values::ValuesExecutor, Execute, Executor},
    expression::{ColumnValueExpression, ConstantExpression, Expression, JoinSide},
    plan::{projection::ProjectionPlanNode, values::ValuesPlanNode, PlanNode},
};
use table::{
    schema::{Column, ColumnType, Schema},
    value::{self, BooleanValue, ColumnValue, IntegerValue},
};

mod b_tree;
mod config;
mod disk;
mod exec;
mod index;
mod parser;
mod table;

fn values_executor() -> (ValuesExecutor, Schema) {
    let schema = Schema::with_types(vec![
        ColumnType::Integer,
        ColumnType::Boolean,
        ColumnType::Decimal,
    ]);

    let values = vec![
        vec![const_int!(1), const_bool!(true), const_decimal!(10.1)],
        vec![const_int!(2), const_bool!(false), const_decimal!(20.2)],
        vec![const_int!(3), const_bool!(true), const_decimal!(30.3)],
        vec![const_int!(4), const_bool!(false), const_decimal!(40.4)],
        vec![const_int!(5), const_bool!(false), const_decimal!(50.5)],
        vec![const_int!(6), const_bool!(false), const_decimal!(60.6)],
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

fn main() {
    let (values_executor, _) = values_executor();
    let (mut projection_executor, schema) = projection_executor(
        PlanNode::Values(values_executor.plan.clone()),
        Executor::Values(values_executor),
    );

    while let Some((tuple, _)) = projection_executor.next() {
        println!("{}", tuple.to_string(&schema));
    }
}
