use exec::{
    executor::{projection::ProjectionExecutor, values::ValuesExecutor, Execute, Executor},
    expression::{ColumnValueExpression, ConstantExpression, Expression, JoinSide},
    plan::PlanNode,
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
    let schema = Schema::new(vec![
        Column::new_fixed("v_int".to_string(), ColumnType::Integer),
        Column::new_fixed("v_bool".to_string(), ColumnType::Boolean),
        Column::new_fixed("v_decimal".to_string(), ColumnType::Decimal),
    ]);

    let values = vec![
        vec![const_int!(1), const_bool!(true), const_decimal!(10.1)],
        vec![const_int!(2), const_bool!(false), const_decimal!(20.2)],
        vec![const_int!(3), const_bool!(true), const_decimal!(30.3)],
        vec![const_int!(4), const_bool!(false), const_decimal!(40.4)],
        vec![const_int!(5), const_bool!(false), const_decimal!(50.5)],
        vec![const_int!(6), const_bool!(false), const_decimal!(60.6)],
    ];

    let values_plan = exec::plan::ValuesPlanNode {
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
    let int_col = Column::new_fixed("p_int".to_string(), ColumnType::Integer);
    let dec_col = Column::new_fixed("p_decimal".to_string(), ColumnType::Decimal);

    let schema = Schema::new(vec![int_col.clone(), dec_col.clone()]);

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

    let projection_plan = exec::plan::ProjectionPlanNode {
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
    let (mut projection_executor, schema) = projection_executor(PlanNode::Values(values_executor.plan.clone()), Executor::Values(values_executor));

    while let Some((tuple, _)) = projection_executor.next() {
        println!("{}", tuple.to_string(&schema));
    }
}
