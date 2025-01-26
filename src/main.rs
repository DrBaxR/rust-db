use exec::{executor::{values::ValuesExecutor, Execute}, expression::{ConstantExpression, Expression}};
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

fn const_integer_expression(value: i32) -> Expression {
    Expression::Constant(ConstantExpression {
        value: ColumnValue::Integer(IntegerValue { value }),
    })
}

fn const_boolean_expression(value: bool) -> Expression {
    Expression::Constant(ConstantExpression {
        value: ColumnValue::Boolean(BooleanValue { value }),
    })
}

fn const_decimal_expression(value: f64) -> Expression {
    Expression::Constant(ConstantExpression {
        value: ColumnValue::Decimal(value::DecimalValue { value }),
    })
}

fn main() {
    // sample usage of the values executor
    let schema = Schema::new(vec![
        Column::new_fixed("int".to_string(), ColumnType::Integer),
        Column::new_fixed("bool".to_string(), ColumnType::Boolean),
        Column::new_fixed("decimal".to_string(), ColumnType::Decimal),
    ]);

    // TODO: Replace function calls with macros!!!
    let values = vec![
        vec![
            const_integer_expression(1),
            const_boolean_expression(true),
            const_decimal_expression(10.1),
        ],
        vec![
            const_integer_expression(2),
            const_boolean_expression(false),
            const_decimal_expression(20.2),
        ],
        vec![
            const_integer_expression(3),
            const_boolean_expression(true),
            const_decimal_expression(30.3),
        ],
        vec![
            const_integer_expression(4),
            const_boolean_expression(false),
            const_decimal_expression(40.4),
        ],
        vec![
            const_integer_expression(5),
            const_boolean_expression(false),
            const_decimal_expression(50.5),
        ],
        vec![
            const_integer_expression(6),
            const_boolean_expression(false),
            const_decimal_expression(60.6),
        ],
    ];

    let values_plan = exec::plan::ValuesPlanNode {
        output_schema: schema.clone(),
        values,
    };

    let mut values_executor = ValuesExecutor {
        plan: values_plan,
        cursor: 0,
    };

    values_executor.init();
    while let Some((tuple, _)) = values_executor.next() {
        println!("{}", tuple.to_string(&schema));
    }
}
