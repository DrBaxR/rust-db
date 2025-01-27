use exec::{
    executor::{values::ValuesExecutor, Execute},
    expression::{ConstantExpression, Expression},
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

fn main() {
    // sample usage of the values executor
    let schema = Schema::new(vec![
        Column::new_fixed("int".to_string(), ColumnType::Integer),
        Column::new_fixed("bool".to_string(), ColumnType::Boolean),
        Column::new_fixed("decimal".to_string(), ColumnType::Decimal),
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

    let mut values_executor = ValuesExecutor {
        plan: values_plan,
        cursor: 0,
    };

    values_executor.init();
    while let Some((tuple, _)) = values_executor.next() {
        println!("{}", tuple.to_string(&schema));
    }
}
