use crate::table::value::DecimalValue;

use super::*;

#[test]
fn constant_expression() {
    let schema = Schema::new(vec![Column::new_named(
        "col1".to_string(),
        ColumnType::Integer,
    )]);
    let tuple = Tuple::new(
        vec![ColumnValue::Integer(IntegerValue { value: 10 })],
        &schema,
    );

    let expr = ConstantExpression {
        value: ColumnValue::Integer(IntegerValue { value: 10 }),
    };
    assert_eq!(
        expr.evaluate(&tuple, &schema),
        ColumnValue::Integer(IntegerValue { value: 10 })
    );

    let expr = ConstantExpression {
        value: ColumnValue::Decimal(DecimalValue { value: 12.3 }),
    };
    assert_eq!(
        expr.evaluate(&tuple, &schema),
        ColumnValue::Decimal(DecimalValue { value: 12.3 })
    );
}

#[test]
fn column_value_expression() {
    let schema = Schema::new(vec![
        Column::new_named("col1".to_string(), ColumnType::Integer),
        Column::new_named("col2".to_string(), ColumnType::Integer),
    ]);
    let tuple = Tuple::new(
        vec![
            ColumnValue::Integer(IntegerValue { value: 10 }),
            ColumnValue::Integer(IntegerValue { value: 20 }),
        ],
        &schema,
    );

    let expr = ColumnValueExpression {
        join_side: JoinSide::Left,
        col_index: 0,
        return_type: Column::new_named("col1".to_string(), ColumnType::Integer),
    };
    assert_eq!(
        expr.evaluate(&tuple, &schema),
        ColumnValue::Integer(IntegerValue { value: 10 })
    );

    let expr = ColumnValueExpression {
        join_side: JoinSide::Left,
        col_index: 1,
        return_type: Column::new_named("col2".to_string(), ColumnType::Integer),
    };
    assert_eq!(
        expr.evaluate(&tuple, &schema),
        ColumnValue::Integer(IntegerValue { value: 20 })
    );
}

#[test]
fn arithmetic_expression() {
    let schema = Schema::new(vec![
        Column::new_named("col1".to_string(), ColumnType::Integer),
        Column::new_named("col2".to_string(), ColumnType::Integer),
    ]);
    let tuple = Tuple::new(
        vec![
            ColumnValue::Integer(IntegerValue { value: 10 }),
            ColumnValue::Integer(IntegerValue { value: 20 }),
        ],
        &schema,
    );

    let expr = ArithmeticExpression {
        left: Box::new(Expression::ColumnValue(ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 0,
            return_type: Column::new_named("col1".to_string(), ColumnType::Integer),
        })),
        right: Box::new(Expression::ColumnValue(ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 1,
            return_type: Column::new_named("col2".to_string(), ColumnType::Integer),
        })),
        typ: ArithmeticType::Plus,
    };
    assert_eq!(
        expr.evaluate(&tuple, &schema),
        ColumnValue::Integer(IntegerValue { value: 30 })
    );

    let expr = ArithmeticExpression {
        left: Box::new(Expression::ColumnValue(ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 0,
            return_type: Column::new_named("col1".to_string(), ColumnType::Integer),
        })),
        right: Box::new(Expression::ColumnValue(ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 1,
            return_type: Column::new_named("col2".to_string(), ColumnType::Integer),
        })),
        typ: ArithmeticType::Minus,
    };
    assert_eq!(
        expr.evaluate(&tuple, &schema),
        ColumnValue::Integer(IntegerValue { value: -10 })
    );
}

#[test]
fn boolean_expression() {
    let schema = Schema::new(vec![
        Column::new_named("col1".to_string(), ColumnType::Boolean),
        Column::new_named("col2".to_string(), ColumnType::Boolean),
    ]);
    let tuple = Tuple::new(
        vec![
            ColumnValue::Boolean(BooleanValue { value: true }),
            ColumnValue::Boolean(BooleanValue { value: false }),
        ],
        &schema,
    );

    let expr = BooleanExpression {
        left: Box::new(Expression::ColumnValue(ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 0,
            return_type: Column::new_named("col1".to_string(), ColumnType::Boolean),
        })),
        right: Box::new(Expression::ColumnValue(ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 1,
            return_type: Column::new_named("col2".to_string(), ColumnType::Boolean),
        })),
        typ: BooleanType::And,
    };
    assert_eq!(
        expr.evaluate(&tuple, &schema),
        ColumnValue::Boolean(BooleanValue { value: false })
    );

    let expr = BooleanExpression {
        left: Box::new(Expression::ColumnValue(ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 0,
            return_type: Column::new_named("col1".to_string(), ColumnType::Boolean),
        })),
        right: Box::new(Expression::ColumnValue(ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 1,
            return_type: Column::new_named("col2".to_string(), ColumnType::Boolean),
        })),
        typ: BooleanType::Or,
    };
    assert_eq!(
        expr.evaluate(&tuple, &schema),
        ColumnValue::Boolean(BooleanValue { value: true })
    );
}
