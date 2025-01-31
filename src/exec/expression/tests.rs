use crate::{
    exec::expression::{
        arithmetic::ArithmeticType,
        boolean::BooleanType,
        constant::{const_decimal, const_int, ConstantExpression},
        value::JoinSide,
    },
    table::{
        schema::ColumnType,
        value::{BooleanValue, DecimalValue, IntegerValue},
    },
};

use super::{constant::{const_timestamp, const_varchar}, *};

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

fn dummy_schema() -> Schema {
    Schema::new(vec![
        Column::new_named("col1".to_string(), ColumnType::Integer),
        Column::new_named("col2".to_string(), ColumnType::Integer),
    ])
}

fn dummy_tuple() -> Tuple {
    Tuple::new(
        vec![
            ColumnValue::Integer(IntegerValue { value: 10 }),
            ColumnValue::Integer(IntegerValue { value: 20 }),
        ],
        &dummy_schema(),
    )
}

#[test]
fn bool_same_type_comparisson() {
    // 1 == 2
    let left = const_int(1);
    let right = const_int(2);

    let expr = BooleanExpression {
        left: Box::new(left),
        right: Box::new(right),
        typ: BooleanType::EQ,
    };

    assert_eq!(
        expr.evaluate(&dummy_tuple(), &dummy_schema()),
        ColumnValue::Boolean(BooleanValue { value: false })
    );

    // 1 == 1
    let left = const_int(1);
    let right = const_int(1);

    let expr = BooleanExpression {
        left: Box::new(left),
        right: Box::new(right),
        typ: BooleanType::EQ,
    };

    assert_eq!(
        expr.evaluate(&dummy_tuple(), &dummy_schema()),
        ColumnValue::Boolean(BooleanValue { value: true })
    );
}

#[test]
fn bool_different_numeric_types_comparisson() {
    // 1 < 2.0
    let left = const_int(1);
    let right = const_decimal(2.0);

    let expr = BooleanExpression {
        left: Box::new(left),
        right: Box::new(right),
        typ: BooleanType::LT,
    };

    assert_eq!(
        expr.evaluate(&dummy_tuple(), &dummy_schema()),
        ColumnValue::Boolean(BooleanValue { value: true })
    );

    // 1 > 2.0
    let left = const_int(1);
    let right = const_decimal(2.0);

    let expr = BooleanExpression {
        left: Box::new(left),
        right: Box::new(right),
        typ: BooleanType::GT,
    };

    assert_eq!(
        expr.evaluate(&dummy_tuple(), &dummy_schema()),
        ColumnValue::Boolean(BooleanValue { value: false })
    );
}

#[test]
fn bool_timestamps_comparisson() {
    let expr = BooleanExpression {
        left: Box::new(const_timestamp(12345678)),
        right: Box::new(const_timestamp(12345679)),
        typ: BooleanType::LT,
    };

    assert_eq!(
        expr.evaluate(&dummy_tuple(), &dummy_schema()),
        ColumnValue::Boolean(BooleanValue { value: true })
    );
}

#[test]
fn bool_varchar_comparisson() {
    let expr = BooleanExpression {
        left: Box::new(const_varchar("a".to_string())),
        right: Box::new(const_varchar("b".to_string())),
        typ: BooleanType::LT,
    };
    assert_eq!(
        expr.evaluate(&dummy_tuple(), &dummy_schema()),
        ColumnValue::Boolean(BooleanValue { value: true })
    );

    let expr = BooleanExpression {
        left: Box::new(const_varchar("a".to_string())),
        right: Box::new(const_varchar("a".to_string())),
        typ: BooleanType::NE,
    };
    assert_eq!(
        expr.evaluate(&dummy_tuple(), &dummy_schema()),
        ColumnValue::Boolean(BooleanValue { value: false })
    );
}
