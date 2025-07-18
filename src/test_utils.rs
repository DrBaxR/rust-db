use crate::{
    exec::expression::{constant::ConstantExpression, value::{ColumnValueExpression, JoinSide}, Expression},
    table::{
        schema::{Column, ColumnType},
        value::{
            BooleanValue, ColumnValue, DecimalValue, IntegerValue, TimestampValue, VarcharValue,
        },
    },
};

pub fn const_int(value: i32) -> Expression {
    Expression::Constant(ConstantExpression {
        value: ColumnValue::Integer(IntegerValue { value }),
    })
}

pub fn const_bool(value: bool) -> Expression {
    Expression::Constant(ConstantExpression {
        value: ColumnValue::Boolean(BooleanValue { value }),
    })
}

pub fn const_decimal(value: f64) -> Expression {
    Expression::Constant(ConstantExpression {
        value: ColumnValue::Decimal(DecimalValue { value }),
    })
}

pub fn const_timestamp(value: u64) -> Expression {
    Expression::Constant(ConstantExpression {
        value: ColumnValue::Timestamp(TimestampValue { value }),
    })
}

pub fn const_varchar(value: String) -> Expression {
    Expression::Constant(ConstantExpression {
        value: ColumnValue::Varchar(VarcharValue {
            length: value.len(),
            value,
        }),
    })
}

pub fn column_with(col_index: usize, typ: ColumnType) -> Expression {
    Expression::ColumnValue(ColumnValueExpression {
        join_side: JoinSide::Left,
        col_index: col_index,
        return_type: Column::new(typ),
    })
}

pub fn int_value(value: i32) -> ColumnValue {
    ColumnValue::Integer(IntegerValue { value })
}

pub fn bool_value(value: bool) -> ColumnValue {
    ColumnValue::Boolean(BooleanValue { value })
}

pub fn decimal_value(value: f64) -> ColumnValue {
    ColumnValue::Decimal(DecimalValue { value })
}
