use crate::table::{
    schema::{Column, ColumnType, Schema},
    tuple::Tuple,
    value::{BooleanValue, ColumnValue, DecimalValue, IntegerValue, TimestampValue, VarcharValue},
};

use super::{Evaluate, Expression};

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

#[derive(Clone)]
pub struct ConstantExpression {
    pub value: ColumnValue,
}

impl Evaluate for ConstantExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> ColumnValue {
        self.value.clone()
    }

    fn evaluate_join(
        &self,
        l_tuple: &Tuple,
        l_schema: &Schema,
        r_tuple: &Tuple,
        r_schema: &Schema,
    ) -> ColumnValue {
        self.value.clone()
    }

    fn return_type(&self) -> Column {
        match self.value.typ() {
            ColumnType::Varchar(len) => {
                Column::new_named("_const_".to_string(), ColumnType::Varchar(len))
            }
            typ => Column::new_named("_const_".to_string(), typ),
        }
    }
}
