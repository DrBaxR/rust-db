use crate::table::{
    schema::{Column, ColumnType, Schema},
    tuple::Tuple,
    value::{ColumnValue, IntegerValue},
};

use super::{Evaluate, Expression};

#[derive(Clone)]
pub enum ArithmeticType {
    Plus,
    Minus,
}

#[derive(Clone)]
pub struct ArithmeticExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub typ: ArithmeticType,
}

impl ArithmeticExpression {
    fn compute(&self, l: ColumnValue, r: ColumnValue) -> ColumnValue {
        // TODO: consider casting to decimal before computation
        match (l, r) {
            (ColumnValue::Integer(l), ColumnValue::Integer(r)) => match self.typ {
                ArithmeticType::Plus => ColumnValue::Integer(IntegerValue {
                    value: l.value + r.value,
                }),
                ArithmeticType::Minus => ColumnValue::Integer(IntegerValue {
                    value: l.value - r.value,
                }),
            },
            _ => panic!("Only supprted compute operands are (Integer, Integer)"),
        }
    }
}

impl Evaluate for ArithmeticExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> ColumnValue {
        let l_val = self.left.evaluate(tuple, schema);
        let r_val = self.right.evaluate(tuple, schema);
        self.compute(l_val, r_val)
    }

    fn evaluate_join(
        &self,
        l_tuple: &Tuple,
        l_schema: &Schema,
        r_tuple: &Tuple,
        r_schema: &Schema,
    ) -> ColumnValue {
        let l_val = self
            .left
            .evaluate_join(l_tuple, l_schema, r_tuple, r_schema);
        let r_val = self
            .right
            .evaluate_join(l_tuple, l_schema, r_tuple, r_schema);
        self.compute(l_val, r_val)
    }

    fn return_type(&self) -> Column {
        Column::new_named("_result_".to_string(), ColumnType::Integer)
    }
}
