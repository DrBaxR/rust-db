use std::cmp::Ordering;

use crate::table::{
    schema::{Column, ColumnType, Schema},
    tuple::Tuple,
    value::{BooleanValue, ColumnValue},
};

use super::{Evaluate, Expression};

#[derive(Clone)]
pub enum BooleanType {
    // composite
    And,
    Or,
    // arithmetic
    EQ,
    NE,
    GT,
    GE,
    LT,
    LE,
}

#[derive(Clone)]
pub struct BooleanExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub typ: BooleanType,
}

impl BooleanExpression {
    fn compute(&self, l: ColumnValue, r: ColumnValue) -> ColumnValue {
        // boolean composite
        if let (ColumnValue::Boolean(ref l), ColumnValue::Boolean(ref r)) = (&l, &r) {
            return match self.typ {
                BooleanType::And => ColumnValue::Boolean(BooleanValue {
                    value: l.value && r.value,
                }),
                BooleanType::Or => ColumnValue::Boolean(BooleanValue {
                    value: l.value || r.value,
                }),
                _ => panic!("Only supported boolean operations are (And, Or)"),
            };
        }

        // comparisons
        if let (Ok(dec_l), Ok(dec_r)) = (l.to_decimal(), r.to_decimal()) {
            return self.compute_comparison(dec_l, dec_r).unwrap(); // unwrap is fine because we cast both to decimal
        }

        return self
            .compute_comparison(l, r)
            .expect("Failed to compute comparison. Types do not match.");
    }

    /// Compute the comparison between two `ColumnValue`s.
    ///
    /// # Errors
    /// Will return `Err` if the types of the `ColumnValue`s are not the same.
    ///
    /// # Panics
    /// Will panic if the comparison operation is not supported.
    fn compute_comparison(&self, l: ColumnValue, r: ColumnValue) -> Result<ColumnValue, ()> {
        return match self.typ {
            BooleanType::EQ => Ok(ColumnValue::Boolean(BooleanValue {
                value: l.compare(&r)? == Ordering::Equal,
            })),
            BooleanType::NE => Ok(ColumnValue::Boolean(BooleanValue {
                value: l.compare(&r)? != Ordering::Equal,
            })),
            BooleanType::GT => Ok(ColumnValue::Boolean(BooleanValue {
                value: l.compare(&r)? == Ordering::Greater,
            })),
            BooleanType::GE => Ok(ColumnValue::Boolean(BooleanValue {
                value: l.compare(&r)? != Ordering::Less,
            })),
            BooleanType::LT => Ok(ColumnValue::Boolean(BooleanValue {
                value: l.compare(&r)? == Ordering::Less,
            })),
            BooleanType::LE => Ok(ColumnValue::Boolean(BooleanValue {
                value: l.compare(&r)? != Ordering::Greater,
            })),
            _ => panic!("Only supported comparison operations are (EQ, NE, GT, GE, LT, LE)"),
        };
    }
}

impl Evaluate for BooleanExpression {
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
        Column::new_named("_result_".to_string(), ColumnType::Boolean)
    }

    fn to_string(&self) -> String {
        format!(
            "({} {} {})",
            self.left.to_string(),
            match self.typ {
                BooleanType::And => "AND",
                BooleanType::Or => "OR",
                BooleanType::EQ => "=",
                BooleanType::NE => "!=",
                BooleanType::GT => ">",
                BooleanType::GE => ">=",
                BooleanType::LT => "<",
                BooleanType::LE => "<=",
            },
            self.right.to_string()
        )
    }
}
