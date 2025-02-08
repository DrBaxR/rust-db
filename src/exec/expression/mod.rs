use arithmetic::ArithmeticExpression;
use boolean::BooleanExpression;
use constant::ConstantExpression;
use value::ColumnValueExpression;

use crate::table::{
    schema::{Column, Schema},
    tuple::Tuple,
    value::ColumnValue,
};

pub mod arithmetic;
pub mod boolean;
pub mod constant;
pub mod value;

#[cfg(test)]
mod tests;

pub trait Evaluate {
    /// Evaluate the expression for a single tuple.
    ///
    /// # Arguments
    /// * `tuple` - The tuple to evaluate the expression for.
    /// * `schema` - The schema of the tuple.
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> ColumnValue;

    /// Evaluate the expression in the context of a join operation.
    ///
    /// # Arguments
    /// * `l_tuple` - The left tuple to evaluate the expression for.
    /// * `l_schema` - The schema of the left tuple.
    /// * `r_tuple` - The right tuple to evaluate the expression for.
    /// * `r_schema` - The schema of the right tuple.
    fn evaluate_join(
        &self,
        l_tuple: &Tuple,
        l_schema: &Schema,
        r_tuple: &Tuple,
        r_schema: &Schema,
    ) -> ColumnValue;

    /// Get the type of the return value of the expression.
    fn return_type(&self) -> Column;

    fn to_string(&self) -> String;
}

#[derive(Clone)]
pub enum Expression {
    Constant(ConstantExpression),
    Arithmetic(ArithmeticExpression),
    Boolean(BooleanExpression),
    ColumnValue(ColumnValueExpression),
}

impl Evaluate for Expression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> ColumnValue {
        match self {
            Expression::Constant(expr) => expr.evaluate(tuple, schema),
            Expression::Arithmetic(expr) => expr.evaluate(tuple, schema),
            Expression::Boolean(expr) => expr.evaluate(tuple, schema),
            Expression::ColumnValue(expr) => expr.evaluate(tuple, schema),
        }
    }

    fn evaluate_join(
        &self,
        l_tuple: &Tuple,
        l_schema: &Schema,
        r_tuple: &Tuple,
        r_schema: &Schema,
    ) -> ColumnValue {
        match self {
            Expression::Constant(expr) => expr.evaluate_join(l_tuple, l_schema, r_tuple, r_schema),
            Expression::Arithmetic(expr) => {
                expr.evaluate_join(l_tuple, l_schema, r_tuple, r_schema)
            }
            Expression::Boolean(expr) => expr.evaluate_join(l_tuple, l_schema, r_tuple, r_schema),
            Expression::ColumnValue(expr) => {
                expr.evaluate_join(l_tuple, l_schema, r_tuple, r_schema)
            }
        }
    }

    fn return_type(&self) -> Column {
        match self {
            Expression::Constant(expr) => expr.return_type(),
            Expression::Arithmetic(expr) => expr.return_type(),
            Expression::Boolean(expr) => expr.return_type(),
            Expression::ColumnValue(expr) => expr.return_type(),
        }
    }

    fn to_string(&self) -> String {
        match self {
            Expression::Constant(expr) => expr.to_string(),
            Expression::Arithmetic(expr) => expr.to_string(),
            Expression::Boolean(expr) => expr.to_string(),
            Expression::ColumnValue(expr) => expr.to_string(),
        }
    }
}
