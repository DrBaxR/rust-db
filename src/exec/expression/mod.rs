#[cfg(test)]
mod tests;

use crate::table::{
    schema::{Column, ColumnType, Schema},
    tuple::Tuple,
    value::{BooleanValue, ColumnValue, IntegerValue},
};

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
}

#[macro_export]
macro_rules! const_int {
    ($value: expr) => {
        Expression::Constant(ConstantExpression {
            value: ColumnValue::Integer(IntegerValue { value: $value }),
        })
    };
}

#[macro_export]
macro_rules! const_bool {
    ($value: expr) => {
        Expression::Constant(ConstantExpression {
            value: ColumnValue::Boolean(BooleanValue { value: $value }),
        })
    };
}

#[macro_export]
macro_rules! const_decimal {
    ($value: expr) => {
        Expression::Constant(ConstantExpression {
            value: ColumnValue::Decimal(value::DecimalValue { value: $value }),
        })
    };
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
        match (l, r) {
            (ColumnValue::Boolean(l), ColumnValue::Boolean(r)) => match self.typ {
                BooleanType::And => ColumnValue::Boolean(BooleanValue {
                    value: l.value && r.value,
                }),
                BooleanType::Or => ColumnValue::Boolean(BooleanValue {
                    value: l.value || r.value,
                }),
                _ => panic!("Only supported boolean operations are (And, Or)"),
            },
            (ColumnValue::Integer(l), ColumnValue::Integer(r)) => match self.typ {
                BooleanType::EQ => ColumnValue::Boolean(BooleanValue {
                    value: l.value == r.value,
                }),
                BooleanType::NE => ColumnValue::Boolean(BooleanValue {
                    value: l.value != r.value,
                }),
                BooleanType::GT => ColumnValue::Boolean(BooleanValue {
                    value: l.value > r.value,
                }),
                BooleanType::GE => ColumnValue::Boolean(BooleanValue {
                    value: l.value >= r.value,
                }),
                BooleanType::LT => ColumnValue::Boolean(BooleanValue {
                    value: l.value < r.value,
                }),
                BooleanType::LE => ColumnValue::Boolean(BooleanValue {
                    value: l.value <= r.value,
                }),
                _ => panic!("Only supported boolean operations for integers are (EQ, NE, GT, GE, LT, LE)"),
            },
            (ColumnValue::Decimal(l), ColumnValue::Decimal(r)) => match self.typ {
                BooleanType::EQ => ColumnValue::Boolean(BooleanValue {
                    value: l.value == r.value,
                }),
                BooleanType::NE => ColumnValue::Boolean(BooleanValue {
                    value: l.value != r.value,
                }),
                BooleanType::GT => ColumnValue::Boolean(BooleanValue {
                    value: l.value > r.value,
                }),
                BooleanType::GE => ColumnValue::Boolean(BooleanValue {
                    value: l.value >= r.value,
                }),
                BooleanType::LT => ColumnValue::Boolean(BooleanValue {
                    value: l.value < r.value,
                }),
                BooleanType::LE => ColumnValue::Boolean(BooleanValue {
                    value: l.value <= r.value,
                }),
                _ => panic!("Only supported boolean operations for decimals are (EQ, NE, GT, GE, LT, LE)"),
            },
            _ => panic!("Operands types combinations are not supported"),
        }
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
}

#[derive(Clone)]
pub enum JoinSide {
    Left,
    Right,
}

#[derive(Clone)]
pub struct ColumnValueExpression {
    pub join_side: JoinSide,
    pub col_index: usize,
    pub return_type: Column,
}

impl Evaluate for ColumnValueExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> ColumnValue {
        tuple.get_value(schema, self.col_index)
    }

    fn evaluate_join(
        &self,
        l_tuple: &Tuple,
        l_schema: &Schema,
        r_tuple: &Tuple,
        r_schema: &Schema,
    ) -> ColumnValue {
        match self.join_side {
            JoinSide::Left => l_tuple.get_value(l_schema, self.col_index),
            JoinSide::Right => r_tuple.get_value(r_schema, self.col_index),
        }
    }

    fn return_type(&self) -> Column {
        self.return_type.clone()
    }
}
