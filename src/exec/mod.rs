use crate::table::{schema::Schema, tuple::Tuple, value::ColumnValue};

// TODO: second
struct PlanNode {
    output_schema: Schema,
    children: Vec<PlanNode>,
}

// TODO: first
pub trait Evaluate {
    fn evaluate(&self, tuple: Tuple, schema: Schema) -> ColumnValue;
}

pub enum Expression {
    Constant(ConstantExpression),
    Arithmetic(ArithmeticExpression),
    ColumnValue(ColumnValueExpression),
}

impl Evaluate for Expression {
    fn evaluate(&self, tuple: Tuple, schema: Schema) -> ColumnValue {
        todo!()
    }
}

pub struct ConstantExpression {
    pub value: ColumnValue,
}

impl Evaluate for ConstantExpression {
    fn evaluate(&self, tuple: Tuple, schema: Schema) -> ColumnValue {
        self.value.clone()
    }
}

pub enum ArithmeticType {
    Plus,
    Minus,
}

pub struct ArithmeticExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub typ: ArithmeticType,
}

impl Evaluate for ArithmeticExpression {
    fn evaluate(&self, tuple: Tuple, schema: Schema) -> ColumnValue {
        todo!()
    }
}

pub struct ColumnValueExpression {
    // TODO
}
