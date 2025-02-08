use crate::table::{
    schema::{Column, Schema},
    tuple::Tuple,
    value::ColumnValue,
};

use super::Evaluate;

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

    fn to_string(&self) -> String {
        format!("#{}", self.col_index)
    }
}
