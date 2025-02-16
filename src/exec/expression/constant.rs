use crate::table::{
    schema::{Column, ColumnType, Schema},
    tuple::Tuple,
    value::ColumnValue,
};

use super::Evaluate;

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

    fn to_string(&self) -> String {
        self.value.to_string()
    }
}
