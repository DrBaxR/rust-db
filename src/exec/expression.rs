use crate::table::{
    schema::{Column, ColumnType, Schema},
    tuple::Tuple,
    value::{ColumnValue, IntegerValue},
};

pub trait Evaluate {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> ColumnValue;
    fn evaluate_join(
        &self,
        l_tuple: &Tuple,
        l_schema: &Schema,
        r_tuple: &Tuple,
        r_schema: &Schema,
    ) -> ColumnValue;
    fn return_type(&self) -> Column;
}

pub enum Expression {
    Constant(ConstantExpression),
    Arithmetic(ArithmeticExpression),
    ColumnValue(ColumnValueExpression),
}

impl Evaluate for Expression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> ColumnValue {
        match self {
            Expression::Constant(expr) => expr.evaluate(tuple, schema),
            Expression::Arithmetic(expr) => expr.evaluate(tuple, schema),
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
            Expression::ColumnValue(expr) => {
                expr.evaluate_join(l_tuple, l_schema, r_tuple, r_schema)
            }
        }
    }

    fn return_type(&self) -> Column {
        match self {
            Expression::Constant(expr) => expr.return_type(),
            Expression::Arithmetic(expr) => expr.return_type(),
            Expression::ColumnValue(expr) => expr.return_type(),
        }
    }
}

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
            ColumnType::Varchar(len) => Column::new_varchar("_const_".to_string(), len),
            typ => Column::new_fixed("_const_".to_string(), typ),
        }
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
        Column::new_fixed("_result_".to_string(), ColumnType::Integer)
    }
}

pub enum JoinSide {
    Left,
    Right,
}

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

#[cfg(test)]
mod tests {
    use crate::table::value::DecimalValue;

    use super::*;

    #[test]
    fn constant_expression() {
        let schema = Schema::new(vec![Column::new_fixed(
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
            Column::new_fixed("col1".to_string(), ColumnType::Integer),
            Column::new_fixed("col2".to_string(), ColumnType::Integer),
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
            return_type: Column::new_fixed("col1".to_string(), ColumnType::Integer),
        };
        assert_eq!(
            expr.evaluate(&tuple, &schema),
            ColumnValue::Integer(IntegerValue { value: 10 })
        );

        let expr = ColumnValueExpression {
            join_side: JoinSide::Left,
            col_index: 1,
            return_type: Column::new_fixed("col2".to_string(), ColumnType::Integer),
        };
        assert_eq!(
            expr.evaluate(&tuple, &schema),
            ColumnValue::Integer(IntegerValue { value: 20 })
        );
    }

    #[test]
    fn arithmetic_expression() {
        let schema = Schema::new(vec![
            Column::new_fixed("col1".to_string(), ColumnType::Integer),
            Column::new_fixed("col2".to_string(), ColumnType::Integer),
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
                return_type: Column::new_fixed("col1".to_string(), ColumnType::Integer),
            })),
            right: Box::new(Expression::ColumnValue(ColumnValueExpression {
                join_side: JoinSide::Left,
                col_index: 1,
                return_type: Column::new_fixed("col2".to_string(), ColumnType::Integer),
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
                return_type: Column::new_fixed("col1".to_string(), ColumnType::Integer),
            })),
            right: Box::new(Expression::ColumnValue(ColumnValueExpression {
                join_side: JoinSide::Left,
                col_index: 1,
                return_type: Column::new_fixed("col2".to_string(), ColumnType::Integer),
            })),
            typ: ArithmeticType::Minus,
        };
        assert_eq!(
            expr.evaluate(&tuple, &schema),
            ColumnValue::Integer(IntegerValue { value: -10 })
        );
    }
}
