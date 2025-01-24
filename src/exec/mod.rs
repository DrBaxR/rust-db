use expression::Evaluate;
use plan::{AbstractPlanNode, ValuesPlanNode};

use crate::table::{
    schema::{Column, ColumnType, Schema},
    tuple::{Tuple, RID},
    value::{ColumnValue, IntegerValue},
};

pub mod expression;
pub mod plan;

pub trait Executor {
    fn init(&mut self);
    fn next(&mut self) -> Option<(Tuple, RID)>;
    fn output_schema(&self) -> &Schema;
}

pub struct ValuesExecutor {
    pub plan: ValuesPlanNode,
    pub cursor: usize,
}

impl ValuesExecutor {
    fn dummy_schema() -> Schema {
        Schema::new(vec![Column::new_fixed(
            "dummy".to_string(),
            ColumnType::Integer,
        )])
    }

    fn dummy_tuple() -> Tuple {
        Tuple::new(
            vec![ColumnValue::Integer(IntegerValue { value: 1 })],
            &Self::dummy_schema(),
        )
    }
}

impl Executor for ValuesExecutor {
    fn init(&mut self) {
        self.cursor = 0;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.cursor < self.plan.values.len() {
            let values = &self.plan.values[self.cursor]
                .iter()
                .map(|e| e.evaluate(&Self::dummy_tuple(), &Self::dummy_schema())) // values executor won't work with column expressions
                .collect::<Vec<_>>();

            self.cursor += 1;

            Some((
                Tuple::new(values.clone(), self.output_schema()),
                RID::invalid(),
            ))
        } else {
            None
        }
    }

    fn output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

// TODO: tests