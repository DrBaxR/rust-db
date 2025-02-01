use core::panic;

use crate::{
    exec::{
        expression::Evaluate,
        plan::{filter::FilterNode, AbstractPlanNode},
    },
    table::{
        schema::Schema,
        tuple::{Tuple, RID}, value::ColumnValue,
    },
};

use super::{Execute, Executor};

pub struct FilterExecutor {
    pub plan: FilterNode,
    pub child: Box<Executor>,
}

impl Execute for FilterExecutor {
    fn init(&mut self) {
        self.child.init();
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        loop {
            let (tuple, rid) = self.child.next()?;
            let schema = self.child.output_schema();

            if let ColumnValue::Boolean(val) = self.plan.predicate.evaluate(&tuple, schema) {
                if val.value {
                    return Some((tuple, rid));
                }
            } else {
                panic!("Filter predicate did not evaluate to a boolean value");
            }
        }
    }

    fn output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}
