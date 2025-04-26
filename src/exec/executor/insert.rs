use std::sync::Arc;

use crate::{
    catalog::Catalog,
    exec::plan::{insert::InsertPlanNode, AbstractPlanNode},
    table::{
        schema::Schema,
        tuple::{Tuple, RID},
    },
};

use super::{Execute, ExecutorContext};

pub struct InsertExecutor {
    pub plan: InsertPlanNode,
    pub catalog: Arc<Catalog>,
    /// Whether the executor has already inserted the tuples or not.
    inserted: bool,
}

impl InsertExecutor {
    pub fn new(context: ExecutorContext, plan: InsertPlanNode) -> Self {
        Self {
            plan,
            catalog: context.catalog,
            inserted: false,
        }
    }
}

impl Execute for InsertExecutor {
    fn init(&mut self) {
        self.inserted = false;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        // (ASSUMPTION) can assume that input values have same schema as the table where we are inserting
        // insert child tuples into the table
        // update indexes of table if they exist
        todo!()
    }

    fn output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn to_string(&self, indent_level: usize) -> String {
        todo!("string representation for the insert executor")
    }
}
