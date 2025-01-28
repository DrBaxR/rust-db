use crate::{exec::expression::BooleanExpression, table::schema::Schema};

use super::{AbstractPlanNode, PlanNode};

#[derive(Clone)]
pub struct FilterNode {
    pub output_schema: Schema,
    pub predicate: BooleanExpression,
    pub child: Box<PlanNode>,
}

impl AbstractPlanNode for FilterNode {
    fn get_children(&self) -> Vec<&PlanNode> {
        vec![&self.child]
    }

    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }
}
