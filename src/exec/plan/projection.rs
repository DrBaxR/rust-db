use crate::{exec::expression::Expression, table::schema::Schema};

use super::{AbstractPlanNode, PlanNode};

#[derive(Clone)]
pub struct ProjectionPlanNode {
    pub output_schema: Schema,
    pub expressions: Vec<Expression>,
    pub child: Box<PlanNode>,
}

impl AbstractPlanNode for ProjectionPlanNode {
    fn get_children(&self) -> Vec<&PlanNode> {
        vec![&self.child]
    }

    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }
}
