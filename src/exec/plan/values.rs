use crate::{exec::expression::Expression, table::schema::Schema};

use super::{AbstractPlanNode, PlanNode};

#[derive(Clone)]
pub struct ValuesPlanNode {
    pub output_schema: Schema,
    pub values: Vec<Vec<Expression>>,
}

impl AbstractPlanNode for ValuesPlanNode {
    fn get_children(&self) -> Vec<&PlanNode> {
        vec![]
    }

    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }
}
