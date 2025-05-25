use crate::{catalog::OID, exec::expression::Expression, table::schema::Schema};

use super::{AbstractPlanNode, PlanNode};

#[derive(Clone)]
pub struct UpdatePlanNode {
    /// Schema is always a single column with one integer representing the number of rows updated.
    output_schema: Schema,
    pub table_oid: OID,
    pub table_name: String,
    pub expressions: Vec<Expression>,
    pub child: Box<PlanNode>,
}

impl UpdatePlanNode {
    pub fn new() -> Self {
        todo!()
    }
}

impl AbstractPlanNode for UpdatePlanNode {
    fn get_children(&self) -> Vec<&super::PlanNode> {
        todo!()
    }

    fn get_output_schema(&self) -> &crate::table::schema::Schema {
        todo!()
    }
}