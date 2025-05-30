use crate::{
    catalog::OID,
    exec::expression::Expression,
    table::schema::{ColumnType, Schema},
};

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
    pub fn new(
        table_oid: OID,
        table_name: String,
        expressions: Vec<Expression>,
        child: PlanNode,
    ) -> Self {
        Self {
            output_schema: Schema::with_types(vec![ColumnType::Integer]),
            table_oid,
            table_name,
            expressions,
            child: Box::new(child),
        }
    }
}

impl AbstractPlanNode for UpdatePlanNode {
    fn get_children(&self) -> Vec<&PlanNode> {
        vec![&self.child]
    }

    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }
}
