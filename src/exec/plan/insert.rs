use crate::{
    catalog::OID,
    table::schema::{ColumnType, Schema},
};

use super::{AbstractPlanNode, PlanNode};

#[derive(Clone)]
pub struct InsertPlanNode {
    /// Schema is always a single column with one integer representing the number of rows inserted.
    output_schema: Schema,
    pub table_oid: OID,
    pub child: Box<PlanNode>,
}

impl InsertPlanNode {
    pub fn new(table_oid: OID, child: PlanNode) -> Self {
        Self {
            output_schema: Schema::with_types(vec![ColumnType::Integer]),
            table_oid,
            child: Box::new(child),
        }
    }
}

impl AbstractPlanNode for InsertPlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> Vec<&PlanNode> {
        vec![&self.child]
    }
}
