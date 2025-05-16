use crate::{
    catalog::OID,
    table::schema::{ColumnType, Schema},
};

use super::{AbstractPlanNode, PlanNode};

#[derive(Clone)]
pub struct DeletePlanNode {
    /// Schema is always a single column with one integer representing the number of rows deleted.
    output_schema: Schema,
    pub table_oid: OID,
    pub table_name: String,
    pub child: Box<PlanNode>,
}

impl DeletePlanNode {
    pub fn new(table_oid: OID, table_name: String, child: PlanNode) -> Self {
        Self {
            output_schema: Schema::with_types(vec![ColumnType::Integer]),
            table_oid,
            table_name,
            child: Box::new(child),
        }
    }
}

impl AbstractPlanNode for DeletePlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> Vec<&PlanNode> {
        vec![&self.child]
    }
}
