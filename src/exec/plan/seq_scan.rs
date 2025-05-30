use crate::{catalog::OID, exec::expression::boolean::BooleanExpression, table::schema::Schema};

use super::AbstractPlanNode;

#[derive(Clone)]
pub struct SeqScanPlanNode {
    pub output_schema: Schema,
    pub table_oid: OID,
    pub table_name: String,
    pub filter_expr: Option<BooleanExpression>,
}

impl AbstractPlanNode for SeqScanPlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> Vec<&super::PlanNode> {
        vec![]
    }
}
