use crate::{catalog::OID, exec::expression::boolean::BooleanExpression, table::schema::Schema};

use super::AbstractPlanNode;

pub struct SeqScanPlanNode {
    output_schema: Schema,
    table_oid: OID,
    table_name: String,
    filter_expr: Option<BooleanExpression>,
}

impl AbstractPlanNode for SeqScanPlanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> Vec<&super::PlanNode> {
        vec![]
    }
}
