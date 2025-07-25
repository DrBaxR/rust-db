use crate::{
    catalog::OID,
    exec::{
        expression::boolean::BooleanExpression,
        plan::{AbstractPlanNode, PlanNode},
    },
    table::schema::Schema,
};

#[derive(Clone)]
pub struct IdxScanPlanNode {
    pub output_schema: Schema,
    pub table_oid: OID,
    pub table_name: String,
    pub filter_expr: BooleanExpression,
}

impl AbstractPlanNode for IdxScanPlanNode {
    fn get_children(&self) -> Vec<&PlanNode> {
        vec![]
    }

    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }
}
