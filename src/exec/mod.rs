use crate::table::schema::Schema;

pub mod expression;

pub trait AbstractPlanNopde {
    fn get_children(&self) -> Vec<PlanNode>;
    fn get_output_schema(&self) -> Schema;
}

pub enum PlanNode {
    Values,
    Projection,
    Filter,
}
