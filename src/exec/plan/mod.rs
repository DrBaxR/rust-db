use filter::FilterNode;
use projection::ProjectionPlanNode;
use seq_scan::SeqScanPlanNode;
use values::ValuesPlanNode;

use crate::table::schema::Schema;

pub mod values;
pub mod projection;
pub mod filter;
pub mod seq_scan;

/// Interface (probably) mainly used by the planner to generate the query execution plan. The executors will
/// probably use the interface provided by the specific plan node implementation.
pub trait AbstractPlanNode {
    fn get_children(&self) -> Vec<&PlanNode>;
    fn get_output_schema(&self) -> &Schema;
}

#[derive(Clone)]
pub enum PlanNode {
    Values(ValuesPlanNode),
    Projection(ProjectionPlanNode),
    Filter(FilterNode),
    SeqScan(SeqScanPlanNode),
}

impl AbstractPlanNode for PlanNode {
    fn get_children(&self) -> Vec<&PlanNode> {
        match self {
            PlanNode::Values(node) => node.get_children(),
            PlanNode::Projection(node) => node.get_children(),
            PlanNode::Filter(node) => node.get_children(),
            PlanNode::SeqScan(node) => node.get_children(),
        }
    }

    fn get_output_schema(&self) -> &Schema {
        match self {
            PlanNode::Values(node) => node.get_output_schema(),
            PlanNode::Projection(node) => node.get_output_schema(),
            PlanNode::Filter(node) => node.get_output_schema(),
            PlanNode::SeqScan(node) => node.get_output_schema(),
        }
    }
}
