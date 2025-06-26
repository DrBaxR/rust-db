use delete::DeletePlanNode;
use filter::FilterNode;
use insert::InsertPlanNode;
use projection::ProjectionPlanNode;
use seq_scan::SeqScanPlanNode;
use update::UpdatePlanNode;
use values::ValuesPlanNode;

use crate::{exec::plan::idx_scan::IdxScanPlanNode, table::schema::Schema};

pub mod delete;
pub mod filter;
pub mod idx_scan;
pub mod insert;
pub mod projection;
pub mod seq_scan;
pub mod update;
pub mod values;

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
    Insert(InsertPlanNode),
    Delete(DeletePlanNode),
    Update(UpdatePlanNode),
    IdxScan(IdxScanPlanNode),
}

impl AbstractPlanNode for PlanNode {
    fn get_children(&self) -> Vec<&PlanNode> {
        match self {
            PlanNode::Values(node) => node.get_children(),
            PlanNode::Projection(node) => node.get_children(),
            PlanNode::Filter(node) => node.get_children(),
            PlanNode::SeqScan(node) => node.get_children(),
            PlanNode::Insert(node) => node.get_children(),
            PlanNode::Delete(node) => node.get_children(),
            PlanNode::Update(node) => node.get_children(),
            PlanNode::IdxScan(node) => node.get_children(),
        }
    }

    fn get_output_schema(&self) -> &Schema {
        match self {
            PlanNode::Values(node) => node.get_output_schema(),
            PlanNode::Projection(node) => node.get_output_schema(),
            PlanNode::Filter(node) => node.get_output_schema(),
            PlanNode::SeqScan(node) => node.get_output_schema(),
            PlanNode::Insert(node) => node.get_output_schema(),
            PlanNode::Delete(node) => node.get_output_schema(),
            PlanNode::Update(node) => node.get_output_schema(),
            PlanNode::IdxScan(node) => node.get_output_schema(),
        }
    }
}
