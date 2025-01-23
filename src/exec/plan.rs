use crate::table::schema::Schema;

use super::expression::{BooleanExpression, Expression};

/// Interface (probably) mainly used by the planner to generate the query execution plan. The executors will
/// probably use the interface provided by the specific plan node implementation.
pub trait AbstractPlanNode {
    fn get_children(&self) -> Vec<&PlanNode>;
    fn get_output_schema(&self) -> &Schema;
}

pub enum PlanNode {
    Values(ValuesPlanNode),
    Projection(ProjectionPlanNode),
    Filter,
}

struct ValuesPlanNode {
    pub output_schema: Schema,
    pub values: Vec<Expression>,
}

impl AbstractPlanNode for ValuesPlanNode {
    fn get_children(&self) -> Vec<&PlanNode> {
        vec![]
    }

    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

struct ProjectionPlanNode {
    pub output_schema: Schema,
    pub expressions: Vec<Expression>,
    pub child: Box<PlanNode>,
}

impl AbstractPlanNode for ProjectionPlanNode {
    fn get_children(&self) -> Vec<&PlanNode> {
        vec![&self.child]
    }

    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

struct FilterNode {
    pub output_schema: Schema,
    pub predicate: BooleanExpression,
    pub child: Box<PlanNode>,
}

impl AbstractPlanNode for FilterNode {
    fn get_children(&self) -> Vec<&PlanNode> {
        vec![&self.child]
    }

    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }
}
