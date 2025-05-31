use crate::{
    exec::{
        expression::Evaluate,
        plan::{projection::ProjectionPlanNode, AbstractPlanNode},
    },
    table::{
        schema::Schema,
        tuple::{Tuple, RID},
    },
};

use super::{Execute, Executor};

pub struct ProjectionExecutor {
    pub plan: ProjectionPlanNode,
    pub child: Box<Executor>,
}

impl Execute for ProjectionExecutor {
    fn init(&mut self) {
        self.child.init();
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        let (in_tuple, rid) = self.child.next()?;
        let in_schema = self.child.output_schema();

        let mut out_tuple_values = vec![];
        for expression in &self.plan.expressions {
            out_tuple_values.push(expression.evaluate(&in_tuple, in_schema));
        }

        Some((Tuple::new(out_tuple_values, self.output_schema()), rid))
    }

    fn output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn to_string(&self, indent_level: usize) -> String {
        let self_string = format!(
            "Projection | Schema: {} | Exprs: [ {} ]",
            self.output_schema().to_string(),
            self.plan
                .expressions
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );

        let tabs = "\t".repeat(indent_level + 1);
        format!("{}\n{}-> {}", self_string, tabs, self.child.to_string(indent_level + 1))
    }
}
