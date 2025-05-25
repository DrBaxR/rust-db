use std::sync::Arc;

use crate::{catalog::Catalog, exec::plan::update::UpdatePlanNode};

use super::{Execute, Executor};

pub struct UpdateExecutor {
    pub plan: UpdatePlanNode,
    pub catalog: Arc<Catalog>,
    pub child: Box<Executor>,
    updated: bool,
}

impl Execute for UpdateExecutor {
    fn init(&mut self) {
        todo!()
    }

    fn next(&mut self) -> Option<(crate::table::tuple::Tuple, crate::table::tuple::RID)> {
        todo!()
    }

    fn output_schema(&self) -> &crate::table::schema::Schema {
        todo!()
    }

    fn to_string(&self, indent_level: usize) -> String {
        todo!()
    }
}
