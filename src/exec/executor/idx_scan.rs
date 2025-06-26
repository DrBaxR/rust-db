use std::sync::{Arc, Mutex};

use crate::{
    catalog::info::IndexInfo,
    exec::{
        executor::{Execute, Executor, ExecutorContext},
        expression::{self, boolean::BooleanExpression},
        plan::idx_scan::IdxScanPlanNode,
    },
};

pub struct IdxScanExecutor {
    pub plan: IdxScanPlanNode,
    pub index: Arc<Mutex<IndexInfo>>,
    pub child: Box<Executor>,
}

impl IdxScanExecutor {
    fn get_index_for_expression(
        context: ExecutorContext,
        expression: &BooleanExpression,
        table_name: &str,
    ) -> Option<Arc<Mutex<IndexInfo>>> {
        todo!()
    }

    pub fn new(context: ExecutorContext, plan: IdxScanPlanNode, child: Executor) -> Self {
        Self {
            index: IdxScanExecutor::get_index_for_expression(
                context,
                &plan.filter_expr,
                &plan.table_name,
            )
            .expect("No index matching expression for table"),
            plan,
            child: Box::new(child),
        }
    }
}

impl Execute for IdxScanExecutor {
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