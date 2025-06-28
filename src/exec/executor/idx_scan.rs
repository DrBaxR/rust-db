use std::sync::{Arc, Mutex};

use crate::{
    catalog::info::IndexInfo,
    exec::{
        executor::{Execute, Executor, ExecutorContext},
        expression::{boolean::BooleanType, Expression},
        plan::idx_scan::IdxScanPlanNode,
    },
};

pub struct IdxScanExecutor {
    pub plan: IdxScanPlanNode,
    pub index: Arc<Mutex<IndexInfo>>,
    pub child: Box<Executor>,
}

impl IdxScanExecutor {
    /// Creates a new `IdxScanExecutor`.
    ///
    /// # Panics
    /// Will panic in case the expression passed is **not** an `EQ` that has its left operand be a column
    /// expression.
    pub fn new(context: ExecutorContext, plan: IdxScanPlanNode, child: Executor) -> Self {
        assert_eq!(plan.filter_expr.typ, BooleanType::EQ);
        let col_index = match plan.filter_expr.left.as_ref() {
            Expression::ColumnValue(col_val_expression) => col_val_expression.col_index,
            _ => panic!("Left operand must be a column value expression"),
        };

        Self {
            index: context
                .catalog
                .get_table_index_by_column(&plan.table_name, col_index)
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
