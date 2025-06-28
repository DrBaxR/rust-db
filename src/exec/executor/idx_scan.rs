use std::sync::{Arc, Mutex};

use crate::{
    catalog::info::IndexInfo,
    exec::{
        executor::{Execute, Executor, ExecutorContext},
        expression::{
            boolean::{BooleanExpression, BooleanType},
            Expression,
        },
        plan::idx_scan::IdxScanPlanNode,
    },
};

pub struct IdxScanExecutor {
    pub plan: IdxScanPlanNode,
    pub index: Arc<Mutex<IndexInfo>>,
    pub child: Box<Executor>,
}

impl IdxScanExecutor {
    // TODO: move into catalog and write tests for this
    /// Get index for table with `table_name` that is for matches `expression` (if it exists). In there is
    /// no index matching this criteria, `None` is returned.
    ///
    /// # Panics
    /// Will panic in case the expression passed is **not** an `EQ` that has its left operand be a column
    /// expression.
    fn get_index_for_expression(
        context: ExecutorContext,
        expression: &BooleanExpression,
        table_name: &str,
    ) -> Option<Arc<Mutex<IndexInfo>>> {
        // validate expression
        assert_eq!(expression.typ, BooleanType::EQ);
        let left_operand = match expression.left.as_ref() {
            Expression::ColumnValue(col_val_expression) => col_val_expression,
            _ => panic!("Left operand must be a column value expression"),
        };

        // search for index
        let indexes = context.catalog.get_table_indexes(table_name);
        if indexes.is_empty() {
            return None;
        }

        for index in indexes.into_iter() {
            let index_guard = index.lock().unwrap();

            let meta = index_guard.index.meta();
            if meta.key_attrs().len() == 1 && meta.key_attrs()[0] == left_operand.col_index {
                drop(index_guard);
                return Some(index);
            }
        }

        return None;
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
