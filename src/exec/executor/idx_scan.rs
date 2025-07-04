use std::sync::{Arc, Mutex};

use crate::{
    catalog::info::{IndexInfo, TableInfo},
    exec::{
        executor::{Execute, ExecutorContext},
        expression::{boolean::BooleanType, Evaluate, Expression},
        plan::{idx_scan::IdxScanPlanNode, AbstractPlanNode},
    },
    table::{
        schema::Schema,
        tuple::{Tuple, RID},
    },
};

pub struct IdxScanExecutor {
    pub plan: IdxScanPlanNode,
    pub index: Arc<Mutex<IndexInfo>>,
    pub table: Arc<Mutex<TableInfo>>,
    results: Vec<RID>,
    current: usize,
}

impl IdxScanExecutor {
    /// Creates a new `IdxScanExecutor`.
    ///
    /// # Panics
    /// Will panic in case the expression passed is **not** an `EQ` that has its left operand be a column
    /// expression.
    pub fn new(context: ExecutorContext, plan: IdxScanPlanNode) -> Self {
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
            table: context
                .catalog
                .get_table_by_oid(plan.table_oid)
                .expect("No table with given OID"),
            plan,
            results: vec![],
            current: 0,
        }
    }
}

impl Execute for IdxScanExecutor {
    fn init(&mut self) {
        let right_val = self
            .plan
            .filter_expr
            .right
            .evaluate(&Tuple::empty(), &Schema::with_types(vec![])); // evaluate should be an expression that doesn't depend on specific tuple

        let key = Tuple::new(
            vec![right_val.clone()],
            &Schema::with_types(vec![right_val.typ()]),
        );

        self.results = self.index.lock().unwrap().index.scan(key);
        self.current = 0;
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        if self.current >= self.results.len() {
            return None;
        }

        let current_rid = self.results[self.current].clone();
        let (_, current_tuple) = self
            .table
            .lock()
            .unwrap()
            .table
            .get_tuple(&current_rid)
            .expect("Invalid RID from index");

        self.current += 1;
        Some((current_tuple, current_rid))
    }

    fn output_schema(&self) -> &Schema {
        &self.plan.get_output_schema()
    }

    fn to_string(&self, indent_level: usize) -> String {
        todo!()
    }
}
