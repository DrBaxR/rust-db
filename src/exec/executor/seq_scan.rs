use std::sync::{Arc, Mutex};

use crate::{
    catalog::info::TableInfo,
    exec::{expression::Evaluate, plan::seq_scan::SeqScanPlanNode},
    table::{
        schema::Schema,
        tuple::{Tuple, RID},
        value::ColumnValue,
    },
};

use super::{Execute, ExecutorContext};

pub struct SeqScanExecutor {
    plan: SeqScanPlanNode,
    table_info: Arc<Mutex<TableInfo>>,
    /// When `None`, it means that there are no more tuples in the table.
    current_rid: Option<RID>,
}

impl SeqScanExecutor {
    fn new(context: ExecutorContext, plan: SeqScanPlanNode) -> Self {
        let table_info = context
            .catalog
            .get_table_by_oid(plan.table_oid)
            .expect("Can't create sequencial scan executor for non-existing table");

        Self {
            plan,
            table_info,
            current_rid: None,
        }
    }
}

impl Execute for SeqScanExecutor {
    fn init(&mut self) {
        let first = self.table_info.lock().unwrap().table.first_tuple();

        self.current_rid = match first {
            Some((meta, _, rid)) => {
                if meta.is_deleted {
                    self.next();
                }
                Some(rid)
            }
            None => None,
        }
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        let table_heap = self.table_info.lock().unwrap();
        let current_rid = self.current_rid.clone()?;

        if let Some((next_meta, next_tuple, next_rid)) = table_heap.table.tuple_after(current_rid) {
            self.current_rid = Some(next_rid.clone());

            // filter out deleted tuples
            if next_meta.is_deleted {
                drop(table_heap);
                return self.next();
            }

            // filter out tuples that don't match the predicate
            if let Some(predicate) = &self.plan.filter_expr {
                let filter_result = if let ColumnValue::Boolean(value) =
                    predicate.evaluate(&next_tuple, &table_heap.schema)
                {
                    value.value
                } else {
                    panic!("Filter predicate did not evaluate to a boolean value");
                };

                if !filter_result {
                    drop(table_heap);
                    return self.next();
                }
            }

            Some((next_tuple, next_rid))
        } else {
            self.current_rid = None;
            None
        }
    }

    fn output_schema(&self) -> &Schema {
        &self.plan.output_schema
    }

    fn to_string(&self, indent_level: usize) -> String {
        todo!()
    }
}
