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
    pub plan: SeqScanPlanNode,
    pub table_info: Arc<Mutex<TableInfo>>,
    /// When `None`, it means that there are no more tuples in the table.
    current_rid: Option<RID>,
    is_first: bool,
}

impl SeqScanExecutor {
    pub fn new(context: ExecutorContext, plan: SeqScanPlanNode) -> Self {
        let table_info = context
            .catalog
            .get_table_by_oid(plan.table_oid)
            .expect("Can't create sequencial scan executor for non-existing table");

        Self {
            plan,
            table_info,
            current_rid: None,
            is_first: true,
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

        // get the next tuple (or first tuple, if this is the first call)
        let tuple = if self.is_first {
            self.is_first = false;
            let first = table_heap.table.get_tuple(&current_rid).unwrap();
            Some((first.0, first.1, current_rid.clone()))
        } else {
            table_heap.table.tuple_after(&current_rid)
        };

        if let Some((next_meta, next_tuple, next_rid)) = tuple {
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
        format!(
            "SeqScan | Schema: {} | Table: {}({}) | Filter: {}",
            self.output_schema().to_string(),
            self.plan.table_name,
            self.plan.table_oid,
            self.plan
                .filter_expr
                .as_ref()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "None".to_string())
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{env::temp_dir, fs::remove_file, sync::Arc};

    use crate::{
        catalog::Catalog,
        disk::buffer_pool_manager::BufferPoolManager,
        exec::{
            executor::{Execute, ExecutorContext},
            plan::seq_scan::SeqScanPlanNode,
        },
        table::{
            page::TupleMeta,
            schema::{ColumnType, Schema},
            tuple::Tuple,
            value::{BooleanValue, ColumnValue, DecimalValue, IntegerValue},
            TableHeap,
        },
    };

    use super::SeqScanExecutor;

    fn populate_heap(table_heap: &mut TableHeap, schema: &Schema) {
        table_heap.insert_tuple(
            TupleMeta {
                ts: 0,
                is_deleted: false,
            },
            Tuple::new(
                vec![
                    ColumnValue::Integer(IntegerValue { value: 1 }),
                    ColumnValue::Boolean(BooleanValue { value: true }),
                    ColumnValue::Decimal(DecimalValue { value: 10.1 }),
                ],
                schema,
            ),
        );
        table_heap.insert_tuple(
            TupleMeta {
                ts: 0,
                is_deleted: false,
            },
            Tuple::new(
                vec![
                    ColumnValue::Integer(IntegerValue { value: 2 }),
                    ColumnValue::Boolean(BooleanValue { value: false }),
                    ColumnValue::Decimal(DecimalValue { value: 20.2 }),
                ],
                schema,
            ),
        );
        table_heap.insert_tuple(
            TupleMeta {
                ts: 0,
                is_deleted: false,
            },
            Tuple::new(
                vec![
                    ColumnValue::Integer(IntegerValue { value: 3 }),
                    ColumnValue::Boolean(BooleanValue { value: false }),
                    ColumnValue::Decimal(DecimalValue { value: 30.3 }),
                ],
                schema,
            ),
        );
    }

    fn seq_scan_executor(
        bpm: Arc<BufferPoolManager>,
        populated: bool,
    ) -> (SeqScanExecutor, Schema) {
        let schema = Schema::with_types(vec![
            ColumnType::Integer,
            ColumnType::Boolean,
            ColumnType::Decimal,
        ]);

        // init executor context
        let catalog = Arc::new(Catalog::new(bpm.clone()));
        let executor_context = ExecutorContext {
            catalog: catalog.clone(),
            bpm: bpm.clone(),
        };

        // create a table
        bpm.new_page(); // this is needed as table heaps assume page with PID 0 is not used
        let table_name = "test_table".to_string();
        let table_oid = executor_context
            .catalog
            .create_table(&table_name, schema.clone())
            .unwrap()
            .lock()
            .unwrap()
            .oid;

        // insert some tuples
        let table_info = executor_context
            .catalog
            .get_table_by_oid(table_oid)
            .unwrap();
        let mut table_info = table_info.lock().unwrap();

        if populated {
            populate_heap(&mut table_info.table, &schema);
        }

        drop(table_info);

        // create a sequential scan executor
        let plan = SeqScanPlanNode {
            output_schema: schema.clone(),
            table_oid,
            table_name,
            filter_expr: None,
        };

        (SeqScanExecutor::new(executor_context, plan), schema)
    }

    #[test]
    fn scan_empty_table() {
        // init
        let db_path = temp_dir().join("seq_scan_scan_empty_table.db");
        let db_file_path = db_path.to_str().unwrap().to_string();
        let bpm = BufferPoolManager::new(db_file_path.clone(), 2, 2);

        // test
        let (mut executor, _) = seq_scan_executor(Arc::new(bpm), false);
        let mut tuples = vec![];

        executor.init();
        while let Some((tuple, _)) = executor.next() {
            tuples.push(tuple);
        }

        assert_eq!(tuples.len(), 0);

        // cleanup
        remove_file(db_path).expect("Couldn't remove test DB file");
    }

    #[test]
    fn scan_populated_table() {
        // init
        let db_path = temp_dir().join("seq_scan_scan_populated_table.db");
        let db_file_path = db_path.to_str().unwrap().to_string();
        let bpm = BufferPoolManager::new(db_file_path.clone(), 2, 2);

        // test
        let (mut executor, schema) = seq_scan_executor(Arc::new(bpm), true);
        let mut tuples = vec![];

        executor.init();
        while let Some((tuple, _)) = executor.next() {
            tuples.push(tuple);
        }

        assert_eq!(tuples.len(), 3);
        assert_eq!(
            tuples[0].get_value(&schema, 0),
            ColumnValue::Integer(IntegerValue { value: 1 })
        );
        assert_eq!(
            tuples[1].get_value(&schema, 0),
            ColumnValue::Integer(IntegerValue { value: 2 })
        );
        assert_eq!(
            tuples[2].get_value(&schema, 0),
            ColumnValue::Integer(IntegerValue { value: 3 })
        );

        assert_eq!(
            tuples[0].get_value(&schema, 1),
            ColumnValue::Boolean(BooleanValue { value: true })
        );
        assert_eq!(
            tuples[1].get_value(&schema, 1),
            ColumnValue::Boolean(BooleanValue { value: false })
        );
        assert_eq!(
            tuples[2].get_value(&schema, 1),
            ColumnValue::Boolean(BooleanValue { value: false })
        );

        assert_eq!(
            tuples[0].get_value(&schema, 2),
            ColumnValue::Decimal(DecimalValue { value: 10.1 })
        );
        assert_eq!(
            tuples[1].get_value(&schema, 2),
            ColumnValue::Decimal(DecimalValue { value: 20.2 })
        );
        assert_eq!(
            tuples[2].get_value(&schema, 2),
            ColumnValue::Decimal(DecimalValue { value: 30.3 })
        );

        // cleanup
        remove_file(db_path).expect("Couldn't remove test DB file");
    }
}
