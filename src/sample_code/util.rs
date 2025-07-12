use std::sync::Arc;

use crate::{
    catalog::{Catalog, OID},
    disk::buffer_pool_manager::BufferPoolManager,
    exec::executor::ExecutorContext,
    table::{
        page::TupleMeta,
        schema::{ColumnType, Schema},
        tuple::Tuple,
        value::{BooleanValue, ColumnValue, DecimalValue, IntegerValue},
        TableHeap,
    },
};

/// Populates table with values passed in parameter.
pub fn populate_heap(table_heap: &mut TableHeap, schema: &Schema, values: &[i32]) {
    for (i, val) in values.iter().enumerate() {
        table_heap.insert_tuple(
            TupleMeta {
                ts: 0,
                is_deleted: false,
            },
            Tuple::new(
                vec![
                    ColumnValue::Integer(IntegerValue { value: val.clone() }),
                    ColumnValue::Boolean(BooleanValue { value: i % 2 == 0 }),
                    ColumnValue::Decimal(DecimalValue {
                        value: (*val as f64) * 10.1,
                    }),
                ],
                schema,
            ),
        );
    }
}

/// Creates a table with the schema (int, bool, decimal) and populates it with three tuples.
pub fn create_table(db_file: String) -> (ExecutorContext, Schema, OID, String) {
    create_table_with_values(db_file, &vec![1, 2, 3])
}

/// Creates a table with schema (int, bool, decimal) and populates it with as many tuples as you
/// pass in the `values` parameter, each element in there representing the value of one tuple's first
/// column.
pub fn create_table_with_values(
    db_file: String,
    values: &[i32],
) -> (ExecutorContext, Schema, OID, String) {
    let schema = Schema::with_types(vec![
        ColumnType::Integer,
        ColumnType::Boolean,
        ColumnType::Decimal,
    ]);

    // init executor context
    let bpm = Arc::new(BufferPoolManager::new(db_file, 2, 2));
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

    populate_heap(&mut table_info.table, &schema, values);

    drop(table_info);

    (executor_context, schema, table_oid, table_name)
}
