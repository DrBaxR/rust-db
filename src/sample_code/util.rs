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

pub fn populate_heap(table_heap: &mut TableHeap, schema: &Schema) {
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

/// Creates a table with the schema (int, bool, decimal) and populates it with three tuples.
pub fn create_table() -> (ExecutorContext, Schema, OID, String) {
    // TODO: pass db file as param so it can be used in tests
    let schema = Schema::with_types(vec![
        ColumnType::Integer,
        ColumnType::Boolean,
        ColumnType::Decimal,
    ]);

    // init executor context
    let bpm = Arc::new(BufferPoolManager::new("db/test.db".to_string(), 2, 2));
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

    populate_heap(&mut table_info.table, &schema);

    drop(table_info);

    (executor_context, schema, table_oid, table_name)
}
