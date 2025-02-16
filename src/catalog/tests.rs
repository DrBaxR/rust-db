use std::{
    collections::HashMap,
    env::temp_dir,
    fs::remove_file,
    sync::Arc,
};

use crate::{
    disk::buffer_pool_manager::BufferPoolManager,
    table::{
        self,
        page::TupleMeta,
        schema::{ColumnType, Schema},
        tuple::Tuple,
    },
    test_utils,
};

use super::Catalog;

#[test]
fn create_table_and_use() {
    // init
    let db_path = temp_dir().join("catalog_create_table_and_use.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = BufferPoolManager::new(db_file_path.clone(), 2, 2);
    let mut catalog = Catalog::new(Arc::new(bpm));

    // test
    let schema = Schema::with_types(vec![ColumnType::Integer]);
    let table_info = catalog.create_table("test_table", schema.clone()).unwrap();

    let mut table_guard = table_info.lock().unwrap();
    let table = &mut table_guard.table;

    let meta = TupleMeta {
        ts: 0,
        is_deleted: false,
    };
    let tuple = Tuple::new(vec![test_utils::int_value(1)], &schema);
    let tuple_rid = table.insert_tuple(meta, tuple.clone()).unwrap();

    assert_eq!(table.get_tuple(&tuple_rid).unwrap().1, tuple);
    drop(table_guard); // release table info lock, since we'll be getting it from catalog again

    // getting tabe from catalog and reading from that should lead to same result
    let catalog_table_info = catalog
        .get_table_by_oid(table_info.lock().unwrap().oid)
        .unwrap();
    let catalog_table = &mut catalog_table_info.lock().unwrap().table;

    assert_eq!(catalog_table.get_tuple(&tuple_rid).unwrap().1, tuple);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn create_index_and_use() {
    // init
    let db_path = temp_dir().join("catalog_create_index_and_use.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let bpm = BufferPoolManager::new(db_file_path.clone(), 2, 2);
    let mut catalog = Catalog::new(Arc::new(bpm));

    // test
    // insert 0..10 in table
    let table_schema = Schema::with_types(vec![ColumnType::Integer]);
    let table_info = catalog
        .create_table("test_table", table_schema.clone())
        .unwrap();

    let mut table_guard = table_info.lock().unwrap();
    let table = &mut table_guard.table;
    for i in 0..10 {
        let meta = TupleMeta {
            ts: 0,
            is_deleted: false,
        };
        let tuple = Tuple::new(vec![test_utils::int_value(i)], &table_schema);
        table.insert_tuple(meta, tuple).unwrap();
    }

    let table_oid = table_guard.oid;
    drop(table_guard);

    // create index on table
    let key_schema = Schema::with_types(vec![ColumnType::Integer]);
    let index_oid = catalog
        .create_index(
            "test_index",
            "test_table",
            table_schema.clone(),
            key_schema.clone(),
            vec![0],
            4,
        )
        .unwrap()
        .lock()
        .unwrap()
        .oid;

    // query index for all tuples
    let mut key_mappings = HashMap::new(); // val -> rid

    let index_info = catalog.get_index_by_oid(index_oid).unwrap();
    let index_guard = index_info.lock().unwrap();
    let index = &index_guard.index;

    for i in 0..10 {
        let key = Tuple::new(vec![test_utils::int_value(i)], &key_schema);
        let rid = index.scan(key)[0].clone();
        key_mappings.insert(i, rid);
    }
    drop(index_guard);

    // query table by rid
    let table_info = catalog.get_table_by_oid(table_oid).unwrap();
    let table_guard = table_info.lock().unwrap();
    let table = &table_guard.table;

    for (val, rid) in key_mappings.iter() {
        let tuple = table.get_tuple(rid).unwrap().1;
        let tuple_val = match tuple.get_value(&table_schema, 0) {
            table::value::ColumnValue::Integer(integer_value) => integer_value.value,
            _ => panic!("Expected integer value"),
        };

        assert_eq!(tuple_val, *val);
    }

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}
