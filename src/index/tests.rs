use std::{env::temp_dir, fs::remove_file, sync::Arc};

use crate::{
    disk::buffer_pool_manager::BufferPoolManager,
    table::{
        schema::{ColumnType, Schema},
        tuple::{Tuple, RID},
        value::{ColumnValue, IntegerValue},
    },
};

use super::{Index, IndexMeta};

fn get_index(db_file_path: String) -> Index {
    let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
    let meta = IndexMeta::new(
        Schema::with_types(vec![ColumnType::Integer]),
        String::from("id"),
        vec![0],
    );
    Index::new(meta, bpm)
}

fn integer_value(value: i32) -> ColumnValue {
    ColumnValue::Integer(IntegerValue { value })
}

#[test]
fn insert_read() {
    // init
    let db_path = temp_dir().join("index_insert_read.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let index = get_index(db_file_path.clone());

    // test
    let tuple_1 = Tuple::new(vec![integer_value(1)], index.meta().key_schema());
    let tuple_2 = Tuple::new(vec![integer_value(2)], index.meta().key_schema());
    let tuple_3 = Tuple::new(vec![integer_value(3)], index.meta().key_schema());

    index.insert(tuple_1.clone(), RID::new(1, 1)).unwrap();
    index.insert(tuple_2.clone(), RID::new(2, 2)).unwrap();
    index.insert(tuple_3.clone(), RID::new(3, 3)).unwrap();

    assert_eq!(index.scan(tuple_1), vec![RID::new(1, 1)]);
    assert_eq!(index.scan(tuple_2), vec![RID::new(2, 2)]);
    assert_eq!(index.scan(tuple_3), vec![RID::new(3, 3)]);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn insert_delete_read() {
    // init
    let db_path = temp_dir().join("index_insert_delete_read.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let index = get_index(db_file_path.clone());

    // test
    let tuple_1 = Tuple::new(vec![integer_value(1)], index.meta().key_schema());
    let tuple_2 = Tuple::new(vec![integer_value(2)], index.meta().key_schema());

    index.insert(tuple_1.clone(), RID::new(1, 1)).unwrap();
    index.insert(tuple_2.clone(), RID::new(2, 2)).unwrap();
    index.insert(tuple_1.clone(), RID::new(3, 3)).unwrap();
    index.delete(tuple_2.clone());

    assert_eq!(index.scan(tuple_1), vec![RID::new(1, 1), RID::new(3, 3)]);
    assert_eq!(index.scan(tuple_2), vec![]);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}
