use std::sync::MutexGuard;

use crate::{
    catalog::info::{IndexInfo, TableInfo},
    table::{
        page::TupleMeta, schema::{ColumnType, Schema}, tuple::{Tuple, RID}, value::{ColumnValue, IntegerValue}
    },
};

/// Delete tuple with RID=`rid` from table with `table_info` and all indexes in `index_infos`.
pub fn delete_from_table_and_indexes(
    table_info: &MutexGuard<'_, TableInfo>,
    index_infos: &Vec<MutexGuard<'_, IndexInfo>>,
    rid: &RID,
) -> Tuple {
    let (mut meta, tuple) = table_info
        .table
        .get_tuple(rid)
        .expect(format!("Can't delete tuple that doesn't exist: {:?}", rid).as_str());
    meta.is_deleted = true;
    table_info.table.update_tuple_meta(meta, rid);

    for index_info in index_infos.iter() {
        index_info.index.delete(&tuple, &table_info.schema);
    }

    tuple
}

/// Insert `tuple` in table with `table_info` and indexes in `index_infos`.
pub fn insert_tuple_in_table_and_indexes(
    table_info: &mut MutexGuard<'_, TableInfo>,
    index_infos: &Vec<MutexGuard<'_, IndexInfo>>,
    tuple: Tuple,
) {
    let new_rid = table_info
        .table
        .insert_tuple(
            TupleMeta {
                ts: 0,
                is_deleted: false,
            },
            tuple.clone(),
        )
        .expect("Couldn't insert tuple");

    for index_info in index_infos.iter() {
        index_info
            .index
            .insert(&tuple, &table_info.schema, new_rid.clone())
            .unwrap();
    }
}


/// Create a new `Tuple` with a single integer column containing the given value.
pub fn int_tuple(value: i32) -> Tuple {
    Tuple::new(
        vec![ColumnValue::Integer(IntegerValue { value })],
        &Schema::with_types(vec![ColumnType::Integer]),
    )
}
