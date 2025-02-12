use std::{collections::HashMap, sync::atomic::AtomicU32};

use crate::{
    index::Index,
    table::{schema::Schema, TableHeap},
};

type OID = u32;

// TODO: will need to be thread safe
pub struct Catalog {
    next_oid: AtomicU32,
    /// oid -> table info
    tables: HashMap<OID, TableInfo>,
    /// table name -> oid
    table_names: HashMap<String, OID>,
    /// oid -> index info
    indexes: HashMap<OID, IndexInfo>,
    /// table name -> index name -> oid
    index_names: HashMap<String, HashMap<String, OID>>,
}

impl Catalog {
    fn new() -> Self {
        todo!()
    }

    fn create_table(&mut self, name: &str, schema: Schema) -> Result<OID, ()> {
        todo!()
    }

    fn get_table_by_name(&self, name: &str) -> Option<&TableInfo> {
        todo!()
    }

    fn get_table_by_oid(&self, oid: OID) -> Option<&TableInfo> {
        todo!()
    }

    fn create_index(
        &mut self,
        index_name: &str,
        table_name: &str,
        schema: Schema,
        key_schema: Schema,
        key_attrs: Vec<usize>,
        keysize: usize,
    ) -> OID {
        todo!()
    }

    fn get_index_by_name(&self, index_name: &str, table_name: &str) -> Option<&IndexInfo> {
        todo!()
    }

    fn get_index_by_oid(&self, oid: OID) -> Option<&IndexInfo> {
        todo!()
    }

    fn get_table_indexes(&self, name: &str) -> Vec<&IndexInfo> {
        todo!()
    }

    fn get_table_names(&self) -> Vec<String> {
        todo!()
    }
}

pub struct TableInfo {
    name: String,
    oid: OID,
    schema: Schema,
    table: TableHeap,
}

pub struct IndexInfo {
    name: String,
    oid: OID,
    index: Index,
    key_size: usize,
}
