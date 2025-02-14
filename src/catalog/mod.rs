use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use crate::{
    disk::buffer_pool_manager::BufferPoolManager,
    index::Index,
    table::{schema::Schema, TableHeap},
};

type OID = u32;

// TODO: will need to be thread safe
pub struct Catalog {
    bpm: Arc<BufferPoolManager>,
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
    pub fn new(bpm: Arc<BufferPoolManager>) -> Self {
        Self {
            bpm,
            next_oid: AtomicU32::new(0),
            tables: HashMap::new(),
            table_names: HashMap::new(),
            indexes: HashMap::new(),
            index_names: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<&TableInfo, ()> {
        if let Some(_) = self.table_names.get(name) {
            return Err(());
        }

        let oid = self.next_oid.fetch_add(1, Ordering::SeqCst);
        let heap = TableHeap::new(self.bpm.clone());
        let table_info = TableInfo {
            name: name.to_string(),
            oid,
            schema,
            table: heap,
        };

        self.table_names.insert(name.to_string(), oid);
        self.index_names.insert(name.to_string(), HashMap::new());
        self.tables.insert(oid, table_info);

        Ok(self.tables.get(&oid).unwrap())
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<&TableInfo> {
        match self.table_names.get(name) {
            Some(oid) => self.tables.get(oid),
            None => None,
        }
    }

    pub fn get_table_by_oid(&self, oid: OID) -> Option<&TableInfo> {
        self.tables.get(&oid)
    }

    pub fn create_index(
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

    pub fn get_index_by_name(&self, index_name: &str, table_name: &str) -> Option<&IndexInfo> {
        todo!()
    }

    pub fn get_index_by_oid(&self, oid: OID) -> Option<&IndexInfo> {
        todo!()
    }

    pub fn get_table_indexes(&self, name: &str) -> Vec<&IndexInfo> {
        todo!()
    }

    pub fn get_table_names(&self) -> Vec<String> {
        self.table_names.keys().cloned().collect()
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
