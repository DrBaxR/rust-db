use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

use crate::{
    disk::buffer_pool_manager::BufferPoolManager,
    index::Index,
    table::{schema::Schema, TableHeap},
};

type OID = u32;

type TablesMapping = Mutex<HashMap<OID, Arc<Mutex<TableInfo>>>>;
type TableNamesMapping = Mutex<HashMap<String, OID>>;

type IndexesMapping = Mutex<HashMap<OID, Arc<Mutex<IndexInfo>>>>;
type IndexNamesMapping = Mutex<HashMap<String, HashMap<String, OID>>>;

pub struct Catalog {
    bpm: Arc<BufferPoolManager>,
    next_oid: AtomicU32,
    /// oid -> table info
    tables: TablesMapping,
    /// table name -> oid
    table_names: TableNamesMapping,
    /// oid -> index info
    indexes: IndexesMapping,
    /// table name -> index name -> oid
    index_names: IndexNamesMapping,
}

impl Catalog {
    pub fn new(bpm: Arc<BufferPoolManager>) -> Self {
        Self {
            bpm,
            next_oid: AtomicU32::new(0),
            tables: Mutex::new(HashMap::new()),
            table_names: Mutex::new(HashMap::new()),
            indexes: Mutex::new(HashMap::new()),
            index_names: Mutex::new(HashMap::new()),
        }
    }

    pub fn create_table(
        &mut self,
        name: &str,
        schema: Schema,
    ) -> Result<Arc<Mutex<TableInfo>>, ()> {
        if let Some(_) = self.table_names.lock().unwrap().get(name) {
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

        self.table_names
            .lock()
            .unwrap()
            .insert(name.to_string(), oid);

        self.index_names
            .lock()
            .unwrap()
            .insert(name.to_string(), HashMap::new());

        let mut tables = self.tables.lock().unwrap();
        tables.insert(oid, Arc::new(Mutex::new(table_info)));

        Ok(tables.get(&oid).unwrap().clone())
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<Arc<Mutex<TableInfo>>> {
        match self.table_names.lock().unwrap().get(name) {
            Some(oid) => self.tables.lock().unwrap().get(oid).cloned(),
            None => None,
        }
    }

    pub fn get_table_by_oid(&self, oid: OID) -> Option<Arc<Mutex<TableInfo>>> {
        self.tables.lock().unwrap().get(&oid).cloned()
    }

    pub fn create_index(
        &mut self,
        index_name: &str,
        table_name: &str,
        schema: Schema,
        key_schema: Schema,
        key_attrs: Vec<usize>,
        keysize: usize,
    ) -> Result<Arc<Mutex<IndexInfo>>, ()> {
        // check error cases
        // create index
        // update catalog metadata``
    }

    pub fn get_index_by_name(
        &self,
        index_name: &str,
        table_name: &str,
    ) -> Option<Arc<Mutex<IndexInfo>>> {
        todo!()
    }

    pub fn get_index_by_oid(&self, oid: OID) -> Option<Arc<Mutex<IndexInfo>>> {
        todo!()
    }

    pub fn get_table_indexes(&self, name: &str) -> Vec<Arc<Mutex<IndexInfo>>> {
        todo!()
    }

    pub fn get_table_names(&self) -> Vec<String> {
        self.table_names.lock().unwrap().keys().cloned().collect()
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
