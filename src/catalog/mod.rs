use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
};

use info::{IndexInfo, TableInfo};

use crate::{
    disk::buffer_pool_manager::BufferPoolManager,
    index::{Index, IndexMeta},
    table::{schema::Schema, tuple::Tuple, TableHeap},
};

pub mod info;
#[cfg(test)]
mod tests;

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

    /// Create a new table in the catalog and return the table info.
    ///
    /// # Errors
    /// Will return `Err` if a table with the same name already exists.
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

    /// Get a table by name.
    pub fn get_table_by_name(&self, name: &str) -> Option<Arc<Mutex<TableInfo>>> {
        match self.table_names.lock().unwrap().get(name) {
            Some(oid) => self.tables.lock().unwrap().get(oid).cloned(),
            None => None,
        }
    }

    /// Get a table by oid.
    pub fn get_table_by_oid(&self, oid: OID) -> Option<Arc<Mutex<TableInfo>>> {
        self.tables.lock().unwrap().get(&oid).cloned()
    }

    /// Create a new index in the catalog and return the index info.
    ///
    /// # Errors
    /// Will return `Err` if a table with `table_name` does not exist **or** if an index with the same name already exists.
    pub fn create_index(
        &mut self,
        index_name: &str,
        table_name: &str,
        schema: Schema,
        key_schema: Schema,
        key_attrs: Vec<usize>,
        key_size: usize,
    ) -> Result<Arc<Mutex<IndexInfo>>, ()> {
        // check if table exists
        let table_oid = if let Some(oid) = self.table_names.lock().unwrap().get(table_name) {
            oid.clone()
        } else {
            return Err(());
        };

        // check if index exists
        let mut index_names = self.index_names.lock().unwrap();
        let names_map = index_names.get_mut(table_name).unwrap();
        if names_map.contains_key(index_name) {
            return Err(());
        }

        // create index
        let index_meta = IndexMeta::new(key_schema, index_name.to_string(), key_attrs);
        let index = Index::new(index_meta, self.bpm.clone());

        // add all tuples in table to index
        let table = self.get_table_by_oid(table_oid).unwrap();
        let table = table.lock().unwrap();

        let tuples = table.table.iter();
        for (_, tuple, rid) in tuples {
            let key = Tuple::from_projection(
                &tuple,
                &schema,
                index.meta().key_schema(),
                index.meta().key_attrs(),
            );

            index
                .insert(key, rid)
                .expect("Can't create index. Too many tuples!");
        }

        // update catalog metadata
        let oid = self.next_oid.fetch_add(1, Ordering::SeqCst);
        let index_info = Arc::new(Mutex::new(IndexInfo {
            name: index_name.to_string(),
            oid,
            index,
            key_size,
        }));

        self.indexes.lock().unwrap().insert(oid, index_info.clone());
        names_map.insert(index_name.to_string(), oid);

        Ok(index_info)
    }

    /// Get an index by name.
    pub fn get_index_by_name(
        &self,
        index_name: &str,
        table_name: &str,
    ) -> Option<Arc<Mutex<IndexInfo>>> {
        // check if table exists
        self.table_names.lock().unwrap().get(table_name)?;

        let index_names = self.index_names.lock().unwrap();
        let index_oid = index_names.get(table_name)?.get(index_name)?;

        self.get_index_by_oid(*index_oid)
    }

    /// Get an index by oid.
    pub fn get_index_by_oid(&self, oid: OID) -> Option<Arc<Mutex<IndexInfo>>> {
        self.indexes.lock().unwrap().get(&oid).cloned()
    }

    /// Get all indexes for a table.
    pub fn get_table_indexes(&self, name: &str) -> Vec<Arc<Mutex<IndexInfo>>> {
        let index_names = self.index_names.lock().unwrap();
        let index_oids = if let Some(names) = index_names.get(name) {
            names
        } else {
            return vec![];
        };

        index_oids
            .values()
            .map(|oid| self.get_index_by_oid(*oid).unwrap().clone())
            .collect()
    }

    /// Get all table names in the catalog.
    pub fn get_table_names(&self) -> Vec<String> {
        self.table_names.lock().unwrap().keys().cloned().collect()
    }
}
