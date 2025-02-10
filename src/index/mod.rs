use std::sync::Arc;

use disk_extendible_hash_table::DiskExtendibleHashTable;

use crate::{
    config::{DB_EHT_DIRECTORY_MAX_DEPTH, DB_EHT_HEADER_MAX_DEPTH},
    disk::buffer_pool_manager::BufferPoolManager,
    table::{
        schema::Schema,
        tuple::{Tuple, RID},
    },
};

#[cfg(test)]
mod tests;

pub mod bucket_page;
pub mod directory_page;
pub mod header_page;
pub mod serial;

pub mod disk_extendible_hash_table;

/// Returns the `count` most significant bits of `input`. If value is greater than or equal with `32`, will return `input`.
fn get_msb(input: u32, count: usize) -> u32 {
    if count == 0 {
        return 0;
    }

    let offset = if count > 32 { 32 } else { count };

    input >> (32 - offset)
}

/// Treads `data` as an array of groups of 4 bytes and returns the group that has the index `group_index`.
///
/// # Panics
/// Will panic if trying to index outside of the length of `data`, or if accessing one of the bytes would cause that (e.g. `data.len() == 7`
/// and calling function with `group_index == 1` => will try to access `data[7]`).
fn get_four_bytes_group(data: &[u8], group_index: usize) -> [u8; 4] {
    [
        data[group_index * 4],
        data[group_index * 4 + 1],
        data[group_index * 4 + 2],
        data[group_index * 4 + 3],
    ]
}

pub struct IndexMeta {
    key_schema: Schema,
    index_name: String,
    /// Mapping between the index key attributes and the schema attributes.
    ///
    /// # Example
    /// For example, if the schema has the attributes `["a", "b", "c"]` and the index key is `["b", "a"]`, then the `pub key_attrs` will be `[1, 0]`.
    key_attrs: Vec<usize>,
}

impl IndexMeta {
    pub fn new(key_schema: Schema, index_name: String, key_attrs: Vec<usize>) -> Self {
        Self {
            key_schema,
            index_name,
            key_attrs,
        }
    }

    pub fn key_schema(&self) -> &Schema {
        &self.key_schema
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn key_attrs(&self) -> &[usize] {
        &self.key_attrs
    }
}

pub struct Index {
    meta: IndexMeta,
    deht: DiskExtendibleHashTable<Tuple, RID>,
}

impl Index {
    pub fn new(meta: IndexMeta, bpm: Arc<BufferPoolManager>) -> Self {
        Self {
            deht: DiskExtendibleHashTable::new(
                bpm,
                DB_EHT_HEADER_MAX_DEPTH as u32,
                DB_EHT_DIRECTORY_MAX_DEPTH as u32,
                meta.index_name().to_string(),
            ),
            meta,
        }
    }

    pub fn meta(&self) -> &IndexMeta {
        &self.meta
    }

    fn insert(&self, key: Tuple, rid: RID) -> Result<(), ()> {
        let key_size = key.size() as u32;
        let value_size = RID::size() as u32;

        self.deht.insert(key, rid, key_size, value_size)
    }

    fn delete(&self, key: Tuple) {
        self.deht.remove(key);
    }

    fn scan(&self, key: Tuple) -> Vec<RID> {
        self.deht.lookup(key)
    }
}
