use crate::{
    index::Index,
    table::{schema::Schema, TableHeap},
};

use super::OID;

pub struct TableInfo {
    pub name: String,
    pub oid: OID,
    pub schema: Schema,
    pub table: TableHeap,
}

pub struct IndexInfo {
    pub name: String,
    pub oid: OID,
    pub index: Index,
    pub key_size: usize,
}
