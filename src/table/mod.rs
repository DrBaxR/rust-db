use tuple::{Tuple, RID};

use crate::disk::disk_manager::PageID;

mod schema;
mod tuple;
mod value;

struct TupleMeta {
    ts: u64, // not sure what this is yet, some sort of timestamp
    is_deleted: bool,
}

/// `(offset, size, meta)` of the tuple
type TupleInfo = (u16, u16, TupleMeta);

struct TablePage {
    next_page: PageID,
    num_tuples: u16,
    num_deleted_tuples: u16,
    tuples_info: Vec<TupleInfo>,
}

impl TablePage {
    fn deserialize(data: &[u8]) -> Self {
        todo!()
    }

    fn serialize(&self) -> Vec<u8> {
        todo!()
    }

    /// Inserts tuple in the page and returns the slot number of the tuple. Will return `None` in case of error (i.e. no space left).
    fn insert_tuple(&self, meta: TupleMeta, tuple: Tuple) -> Option<u16> {
        todo!()
    }

    fn update_tuple_meta(&self, meta: TupleMeta, rid: RID) {
        todo!()
    }

    fn get_tuple(&self, rid: RID) -> Option<(TupleMeta, Tuple)> {
        todo!()
    }
}
