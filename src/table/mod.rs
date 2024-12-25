use tuple::{Tuple, RID};

use crate::{config::DB_PAGE_SIZE, disk::disk_manager::PageID};

mod schema;
mod tuple;
mod value;

struct TupleMeta {
    ts: u64, // not sure what this is yet, some sort of timestamp
    is_deleted: bool,
}

/// `(offset, size, meta)` of the tuple
type TupleInfo = (u16, u16, TupleMeta);

const TABLE_PAGE_HEADER_SIZE: u16 = 8;
const TUPLE_INFO_SIZE: u16 = 13; // 2 (offset) + 2 (size) + 8 (meta.ts) + 1 (meta.is_deleted)

/// A page that stores tuples. These can be chained together in a linked-list-like structure.
/// 
/// ```text
/// | next_page_id (4) | num_tuples (2) | num_deleted_tuples (2) | ... tuples_info ... | ... free ... | ... tuples_data ... |
///                                                                    page header end ^              ^ page data start
/// ```
/// 
/// In the memory layout presented above, here is what each of the parts mean:
/// - `next_page_id`: The PID of the next page in the linked list
/// - `num_tuples`: The number of tuples stored in this page
/// - `num_deleted_tuples`: The number of deleted tuples in this page
/// - `tuples_data`: A list of serialzed tuples
/// - `tuples_info`: A list of entries where each one of them has this structure:
/// 
/// ```text
/// | tuple_offset (2) | tuple_size (2) | ts (8) | is_deleted (1) |
/// ```
struct TablePage {
    next_page: PageID,
    num_tuples: u16,
    num_deleted_tuples: u16,
    tuples_info: Vec<TupleInfo>,
}

impl TablePage {
    fn empty() -> Self {
        Self {
            next_page: 0,
            num_tuples: 0,
            num_deleted_tuples: 0,
            tuples_info: vec![],
        }
    }

    pub fn deserialize(data: &[u8]) -> Self {
        todo!()
    }

    pub fn serialize(&self) -> Vec<u8> {
        todo!()
    }

    /// Inserts tuple in the page and returns the slot number of the tuple. Will return `None` in case of error (i.e. no space left).
    pub fn insert_tuple(&self, meta: TupleMeta, tuple: Tuple) -> Option<u16> {
        todo!()
    }

    /// Returns the offset of the next (to be inserted) tuple. Will return `None` if `tuple` doesn't fit in the page.
    fn get_next_tuple_offset(&self, tuple: &Tuple) -> Option<u16> {
        let tuple_end = if self.num_tuples > 0 {
            self.tuples_info[self.num_tuples as usize - 1].0
        } else {
            DB_PAGE_SIZE as u16
        };

        let tuple_offset = tuple_end - tuple.size() as u16;

        let header_end = TABLE_PAGE_HEADER_SIZE + (self.num_tuples + 1) * TUPLE_INFO_SIZE;
        if tuple_offset < header_end {
            return None;
        }

        Some(tuple_offset)
    }

    pub fn update_tuple_meta(&self, meta: TupleMeta, rid: RID) {
        todo!()
    }

    pub fn get_tuple(&self, rid: RID) -> Option<(TupleMeta, Tuple)> {
        todo!()
    }
}
