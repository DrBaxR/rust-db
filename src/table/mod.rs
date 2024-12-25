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
    /// `tuples_info[i]` corresponds to `tuples_data[i]`
    tuples_info: Vec<TupleInfo>,
    /// Note that while in memory tuples are in the same order as their infos (while on disk they are in reverse order)
    tuples_data: Vec<Tuple>,
}

impl TablePage {
    fn empty() -> Self {
        Self {
            next_page: 0,
            num_tuples: 0,
            num_deleted_tuples: 0,
            tuples_info: vec![],
            tuples_data: vec![],
        }
    }

    pub fn deserialize(data: &[u8]) -> Self {
        todo!()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut data = vec![0; DB_PAGE_SIZE as usize];
        data[0..4].copy_from_slice(&self.next_page.to_be_bytes());
        data[4..6].copy_from_slice(&self.num_tuples.to_be_bytes());
        data[6..TABLE_PAGE_HEADER_SIZE as usize].copy_from_slice(&self.num_deleted_tuples.to_be_bytes());

        assert_eq!(self.tuples_data.len(), self.tuples_info.len());
        for i in 0..self.tuples_info.len() {
            // serialize meta
            todo!();
            // serialize data
            todo!();
        }

        data
    }

    /// Inserts tuple in the page and returns the slot number of the tuple. Will return `None` in case of error (i.e. no space left).
    pub fn insert_tuple(&mut self, meta: TupleMeta, tuple: Tuple) -> Option<u16> {
        let tuple_offset = self.get_next_tuple_offset(&tuple)?;

        assert_eq!(self.tuples_info.len(), self.tuples_data.len());
        self.tuples_info.push((
            tuple_offset,
            tuple.size() as u16,
            TupleMeta {
                ts: 0,
                is_deleted: false,
            },
        ));
        self.tuples_data.push(tuple);

        Some(self.tuples_data.len() as u16 - 1)
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

    /// Updates a tuple's meta data, returning the RID of the tuple.
    pub fn update_tuple_meta(&mut self, meta: TupleMeta, rid: RID) -> Result<RID, ()> {
        let slot = rid.slot_num as usize;
        let (offset, size, old_meta) = self.tuples_info.get(slot).ok_or(())?;

        if !old_meta.is_deleted && meta.is_deleted {
            self.num_deleted_tuples += 1;
        } else if old_meta.is_deleted && meta.is_deleted {
            self.num_deleted_tuples -= 1;
        }

        self.tuples_info[slot] = (*offset, *size, meta);

        Ok(rid)
    }

    pub fn get_tuple(&self, rid: RID) -> Option<(&TupleMeta, &Tuple)> {
        assert_eq!(self.tuples_data.len(), self.tuples_info.len());
        let slot = rid.slot_num as usize;
        if slot >= self.tuples_data.len() {
            return None;
        }

        Some((&self.tuples_info[slot].2, &self.tuples_data[slot]))
    }
}

#[cfg(test)]
mod tests {
    fn test() {
        todo!()
    }
}