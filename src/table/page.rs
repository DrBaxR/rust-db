use crate::{config::DB_PAGE_SIZE, disk::disk_manager::PageID, index::serial::{Deserialize, Serialize}};

use super::{
    tuple::{Tuple, RID},
    END_PAGE_ID,
};

#[derive(Debug, PartialEq, Clone)]
pub struct TupleMeta {
    pub ts: u64, // not sure what this is yet, some sort of timestamp
    pub is_deleted: bool,
}

/// `(offset, size, meta)` of the tuple
pub type TupleInfo = (u16, u16, TupleMeta);

const TABLE_PAGE_HEADER_SIZE: u16 = 8;
const TUPLE_INFO_SIZE: u16 = 13; // 2 (offset) + 2 (size) + 8 (meta.ts) + 1 (meta.is_deleted)
const MAX_TUPLE_SIZE: u16 = DB_PAGE_SIZE as u16 - TABLE_PAGE_HEADER_SIZE - TUPLE_INFO_SIZE;

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
#[derive(Debug, PartialEq)]
pub struct TablePage {
    pub next_page: PageID,
    num_tuples: u16,
    num_deleted_tuples: u16,
    /// `tuples_info[i]` corresponds to `tuples_data[i]`
    tuples_info: Vec<TupleInfo>,
    /// Note that while in memory tuples are in the same order as their infos (while on disk they are in reverse order)
    tuples_data: Vec<Tuple>,
}

impl TablePage {
    pub fn empty() -> Self {
        Self {
            next_page: END_PAGE_ID,
            num_tuples: 0,
            num_deleted_tuples: 0,
            tuples_info: vec![],
            tuples_data: vec![],
        }
    }

    pub fn deserialize(data: &[u8]) -> Self {
        assert_eq!(data.len(), DB_PAGE_SIZE as usize);
        let next_page = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let num_tuples = u16::from_be_bytes(data[4..6].try_into().unwrap());
        let num_deleted_tuples =
            u16::from_be_bytes(data[6..TABLE_PAGE_HEADER_SIZE as usize].try_into().unwrap());

        let mut tuples_info = vec![];
        let mut tuples_data = vec![];
        for i in 0..num_tuples as usize {
            // info
            let info_start = TABLE_PAGE_HEADER_SIZE as usize + i * TUPLE_INFO_SIZE as usize;
            let offset = u16::from_be_bytes(data[info_start..info_start + 2].try_into().unwrap());
            let size = u16::from_be_bytes(data[info_start + 2..info_start + 4].try_into().unwrap());
            let meta = TupleMeta {
                ts: u64::from_be_bytes(data[info_start + 4..info_start + 12].try_into().unwrap()),
                is_deleted: if data[info_start + 12] == 1 {
                    true
                } else {
                    false
                },
            };
            let tuple_info = (offset, size, meta);
            tuples_info.push(tuple_info);

            // data
            let tuple_data = Tuple::deserialize(&data[offset as usize..(offset + size) as usize]);
            tuples_data.push(tuple_data);
        }

        Self {
            next_page,
            num_tuples,
            num_deleted_tuples,
            tuples_info,
            tuples_data,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut data = vec![0; DB_PAGE_SIZE as usize];
        data[0..4].copy_from_slice(&self.next_page.to_be_bytes());
        data[4..6].copy_from_slice(&self.num_tuples.to_be_bytes());
        data[6..TABLE_PAGE_HEADER_SIZE as usize]
            .copy_from_slice(&self.num_deleted_tuples.to_be_bytes());

        assert_eq!(self.tuples_data.len(), self.tuples_info.len());
        for i in 0..self.tuples_info.len() {
            // serialize info
            let (offset, size, meta) = &self.tuples_info[i];

            let mut serialized_info = offset.to_be_bytes().to_vec();
            serialized_info.append(&mut size.to_be_bytes().to_vec());
            serialized_info.append(&mut meta.ts.to_be_bytes().to_vec());
            serialized_info.append(&mut vec![if meta.is_deleted { 1u8 } else { 0u8 }]);
            assert_eq!(serialized_info.len() as u16, TUPLE_INFO_SIZE);

            let info_start = TABLE_PAGE_HEADER_SIZE + i as u16 * TUPLE_INFO_SIZE;
            let info_end = info_start + TUPLE_INFO_SIZE;
            data[info_start as usize..info_end as usize].copy_from_slice(&serialized_info);

            // serialize data
            assert_eq!(*size as usize, self.tuples_data[i as usize].size());
            let tuple_end = *offset as usize + *size as usize;
            data[*offset as usize..tuple_end]
                .copy_from_slice(&self.tuples_data[i as usize].serialize());
        }

        data
    }

    /// Inserts tuple in the page and returns the slot number of the tuple. Will return `None` in case of error (i.e. no space left).
    pub fn insert_tuple(&mut self, meta: TupleMeta, tuple: Tuple) -> Option<u16> {
        let tuple_offset = self.get_next_tuple_offset(&tuple)?;

        assert_eq!(self.tuples_info.len(), self.tuples_data.len());
        self.tuples_info
            .push((tuple_offset, tuple.size() as u16, meta));
        self.tuples_data.push(tuple);
        self.num_tuples = self.tuples_data.len() as u16;

        Some(self.tuples_data.len() as u16 - 1)
    }

    /// Returns the offset of the next (to be inserted) tuple. Will return `None` if `tuple` doesn't fit in the page.
    fn get_next_tuple_offset(&self, tuple: &Tuple) -> Option<u16> {
        if tuple.size() > MAX_TUPLE_SIZE as usize {
            return None;
        }

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
    pub fn update_tuple_meta(&mut self, meta: TupleMeta, rid: &RID) -> Result<RID, ()> {
        let slot = rid.slot_num as usize;
        let (offset, size, old_meta) = self.tuples_info.get(slot).ok_or(())?;

        if !old_meta.is_deleted && meta.is_deleted {
            self.num_deleted_tuples += 1;
        } else if old_meta.is_deleted && meta.is_deleted {
            self.num_deleted_tuples -= 1;
        }

        self.tuples_info[slot] = (*offset, *size, meta);

        Ok((*rid).clone())
    }

    pub fn get_tuple(&self, rid: &RID) -> Option<(&TupleMeta, &Tuple)> {
        assert_eq!(self.tuples_data.len(), self.tuples_info.len());
        let slot = rid.slot_num as usize;
        if slot >= self.tuples_data.len() {
            return None;
        }

        Some((&self.tuples_info[slot].2, &self.tuples_data[slot]))
    }

    pub fn get_tuples(&self) -> Vec<(&TupleMeta, &Tuple)> {
        assert_eq!(self.tuples_data.len(), self.tuples_info.len());

        self.tuples_info
            .iter()
            .map(|(_, _, meta)| meta)
            .zip(self.tuples_data.iter())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::table::{
        page::MAX_TUPLE_SIZE,
        schema::{Column, ColumnType, Schema},
        tuple::{Tuple, RID},
        value::{BooleanValue, ColumnValue, VarcharValue},
    };

    use super::{TablePage, TupleMeta};

    fn get_simple_tuple() -> Tuple {
        Tuple::new(
            vec![ColumnValue::Boolean(BooleanValue { value: true })],
            &Schema::new(vec![Column::new_named(
                "bool".to_string(),
                ColumnType::Boolean,
            )]),
        )
    }

    fn get_varchar_tuple(len: usize) -> Tuple {
        Tuple::new(
            vec![ColumnValue::Varchar(VarcharValue {
                value: "hi :)".to_string(),
                length: len,
            })],
            &Schema::new(vec![Column::new_named("big".to_string(), ColumnType::Varchar(len))]),
        )
    }

    #[test]
    fn serialization_consistency() {
        let tuple = get_simple_tuple();
        let page = TablePage {
            next_page: 12,
            num_tuples: 2,
            num_deleted_tuples: 1,
            tuples_info: vec![
                (
                    4091,
                    5,
                    TupleMeta {
                        ts: 123,
                        is_deleted: true,
                    },
                ),
                (
                    4086,
                    5,
                    TupleMeta {
                        ts: 456,
                        is_deleted: false,
                    },
                ),
            ],
            tuples_data: vec![tuple.clone(), tuple.clone()],
        };

        let deserialized = TablePage::deserialize(&page.serialize());
        assert_eq!(page, deserialized);
    }

    #[test]
    fn insert_tuple() {
        let mut page = TablePage::empty();
        let meta = TupleMeta {
            ts: 0,
            is_deleted: false,
        };
        let tuple = get_simple_tuple();
        let slot = page.insert_tuple(meta.clone(), tuple.clone()).unwrap();

        let (page_meta, page_tuple) = page.get_tuple(&RID::new(0, slot)).unwrap();
        assert_eq!(page_meta.clone(), meta);
        assert_eq!(page_tuple.clone(), tuple);
    }

    #[test]
    fn serialization_with_insert() {
        let mut page = TablePage::empty();
        let meta = TupleMeta {
            ts: 0,
            is_deleted: false,
        };
        let tuple = get_simple_tuple();
        let _ = page.insert_tuple(meta.clone(), tuple.clone()).unwrap();

        let deserialized = TablePage::deserialize(&page.serialize());
        assert_eq!(page, deserialized);
    }

    #[test]
    fn insert_tuple_overflow() {
        let mut page = TablePage::empty();
        let meta = TupleMeta {
            ts: 0,
            is_deleted: false,
        };

        // 8 = 4 (length of the string) + 4 (length of the tuple)
        assert!(page
            .insert_tuple(meta.clone(), get_varchar_tuple(MAX_TUPLE_SIZE as usize - 8))
            .is_some());
        assert!(page
            .insert_tuple(meta.clone(), get_varchar_tuple(MAX_TUPLE_SIZE as usize - 7))
            .is_none());
    }

    #[test]
    fn update_tuple_meta() {
        let mut page = TablePage::empty();
        let meta = TupleMeta {
            ts: 0,
            is_deleted: false,
        };
        let tuple = get_simple_tuple();
        let slot = page.insert_tuple(meta.clone(), tuple.clone()).unwrap();
        let rid = RID::new(0, slot);
        let _ = page.update_tuple_meta(
            TupleMeta {
                ts: 1,
                is_deleted: true,
            },
            &rid,
        );

        let (page_meta, _) = page.get_tuple(&rid).unwrap();
        assert_eq!(
            page_meta.clone(),
            TupleMeta {
                ts: 1,
                is_deleted: true
            }
        );
    }
}
