use std::sync::Arc;

use page::{TablePage, TupleMeta};
use tuple::{Tuple, RID};

use crate::disk::{
    buffer_pool_manager::{BufferPoolManager, DiskRead, DiskWrite},
    disk_manager::PageID,
};

pub mod page;
pub mod schema;
pub mod tuple;
pub mod value;

const END_PAGE_ID: PageID = 0;

/// A table heap is a collection of pages that store tuples. The table heap is a linked list of pages, where each page points to the next page 
/// in the list. The first page is the head of the list, and the last page is the tail of the list.
/// 
/// # Assumptions
/// The table heap assumes that the page with `PageID` 0 will never be used in the context of it.
pub struct TableHeap {
    bpm: Arc<BufferPoolManager>,
    first_page: PageID,
    last_page: PageID,
}

impl TableHeap {
    pub fn new(bpm: Arc<BufferPoolManager>) -> Self {
        let first_page = bpm.new_page();

        let mut page = bpm.get_write_page(first_page);
        page.write(TablePage::empty().serialize());
        drop(page);

        Self {
            bpm,
            first_page,
            last_page: first_page,
        }
    }

    /// Insert a tuple in the table heap (**NOT THREAD SAFE**). Will return the RID of the inserted tuple or `None` if the tuple is too large to fit in a single page.
    pub fn insert_tuple(&mut self, meta: TupleMeta, tuple: Tuple) -> Option<RID> {
        let mut page = self.bpm.get_write_page(self.last_page);
        let mut t_page = TablePage::deserialize(page.read());

        if let Some(slot) = t_page.insert_tuple(meta.clone(), tuple.clone()) {
            page.write(t_page.serialize());
            drop(page);

            return Some(RID {
                page_id: self.last_page,
                slot_num: slot,
            });
        }

        // no space in page, create new one
        let new_pid = self.bpm.new_page();
        let mut new_t_page = TablePage::empty();
        let slot = new_t_page.insert_tuple(meta, tuple)?;

        // update next page pointer in old page
        t_page.next_page = new_pid;
        page.write(t_page.serialize());
        drop(page);
        self.last_page = new_pid;

        // write content of new page
        let mut new_page = self.bpm.get_write_page(new_pid);
        new_page.write(new_t_page.serialize());
        drop(new_page);

        Some(RID {
            page_id: new_pid,
            slot_num: slot,
        })
    }

    pub fn update_tuple_meta(&self, meta: TupleMeta, rid: &RID) {
        let mut page = self.bpm.get_write_page(rid.page_id);
        let mut t_page = TablePage::deserialize(page.read());

        t_page
            .update_tuple_meta(meta, rid)
            .expect("Invalid RID received for updating tuple meta");

        page.write(t_page.serialize());
    }

    pub fn get_tuple(&self, rid: &RID) -> Option<(TupleMeta, Tuple)> {
        let page = self.bpm.get_read_page(rid.page_id);
        let t_page = TablePage::deserialize(page.read());
        let (meta, tuple) = t_page.get_tuple(rid)?;

        Some((meta.clone(), tuple.clone()))
    }

    pub fn sequencial_dump(&self) -> Vec<(TupleMeta, Tuple)> {
        let mut data = vec![];
        let mut current_pid = self.first_page;

        while current_pid != END_PAGE_ID {
            let page = self.bpm.get_read_page(current_pid);
            let t_page = TablePage::deserialize(page.read());

            data.append(
                &mut t_page
                    .get_tuples()
                    .iter()
                    .map(|(m, t)| ((*m).clone(), (*t).clone()))
                    .collect(),
            );

            current_pid = t_page.next_page;
        }

        data
    }

    pub fn iter(&self) -> TableHeapIterator {
        TableHeapIterator::new(self)
    }

    /// Returns the first tuple in the table heap. Returns `None` if the table is empty.
    pub fn first_tuple(&self) -> Option<(TupleMeta, Tuple, RID)> {
        let page = self.bpm.get_read_page(self.first_page);
        let t_page = TablePage::deserialize(page.read());

        let (meta, tuple) = t_page.get_tuples().first()?.clone();

        Some((
            (*meta).clone(),
            (*tuple).clone(),
            RID::new(self.first_page, 0),
        ))
    }

    /// Returns the tuple after the given RID. Returns `None` if if the given RID is the last tuple in the table heap.
    ///
    /// # Panics
    /// Panics if there is no tuple with the given RID in the table.
    pub fn tuple_after(&self, rid: &RID) -> Option<(TupleMeta, Tuple, RID)> {
        let page = self.bpm.get_read_page(rid.page_id);
        let t_page = TablePage::deserialize(page.read());

        let next_rid = RID::new(rid.page_id, rid.slot_num + 1);
        match t_page.get_tuple(&next_rid) {
            Some((meta, tuple)) => Some(((*meta).clone(), (*tuple).clone(), next_rid)),
            None => {
                if t_page.next_page == END_PAGE_ID {
                    None
                } else {
                    let next_pid = t_page.next_page;
                    let next_page = self.bpm.get_read_page(t_page.next_page);
                    let next_t_page = TablePage::deserialize(next_page.read());
                    drop(t_page);

                    let (meta, tuple) = next_t_page.get_tuples().first()?.clone();

                    Some(((*meta).clone(), (*tuple).clone(), RID::new(next_pid, 0)))
                }
            }
        }
    }
}

pub struct TableHeapIterator<'a> {
    heap: &'a TableHeap,
    current_page: PageID,
    current_slot: usize,
}

impl<'a> TableHeapIterator<'a> {
    fn new(heap: &'a TableHeap) -> Self {
        Self {
            heap,
            current_page: heap.first_page,
            current_slot: 0,
        }
    }
}

impl<'a> Iterator for TableHeapIterator<'a> {
    type Item = (TupleMeta, Tuple, RID);

    fn next(&mut self) -> Option<Self::Item> {
        let page = self.heap.bpm.get_read_page(self.current_page);
        let t_page = TablePage::deserialize(page.read());
        let tuples = t_page.get_tuples();

        if self.current_page == self.heap.last_page && self.current_slot >= tuples.len() {
            return None;
        }

        if self.current_slot >= tuples.len() {
            self.current_page = t_page.next_page;
            self.current_slot = 0;
            return self.next();
        }

        let (meta, tuple) = &tuples[self.current_slot];
        let rid = RID::new(self.current_page, self.current_slot as u16);

        self.current_slot += 1;

        Some(((*meta).clone(), (*tuple).clone(), rid))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env::temp_dir, fs::remove_file, sync::Arc};

    use crate::{
        disk::buffer_pool_manager::BufferPoolManager,
        table::{
            page::TupleMeta,
            schema::{Column, ColumnType, Schema},
            tuple::Tuple,
            value::{ColumnValue, SmallIntValue, VarcharValue},
            TableHeap,
        },
    };

    use super::tuple::RID;

    fn simple_schema() -> Schema {
        Schema::new(vec![
            Column::new_named("name".to_string(), ColumnType::Varchar(10)),
            Column::new_named("count".to_string(), ColumnType::SmallInt),
        ])
    }

    fn simple_tuple(name: &str, count: i16, simple_schema: &Schema) -> Tuple {
        Tuple::new(
            vec![
                ColumnValue::Varchar(VarcharValue {
                    value: name.to_string(),
                    length: 10,
                }),
                ColumnValue::SmallInt(SmallIntValue { value: count }),
            ],
            &simple_schema,
        )
    }

    #[test]
    fn insert_different_rids() {
        // init
        let db_path = temp_dir().join("th_insert_different_rids.db");
        let db_file_path = db_path.to_str().unwrap().to_string();
        let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
        let mut table_heap = TableHeap::new(bpm);

        // test
        let simple_schema = simple_schema();
        let rids: Vec<_> = (0..10)
            .map(|i| {
                table_heap.insert_tuple(
                    TupleMeta {
                        ts: 0,
                        is_deleted: false,
                    },
                    simple_tuple(&format!("name {i}"), i, &simple_schema),
                )
            })
            .collect();

        let mut duplicate_rids = false;
        for i in 0..rids.len() {
            for j in i + 1..rids.len() {
                duplicate_rids = duplicate_rids || rids[i] == rids[j];
            }
        }

        // assert all rids are different
        assert!(!duplicate_rids);

        // cleanup
        remove_file(db_path).expect("Couldn't remove test DB file");
    }

    #[test]
    fn inserted_tuples_accessible() {
        // init
        let db_path = temp_dir().join("th_inserted_tuples_accessible.db");
        let db_file_path = db_path.to_str().unwrap().to_string();
        let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
        let mut table_heap = TableHeap::new(bpm);

        // test
        let simple_schema = simple_schema();
        let mut tuples_map = HashMap::new();
        for i in 0..4096 {
            let tuple = simple_tuple(&format!("name {i}"), i, &simple_schema);
            let rid = table_heap
                .insert_tuple(
                    TupleMeta {
                        ts: 0,
                        is_deleted: false,
                    },
                    tuple.clone(),
                )
                .unwrap();

            tuples_map.insert(rid, tuple);
        }

        // assert all tuples inserted can also be read
        for (rid, tuple) in tuples_map.iter() {
            let (_, heap_tuple) = table_heap.get_tuple(rid).unwrap();
            assert_eq!(tuple.clone(), heap_tuple);
        }

        // cleanup
        remove_file(db_path).expect("Couldn't remove test DB file");
    }

    #[test]
    fn update_tuple_meta() {
        // init
        let db_path = temp_dir().join("th_update_tuple_meta.db");
        let db_file_path = db_path.to_str().unwrap().to_string();
        let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
        let mut table_heap = TableHeap::new(bpm);

        // test
        let simple_schema = simple_schema();
        let rid1 = table_heap
            .insert_tuple(
                TupleMeta {
                    ts: 0,
                    is_deleted: false,
                },
                simple_tuple("tuple 1", 1, &simple_schema),
            )
            .unwrap();
        let rid2 = table_heap
            .insert_tuple(
                TupleMeta {
                    ts: 0,
                    is_deleted: false,
                },
                simple_tuple("tuple 2", 2, &simple_schema),
            )
            .unwrap();

        let (meta, _) = table_heap.get_tuple(&rid2).unwrap();
        assert_eq!(
            meta,
            TupleMeta {
                ts: 0,
                is_deleted: false
            }
        );

        table_heap.update_tuple_meta(
            TupleMeta {
                ts: 1,
                is_deleted: true,
            },
            &rid2,
        );
        let (meta, _) = table_heap.get_tuple(&rid2).unwrap();
        assert_eq!(
            meta,
            TupleMeta {
                ts: 1,
                is_deleted: true
            }
        );

        // cleanup
        remove_file(db_path).expect("Couldn't remove test DB file");
    }

    fn sample_heap(bpm: Arc<BufferPoolManager>) -> (TableHeap, Vec<Option<RID>>) {
        let mut table_heap = TableHeap::new(bpm);

        // test
        let simple_schema = simple_schema();
        let tuples: Vec<_> = (0..4096)
            .map(|i| {
                table_heap.insert_tuple(
                    TupleMeta {
                        ts: 0,
                        is_deleted: false,
                    },
                    simple_tuple(&format!("name {i}"), i, &simple_schema),
                )
            })
            .collect();

        (table_heap, tuples)
    }

    #[test]
    fn iterator() {
        // init
        let db_path = temp_dir().join("th_iterator.db");
        let db_file_path = db_path.to_str().unwrap().to_string();
        let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
        let (table_heap, tuples) = sample_heap(bpm);

        // test
        let tuples = tuples
            .clone()
            .iter()
            .map(|rid| table_heap.get_tuple(&rid.clone().unwrap()).unwrap().1)
            .collect::<Vec<_>>();

        let mut iter = table_heap.iter();
        let mut tuples_actual = vec![];
        while let Some(tuple) = iter.next() {
            tuples_actual.push(tuple.1);
        }

        assert_eq!(tuples, tuples_actual);

        // cleanup
        remove_file(db_path).expect("Couldn't remove test DB file");
    }

    #[test]
    fn first_tuple() {
        // init
        let db_path = temp_dir().join("th_first_tuple.db");
        let db_file_path = db_path.to_str().unwrap().to_string();
        let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
        let (table_heap, _) = sample_heap(bpm);

        // test
        let (meta, tuple, rid) = table_heap.first_tuple().unwrap();
        assert_eq!(rid, RID::new(table_heap.first_page, 0));
        assert_eq!(meta, TupleMeta { ts: 0, is_deleted: false });
        assert_eq!(tuple, simple_tuple("name 0", 0, &simple_schema()));

        // cleanup
        remove_file(db_path).expect("Couldn't remove test DB file");
    }

    #[test]
    fn next_tuple() {
        // init
        let db_path = temp_dir().join("th_next_tuple.db");
        let db_file_path = db_path.to_str().unwrap().to_string();
        let bpm = Arc::new(BufferPoolManager::new(String::from(db_file_path), 100, 2));
        let (table_heap, _) = sample_heap(bpm);

        // test
        let (_, _, first_rid) = table_heap.first_tuple().unwrap();
        let (meta, tuple, rid) = table_heap.tuple_after(&first_rid).unwrap();
        assert_eq!(rid, RID::new(table_heap.first_page, 1));
        assert_eq!(meta, TupleMeta { ts: 0, is_deleted: false });
        assert_eq!(tuple, simple_tuple("name 1", 1, &simple_schema()));

        // cleanup
        remove_file(db_path).expect("Couldn't remove test DB file");
    }
}
