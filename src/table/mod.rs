use std::sync::Arc;

use page::{TablePage, TupleMeta};
use tuple::{Tuple, RID};

use crate::disk::{
    buffer_pool_manager::{BufferPoolManager, DiskRead, DiskWrite},
    disk_manager::PageID,
};

mod page;
mod schema;
mod tuple;
mod value;

const END_PAGE_ID: PageID = 0;

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

    pub fn sequencial_dump(&self) -> Vec<(&TupleMeta, &Tuple)> {
        todo!()
    }
}
