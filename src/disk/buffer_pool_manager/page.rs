use std::sync::{atomic::Ordering, Mutex, RwLockReadGuard, RwLockWriteGuard};

use crate::disk::{disk_manager::PageID, lruk_replacer::LRUKReplacer};

use super::Frame;

pub struct Page {
    pub page_id: PageID,
    pub data: Vec<u8>,
}

pub struct PageReadGuard<'a> {
    pub page: RwLockReadGuard<'a, Option<Page>>,
    frame: &'a Frame,
    replacer: &'a Mutex<LRUKReplacer>,
}

impl<'a> Drop for PageReadGuard<'a> {
    fn drop(&mut self) {
        let prev_pin_count = self.frame.pin_count.fetch_sub(1, Ordering::SeqCst);

        let mut replacer = self.replacer.lock().unwrap();
        if prev_pin_count == 1 {
            let _ = replacer.set_evictable(self.frame.frame_id, true); // result ignored, beacause already evicted
        }
    }
}

impl<'a> PageReadGuard<'a> {
    pub fn new(
        page: RwLockReadGuard<'a, Option<Page>>,
        frame: &'a Frame,
        replacer: &'a Mutex<LRUKReplacer>,
    ) -> Self {
        Self {
            page,
            frame,
            replacer,
        }
    }

    pub fn read(&'a self) -> &'a Vec<u8> {
        &self.page.as_ref().unwrap().data
    }
}

pub struct PageWriteGuard<'a> {
    page: RwLockWriteGuard<'a, Option<Page>>,
    frame: &'a Frame,
    replacer: &'a Mutex<LRUKReplacer>,
}

impl<'a> Drop for PageWriteGuard<'a> {
    fn drop(&mut self) {
        let prev_pin_count = self.frame.pin_count.fetch_sub(1, Ordering::SeqCst);

        let mut replacer = self.replacer.lock().unwrap();
        if prev_pin_count == 1 {
            let _ = replacer.set_evictable(self.frame.frame_id, true); // result ignored, beacause already evicted
        }
    }
}

impl<'a> PageWriteGuard<'a> {
    pub fn new(
        page: RwLockWriteGuard<'a, Option<Page>>,
        frame: &'a Frame,
        replacer: &'a Mutex<LRUKReplacer>,
    ) -> Self {
        Self {
            page,
            frame,
            replacer,
        }
    }

    pub fn read(&'a self) -> &'a Vec<u8> {
        &self.page.as_ref().unwrap().data
    }

    pub fn write(&mut self, data: Vec<u8>) {
        self.page.as_mut().unwrap().data = data;
        self.frame.is_dirty.store(true, Ordering::SeqCst);
    }
}
