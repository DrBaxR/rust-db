use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
};

use super::{
    disk_manager::{DiskManager, PageID},
    disk_scheduler::DiskScheduler,
    lruk_replacer::{FrameID, LRUKReplacer},
};

struct Frame {
    frame_id: FrameID,
    pin_count: AtomicUsize,
    is_dirty: AtomicBool,
    page: Option<RwLock<Page>>,
}

impl Frame {
    fn new(frame_id: FrameID) -> Self {
        Self {
            frame_id,
            pin_count: AtomicUsize::new(0),
            is_dirty: AtomicBool::new(false),
            page: None,
        }
    }
}

struct Page {
    page_id: PageID,
    data: Vec<u8>,
}

struct BufferPoolManager {
    disk_scheduler: DiskScheduler,
    replacer: LRUKReplacer,
    /// Statically allocated on construct, does not grow or shrink
    frames: Vec<Frame>,
    /// Maps id of a page to an index in the frames vec
    page_table: RwLock<HashMap<PageID, usize>>,
}

impl BufferPoolManager {
    pub fn new(db_file_path: String, num_frames: usize, k_dist: usize) -> Self {
        let disk_scheduler = DiskScheduler::new(DiskManager::new(db_file_path));
        let replacer = LRUKReplacer::new(num_frames, k_dist);

        let mut frames = vec![];
        for i in 0..num_frames {
            frames.push(Frame::new(i as FrameID));
        }

        let page_table = RwLock::new(HashMap::new());

        Self {
            disk_scheduler,
            replacer,
            frames,
            page_table,
        }
    }

    // TODO: make this be an option once implementing other non-trivial cases
    pub fn get_read_page(&self, page_id: PageID) -> RwLockReadGuard<Page> {
        // simple case, when in memory
        let page_table = self.page_table.read().unwrap();
        let frame_index = page_table.get(&page_id).expect("case").clone(); // TODO: treat case when not in memory
        drop(page_table);

        let frame = self
            .frames
            .get(frame_index)
            .expect("Wrong value in page table or frames not properly allocated");

        let page = frame
            .page
            .as_ref()
            .expect("Page table entry points to empty frame")
            .read()
            .unwrap();

        // TODO: also pin page?

        page
    }

    // TODO: option
    pub fn get_write_page(&self, page_id: PageID) -> RwLockWriteGuard<Page> {
        todo!()
    }

    pub fn flush_page(&self, page_id: PageID) -> bool {
        todo!()
    }

    pub fn new_page(&self) -> PageID {
        todo!()
    }

    pub fn delete_page(&self, page_id: PageID) -> bool {
        todo!()
    }

    pub fn flush_all_pages(&self) {
        todo!()
    }
}
