use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
};

use crate::disk::disk_scheduler::{DiskRequest, DiskRequestType, DiskResponse};

use super::{
    disk_manager::{DiskManager, PageID},
    disk_scheduler::DiskScheduler,
    lruk_replacer::{FrameID, LRUKReplacer},
};

struct Frame {
    frame_id: FrameID,
    /// Number of workers that require this page to remain in memory
    pin_count: AtomicUsize,
    is_dirty: AtomicBool,
    page: RwLock<Option<Page>>,
}

impl Frame {
    fn new(frame_id: FrameID) -> Self {
        Self {
            frame_id,
            pin_count: AtomicUsize::new(0),
            is_dirty: AtomicBool::new(false),
            page: RwLock::new(None),
        }
    }
}

struct Page {
    page_id: PageID,
    data: Vec<u8>,
}

// TODO: for simplicity sake, at the moment manager assumes that database is empty every time it gets constructed. change this
struct BufferPoolManager {
    disk_scheduler: DiskScheduler,
    replacer: LRUKReplacer, // TODO: this is not thread-safe, will need to wrap it in a mutex
    /// Statically allocated on construct, does not grow or shrink
    frames: Vec<Frame>,
    /// Indexes of free frames in `frames` vec
    free_frames: Mutex<Vec<usize>>,
    /// Maps id of a page to an index in the frames vec
    page_table: RwLock<HashMap<PageID, usize>>,
}

impl BufferPoolManager {
    pub fn new(db_file_path: String, num_frames: usize, k_dist: usize) -> Self {
        let disk_scheduler = DiskScheduler::new(DiskManager::new(db_file_path));
        let replacer = LRUKReplacer::new(num_frames, k_dist);

        let mut frames = vec![];
        let mut free_frames = vec![];
        for i in 0..num_frames {
            frames.push(Frame::new(i as FrameID));
            free_frames.push(i);
        }

        let page_table = RwLock::new(HashMap::new());

        Self {
            disk_scheduler,
            replacer,
            frames,
            free_frames: Mutex::new(free_frames),
            page_table,
        }
    }

    // TODO: make this be an option once implementing other non-trivial cases
    pub fn get_read_page(&self, page_id: PageID) -> RwLockReadGuard<Option<Page>> {
        // get frame index
        let page_table = self.page_table.read().unwrap();
        let frame_index = page_table.get(&page_id).cloned();
        drop(page_table);

        let frame_index = if let Some(index) = frame_index {
            index
        } else {
            // the page id is not in memory
            self.bring_page_in_memory(page_id)
        };

        // get frame from memory
        let frame = self
            .frames
            .get(frame_index)
            .expect("Wrong value in page table or frames not properly allocated");

        // get page from frame
        let page = frame
            .page
            .read()
            .expect("Page table entry points to empty frame");

        // record access to the frame
        // TODO: also record access to the frame
        // TODO: also set frame not evictable
        // TODO: also pin page
        // TODO: how do you decrease pin??

        page
    }

    /// Brings to memory page that is **NOT** in memory. Returns index in the `frames` array of the page.
    fn bring_page_in_memory(&self, page_id: PageID) -> usize {
        // TODO: move to method
        let mut free_frames = self.free_frames.lock().unwrap();
        let free_frame_index = if let Some(i) = free_frames.first().cloned() {
            free_frames.remove(i);
            Some(i)
        } else {
            None
        };
        drop(free_frames);

        let response = self
            .disk_scheduler
            .schedule(DiskRequest {
                page_id,
                req_type: DiskRequestType::Read,
            })
            .recv()
            .unwrap();

        let page_data = match response {
            DiskResponse::ReadResponse(vec) => {
                vec.expect("Trying to read unallocated page from disk")
            }
            DiskResponse::WriteResponse => panic!("Wrong response type"),
        };

        if let Some(free_frame_index) = free_frame_index {
            // TODO: move to separate method and reevaluate after that if this is thread-safe
            // there are free slots, do a disk read for page id and store it in frames
            let frame = self.frames.get(free_frame_index).expect(&format!(
                "Incorrect free frame index: {} (frames size is {})",
                free_frame_index,
                self.frames.len()
            ));

            let mut page = frame.page.write().unwrap();
            let _ = page.insert(Page {
                page_id,
                data: page_data,
            });

            // update page table
            let mut page_table = self.page_table.write().unwrap();
            page_table.insert(page_id, free_frame_index);

            free_frame_index
        } else {
            // TODO there are NOT free slots, evict something, do disk read for page id and store it in frames
            // will need to protect the replacer since it's not thread-safe
            todo!()
        }
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
