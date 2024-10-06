// TODO: split in mode modules
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
};

use crate::{
    config::DB_PAGE_SIZE,
    disk::disk_scheduler::{DiskRequest, DiskRequestType, DiskResponse},
};

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

    /// Empties all data in frame, setting page data to `page`.
    fn reset(&self, page: Page) {
        self.pin_count.store(0, Ordering::SeqCst);
        self.is_dirty.store(false, Ordering::SeqCst);

        let _ = self.page.write().unwrap().insert(page);
    }
}

struct Page {
    page_id: PageID,
    data: Vec<u8>,
}

pub struct PageReadGuard<'a> {
    page: RwLockReadGuard<'a, Option<Page>>,
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
    fn new(
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

    // TODO: a way to access data inside page
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
    fn new(
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

    // TODO: a way to access data inside page
    pub fn write(&mut self, data: Vec<u8>) {
        self.page.as_mut().unwrap().data = data;
        self.frame.is_dirty.store(true, Ordering::SeqCst);
    }
}

// TODO: for simplicity sake, at the moment manager assumes that database is empty every time it gets constructed. Change this in the future
pub struct BufferPoolManager {
    disk_scheduler: DiskScheduler,
    replacer: Mutex<LRUKReplacer>,
    /// Statically allocated on construct, does not grow or shrink
    frames: Vec<Frame>,
    /// Indexes of free frames in `frames` vec
    free_frames: Mutex<Vec<usize>>,
    /// Maps id of a page to an index in the frames vec
    page_table: RwLock<HashMap<PageID, usize>>,
    /// ID of the next page that will get allocated
    next_page_id: AtomicUsize,
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
            replacer: Mutex::new(replacer),
            frames,
            free_frames: Mutex::new(free_frames),
            page_table,
            next_page_id: AtomicUsize::new(0),
        }
    }

    pub fn get_read_page(&self, page_id: PageID) -> PageReadGuard {
        let frame = self.fetch_page(page_id);

        let page = frame
            .page
            .read()
            .expect("Page table entry points to empty frame");

        self.record_frame_access(frame);

        PageReadGuard::new(page, &frame, &self.replacer)
    }

    /// Returns a reference to a frame that contains the page with `page_id`. Will also bring the page in memory if not already there.
    fn fetch_page(&self, page_id: PageID) -> &Frame {
        // get frame index
        let page_table = self.page_table.read().unwrap();
        let frame_index = page_table.get(&page_id).cloned();
        drop(page_table);

        let frame_index = if let Some(index) = frame_index {
            index
        } else {
            // the page id is not in memory
            self.bring_page_in_memory(page_id)
                .expect("Buffer full and can't evict anything")
        };

        // get frame from memory
        self.frames
            .get(frame_index)
            .expect("Wrong value in page table or frames not properly allocated")
    }

    fn record_frame_access(&self, frame: &Frame) {
        // record access to the frame
        let mut replacer = self.replacer.lock().unwrap();
        replacer
            .record_access(frame.frame_id)
            .expect("Replacer frame buffer full, internal frames not synced with manager frames");

        replacer
            .set_evictable(frame.frame_id, false)
            .expect("Trying to set evictable value for untracked frame");
        drop(replacer);

        frame.pin_count.fetch_add(1, Ordering::SeqCst); // pin decrease handled on page read guard drop
    }

    /// Brings to memory page that is **NOT** in memory. Returns index in the `frames` array of the page. Will return `None` if the buffer is full and can't evict anything.
    fn bring_page_in_memory(&self, page_id: PageID) -> Option<usize> {
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

        let free_frame_index = self.get_first_free_frame();

        if let Some(free_frame_index) = free_frame_index {
            // there are free slots, do a disk read for page id and store it in frames
            self.associate_page_to_frame(page_id, page_data, free_frame_index);

            Some(free_frame_index)
        } else {
            // there are no free slots, evict a frame
            let mut replacer = self.replacer.lock().unwrap();
            let evicted_frame_id = replacer.evict()? as usize;
            drop(replacer);

            // frame id is equal to index in frames vec, check constructor
            self.associate_page_to_frame(page_id, page_data, evicted_frame_id);

            Some(evicted_frame_id)
        }
    }

    /// Returns the index of the first free frame and removes it from the free frames. Will return `None` if there are no free frames.
    fn get_first_free_frame(&self) -> Option<usize> {
        let mut free_frames = self.free_frames.lock().unwrap();

        if let Some(i) = free_frames.first().cloned() {
            free_frames.remove(i);
            Some(i)
        } else {
            None
        }
    }

    /// Updates the page data in the frame with `frame_index` and creates a mapping `page_id -> frame_index` in the `page_table`.
    fn associate_page_to_frame(&self, page_id: PageID, data: Vec<u8>, frame_index: usize) {
        // set page data for frame
        let frame = self.frames.get(frame_index).expect(&format!(
            "Incorrect free frame index: {} (frames size is {})",
            frame_index,
            self.frames.len()
        ));

        let mut page = frame.page.write().unwrap();
        let _ = page.insert(Page { page_id, data });
        drop(page);

        // update page table
        let mut page_table = self.page_table.write().unwrap();
        page_table.insert(page_id, frame_index);
    }

    pub fn get_write_page(&self, page_id: PageID) -> PageWriteGuard {
        let frame = self.fetch_page(page_id);

        let page = frame
            .page
            .write()
            .expect("Page table entry points to empty frame");

        self.record_frame_access(frame);

        PageWriteGuard::new(page, &frame, &self.replacer)
    }

    /// Returns `false` if the page is not in memory. Will write the page to disk if it's dirty.
    pub fn flush_page(&self, page_id: PageID) -> bool {
        let frame_index = self.page_table.read().unwrap().get(&page_id).cloned();
        let frame_index = match frame_index {
            Some(index) => index,
            None => return false,
        };

        self.flush_frame_to_disk(frame_index, page_id);

        true
    }

    /// Flush page with `page_id` to disk **IF** frame with `frame_index` is marked as dirty. This means that in order to behave correctly
    /// this method expects that the frame and page are correctly mapped in `page_table`.
    fn flush_frame_to_disk(&self, frame_index: usize, page_id: PageID) {
        let frame_is_dirty = self
            .frames
            .get(frame_index)
            .expect("Page table points to invalid frame")
            .is_dirty
            .load(Ordering::SeqCst);

        if !frame_is_dirty {
            return;
        }

        // reaches here if is in memory AND is dirty
        let page_guard = self.get_read_page(page_id);
        let page = page_guard.page.as_ref().unwrap();

        // write page contents to disk
        let _ = self
            .disk_scheduler
            .schedule(DiskRequest {
                page_id: page.page_id,
                req_type: DiskRequestType::Write(page.data.clone()),
            })
            .recv()
            .unwrap();
    }

    /// Allocates a new page in memory and on disk and returns the id you can use to get it. Access to the page has to be done via the `get_read_page` or `get_write_page` methods.
    pub fn new_page(&self) -> PageID {
        let new_page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst) as PageID;
        let new_page = Page {
            page_id: new_page_id,
            data: [0 as u8; DB_PAGE_SIZE as usize].to_vec(),
        };

        // allocate the new page on disk and overwrite previous data
        self.disk_scheduler.increase_disk_size(new_page_id as usize);
        self.disk_scheduler
            .schedule(DiskRequest {
                page_id: new_page_id,
                req_type: DiskRequestType::Write([0 as u8; DB_PAGE_SIZE as usize].to_vec()),
            })
            .recv()
            .unwrap();

        // read the page and store it in the page table and frames array in memory
        let new_page_index = self
            .bring_page_in_memory(new_page_id)
            .expect("Buffer full and can't evict anything");

        self.free_frames.lock().unwrap().push(new_page_index);

        let new_page_frame = self.frames.get(new_page_index).unwrap();
        new_page_frame.reset(new_page);

        new_page_id
    }

    /// Deallocates page with `page_id`.
    pub fn delete_page(&self, page_id: PageID) -> bool {
        // deallocate from disk no necessary since old data overwritten by allocating page
        // deallocate from the page table: add frame index to free_frames, remove entry from the page_table
        let mut page_table = self.page_table.write().unwrap();
        let frame_index = match page_table.get(&page_id) {
            Some(index) => *index,
            None => return false,
        };

        // lock on page is acquired so nobody does anything with page while it is being deleted
        let page = self.get_write_page(page_id);

        // delete all metadata for frame where page was
        self.free_frames.lock().unwrap().push(frame_index);
        page_table.remove(&page_id);

        // delete access history from the replacer
        let mut replacer = self.replacer.lock().unwrap();
        let _ = replacer.set_evictable(frame_index as FrameID, true);
        replacer
            .remove(frame_index as FrameID)
            .expect("Frame was just set as evictable!");

        drop(replacer);
        drop(page);

        true
    }

    /// Writes all dirty pages to disk.
    pub fn flush_all_pages(&self) {
        // TODO: i think this should lock all operations (maybe? - idk)
        let pages: Vec<(PageID, usize)> = self
            .page_table
            .read()
            .unwrap()
            .iter()
            .map(|(p, f)| (p.clone(), f.clone()))
            .collect();

        for (page_id, frame_index) in pages {
            self.flush_frame_to_disk(frame_index, page_id);
        }
    }
}
