use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex, MutexGuard, RwLock,
    },
};

use page::{Page, PageReadGuard, PageWriteGuard};

use crate::disk::disk_scheduler::{DiskRequest, DiskRequestType, DiskResponse};

use super::{
    disk_manager::{DiskManager, PageID},
    disk_scheduler::DiskScheduler,
    lruk_replacer::{FrameID, LRUKReplacer},
};

mod page;
#[cfg(test)]
mod tests;

pub struct Frame {
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

    /// Returns the id of the stored page.
    fn page_id(&self) -> Option<PageID> {
        self.page.read().unwrap().as_ref().map(|p| p.page_id)
    }
}

pub struct BufferPoolManager {
    disk_scheduler: DiskScheduler,
    replacer: Mutex<LRUKReplacer>,
    /// Statically allocated on construct, does not grow or shrink
    frames: Vec<Frame>,
    /// Indexes of free frames in `frames` vec
    free_frames: Mutex<Vec<usize>>,
    /// Maps id of a page to an index in the frames vec
    page_table: Mutex<HashMap<PageID, usize>>,
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

        let page_table = Mutex::new(HashMap::new());

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
        let page_table = self.page_table.lock().unwrap();
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
        let mut page_table = self.page_table.lock().unwrap();
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
            self.associate_page_to_frame(page_id, page_data, free_frame_index, page_table);

            Some(free_frame_index)
        } else {
            // there are no free slots, evict a frame
            let mut replacer = self.replacer.lock().unwrap();
            let evicted_frame_id = replacer.evict()? as usize;
            drop(replacer);

            // flush evicted frame to disk
            let evicted_page_lock = self
                .frames
                .get(evicted_frame_id)
                .unwrap()
                .page
                .read()
                .unwrap();
            let evicted_page = evicted_page_lock.as_ref().unwrap();
            let evicted_page_id = evicted_page.page_id;

            self.write_page_data_to_disk(evicted_page.page_id, evicted_page.data.clone());
            drop(evicted_page_lock);

            // frame id is equal to index in frames vec, check constructor
            page_table.remove(&evicted_page_id);
            self.associate_page_to_frame(page_id, page_data, evicted_frame_id, page_table);

            Some(evicted_frame_id)
        }
    }

    /// Returns the index of the first free frame and removes it from the free frames. Will return `None` if there are no free frames.
    fn get_first_free_frame(&self) -> Option<usize> {
        let mut free_frames = self.free_frames.lock().unwrap();

        if let Some(i) = free_frames.first().cloned() {
            free_frames.remove(0);
            Some(i)
        } else {
            None
        }
    }

    /// Updates the page data in the frame with `frame_index` and creates a mapping `page_id -> frame_index` in the `page_table`.
    fn associate_page_to_frame(
        &self,
        page_id: PageID,
        data: Vec<u8>,
        frame_index: usize,
        mut page_table: MutexGuard<'_, HashMap<PageID, usize>>,
    ) {
        // set page data for frame
        let frame = self.frames.get(frame_index).expect(&format!(
            "Incorrect free frame index: {} (frames size is {})",
            frame_index,
            self.frames.len()
        ));

        frame.reset(Page { page_id, data });

        // update page table
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
        let frame_index = self.page_table.lock().unwrap().get(&page_id).cloned();
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
        self.write_page_data_to_disk(page.page_id, page.data.clone());
    }

    /// Write `data` to disk for the page with `page_id`.
    fn write_page_data_to_disk(&self, page_id: PageID, data: Vec<u8>) {
        let _ = self
            .disk_scheduler
            .schedule(DiskRequest {
                page_id,
                req_type: DiskRequestType::Write(data),
            })
            .recv()
            .unwrap();
    }

    /// Allocates a new page in memory and on disk and returns the id you can use to get it. Access to the page has to be done via the `get_read_page` or `get_write_page` methods,
    /// this method **DOES NOT** also bring the page in memory.
    pub fn new_page(&self) -> PageID {
        let new_page_id = self.next_page_id.fetch_add(1, Ordering::SeqCst) as PageID;

        // allocate the new page on disk and overwrite previous data
        self.disk_scheduler.increase_disk_size(new_page_id as usize);

        new_page_id
    }

    /// Deallocates page with `page_id`. Will return `false` if the page with `page_id` is not currently in memory
    pub fn delete_page(&self, page_id: PageID) -> bool {
        // lock on page is acquired so nobody does anything with page while it is being deleted
        let page = self.get_write_page(page_id);

        // deallocate from disk not necessary since old data overwritten by allocating page
        // deallocate from the page table: add frame index to free_frames, remove entry from the page_table
        let mut page_table = self.page_table.lock().unwrap();
        let frame_index = match page_table.get(&page_id) {
            Some(index) => *index,
            None => return false,
        };

        page_table.remove(&page_id);
        drop(page_table);

        // delete all metadata for frame where page was
        self.free_frames.lock().unwrap().push(frame_index);

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
        let pages: Vec<(PageID, usize)> = self
            .page_table
            .lock()
            .unwrap()
            .iter()
            .map(|(p, f)| (p.clone(), f.clone()))
            .collect();

        for (page_id, frame_index) in pages {
            self.flush_frame_to_disk(frame_index, page_id);
        }
    }
}
