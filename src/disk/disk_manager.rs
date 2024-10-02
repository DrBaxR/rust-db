use std::{fs::File, os::unix::fs::FileExt};

use crate::{DB_DEFAULT_SIZE, DB_PAGE_SIZE};

type PageID = u32;

pub struct DiskManager {
    db_file: File,
    pages_capacity: usize,
}

impl DiskManager {
    /// Create a new `DiskManager`. The `db_file_path` should be a Unix-like path (no Windows support atm).
    pub fn new(db_file_path: String) -> Self {
        let db_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file_path)
            .expect("Database file couldn't be opened");
        let pages_capacity = DB_DEFAULT_SIZE;

        let new_dm = Self {
            db_file,
            pages_capacity,
        };

        let default_db_size = pages_capacity as u64 * DB_PAGE_SIZE as u64;
        if new_dm.get_file_size() < default_db_size {
            // resize db file in case it was just created
            new_dm
                .db_file
                .set_len(default_db_size)
                .expect("Database file not opened for writing while initializing");
        }

        new_dm
    }

    /// Increase disk size of the database file so it is capable of holding `pages_amount` pages. Will do nothing if database file is already large enough.
    pub fn increase_disk_size(&mut self, pages_amount: usize) {
        // TODO: make thread safe (probably just throw the file behind a mutex)
        if pages_amount < self.pages_capacity {
            return;
        }

        while self.pages_capacity < pages_amount {
            self.pages_capacity *= 2;
        }

        self.db_file
            .set_len(self.pages_capacity as u64 * DB_PAGE_SIZE as u64)
            .expect("Database file not opened for writing while increasing disk size");
    }

    /// Read page with `id`. Will return `None` if trying to read from an address that is beyond the allocated space.
    pub fn read_page(&self, id: PageID) -> Option<Vec<u8>> {
        // TODO: make thread safe
        let offset = id * DB_PAGE_SIZE;

        if offset as u64 > self.get_file_size() {
            return None;
        }

        let mut buffer = [0 as u8; DB_PAGE_SIZE as usize];
        let bytes_read = self
            .db_file
            .read_at(&mut buffer, offset as u64)
            .expect("Page read to buffer failed");

        if bytes_read != DB_PAGE_SIZE as usize {
            buffer.to_vec().resize(DB_PAGE_SIZE as usize, 0);
        }

        Some(buffer.to_vec())
    }

    /// Write `data` to the page with `id`.
    pub fn write_page(&self, id: PageID, data: &[u8]) {
        // TODO: make thread safe
        let offset = id * DB_PAGE_SIZE;
        let data: &[u8] = &DiskManager::pad_to_page_size(data);

        self.db_file
            .write_at(data, offset as u64)
            .expect("Page write to database file failed");
        self.db_file.sync_all().expect("Write flush to disk failed");
    }

    /// Resize `data` to have the length of a database page. This either truncates the input (if larger than a database page), or pads it with `0`s (if smaller than a database file).
    fn pad_to_page_size(data: &[u8]) -> Vec<u8> {
        let mut res = data[..(DB_PAGE_SIZE as usize).min(data.len())].to_vec();
        res.resize(DB_PAGE_SIZE as usize, 0);

        res
    }

    /// Get the size of the database file on disk.
    fn get_file_size(&self) -> u64 {
        self.db_file
            .metadata()
            .expect("Failed to acquire metadata of the database file")
            .len()
    }
}
