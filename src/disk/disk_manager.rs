use std::{fs::File, io::Read, os::unix::fs::FileExt};

use crate::{DB_DEFAULT_SIZE, DB_PAGE_SIZE};

type PageID = u32;

pub struct DiskManager {
    db_file: File,
    pages_capacity: usize,
}

impl DiskManager {
    pub fn new(db_file_path: String) -> Self {
        let db_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file_path)
            .expect("Database file couldn't be opened");
        let pages_capacity = DB_DEFAULT_SIZE;

        // TODO: only set file size if file was created, otherwise might lose data
        db_file
            .set_len(pages_capacity as u64 * DB_PAGE_SIZE as u64)
            .expect("Database file not opened for writing while initializing");

        Self {
            db_file,
            pages_capacity,
        }
    }

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

    pub fn read_page(&self, id: PageID) -> Vec<u8> {
        // TODO: make thread safe
        let offset = id * DB_PAGE_SIZE;

        // TODO: check if read is beyond file size

        let mut buffer = [0 as u8; DB_PAGE_SIZE as usize];
        let bytes_read = self
            .db_file
            .read_at(&mut buffer, offset as u64)
            .expect("Page read to buffer failed");

        if bytes_read != DB_PAGE_SIZE as usize {
            buffer.to_vec().resize(DB_PAGE_SIZE as usize, 0);
        }

        buffer.to_vec()
    }

    pub fn write_page(&self, id: PageID, data: &[u8]) {
        // TODO: make thread safe
        let offset = id * DB_PAGE_SIZE;
        let data: &[u8] = &DiskManager::pad_to_page_size(data);

        self.db_file
            .write_at(data, offset as u64)
            .expect("Page write to database file failed");
        self.db_file.sync_all().expect("Write flush to disk failed");
    }

    fn pad_to_page_size(data: &[u8]) -> Vec<u8> {
        let mut res = data[..(DB_PAGE_SIZE as usize).min(data.len())].to_vec();
        res.resize(DB_PAGE_SIZE as usize, 0);

        res
    }
}
