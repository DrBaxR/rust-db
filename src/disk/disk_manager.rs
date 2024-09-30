use std::{fs::File, io::Read, os::unix::fs::FileExt};

use crate::DB_PAGE_SIZE;

type PageID = u32;

pub struct DiskManager {
    db_file: File,
}

impl DiskManager {
    pub fn new(db_file_path: String) -> Self {
        Self {
            db_file: File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(db_file_path)
                .expect("Database file couldn't be opened"),
        }
    }

    pub fn increase_disk_size(pages_amount: usize) {
        // TODO: works like dynamic array where size doubled until all pages can fit
        todo!("increase size of file so it fits number of pages"); 
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
