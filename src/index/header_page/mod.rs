use crate::{config::DB_PAGE_SIZE, disk::disk_manager::PageID};

use super::{
    get_four_bytes_group, get_msb,
    serial::{Deserialize, Serialize},
};

#[cfg(test)]
mod tests;

const HASH_TABLE_HEADER_PAGE_MAX_IDS: usize = 512;

/// Header page for extendible hashing index. The data structure looks like this:
/// - `directory_page_ids` (0-2047): An array of directory page IDs (4 bytes each)
/// - `max_depth` (2048-2051): The max depth the header can handle - configured on index creation
#[derive(Debug)]
pub struct HashTableHeaderPage {
    directory_page_ids: Vec<PageID>,
    max_depth: u32,
}

impl HashTableHeaderPage {
    pub fn new(directory_page_ids: Vec<PageID>, max_depth: u32) -> Self {
        Self {
            directory_page_ids,
            max_depth,
        }
    }

    pub fn hash_to_directory_page_index(&self, hash: u32) -> usize {
        get_msb(hash, self.max_depth as usize) as usize
    }

    /// Returns the page ID of the directory page with the index `directory_index`. Will return `None` if trying to access index greater than `max_size()`.
    pub fn get_directory_page_id(&self, directory_index: usize) -> Option<PageID> {
        if directory_index > self.max_size() - 1 {
            return None;
        }

        Some(self.directory_page_ids[directory_index])
    }

    /// Returns the page ID of the replaced page.
    ///
    /// # Errors
    /// Will return `Err` if trying to replace the page ID of a index that is greater than `max_size()`.
    pub fn set_directory_page_id(
        &mut self,
        directory_index: usize,
        directory_page_id: PageID,
    ) -> Result<PageID, ()> {
        if directory_index > self.max_size() - 1 {
            return Err(());
        }

        let previous_page_id = self.get_directory_page_id(directory_index).unwrap();
        self.directory_page_ids[directory_index] = directory_page_id;

        Ok(previous_page_id)
    }

    /// Returns the maximum number of directory pages taht the header can handle. Note, this is not the `max_depth`.
    pub fn max_size(&self) -> usize {
        1 << self.max_depth
    }
}

impl Serialize for HashTableHeaderPage {
    fn serialize(&self) -> Vec<u8> {
        let mut data = vec![];

        for index in 0..HASH_TABLE_HEADER_PAGE_MAX_IDS {
            let page_id = self.directory_page_ids.get(index).unwrap_or(&0);

            data.extend_from_slice(&page_id.to_be_bytes()); // endian picked here needs to match the one in from_serialized
        }

        data.extend_from_slice(&self.max_depth.to_be_bytes());
        data.resize(DB_PAGE_SIZE as usize, 0);

        data
    }
}

impl Deserialize for HashTableHeaderPage {
    fn deserialize(data: &[u8]) -> Self {
        let mut page_ids = vec![];
        for i in 0..HASH_TABLE_HEADER_PAGE_MAX_IDS {
            page_ids.push(u32::from_be_bytes(get_four_bytes_group(data, i))); // endian picked here needs to match the one in serialize
        }

        let max_depth = u32::from_be_bytes(get_four_bytes_group(data, 512));

        Self {
            directory_page_ids: page_ids,
            max_depth,
        }
    }
}
