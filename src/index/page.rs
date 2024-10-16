use crate::disk::disk_manager::PageID;

/// Header page for extendible hashing index. The data structure looks like this:
/// - `directory_page_ids` (0-2047): An array of directory page IDs (4 bytes each)
/// - `max_depth` (2048-2051): The max depth the header can handle - configured on index creation
#[derive(Debug)]
pub struct HashTableHeaderPage {
    directory_page_ids: Vec<PageID>,
    max_depth: u32,
}

impl HashTableHeaderPage {
    pub fn new(data: &[u8]) -> Self {
        let mut page_ids = vec![];
        for i in 0..511 {
            page_ids.push(u32::from_be_bytes(get_four_bytes_group(data, i))); // TODO: might break when reading from disk, because of endian, make a test for this
        }

        let max_depth = u32::from_be_bytes(get_four_bytes_group(data, 512));

        Self {
            directory_page_ids: page_ids,
            max_depth,
        }
    }

    pub fn serialize() -> Vec<u8> {
        todo!("Reverse of new")
    }

    pub fn hash_to_directory_page_index(&self, hash: u32) -> usize {
        todo!("Double check how lookup works")
    }

    pub fn get_directory_page_id(&self, directory_index: usize) -> PageID {
        self.directory_page_ids[directory_index]
    }

    pub fn set_directory_page_id(&mut self, directory_index: usize, directory_page_id: PageID) {
        self.directory_page_ids[directory_index] = directory_page_id;
    }

    /// Returns the maximum number of directory pages taht the header can handle. Note, this is not the `max_depth`.
    pub fn max_size(&self) -> u32 {
        1 << self.max_depth
    }
}

fn get_four_bytes_group(data: &[u8], group_index: usize) -> [u8; 4] {
    [
        data[group_index * 4],
        data[group_index * 4 + 1],
        data[group_index * 4 + 2],
        data[group_index * 4 + 3],
    ]
}
