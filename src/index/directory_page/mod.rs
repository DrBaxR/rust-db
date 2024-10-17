use crate::disk::disk_manager::PageID;

#[cfg(test)]
mod tests;

const HASH_TABLE_DIRECTORY_PAGE_MAX_IDS: usize = 512;
const HASH_TABLE_DIRECTORY_PAGE_MAX_LOCAL_DEPTHS: usize = HASH_TABLE_DIRECTORY_PAGE_MAX_IDS;

/// Directory page for extendinble hashing index. Its structure looks like this on disk:
/// - `bucket_page_ids` (0-2047): An array of bucket page IDs (4 bytes each).
/// - `local_depths` (2048-2559): An array of local depths (1 byte each) corresponding to the stored page IDs.
/// - `max_depth` (2560-2563): The max depth the directory page can handle (same constraint as the header page).
/// - `global_depth` (2564-2567): The current global depth of the directory page.
pub struct HashTableDirectoryPage {
    bucket_page_ids: Vec<PageID>,
    local_depths: Vec<u8>,
    max_depth: u32,
    global_depth: u32,
}

impl HashTableDirectoryPage {
    pub fn new(
        bucket_page_ids: Vec<PageID>,
        local_depths: Vec<u8>,
        max_depth: u32,
        global_depth: u32,
    ) -> Self {
        Self {
            bucket_page_ids,
            local_depths,
            max_depth,
            global_depth,
        }
    }

    pub fn from_serialized(data: &[u8]) -> Self {
        todo!()
    }
    
    pub fn serialize(&self) -> Vec<u8> {
        todo!()
    }

    fn hash_to_bucket_index(&self, hash: u32) -> usize {
        todo!()
    }

    fn get_bucket_page_id(&self, bucket_index: usize) -> PageID {
        todo!()
    }

    fn set_bucket_page_id(&mut self, directory_index: usize, bucket_page_id: PageID) -> Result<PageID, ()> {
        todo!()
    }

    // TODO: define what a split image is
    fn get_split_image_index(&self, bucket_index: usize) -> usize {
        todo!()
    }

    /// Returns a mask of `global_depth` 1's and the rest of 0's. This mask is to be used to obtain an index from a hash: `hash & mask`.
    fn get_global_depth_mask(&self) -> u32 {
        todo!()
    }

    /// Same as `get_global_depth_mask`, but uses the local depth of the bucket at `bucket_index` to generate the mask.
    fn get_local_depth_mask(&self, bucket_index: usize) -> u32 {
        todo!()
    }

    fn global_depth(&self) -> u32 {
        todo!()
    }

    fn max_depth(&self) -> u32 {
        todo!()
    }

    fn increment_global_depth(&mut self) {
        todo!()
    }

    fn decrement_global_depth(&mut self) {
        todo!()
    }

    fn can_shrink(&self) -> bool {
        todo!()
    }

    fn size(&self) -> u32 {
        todo!()
    }

    fn max_size(&self) -> u32 {
        todo!()
    }

    fn get_local_depth(&self, bucket_index: usize) -> u32 {
        todo!()
    }

    fn set_local_depth(&mut self, bucket_index: usize, local_depth: u8) {
        todo!()
    }

    fn increment_local_depth(&mut self, bucket_index: usize) {
        todo!()
    }

    fn decrement_local_depth(&mut self, bucket_index: usize) {
        todo!()
    }

    fn is_valid(&self) -> bool {
        todo!()
    }
}
