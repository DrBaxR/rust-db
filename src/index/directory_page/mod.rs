use std::collections::HashMap;

use crate::{config::DB_PAGE_SIZE, disk::disk_manager::PageID};

use super::get_four_bytes_group;

#[cfg(test)]
mod tests;

const HASH_TABLE_DIRECTORY_PAGE_MAX_IDS: usize = 512;
const HASH_TABLE_DIRECTORY_PAGE_MAX_LOCAL_DEPTHS: usize = HASH_TABLE_DIRECTORY_PAGE_MAX_IDS;

const MAX_DEPTH_GROUP_INDEX: usize = 640;
const GLOBAL_DEPTH_GROUP_INDEX: usize = MAX_DEPTH_GROUP_INDEX + 1;

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
        let mut bucket_page_ids = vec![];
        for i in 0..HASH_TABLE_DIRECTORY_PAGE_MAX_IDS {
            bucket_page_ids.push(u32::from_be_bytes(get_four_bytes_group(data, i)));
        }

        let local_depths_offset = HASH_TABLE_DIRECTORY_PAGE_MAX_IDS * size_of::<PageID>();
        let mut local_depths = vec![];
        for i in 0..HASH_TABLE_DIRECTORY_PAGE_MAX_LOCAL_DEPTHS {
            local_depths.push(data[local_depths_offset + i]);
        }

        let max_depth = u32::from_be_bytes(get_four_bytes_group(data, MAX_DEPTH_GROUP_INDEX));
        let global_depth = u32::from_be_bytes(get_four_bytes_group(data, GLOBAL_DEPTH_GROUP_INDEX));

        Self {
            bucket_page_ids,
            local_depths,
            max_depth,
            global_depth,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut data = vec![];

        for i in 0..HASH_TABLE_DIRECTORY_PAGE_MAX_IDS {
            let page_id = self.bucket_page_ids.get(i).unwrap_or(&0);

            data.extend_from_slice(&page_id.to_be_bytes());
        }

        for i in 0..HASH_TABLE_DIRECTORY_PAGE_MAX_LOCAL_DEPTHS {
            let local_depth = self.local_depths.get(i).unwrap_or(&0);

            data.push(*local_depth);
        }

        data.extend_from_slice(&self.max_depth.to_be_bytes());
        data.extend_from_slice(&self.global_depth.to_be_bytes());
        data.resize(DB_PAGE_SIZE as usize, 0);

        return data;
    }

    /// Index bucket IDs with `hash`. Will use the `global_depth` LSB's.
    pub fn hash_to_bucket_index(&self, hash: u32) -> usize {
        (hash & self.get_global_depth_mask()) as usize
    }

    /// Returns the page ID of the bucket page stored at index `bucket_index`. Will return `None` when `bucket_index` is greater than `max_size()`
    pub fn get_bucket_page_id(&self, bucket_index: usize) -> Option<PageID> {
        if bucket_index > self.size() - 1 {
            return None;
        }

        Some(self.bucket_page_ids[bucket_index])
    }

    /// Sets the bucket page ID at `bucket_index` to `bucket_page_id`. Returns old page ID before replacing it.
    ///
    /// # Errors
    /// Will return `Err` if trying to replace invalid index.
    fn set_bucket_page_id(
        &mut self,
        bucket_index: usize,
        bucket_page_id: PageID,
    ) -> Result<PageID, ()> {
        let previous_page_id = self.get_bucket_page_id(bucket_index).ok_or(())?;
        self.bucket_page_ids[bucket_index] = bucket_page_id;

        Ok(previous_page_id)
    }

    // TODO: define what a split image is
    fn get_split_image_index(&self, bucket_index: usize) -> usize {
        todo!()
    }

    /// Returns a mask of `global_depth` 1's and the rest of 0's. This mask is to be used to obtain an index from a hash: `hash & mask`.
    fn get_global_depth_mask(&self) -> u32 {
        (1 << self.global_depth) - 1
    }

    /// Same as `get_global_depth_mask`, but uses the local depth of the bucket at `bucket_index` to generate the mask. Will return `None` if index larger than size.
    fn get_local_depth_mask(&self, bucket_index: usize) -> Option<u32> {
        Some((1 << self.get_local_depth(bucket_index)?) - 1)
    }

    pub fn global_depth(&self) -> u32 {
        self.global_depth
    }

    pub fn max_depth(&self) -> u32 {
        self.max_depth
    }

    pub fn increment_global_depth(&mut self) {
        self.global_depth += 1;
    }

    pub fn decrement_global_depth(&mut self) {
        self.global_depth -= 1;
    }

    /// Return `true` if all local depths are less than the global depth.
    pub fn can_shrink(&self) -> bool {
        for local_depth in self.local_depths.iter() {
            if *local_depth as u32 >= self.global_depth {
                return false;
            }
        }

        true
    }

    /// Returns the current size of the directory (computed using `global_depth`).
    pub fn size(&self) -> usize {
        1 << self.global_depth()
    }

    /// Returns the max size that the directory can handle (computed using `max_depth`).
    pub fn max_size(&self) -> usize {
        1 << self.max_depth()
    }

    /// Returns local depth of bucket with index `bucket_index`. Will return `None` if bucket index larger than size.
    fn get_local_depth(&self, bucket_index: usize) -> Option<u8> {
        if bucket_index > self.size() - 1 {
            return None;
        }

        Some(self.local_depths[bucket_index])
    }

    /// Sets value of local depth for bucket at `bucket_index` to `local_depth`. Returns old local depth of that bucket.
    ///
    /// # Errors
    /// Returns `Err` if bucket index is greater than size.
    fn set_local_depth(&mut self, bucket_index: usize, local_depth: u8) -> Result<u8, ()> {
        let previous_depth = self.get_local_depth(bucket_index).ok_or(())?;
        self.local_depths[bucket_index] = local_depth;

        Ok(previous_depth)
    }

    /// Returns previous value of the local depth.
    ///
    /// # Errors
    /// Returns `Err` if index is greater than size.
    fn increment_local_depth(&mut self, bucket_index: usize) -> Result<u8, ()> {
        let previous_depth = self.get_local_depth(bucket_index).ok_or(())?;
        self.set_local_depth(bucket_index, previous_depth + 1)?;

        Ok(previous_depth)
    }

    /// Returns previous value of the local depth.
    ///
    /// # Errors
    /// Returns `Err` if index is greater than size.
    fn decrement_local_depth(&mut self, bucket_index: usize) -> Result<u8, ()> {
        let previous_depth = self.get_local_depth(bucket_index).ok_or(())?;
        self.set_local_depth(bucket_index, previous_depth - 1)?;

        Ok(previous_depth)
    }

    /// Returns `true` if values are valid:
    /// - all local depths <= global depth
    /// - each bucket has exactly 2^(GD - LD) pointers pointing to it
    /// - the local depth is the same at each index with the same bucket_page_id
    pub fn is_valid(&self) -> bool {
        self.is_local_depths_constraint_valid()
            && self.is_pointers_count_valid()
            && self.is_local_depth_valid()
    }

    fn is_local_depths_constraint_valid(&self) -> bool {
        for local_depth in &self.local_depths {
            if *local_depth as u32 > self.global_depth() {
                return false;
            }
        }

        true
    }

    fn is_pointers_count_valid(&self) -> bool {
        let mut pointers_count = HashMap::new();

        for pointer in self.bucket_page_ids.iter() {
            let counter = pointers_count.entry(pointer).or_insert(0).clone();
            pointers_count.insert(pointer, counter + 1);
        }

        for (i, local_depth) in self.local_depths.iter().enumerate() {
            let pointer = match self.bucket_page_ids.get(i) {
                Some(i) => i,
                None => return false,
            };

            let pointer_count = match pointers_count.get(pointer) {
                Some(i) => i,
                None => return false,
            };

            if *pointer_count != (1 << (self.global_depth as u8 - local_depth)) {
                return false;
            }
        }

        true
    }

    fn is_local_depth_valid(&self) -> bool {
        let mut pointers_local_depths: HashMap<PageID, u8> = HashMap::new();

        for (i, pointer) in self.bucket_page_ids.iter().enumerate() {
            if let Some(local_depth) = pointers_local_depths.get(pointer) {
                if *local_depth != self.local_depths[i] {
                    return false;
                }
            } else {
                pointers_local_depths.insert(*pointer, self.local_depths[i]);
            }
        }

        true
    }
}
