use std::{io::Cursor, marker::PhantomData, sync::Arc};

use ascii_tree::{write_tree, Tree};
use murmur3::murmur3_32;

use crate::{
    disk::{
        buffer_pool_manager::{BufferPoolManager, DiskRead, DiskWrite},
        disk_manager::PageID,
    },
    index::directory_page::{self, HashTableDirectoryPage},
};

use super::{
    bucket_page::HashTableBucketPage,
    header_page::HashTableHeaderPage,
    serial::{Deserialize, Serialize},
};

#[cfg(test)]
mod tests;

pub struct DiskExtendibleHashTable<K, V>
where
    K: Serialize + Deserialize + Eq + Clone,
    V: Serialize + Deserialize + Clone,
{
    _marker: PhantomData<(K, V)>, // this is more of a logical struct, since it uses the buffer pool manager to retrieve "its" data.
    bpm: Arc<BufferPoolManager>,
    header_max_depth: u32, // OPTIMIZATION: redindant to store max depths here since we already store header pid
    directory_max_depth: u32,
    header_page_id: PageID,
    name: String,
}

impl<K, V> DiskExtendibleHashTable<K, V>
where
    K: Serialize + Deserialize + Eq + Clone,
    V: Serialize + Deserialize + Clone,
{
    pub fn new(
        bpm: Arc<BufferPoolManager>,
        header_max_depth: u32,
        directory_max_depth: u32,
        name: String,
    ) -> Self {
        let header_page_id = bpm.new_page();

        let header = HashTableHeaderPage::new(header_max_depth, directory_max_depth);
        let mut header_page = bpm.get_write_page(header_page_id);
        header_page.write(header.serialize());
        drop(header_page);

        Self {
            _marker: PhantomData,
            bpm,
            header_max_depth,
            directory_max_depth,
            header_page_id,
            name,
        }
    }

    pub fn from_disk(bpm: Arc<BufferPoolManager>, header_pid: PageID, name: String) -> Self {
        let header_page = bpm.get_read_page(header_pid);
        let header = HashTableHeaderPage::deserialize(header_page.read());
        drop(header_page);

        Self {
            _marker: PhantomData,
            bpm,
            header_max_depth: header.max_depth(),
            directory_max_depth: header.directory_max_depth(),
            header_page_id: header_pid,
            name,
        }
    }

    /// Insert the `key`-`value` pair.
    ///
    /// # Errors
    /// Will return `Err` if it's not possible to insert.
    pub fn insert(&self, key: K, value: V) -> Result<(), ()> {
        let hash = self.hash(&key);

        // get directory page ID from header if exists, if not create empty one
        let mut h_page = self.bpm.get_write_page(self.header_page_id); // OPTIMIZATION: bottleneck since this is what everyone does first
        let mut header = HashTableHeaderPage::deserialize(h_page.read());
        let d_index = header.hash_to_directory_page_index(hash);
        let d_pid = match header.get_directory_page_id(d_index) {
            Some(pid) => pid,
            None => {
                let empty_dir_pid = self.new_empty_directory();

                header
                    .set_directory_page_id(d_index, empty_dir_pid)
                    .unwrap();
                h_page.write(header.serialize());

                empty_dir_pid
            }
        };
        drop(h_page);

        // get ID of bucket where to store entry
        let d_page = self.bpm.get_read_page(d_pid);
        let directory = HashTableDirectoryPage::deserialize(d_page.read());
        let b_index = directory.hash_to_bucket_index(hash);
        let b_pid = directory.get_bucket_page_id(b_index).unwrap();
        drop(d_page); // TODO: to make thread safe will need to hold onto this until sure we don't need to change directory

        // insert entry into bucket
        let mut b_page = self.bpm.get_write_page(b_pid);
        let mut bucket = HashTableBucketPage::<K, V>::deserialize(b_page.read());

        if !bucket.is_full() {
            bucket.insert(key, value).unwrap();
            b_page.write(bucket.serialize());
            drop(b_page);

            return Ok(());
        }

        // bucket is full
        let mut d_page = self.bpm.get_write_page(d_pid);
        let mut directory = HashTableDirectoryPage::deserialize(d_page.read());

        // TODO: assumes that b_index hasn't changed up until this point (might not be the case if directory was doubled)
        if directory.get_local_depth(b_index).unwrap() as u32 >= directory.global_depth() {
            directory.increment_global_depth()?;
        }

        // split the bucket
        let mut split_image_bucket = HashTableBucketPage::<K, V>::new_empty();

        // increase local depth or buckets
        let old_local_depth = directory.increment_local_depth(b_index).unwrap();
        let split_image_bucket_index = directory.get_split_image_index(b_index).unwrap();
        directory
            .set_local_depth(split_image_bucket_index, old_local_depth + 1)
            .unwrap();

        // set pointer in directory to split image
        let split_image_bucket_pid = self.bpm.new_page();
        directory
            .set_bucket_page_id(split_image_bucket_index, split_image_bucket_pid)
            .unwrap();

        // move entries that hash to the new bucket to the split image
        let mut entries_to_remove = vec![];
        for i in 0..bucket.size() {
            let (key, value) = bucket.entry_at(i).unwrap();
            let hash = self.hash(key);
            let index = directory.hash_to_bucket_index(hash);
            assert!(index == b_index || index == split_image_bucket_index);

            if index != b_index {
                entries_to_remove.push((key.clone(), value.clone()));
            }
        }

        for (k, v) in entries_to_remove {
            bucket.remove(k.clone());
            split_image_bucket.insert(k, v).unwrap();
        }

        // insert element into the bucket it hashes to
        let b_index = directory.hash_to_bucket_index(hash);
        let insert_bucket_pid = directory.get_bucket_page_id(b_index).unwrap();

        d_page.write(directory.serialize()); // directory no longer needed
        drop(d_page);

        assert!(insert_bucket_pid == b_pid || insert_bucket_pid == split_image_bucket_pid);
        if insert_bucket_pid == b_pid {
            bucket.insert(key, value).unwrap();
        } else {
            split_image_bucket.insert(key, value).unwrap();
        }

        // write all bucket and directory data
        b_page.write(bucket.serialize());
        drop(b_page);

        let mut split_image_bucket_page = self.bpm.get_write_page(split_image_bucket_pid);
        split_image_bucket_page.write(split_image_bucket.serialize());
        drop(split_image_bucket_page);

        Ok(())
    }

    fn new_empty_directory(&self) -> PageID {
        // create bucket page
        let empty_bucket_pid = self.bpm.new_page();
        let bucket = HashTableBucketPage::<K, V>::new_empty();

        let mut empty_bucket_page = self.bpm.get_write_page(empty_bucket_pid);
        empty_bucket_page.write(bucket.serialize());
        drop(empty_bucket_page);

        // create directory page
        let directory =
            HashTableDirectoryPage::new_empty(empty_bucket_pid, self.directory_max_depth);
        let empty_directory_pid = self.bpm.new_page();

        let mut empty_directory_page = self.bpm.get_write_page(empty_directory_pid);
        empty_directory_page.write(directory.serialize());
        drop(empty_directory_page);

        empty_directory_pid
    }

    /// Get values associated with `key`.
    pub fn lookup(&self, key: K) -> Vec<V> {
        let hash = self.hash(&key);

        let h_page = self.bpm.get_read_page(self.header_page_id);
        let header = HashTableHeaderPage::deserialize(h_page.read());
        drop(h_page);

        let d_index = header.hash_to_directory_page_index(hash);
        let d_pid = match header.get_directory_page_id(d_index) {
            Some(pid) => pid,
            None => return vec![],
        };
        let d_page = self.bpm.get_read_page(d_pid);
        let directory = HashTableDirectoryPage::deserialize(d_page.read());
        drop(d_page);

        let b_index = directory.hash_to_bucket_index(hash);
        let b_pid = directory.get_bucket_page_id(b_index).unwrap();
        let b_page = self.bpm.get_read_page(b_pid);
        let bucket = HashTableBucketPage::<K, V>::deserialize(b_page.read());
        drop(b_page);

        bucket
            .lookup(key)
            .iter()
            .map(|v| (*v).clone())
            .collect::<Vec<_>>()
    }

    /// Remove entries associated with `key` from the table. Returns the amount of entries that were removed.
    pub fn remove(&self, key: K) -> usize {
        let hash = self.hash(&key);

        let h_page = self.bpm.get_read_page(self.header_page_id);
        let header = HashTableHeaderPage::deserialize(h_page.read());
        drop(h_page);

        let d_index = header.hash_to_directory_page_index(hash);
        let d_pid = match header.get_directory_page_id(d_index) {
            Some(pid) => pid,
            None => return 0,
        };
        let d_page = self.bpm.get_read_page(d_pid);
        let directory = HashTableDirectoryPage::deserialize(d_page.read());
        drop(d_page);

        let b_index = directory.hash_to_bucket_index(hash);
        let b_pid = directory.get_bucket_page_id(b_index).unwrap();
        let b_page = self.bpm.get_read_page(b_pid);
        let mut bucket = HashTableBucketPage::<K, V>::deserialize(b_page.read());
        drop(b_page);

        // remove entries
        let removed_count = bucket.remove(key);
        if !bucket.is_empty() {
            return removed_count;
        }

        // try merging
        let d_page = self.bpm.get_write_page(d_pid);
        let mut directory = HashTableDirectoryPage::deserialize(d_page.read());

        // check local depths
        let mut split_image_index = directory.get_split_image_index(b_index).unwrap();
        let mut bucket_is_empty = bucket.is_empty(); // is the current merge result bucket empty?
        while bucket_is_empty {
            let bucket_ld = directory.get_local_depth(b_index).unwrap();
            let split_image_ld = directory.get_local_depth(split_image_index).unwrap();

            if bucket_ld != split_image_ld { // merge only possible when same local depths
                break;
            }

            // merge buckets
            self.bpm.delete_page(b_pid); // TODO: might need to hold onto the bucket lock up to this point for thread safety

            directory.decrement_local_depth(split_image_index).unwrap();
            directory
                .set_split_images_pointers_to_reference(split_image_index)
                .unwrap();


            // check if merge result is empty
            let split_image_pid = directory.get_bucket_page_id(split_image_index).unwrap();
            let split_image_page = self.bpm.get_read_page(split_image_pid);
            bucket_is_empty = HashTableBucketPage::<K, V>::deserialize(split_image_page.read()).is_empty();
            drop(split_image_page);

            split_image_index = directory.get_split_image_index(split_image_index).unwrap();
        }

        // try shrink if possible
        todo!()

        // todo write to dorectory page
        // todo drop directory page
    }

    /// Returns 32-bit hashed value of `key`.
    fn hash(&self, key: &K) -> u32 {
        murmur3_32(&mut Cursor::new(key.serialize()), 0).expect("Hashing error")
    }

    /// Prints whole hash table **for debugging**.
    pub fn print(&self) {
        let h_page = self.bpm.get_read_page(self.header_page_id);
        let header = HashTableHeaderPage::deserialize(h_page.read()); // header page
        drop(h_page);

        let mut directories_nodes = vec![];
        for i in 0..header.max_size() {
            let d_pid = header.get_directory_page_id(i);
            if let Some(d_pid) = d_pid {
                let d_page = self.bpm.get_read_page(d_pid);
                let directory = HashTableDirectoryPage::deserialize(d_page.read()); // directory page
                drop(d_page);

                let mut buckets_nodes = vec![];
                for j in 0..directory.size() {
                    let b_pid = directory.get_bucket_page_id(j).unwrap();
                    let b_page = self.bpm.get_read_page(b_pid);
                    let bucket = HashTableBucketPage::<K, V>::deserialize(b_page.read()); // bucket page
                    drop(b_page);

                    buckets_nodes.push(Tree::Leaf(vec![format!(
                        "[{}] pid: {} | d: {} (sz: {}/{})",
                        to_binary(j as u32, directory.global_depth()),
                        b_pid,
                        directory.get_local_depth(j).unwrap(),
                        bucket.size(),
                        bucket.max_size()
                    )
                    .to_string()]));
                }

                directories_nodes.push(Tree::Node(
                    format!(
                        "[{}] pid: {} | d: {}/{} (sz: {})",
                        to_binary(i as u32, self.header_max_depth),
                        d_pid,
                        directory.global_depth(),
                        directory.max_depth(),
                        directory.size()
                    )
                    .to_string(),
                    buckets_nodes,
                ));
            } else {
                directories_nodes.push(Tree::Leaf(vec!["None".to_string()]));
            }
        }

        let header_node = Tree::Node(
            format!(
                "[{}] pid: {} | d: {}",
                self.name,
                self.header_page_id,
                header.max_size()
            ),
            directories_nodes,
        );

        let mut output = String::new();
        write_tree(&mut output, &header_node).unwrap();
        println!("{output}");
    }
}

fn to_binary(n: u32, count: u32) -> String {
    if count == 0 {
        return "-".to_string();
    }

    // Extract the last `count` bits
    let mask = (1 << count) - 1; // Create a mask with the last `count` bits set to 1
    let bits = n & mask; // Get the last `count` bits of `n`

    // Convert to binary string and pad with leading zeros if needed
    let mut binary_str = format!("{:b}", bits);
    if binary_str.len() < count as usize {
        binary_str = format!("{:0>width$}", binary_str, width = count as usize);
    }

    binary_str
}
