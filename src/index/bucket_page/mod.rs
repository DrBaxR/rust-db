use crate::config::DB_PAGE_SIZE;

use super::{
    get_four_bytes_group,
    serial::{Deserialize, Serialize},
};

#[cfg(test)]
mod tests;

const HASH_TABLE_BUCKET_PAGE_DATA_SIZE: usize = 4088;

/// Bucket page for extendinble hashing index. Its structure looks like this on disk:
/// - `max_size` (0-3): The number of key-value pairs in bucket
/// - `size` (4-7): The max number of key-value pairs that the bucket can hold
/// - `data` (8-4095): The data of the key-value pairs stored, in an array form
///
/// # Note
/// This bucket supports **non-unique** keys.
#[derive(Debug)]
pub struct HashTableBucketPage<K, V>
where
    K: Serialize + Deserialize + Eq,
    V: Serialize + Deserialize,
{
    max_size: u32,
    data: Vec<(K, V)>,
}

impl<K, V> HashTableBucketPage<K, V>
where
    K: Serialize + Deserialize + Eq,
    V: Serialize + Deserialize,
{
    pub fn new(data: Vec<(K, V)>) -> Self {
        // using this instead of `size_of::<K, V>()` because of memory alignment, which is undesirable for serialization
        let pair_size = size_of::<K>() + size_of::<V>();
        let max_size = (HASH_TABLE_BUCKET_PAGE_DATA_SIZE / pair_size) as u32;

        let size = data.len() as u32;
        assert!(size <= max_size);

        Self { max_size, data }
    }

    pub fn new_empty() -> Self {
        HashTableBucketPage::<K, V>::new(vec![])
    }

    /// Returns the values associated to `key`.
    pub fn lookup(&self, key: K) -> Vec<&V> {
        self.data
            .iter()
            .filter(|(k, _)| *k == key)
            .map(|(_, v)| v)
            .collect()
    }

    /// Inserts the `key`-`value` pair.
    ///
    /// # Errors
    /// Will return `Err` if trying to insert when bucket is full.
    pub fn insert(&mut self, key: K, value: V) -> Result<(), ()> {
        if self.is_full() {
            return Err(());
        }

        self.data.push((key, value));

        Ok(())
    }

    /// Removes all elements associated with `key`. Returns how many elements were removed.
    pub fn remove(&mut self, key: K) -> usize {
        let mut count = 0;

        self.data.retain(|(k, _)| {
            if *k == key {
                count += 1;
                false
            } else {
                true
            }
        });

        count
    }

    /// Removes the entry at `index`. Will return the removed entry, or `None` if trying to index outside of bounds.
    pub fn remove_at(&mut self, index: usize) -> Option<(K, V)> {
        if index > self.size() - 1 {
            return None;
        }

        Some(self.data.remove(index))
    }

    /// Returns key at `index`.
    pub fn key_at(&self, index: usize) -> Option<&K> {
        Some(&self.entry_at(index)?.0)
    }

    /// Returns value at `index`.
    pub fn value_at(&self, index: usize) -> Option<&V> {
        Some(&self.entry_at(index)?.1)
    }

    /// Returns the entry a
    pub fn entry_at(&self, index: usize) -> Option<&(K, V)> {
        if index > self.size() - 1 {
            return None;
        }

        self.data.get(index)
    }

    /// Returns current number of key-value pairs in the bucket.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Returns max size that the header page can handle.
    pub fn max_size(&self) -> u32 {
        self.max_size
    }

    /// Returns `true` if the bucket is full.
    pub fn is_full(&self) -> bool {
        self.data.len() as u32 >= self.max_size
    }

    /// Returns `true` if the current size of the bucket is `0`.
    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }
}

impl<K, V> Serialize for HashTableBucketPage<K, V>
where
    K: Serialize + Deserialize + Eq,
    V: Serialize + Deserialize,
{
    fn serialize(&self) -> Vec<u8> {
        let mut data = vec![];

        data.extend_from_slice(&self.max_size.to_be_bytes());
        data.extend_from_slice(&(self.size() as u32).to_be_bytes()); // usize needs cast to u32 for serialization

        for (key, value) in self.data.iter() {
            data.extend_from_slice(&key.serialize());
            data.extend_from_slice(&value.serialize());
        }

        data.resize(DB_PAGE_SIZE as usize, 0);

        data
    }
}

const DATA_OFFSET: usize = 8;

impl<K, V> Deserialize for HashTableBucketPage<K, V>
where
    K: Serialize + Deserialize + Eq,
    V: Serialize + Deserialize,
{
    fn deserialize(data: &[u8]) -> Self {
        let max_size = u32::from_be_bytes(get_four_bytes_group(data, 0));
        let size = u32::from_be_bytes(get_four_bytes_group(data, 1));
        let mut entries = vec![];

        let key_size = size_of::<K>();
        let value_size = size_of::<V>();
        let entry_size = key_size + value_size;
        for i in 0..size {
            let pair_offset = DATA_OFFSET + i as usize * entry_size;

            let key = K::deserialize(&data[pair_offset..pair_offset + key_size]);
            let value = V::deserialize(&data[pair_offset + key_size..pair_offset + entry_size]);
            entries.push((key, value));
        }

        Self {
            max_size,
            data: entries,
        }
    }
}
