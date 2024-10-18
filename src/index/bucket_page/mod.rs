use super::serial::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

const HASH_TABLE_BUCKET_PAGE_DATA_SIZE: usize = 4088;

/// Bucket page for extendinble hashing index. Its structure looks like this on disk:
/// - `size` (0-3): The number of key-value pairs in bucket
/// - `max_size` (4-7): The max number of key-value pairs that the bucket can hold
/// - `data` (8-4095): The data of the key-value pairs stored, in an array form
///
/// # Note
/// This bucket supports **non-unique** keys.
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

    /// Returns `true` if the bucket is full.
    pub fn is_full(&self) -> bool {
        self.data.len() as u32 >= self.max_size
    }

    /// Returns `true` if the current size of the bucket is `0`.
    pub fn is_empty(&self) -> bool {
        todo!()
    }
}

impl<K, V> Serialize for HashTableBucketPage<K, V>
where
    K: Serialize + Deserialize + Eq,
    V: Serialize + Deserialize,
{
    fn serialize(&self) -> Vec<u8> {
        todo!()
    }
}

impl<K, V> Deserialize for HashTableBucketPage<K, V>
where
    K: Serialize + Deserialize + Eq,
    V: Serialize + Deserialize,
{
    fn deserialize(data: &[u8]) -> Self {
        todo!()
    }
}
