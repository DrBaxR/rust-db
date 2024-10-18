use super::serial::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

const HASH_TABLE_BUCKET_PAGE_DATA_SIZE: usize = 4088;

/// Bucket page for extendinble hashing index. Its structure looks like this on disk:
/// - `size` (0-3): The number of key-value pairs in bucket
/// - `max_size` (4-7): The max number of key-value pairs that the bucket can hold
/// - `data` (8-4095): The data of the key-value pairs stored, in an array form
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

    pub fn lookup(&self, key: K) -> V {
        todo!()
    }

    pub fn insert(&mut self, key: K, value: V) {
        todo!()
    }

    pub fn remove(&mut self, key: K) -> Option<V> {
        todo!()
    }

    pub fn remove_at(&mut self, index: usize) -> Option<(K, V)> {
        todo!()
    }

    pub fn key_at(&self, index: usize) -> Option<&K> {
        todo!()
    }

    pub fn value_at(&self, index: usize) -> Option<&V> {
        todo!()
    }

    pub fn entry_at(&self, index: usize) -> Option<(&K, &V)> {
        todo!()
    }

    pub fn size(&self) -> u32 {
        todo!()
    }

    pub fn is_full(&self) -> bool {
        todo!()
    }

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