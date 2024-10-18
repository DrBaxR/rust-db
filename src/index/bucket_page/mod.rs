/// Bucket page for extendinble hashing index. Its structure looks like this on disk:
/// - `size` (0-3): The number of key-value pairs in bucket
/// - `max_size` (4-7): The max number of key-value pairs that the bucket can hold
/// - `data` (8-4095): The data of the key-value pairs stored, in an array form
pub struct HashTableBucketPage<K, V> {
    // TODO: make these constrained by traits
    size: u32,
    max_size: u32,
    data: Vec<(K, V)>,
}

impl<K, V> HashTableBucketPage<K, V> {
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
