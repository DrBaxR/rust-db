use crate::index::serial::{Deserialize, Serialize};

use super::HashTableBucketPage;

#[test]
fn new_max_size() {
    let bucket = HashTableBucketPage::new(vec![(1, 2)], 4, 4);
    assert_eq!(bucket.max_size, 510); // (i32, i32) = 8 => max_size = 4080 / 8 = 510

    let bucket = HashTableBucketPage::new(vec![(1u32, 2u8)], 4, 1);
    assert_eq!(bucket.max_size, 816); // (u32, u8) = 5 => max_size = 4080 / 5 = 816
}

#[test]
#[should_panic]
fn new_panic() {
    let _ = HashTableBucketPage::new([(1, 2); 512].to_vec(), 4, 4); // 512 > 510
}

#[test]
fn lookup() {
    let bucket = HashTableBucketPage::new(vec![(3, 4), (1, 2), (1, 5)], 4, 4);

    let res: Vec<i32> = bucket.lookup(1).iter().map(|v| **v).collect();
    assert_eq!(res, vec![2, 5]);

    let res: Vec<i32> = bucket.lookup(3).iter().map(|v| **v).collect();
    assert_eq!(res, vec![4]);

    let res: Vec<i32> = bucket.lookup(99).iter().map(|v| **v).collect();
    assert_eq!(res, vec![]);
}

#[test]
fn insert() {
    let mut bucket = HashTableBucketPage::<i32, i32>::new(vec![], 4, 4);
    bucket.insert(1, 2).unwrap();

    let res: Vec<i32> = bucket.lookup(1).iter().map(|v| **v).collect();
    assert_eq!(res, vec![2]);

    for i in 0..509 {
        bucket.insert(1, i).unwrap();
    }

    let res = bucket.insert(1, 999);
    assert!(res.is_err());
}

#[test]
fn remove() {
    let mut bucket = HashTableBucketPage::new(vec![(1, 1), (1, 2), (1, 3), (3, 4)], 4, 4);

    let removed = bucket.remove(1);
    assert_eq!(removed, 3);

    let removed = bucket.remove(4);
    assert_eq!(removed, 0);

    let removed = bucket.remove(3);
    assert_eq!(removed, 1);

    let removed = bucket.remove(1);
    assert_eq!(removed, 0);
}

#[test]
fn remove_at() {
    let mut bucket = HashTableBucketPage::new(vec![(1, 1), (1, 2), (1, 3), (3, 4)], 4, 4);

    let res = bucket.remove_at(1).unwrap();
    assert_eq!(res, (1, 2));
    assert_eq!(bucket.size(), 3);

    assert!(bucket.remove_at(3).is_none());
    assert!(bucket.remove_at(2).is_some());
}

#[test]
fn entry_key_value_at() {
    let bucket = HashTableBucketPage::new(vec![(1, 1), (1, 2), (1, 3), (3, 4)], 4, 4);

    assert_eq!(*bucket.entry_at(3).unwrap(), (3, 4));
    assert!(bucket.entry_at(4).is_none());

    assert_eq!(*bucket.key_at(3).unwrap(), 3);
    assert_eq!(*bucket.value_at(3).unwrap(), 4);
}

#[test]
fn serialization() {
    let bucket = HashTableBucketPage::new(vec![(1, 1), (1, 2), (1, 3), (3, 4)], 4, 4);
    let bucked_deserialized = HashTableBucketPage::<i32, i32>::deserialize(&bucket.serialize());

    assert_eq!(bucket.max_size, bucked_deserialized.max_size);
    assert_eq!(bucket.data, bucked_deserialized.data);
}
