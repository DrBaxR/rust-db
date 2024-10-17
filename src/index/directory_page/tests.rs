use crate::index::directory_page::HashTableDirectoryPage;

#[test]
fn directory_serialization() {
    let header = HashTableDirectoryPage::new(vec![1, 2, 3], vec![2, 2, 2], 9, 10);

    let serialized_data = header.serialize();
    let header_deserialized = HashTableDirectoryPage::from_serialized(&serialized_data);

    assert_eq!(
        header.bucket_page_ids,
        header_deserialized
            .bucket_page_ids
            .iter()
            .filter(|e| **e != 0)
            .map(|e| *e)
            .collect::<Vec<u32>>()
    );
    assert_eq!(
        header.local_depths,
        header_deserialized
            .local_depths
            .iter()
            .filter(|e| **e != 0)
            .map(|e| *e)
            .collect::<Vec<u8>>()
    );
    assert_eq!(header.max_depth, header_deserialized.max_depth);
    assert_eq!(header.global_depth, header_deserialized.global_depth);
}

#[test]
fn hash_to_bucket_index() {
    // TODO: double-check logic, might need to take local depths into account
    let hash = 0x0000000b6u32; // ...00 10110110

    let header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);
    assert_eq!(header.hash_to_bucket_index(hash), 2);

    let header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 3);
    assert_eq!(header.hash_to_bucket_index(hash), 6);

    let header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 5);
    assert_eq!(header.hash_to_bucket_index(hash), 22);
}

#[test]
fn get_bucket_page_id() {
    let header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);

    assert_eq!(header.get_bucket_page_id(0).unwrap(), 1);
    assert_eq!(header.get_bucket_page_id(3).unwrap(), 4);
    assert!(header.get_bucket_page_id(4).is_none());
}

#[test]
fn set_bucket_page_id() {
    let mut header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);

    let res = header.set_bucket_page_id(0, 123).unwrap();
    assert_eq!(res, 1);

    assert!(header.set_bucket_page_id(12, 12).is_err());
}

#[test]
fn increment_decrement_global_depth() {
    let mut header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);

    assert_eq!(header.global_depth(), 2);

    header.increment_global_depth();
    header.increment_global_depth();
    assert_eq!(header.global_depth(), 4);

    header.decrement_global_depth();
    assert_eq!(header.global_depth(), 3);
}

#[test]
fn get_local_depth() {
    let header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);

    assert_eq!(header.get_local_depth(2).unwrap(), 7);
    assert!(header.get_local_depth(5).is_none());
}

#[test]
fn set_local_depth() {
    let mut header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);
    
    let res = header.set_local_depth(1, 12);
    assert_eq!(res.unwrap(), 6);
    assert!(header.set_local_depth(44, 44).is_err());
}

#[test]
fn increment_decrement_local_depth() {
    let mut header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);
    assert_eq!(header.get_local_depth(0).unwrap(), 5);

    let res = header.increment_local_depth(0);
    assert_eq!(res.unwrap(), 5);
    assert_eq!(header.get_local_depth(0).unwrap(), 6);

    assert!(header.increment_local_depth(12).is_err());

    let res = header.decrement_local_depth(0);
    assert_eq!(res.unwrap(), 6);
    assert_eq!(header.get_local_depth(0).unwrap(), 5);

    assert!(header.decrement_local_depth(44).is_err());
}

#[test]
fn is_valid() {
    let header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 10], 9, 2);
    assert!(!header.is_valid());

    let header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![1, 1, 1, 2], 9, 2);
    assert!(!header.is_valid());

    let header = HashTableDirectoryPage::new(vec![100, 100, 200, 300], vec![1, 1, 2, 2], 9, 2);
    assert!(header.is_valid());
}

#[test]
fn can_shrink() {
    let header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![1, 1, 1, 1], 9, 2);
    assert!(header.can_shrink());

    let header = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![1, 1, 1, 2], 9, 2);
    assert!(!header.can_shrink());
}
