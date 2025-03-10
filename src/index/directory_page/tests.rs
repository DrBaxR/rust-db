use crate::index::{
    directory_page::HashTableDirectoryPage,
    serial::{Deserialize, Serialize},
};

#[test]
fn directory_serialization() {
    let directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![2, 2, 2, 2], 9, 2);

    let serialized_data = directory.serialize();
    let directory_deserialized = HashTableDirectoryPage::deserialize(&serialized_data);

    assert_eq!(
        directory.bucket_page_ids,
        directory_deserialized.bucket_page_ids
    );
    assert_eq!(directory.local_depths, directory_deserialized.local_depths);
    assert_eq!(directory.max_depth, directory_deserialized.max_depth);
    assert_eq!(directory.global_depth, directory_deserialized.global_depth);
}

#[test]
fn hash_to_bucket_index() {
    let hash = 0x0000000b6u32; // ...00 10110110

    let directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);
    assert_eq!(directory.hash_to_bucket_index(hash), 2);

    let directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 3);
    assert_eq!(directory.hash_to_bucket_index(hash), 6);

    let directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 5);
    assert_eq!(directory.hash_to_bucket_index(hash), 22);
}

#[test]
fn get_bucket_page_id() {
    let directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);

    assert_eq!(directory.get_bucket_page_id(0).unwrap(), 1);
    assert_eq!(directory.get_bucket_page_id(3).unwrap(), 4);
    assert!(directory.get_bucket_page_id(4).is_none());
}

#[test]
fn set_bucket_page_id() {
    let mut directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);

    let res = directory.set_bucket_page_id(0, 123).unwrap();
    assert_eq!(res, 1);

    assert!(directory.set_bucket_page_id(12, 12).is_err());
}

#[test]
fn increment_global_depth() {
    let mut directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);
    assert_eq!(directory.global_depth(), 2);

    directory.increment_global_depth().unwrap();
    assert_eq!(directory.global_depth(), 3);

    assert_eq!(directory.bucket_page_ids, vec![1, 2, 3, 4, 1, 2, 3, 4]);
    assert_eq!(directory.local_depths, vec![5, 6, 7, 8, 5, 6, 7, 8]);
}

#[test]
fn increment_global_depth_error() {
    let mut directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 2, 2);

    assert!(directory.increment_global_depth().is_err());
}

#[test]
fn decrement_global_depth() {
    let mut directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);
    assert_eq!(directory.global_depth(), 2);

    directory.decrement_global_depth().unwrap();
    assert_eq!(directory.global_depth(), 1);

    assert_eq!(directory.bucket_page_ids, vec![1, 2]);
    assert_eq!(directory.local_depths, vec![5, 6]);
}

#[test]
fn get_local_depth() {
    let directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);

    assert_eq!(directory.get_local_depth(2).unwrap(), 7);
    assert!(directory.get_local_depth(5).is_none());
}

#[test]
fn set_local_depth() {
    let mut directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);

    let res = directory.set_local_depth(1, 12);
    assert_eq!(res.unwrap(), 6);
    assert!(directory.set_local_depth(44, 44).is_err());
}

#[test]
fn increment_decrement_local_depth() {
    let mut directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 8], 9, 2);
    assert_eq!(directory.get_local_depth(0).unwrap(), 5);

    let res = directory.increment_local_depth(0);
    assert_eq!(res.unwrap(), 5);
    assert_eq!(directory.get_local_depth(0).unwrap(), 6);

    assert!(directory.increment_local_depth(12).is_err());

    let res = directory.decrement_local_depth(0);
    assert_eq!(res.unwrap(), 6);
    assert_eq!(directory.get_local_depth(0).unwrap(), 5);

    assert!(directory.decrement_local_depth(44).is_err());
}

#[test]
fn is_valid() {
    let directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![5, 6, 7, 10], 9, 2);
    assert!(!directory.is_valid());

    let directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![1, 1, 1, 2], 9, 2);
    assert!(!directory.is_valid());

    let directory = HashTableDirectoryPage::new(vec![100, 100, 200, 300], vec![1, 1, 2, 2], 9, 2);
    assert!(directory.is_valid());
}

#[test]
fn can_shrink() {
    let directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![1, 1, 1, 1], 9, 2);
    assert!(directory.can_shrink());

    let directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![1, 1, 1, 2], 9, 2);
    assert!(!directory.can_shrink());
}

#[test]
fn get_split_image_index() {
    // represents hash table from this image: https://media.geeksforgeeks.org/wp-content/uploads/20190803104123/hash101.png
    let directory = HashTableDirectoryPage::new(
        vec![1, 5, 3, 5, 2, 5, 4, 5],
        vec![3, 1, 3, 1, 3, 1, 3, 1],
        9,
        3,
    );
    assert!(directory.is_valid());
    assert!(directory.get_split_image_index(8).is_none());

    assert_eq!(directory.get_split_image_index(6).unwrap(), 2);
    assert_eq!(directory.get_split_image_index(2).unwrap(), 6);

    assert_eq!(directory.get_split_image_index(0).unwrap(), 4);
    assert_eq!(directory.get_split_image_index(7).unwrap(), 6);
}

#[test]
fn set_split_images_pointers_to_reference() {
    let mut directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4], vec![2, 2, 2, 1], 9, 2);

    assert!(directory.set_split_images_pointers_to_reference(4).is_err());
    assert!(directory.set_split_images_pointers_to_reference(3).is_ok());
    assert_eq!(directory.bucket_page_ids, vec![1, 4, 3, 4]);
    assert_eq!(directory.local_depths, vec![2, 1, 2, 1]);

    let mut directory = HashTableDirectoryPage::new(vec![1, 2, 3, 4, 5, 6, 7, 8], vec![3, 2, 1, 1, 3, 2, 2, 1], 9, 3);

    assert!(directory.set_split_images_pointers_to_reference(7).is_ok());
    assert_eq!(directory.bucket_page_ids, vec![1, 8, 3, 8, 5, 8, 7, 8]);
    assert_eq!(directory.local_depths, vec![3, 1, 1, 1, 3, 1, 2, 1]);

    assert!(directory.set_split_images_pointers_to_reference(6).is_ok());
    assert_eq!(directory.bucket_page_ids, vec![1, 8, 7, 8, 5, 8, 7, 8]);
    assert_eq!(directory.local_depths, vec![3, 1, 2, 1, 3, 1, 2, 1]);

    assert!(directory.set_split_images_pointers_to_reference(4).is_ok());
    assert_eq!(directory.bucket_page_ids, vec![1, 8, 7, 8, 5, 8, 7, 8]);
    assert_eq!(directory.local_depths, vec![3, 1, 2, 1, 3, 1, 2, 1]);
}