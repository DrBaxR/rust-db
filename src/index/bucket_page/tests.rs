use super::HashTableBucketPage;

#[test]
fn new_max_size() {
    let bucket = HashTableBucketPage::new(vec![(1, 2)]);
    assert_eq!(bucket.max_size, 511); // (i32, i32) = 8 => max_size = 4088 / 8 = 511

    let bucket = HashTableBucketPage::new(vec![(1u32, 2u8)]);
    dbg!(size_of::<(u32, u8)>());
    assert_eq!(bucket.max_size, 817); // (u32, u8) = 5 => max_size = 4088 / 5 = 817
}

#[test]
#[should_panic]
fn new_panic() {
    let _ = HashTableBucketPage::new([(1, 2); 512].to_vec()); // 512 > 511
}
