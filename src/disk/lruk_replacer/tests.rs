use super::*;

#[test]
fn evict_no_evictables() {
    let mut replacer = LRUKReplacer::new(10, 2);
    
    // by default new frames are non-evictable
    replacer.record_access_at(1, 1).unwrap();
    replacer.record_access_at(2, 2).unwrap();

    let evicted = replacer.evict();
    assert!(evicted.is_none());
}

#[test]
fn evict_single_evictable() {
    let mut replacer = LRUKReplacer::new(10, 2);

    replacer.record_access_at(1, 1).unwrap();
    replacer.record_access_at(2, 2).unwrap();

    replacer.set_evictable(2, true).unwrap();

    let evicted = replacer.evict();
    assert_eq!(evicted.unwrap(), 2);
}

#[test]
fn evict_multiple_evictable_not_enough_accesses() {
    let mut replacer = LRUKReplacer::new(10, 2);

    replacer.record_access_at(1, 2).unwrap();
    replacer.record_access_at(2, 3).unwrap();
    replacer.record_access_at(3, 1).unwrap();

    replacer.set_evictable(1, true).unwrap();
    replacer.set_evictable(2, true).unwrap();
    replacer.set_evictable(3, true).unwrap();

    let evicted = replacer.evict();
    assert_eq!(evicted.unwrap(), 3);
}

#[test]
fn evict_multiple_evictable_enough_accesses() {
    let mut replacer = LRUKReplacer::new(10, 2);

    replacer.record_access_at(1, 2).unwrap();
    replacer.record_access_at(1, 10).unwrap(); // k-dist = 8

    replacer.record_access_at(2, 3).unwrap();
    replacer.record_access_at(2, 7).unwrap(); // k-dist = 4

    replacer.record_access_at(3, 1).unwrap();
    replacer.record_access_at(3, 8).unwrap(); // k-dist = 7

    replacer.set_evictable(1, true).unwrap();
    replacer.set_evictable(2, true).unwrap();
    replacer.set_evictable(3, true).unwrap();

    let evicted = replacer.evict();
    assert_eq!(evicted.unwrap(), 1);
}

#[test]
fn evict_complex() {
    let mut replacer = LRUKReplacer::new(10, 2);

    replacer.record_access_at(1, 2).unwrap();
    replacer.record_access_at(1, 10).unwrap(); // k-dist = 8, non-evictable

    replacer.record_access_at(2, 3).unwrap();
    replacer.record_access_at(2, 7).unwrap(); // k-dist = 4, evictable

    replacer.record_access_at(3, 1).unwrap();
    replacer.record_access_at(3, 8).unwrap(); // k-dist = 7, evictable

    replacer.record_access_at(4, 12).unwrap(); // LRU = 12, non-evictable
    replacer.record_access_at(5, 11).unwrap(); // LRU = 11, evictable
    replacer.record_access_at(6, 5).unwrap(); // LRU = 5, evictable

    replacer.set_evictable(2, true).unwrap();
    replacer.set_evictable(3, true).unwrap();
    replacer.set_evictable(5, true).unwrap();
    replacer.set_evictable(6, true).unwrap();

    let evicted = replacer.evict();
    assert_eq!(evicted.unwrap(), 3);
}

#[test]
fn size() {
    let mut replacer = LRUKReplacer::new(10, 2);
    
    replacer.record_access_at(1, 1).unwrap();
    replacer.record_access_at(2, 2).unwrap();

    replacer.set_evictable(1, true).unwrap();

    assert_eq!(replacer.size(), 1);
}

#[test]
fn record_access_full() {
    let mut replacer = LRUKReplacer::new(2, 2);
    
    replacer.record_access_at(1, 1).unwrap();
    replacer.record_access_at(2, 2).unwrap();

    assert!(replacer.record_access_at(2, 3).is_ok());
    assert!(replacer.record_access_at(3, 12).is_err());
}

#[test]
fn set_evictable_non_existent() {
    let mut replacer = LRUKReplacer::new(2, 2);
    
    assert!(replacer.set_evictable(1, true).is_err());
}

#[test]
fn remove_evictable() {
    let mut replacer = LRUKReplacer::new(2, 2);
    
    replacer.record_access_at(1, 1).unwrap();
    replacer.record_access_at(2, 2).unwrap();

    replacer.set_evictable(1, true).unwrap();
    replacer.set_evictable(2, true).unwrap();

    assert_eq!(replacer.size(), 2);
    assert!(replacer.remove(1).is_ok());
    assert_eq!(replacer.size(), 1);
}

#[test]
fn remove_non_evictable() {
    let mut replacer = LRUKReplacer::new(2, 2);
    
    replacer.record_access_at(1, 1).unwrap();
    replacer.record_access_at(2, 2).unwrap();

    assert!(replacer.remove(1).is_err());
}