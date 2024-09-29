use std::{thread, time::Duration};

use disk::LRUKReplacer;

mod node;
mod tree;
mod disk;

fn main() {
    // TODO: some tests
    let mut replacer = LRUKReplacer::new(100, 2);

    let _ = replacer.record_access(2);
    thread::sleep(Duration::from_millis(100));
    let _ = replacer.record_access(1);

    let _ = replacer.set_evictable(1, true);
    let _ = replacer.set_evictable(2, true);

    dbg!(replacer.evict());
}
