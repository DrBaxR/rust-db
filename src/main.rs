use b_tree::{node::BTreeNodeEntry, BTree};

pub mod b_tree;

fn main() {
    // entries
    let entry1 = BTreeNodeEntry::new(1, 1, None);
    let entry2 = BTreeNodeEntry::new(2, 2, None);
    let entry3 = BTreeNodeEntry::new(3, 3, None);

    // tree init
    let mut tree = BTree::new(3, None);
    tree.insert(entry3);
    tree.insert(entry1);
    tree.insert(entry2); // SHOULD SPLIT

    println!("{:?}", tree);
}
