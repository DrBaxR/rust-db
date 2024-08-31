use super::*;

#[test]
fn insert_in_empty() {
    let mut tree = BTree::new(2);

    assert_eq!(tree.root.keys(), vec![]);
    tree.insert(1, 1);
    assert_eq!(tree.root.keys(), vec![1]);
}

#[test]
fn insert_without_split_in_order() {
    let mut tree = BTree::new(2);

    tree.insert(5, 5);
    tree.insert(1, 1);
    assert_eq!(tree.root.keys(), vec![1, 5]);
}

// TODO: test for splitting once

// TODO: test for splitting twice