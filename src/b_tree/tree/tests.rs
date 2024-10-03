use super::*;

#[test]
fn insert_in_empty() {
    let mut tree = BTree::new(2);

    assert_eq!(tree.root.keys, vec![]);
    tree.insert(1, 1);
    assert_eq!(tree.root.keys, vec![1]);
}

#[test]
fn insert_without_split_in_order() {
    let mut tree = BTree::new(2);

    tree.insert(5, 5);
    tree.insert(1, 1);
    assert_eq!(tree.root.keys, vec![1, 5]);
}

#[test]
fn insert_with_one_split() {
    let mut tree = BTree::new(2);
    tree.insert(5, 5);
    tree.insert(1, 1);

    assert_eq!(1, tree.root.depth());
    tree.insert(2, 2);

    assert_eq!(2, tree.root.depth());
    assert_eq!(2, tree.root.keys[0]);

}

#[test]
fn insert_with_two_splits() {
    let mut tree = BTree::new(2);

    tree.insert(5, 5);
    tree.insert(1, 1);
    tree.insert(4, 4);
    tree.insert(2, 2);
    tree.insert(3, 3);
    tree.insert(3, 3);
    assert_eq!(2, tree.root.depth());

    tree.insert(3, 3);
    assert_eq!(3, tree.root.depth());
    assert_eq!(3, tree.root.keys[0]);

}