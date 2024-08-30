#[cfg(test)]
use super::*;

#[test]
fn is_leaf_if_empty() {
    let node = Node::new(2);

    assert!(node.is_leaf());
}

#[test]
fn is_leaf_if_no_children() {
    let mut node = Node::new(2);
    node = node.push(1, 1);

    assert!(node.is_leaf());
}

#[test]
fn is_not_leaf_if_has_child() {
    let mut node = Node::new(2);
    node = node.push_with_children(1, 1, Some(node.clone()), Some(node.clone()));

    assert!(!node.is_leaf());
}

#[test]
fn is_parent_of_false() {
    let node = Node::new(2);
    let not_child = node.clone();

    assert!(!node.is_parent_of(&not_child));
}

#[test]
fn is_parent_of_true() {
    let mut node = Node::new(2);
    node = node.push_with_children(1, 1, Some(node.clone()), None);

    let child = node.edges[0].as_ref().unwrap();

    assert!(node.is_parent_of(child));
}
