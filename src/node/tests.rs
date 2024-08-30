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

#[test]
fn find_parent_of_node_in_subtree() {
    let mut node = Node::new(2);
    node = node.push_with_children(1, 1, Some(node.clone()), Some(node.clone()));

    let child_left = node.edges[0].as_ref().unwrap();
    let child_right = node.edges[1].as_ref().unwrap();

    let left_parent = node.find_parent_of(&child_left).unwrap();
    let right_parent = node.find_parent_of(&child_right).unwrap();

    assert!(std::ptr::addr_eq(left_parent, &node));
    assert!(std::ptr::addr_eq(right_parent, &node));
}

#[test]
fn find_parent_of_node_not_in_subtree() {
    let node = Node::new(2);
    let inexistent_child = Node::new(2);

    let parent = node.find_parent_of(&inexistent_child);

    assert!(parent.is_none());
}

#[test]
fn is_full_on_empty_node() {
    let node = Node::new(2);
    assert!(!node.is_full());
}

#[test]
fn is_full_on_non_full_node() {
    let mut node = Node::new(2);
    node = node.push(1, 1);

    assert!(!node.is_full());
}

#[test]
fn is_full_on_full_node() {
    let mut node = Node::new(2); // b = 2 means that max elements < 2*b-1=3
    node = node.push(1, 1);
    node = node.push(2, 2);
    node = node.push(3, 3);

    assert!(node.is_full());
}

#[test]
fn find_leaf_for_node_with_no_children() {
    let mut node = Node::new(2);
    node = node.push(1, 1);

    let insert_in = node.find_leaf_for(2);
    assert!(std::ptr::eq(&node, insert_in));
}

#[test]
fn find_leaf_for_node_with_child() {
    // given
    let mut node = Node::new(2);

    let left = Node::new(2);
    left.push(1, 1);

    let right = Node::new(2);
    right.push(3, 3);

    node = node.push_with_children(2, 2, Some(left), Some(right));

    // when
    let insert_in = node.find_leaf_for(1);

    // then
    let left = node.edges[0].as_ref().unwrap();
    assert!(std::ptr::eq(insert_in, left));
}

#[test]
fn find_leaf_for_node_with_child_right() {
    // given
    let mut node = Node::new(2);

    let left = Node::new(2);
    left.push(1, 1);

    let right = Node::new(2);
    right.push(3, 3);

    node = node.push_with_children(2, 2, Some(left), Some(right));

    // when
    let insert_in = node.find_leaf_for(4);

    // then
    let right = node.edges[1].as_ref().unwrap();
    assert!(std::ptr::eq(insert_in, right));
}