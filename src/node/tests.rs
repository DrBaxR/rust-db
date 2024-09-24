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

#[test]
fn push_with_children_empty_node() {
    let mut node = Node::new(2);
    node = node.push_with_children(1, 1, Some(node.clone()), Some(node.clone()));

    assert_eq!(node.keys, vec![1]);
}

#[test]
fn push_with_children_less_than_all() {
    let mut node = Node::new(2);
    node = node.push_with_children(2, 2, Some(node.clone()), Some(node.clone()));
    node = node.push_with_children(1, 1, Some(node.clone()), Some(node.clone()));

    assert_eq!(node.keys, vec![1, 2]);
}

#[test]
fn push_with_children_middle() {
    let mut node = Node::new(2);
    node = node.push_with_children(3, 3, Some(node.clone()), Some(node.clone()));
    node = node.push_with_children(1, 1, Some(node.clone()), Some(node.clone()));
    node = node.push_with_children(2, 2, Some(node.clone()), Some(node.clone()));

    assert_eq!(node.keys, vec![1, 2, 3]);
}

#[test]
fn push_with_children_greater_than_all() {
    let mut node = Node::new(2);
    node = node.push_with_children(1, 1, Some(node.clone()), Some(node.clone()));
    node = node.push_with_children(3, 3, Some(node.clone()), Some(node.clone()));

    assert_eq!(node.keys, vec![1, 3]);
}

#[test]
#[should_panic]
fn get_split_non_full() {
    let node = Node::new(2);
    node.get_split();
}

#[test]
fn get_split_regular() {
    let mut node = Node::new(2);
    node = node.push(1, 1);
    node = node.push(2, 2);
    node = node.push(3, 3);

    let split = node.get_split();

    assert_eq!(split.median, (2, 2));
    assert_eq!(split.left.keys, vec![1]);
    assert_eq!(split.right.keys, vec![3]);
}

#[test]
fn clone_with_replaced_node_regular() {
    // given
    let original_value = 2;
    let new_value = 69;

    let mut node = Node::new(2);
    let mut original_left = Node::new(2);
    original_left = original_left.push(original_value, 2);

    node = node.push_with_children(1, 1, Some(original_left), Some(node.clone()));

    // when
    let to_replace = node.edges[0].as_ref().unwrap();
    let mut replace_with = node.clone();
    replace_with.keys[0] = new_value;

    let new_node = node.clone_with_replaced_node(to_replace, &replace_with);

    // when
    let new_node_left = new_node.edges[0].as_ref().unwrap();
    assert_eq!(new_value, new_node_left.keys[0]);
}

#[test]
fn depth_leaf() {
    let node = Node::new(2);

    assert_eq!(1, node.depth());
}

#[test]
fn depth_one_layer() {
    let mut node = Node::new(2);
    let left = node.clone().push(1, 1);

    node = node.push_with_children(1, 1, Some(left), Some(node.clone()));

    assert_eq!(2, node.depth());
}

#[test]
fn find_node_with_present() {
    let node = Node::new(2).push(1, 1).push(2, 2);

    let result = node.find_node_with(2);
    assert!(result.is_some());
    assert!(std::ptr::addr_eq(&node, result.unwrap()))
}

#[test]
fn find_node_with_absent() {
    let node = Node::new(2).push(1, 1).push(2, 2);

    let result = node.find_node_with(3);
    assert!(result.is_none());
}

#[test]
fn find_node_with_present_in_child() {
    let left = Node::new(2).push(1, 1);
    let right = Node::new(2).push(3, 3);
    let node = Node::new(2).push_with_children(2, 2, Some(left), Some(right));

    let result = node.find_node_with(3);
    let expected = node.edges[1].as_ref().unwrap();
    assert!(result.is_some());
    assert!(std::ptr::addr_eq(expected, result.unwrap()));
}

#[test]
fn delete_entry_when_exists() {
    let node = Node::new(2).push(1, 2).push(3, 4);

    let (new_node, removed) = node.delete_entry(3);

    assert_eq!(new_node.keys.len(), 1);
    assert_eq!(new_node.values.len(), 1);
    assert_eq!(new_node.edges.len(), 2);

    assert!(new_node.contains(1));
    assert!(!new_node.contains(3));

    assert_eq!(removed.unwrap(), 4);
}

#[test]
fn delete_entry_when_not_exists() {
    let node = Node::new(2).push(1, 2).push(3, 4);

    let (new_node, removed) = node.delete_entry(5);

    assert_eq!(new_node.keys.len(), 2);
    assert_eq!(new_node.values.len(), 2);
    assert_eq!(new_node.edges.len(), 3);

    assert!(removed.is_none());
}

#[test]
fn get_right_child_exists() {
    let left = Node::new(2).push(1, 1);
    let right = Node::new(2).push(3, 3);
    let node = Node::new(2).push_with_children(2, 2, Some(left), Some(right));

    let right_child = node.get_right_child(2);
    
    assert!(std::ptr::addr_eq(right_child.unwrap(), node.edges[1].as_ref().unwrap()))
}

#[test]
fn get_right_child_not_exists() {
    let left = Node::new(2).push(1, 1);
    let right = Node::new(2).push(3, 3);
    let node = Node::new(2).push_with_children(2, 2, Some(left), Some(right));

    let right_child = node.get_right_child(99);
    
    assert!(right_child.is_none());
}

#[test]
fn largest_key_normal() {
    let node = Node::new(3).push(1, 1).push(2, 2).push(3, 3);

    assert_eq!(node.largest_key().unwrap(), 3);
}

#[test]
fn largest_key_empty() {
    let node = Node::new(3);

    assert!(node.largest_key().is_none());
}

#[test]
fn replace_entry_with_exists() {
    let node = Node::new(3).push(1, 1).push(2, 2).push(3, 3);

    let (new_node, old_value) = node.replace_entry_with(1, (6, 9)).expect("Replace entry should not return None when key is in node");

    assert_eq!(old_value, 1);
    assert_eq!(new_node.keys, vec![6, 2, 3]);
    assert_eq!(new_node.values, vec![9, 2, 3]);
}

#[test]
fn replace_entry_with_not_exists() {
    let node = Node::new(3).push(1, 1).push(2, 2).push(3, 3);

    let result = node.replace_entry_with(9, (4, 4));
    assert!(result.is_none());
}

#[test]
fn is_deficient() {
    let mut node = Node::new(3).push(1, 1);
    assert!(node.is_deficient());

    node = node.push(2, 2);
    assert!(node.is_deficient());
    
    node = node.push(3, 3);
    assert!(!node.is_deficient());

    node = node.push(4, 4);
    assert!(!node.is_deficient());
}

#[test]
#[should_panic]
fn get_siblings_of_not_child() {
    let left = Node::new(2).push(1, 1);
    let right = Node::new(2).push(3, 3);
    let node = Node::new(2).push_with_children(2, 2, Some(left), Some(right));

    let new = Node::new(2).push(4, 4);
    node.get_siblings_of(&new);
}

#[test]
fn get_siblings_of_one_sibling() {
    let left = Node::new(2).push(1, 1);
    let right = Node::new(2).push(3, 3);
    let node = Node::new(2).push_with_children(2, 2, Some(left), Some(right));

    let left = node.edges[0].as_ref().unwrap();
    let result = node.get_siblings_of(left);
    assert!(result.0.is_none());
    assert!(std::ptr::addr_eq(result.1.unwrap(), node.edges[1].as_ref().unwrap()));

    let right = node.edges[1].as_ref().unwrap();
    let result = node.get_siblings_of(right);
    assert!(std::ptr::addr_eq(result.0.unwrap(), node.edges[0].as_ref().unwrap()));
    assert!(result.1.is_none());
}

#[test]
fn get_siblings_of_two_siblings() {
    let left = Node::new(2).push(1, 1);
    let mut node = Node::new(2).push_with_children(2, 2, Some(left), None);
    let mid = Node::new(2).push(3, 3);
    let right = Node::new(2).push(5, 5);
    node = node.push_with_children(4, 4, Some(mid), Some(right));

    let children: Vec<usize> = node.edges.iter().map(|e| e.as_ref().unwrap().keys[0]).collect();
    assert_eq!(children, vec![1, 3, 5]);

    let mid = node.edges[1].as_ref().unwrap();
    let result = node.get_siblings_of(mid);
    assert!(std::ptr::eq(result.0.unwrap(), node.edges[0].as_ref().unwrap()));
    assert!(std::ptr::eq(result.1.unwrap(), node.edges[2].as_ref().unwrap()));
}

#[test]
fn get_rotated_left() {
    let left = Node::new(2).push(1, 1);
    let right = Node::new(2).push(3, 3).push(4, 4);
    let node = Node::new(2).push_with_children(2, 2, Some(left), Some(right));

    let left = node.edges[0].as_ref().unwrap();
    let right = node.edges[1].as_ref().unwrap();
    let rotated = node.get_rotated_left(left, right);

    assert_eq!(rotated.keys, vec![3]);
    assert_eq!(rotated.edges[0].as_ref().unwrap().keys, vec![1, 2]);
    assert_eq!(rotated.edges[1].as_ref().unwrap().keys, vec![4]);
}

#[test]
#[should_panic]
fn get_rotated_left_incorrect_order() {
    let left = Node::new(2).push(1, 1);
    let right = Node::new(2).push(3, 3).push(4, 4);
    let node = Node::new(2).push_with_children(2, 2, Some(left), Some(right));

    let left = node.edges[0].as_ref().unwrap();
    let right = node.edges[1].as_ref().unwrap();
    node.get_rotated_left(right, left);
}

#[test]
#[should_panic]
fn get_rotated_left_inexistent_nodes() {
    let left = Node::new(2).push(1, 1);
    let right = Node::new(2).push(3, 3).push(4, 4);
    let node = Node::new(2).push_with_children(2, 2, Some(left), Some(right));

    let left = Node::new(2);
    let right = Node::new(2);
    node.get_rotated_left(&right, &left);
}

#[test]
fn get_rotated_right() {
    let left = Node::new(2).push(1, 1).push(2, 2);
    let right = Node::new(2).push(4, 4);
    let node = Node::new(2).push_with_children(3, 3, Some(left), Some(right));

    let left = node.edges[0].as_ref().unwrap();
    let right = node.edges[1].as_ref().unwrap();
    let rotated = node.get_rotated_right(left, right);

    assert_eq!(rotated.keys, vec![2]);
    assert_eq!(rotated.values, vec![2]);
    assert_eq!(rotated.edges[0].as_ref().unwrap().keys, vec![1]);
    assert_eq!(rotated.edges[0].as_ref().unwrap().values, vec![1]);
    assert_eq!(rotated.edges[1].as_ref().unwrap().keys, vec![3, 4]);
    assert_eq!(rotated.edges[1].as_ref().unwrap().values, vec![3, 4]);
}