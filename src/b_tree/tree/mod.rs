use super::node::{Node, NodeSplit};

#[cfg(test)]
mod tests;

pub struct BTree {
    root: Node,
    b: usize,
}

enum NodeReplace {
    Node(Node, *const Node), // what node you need to replace with what value
    Root(Node),              // root needs to be replaced with value
}

impl BTree {
    pub fn new(b: usize) -> Self {
        Self {
            root: Node::new(b),
            b,
        }
    }

    /// Insert `key` -> `value` pair into tree.
    pub fn insert(&mut self, key: usize, value: usize) {
        // find leaf to insert into
        let node_to_insert = self.root.find_leaf_for(key);
        // insert inside it
        let new_node_to_insert = node_to_insert.push(key, value);
        if !new_node_to_insert.is_full() {
            self.root =
                self.get_root_after_replace(NodeReplace::Node(new_node_to_insert, node_to_insert));
            return;
        }

        // if full split in median, left and right
        let split_node = new_node_to_insert.get_split();

        // insert median in parent (potentiallt split again, again, ...)
        let node_replace = self.insert_split_in_parent(&node_to_insert, split_node);
        self.root = self.get_root_after_replace(node_replace);
    }

    // constructs a new node subtree that represents the correct result post insert
    // return value represents instructions of where you need to replace a node to have a correct post-insert tree
    /// Inserts a `NodeSplit` into the parent of `current` from the `self` tree.
    ///
    /// # Details
    /// It builds a new node subtree that represents correct retulst post insertion into parent. The return value
    /// represents information about what needs to be replaced into `self` with the newly formed subtree.
    fn insert_split_in_parent(&self, current: &Node, split: NodeSplit) -> NodeReplace {
        // find parent of node
        let parent = self.find_parent(current);

        // insert median in the parent
        if let Some(parent) = parent {
            // regular node
            let new_parent = parent.push_with_children(
                split.median.0,
                split.median.1,
                Some(split.left),
                Some(split.right),
            );

            // if not full, return NodeReplace
            if !new_parent.is_full() {
                return NodeReplace::Node(new_parent, parent);
            }

            // if full, split
            let new_split = new_parent.get_split();

            // recurse
            return self.insert_split_in_parent(parent, new_split);
        } else {
            // root
            let mut new_root = Node::new(self.b);
            new_root = new_root.push_with_children(
                split.median.0,
                split.median.1,
                Some(split.left),
                Some(split.right),
            );

            return NodeReplace::Root(new_root);
        };
    }

    /// Return reference to node that is parent of `node`, or `None` if `node` is the root of the tree.
    ///
    /// # Panics
    /// Panics if `node` is not in tree.
    fn find_parent(&self, node: &Node) -> Option<&Node> {
        if std::ptr::addr_eq(&self.root, node) {
            return None;
        }

        let parent = self.root.find_parent_of(node);
        if parent.is_none() {
            panic!("Node has no parent in tree");
        }

        parent
    }

    /// Return new root of tree, after the `replace` has been applied in `self`.
    fn get_root_after_replace(&self, replace: NodeReplace) -> Node {
        match replace {
            NodeReplace::Node(node, to_replace) => {
                self.root.clone_with_replaced_node(to_replace, &node)
            }
            NodeReplace::Root(root) => root,
        }
    }

    pub fn print_tree(&self) {
        self.root.print_node(0);
    }

    /// Remove element with `key` from `self`. Returns `(key, value)` that was removed if node exists in tree, or `Err` otherwise.
    pub fn remove(&mut self, key: usize) -> Result<(usize, usize), ()> {
        let node_with_key = self.root.find_node_with(key);

        if node_with_key.is_none() {
            return Err(());
        }

        let found = node_with_key.unwrap();
        if found.is_leaf() {
            let (new_leaf, deleted_value) = found.delete_entry(key);
            let replace = self.rebalance_node(&found, new_leaf);

            self.root = self.get_root_after_replace(replace);

            return deleted_value.map(|v| Ok((key, v))).unwrap_or(Err(()));
        } else {
            // unwrap is fine, because found is the node that contains the key
            let right_child = found.get_right_child(key).unwrap();

            let largest_key_right = right_child.smallest_key();
            if largest_key_right.is_none() {
                return Err(());
            }
            let largest_key_right = largest_key_right.unwrap();

            // unwrap is fine, because on recursive calls it's not possible to have element not exist in tree
            let replace_with = self.remove(largest_key_right).unwrap();

            // this is pretty bad, but couldn't think of a way to please the borrow checker
            let found = self.root.find_node_with(key).unwrap();
            let (found_replaced, replaced_value) = found
                .replace_entry_with(key, replace_with)
                .expect("Found node should have the key to replace inside it");

            self.root = self.get_root_after_replace(NodeReplace::Node(found_replaced, found));

            return Ok((key, replaced_value));
        }
    }

    /// Build a new subtree, starting from the `start_node`, where no nodes have fewer than the minimum amount of entries. The `node_replacement`
    /// indicates the new value of the `start_node` (post-removal of leaf entry).
    /// Returns a `NodeReplace` that indicates what needs to be replaced in the tree in order to have it balanced.
    ///
    /// # Asserts
    /// Asserts that `start_node` is a valid pointer to a node in the tree.
    fn rebalance_node(&self, start_node: &Node, node_replacement: Node) -> NodeReplace {
        if !node_replacement.is_deficient() {
            return NodeReplace::Node(node_replacement, start_node);
        }

        let parent = self.find_parent(start_node);
        if parent.is_none() {
            return NodeReplace::Root(node_replacement);
        }

        let parent = parent.unwrap();
        let start_node_index = parent.get_child_index(start_node);

        let mut new_parent = parent.clone();
        let _ = std::mem::replace(&mut new_parent.edges[start_node_index], Some(node_replacement));
        let node_replacement = new_parent.edges[start_node_index].as_ref().unwrap();

        let (left_sibling, right_sibling) = new_parent.get_siblings_of(node_replacement);
        if let Some(right_sibling) = right_sibling {
            if !right_sibling.is_deficient() {
                let new_parent = new_parent.get_rotated_left(node_replacement, right_sibling);
                return NodeReplace::Node(new_parent, parent);
            }
        }

        if let Some(left_sibling) = left_sibling {
            if !left_sibling.is_deficient() {
                let new_parent = new_parent.get_rotated_right(left_sibling, node_replacement);
                return NodeReplace::Node(new_parent, parent);
            }
        }

        // sandwitch
        let (merge_left, merge_right) = if let Some(right_sibling) = right_sibling {
            (node_replacement, right_sibling)
        } else {
            (
                left_sibling.expect("At least one of the siblings must be present"),
                node_replacement,
            )
        };

        let mut new_parent = new_parent.get_sandwitched_for(merge_left, merge_right);
        if !new_parent.is_deficient() {
            // the good ending
            return NodeReplace::Node(new_parent, parent);
        }

        if std::ptr::addr_eq(parent, &self.root) && new_parent.keys.len() == 0 {
            let only_child = new_parent.edges.remove(0).expect("When post-sandwitch the new parent is the root and empty, should have 1 single child in the first edge");
            NodeReplace::Root(only_child)
        } else {
            // after sandwitching the two siblings, now the parent of them is deficient => recursive call to rebalance
            self.rebalance_node(parent, new_parent)
        }
    }
}
