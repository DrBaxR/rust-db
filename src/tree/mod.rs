use crate::node::{Node, NodeSplit};

#[cfg(test)]
mod tests;

pub struct BTree {
    root: Node,
    b: usize,
}

enum NodeReplace<'a> {
    Node(Node, &'a Node), // what node you need to replace with what value
    Root(Node),           // root needs to be replaced with value
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

    /// Remove element with `key` from `self`.
    pub fn remove(&mut self, key: usize) {
        // delete from leaf
            // search for value to delete
        // self.root.find_node_with(key);
            // if value in leaf delete
            // if underflow, rebalance

        // delete from internal node
            // choose largest element in left or smallest element in right, remove from subtree and replace instead of deleted element
            // if deleting from leaf follow above

        // rebalance after delete
            // if deficient right sibling exists and has mode than min elements => rotate left
                // copy separator from parent and move to deficient node
                // replace separator of parent with first element in right sibling
            // else if left exists and has more than min elements => rotate right
                // move parent separator to deficient node
                // replace separator with last element from left node
            // else merge with one of siblings, sandwitching their separator taken from parent
                // copy separator to end of left node
                // move elements from right node to left node
                // remove separator from parent along with empty right child
                    // if parent is root and now has no elements, make child new root
                    // if parent has fewer elements than min, rebalance
    }
}
