use std::{fmt::Debug, ptr};

use node::{BTreeNode, BTreeNodeEntry};

pub mod node;

pub struct BTree {
    order: usize,
    root: Option<BTreeNode>,
}

impl Debug for BTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl BTree {
    pub fn new(order: usize, root: Option<BTreeNode>) -> Self {
        Self { order, root }
    }

    fn to_string(&self) -> String {
        let mut string = String::new();
        string.push_str(format!("Tree order: {}\n", self.order).as_str());

        if let Some(root) = &self.root {
            string.push_str(format!("{:?}\n", root).as_str());
        } else {
            string.push_str("Tree is empty");
        }

        string
    }

    // find leaf to insert into
    // push in leaf
    // if node not full => DONE
    // if node full after push =>
    // 1. find median
    // 2. split node into left and right nodes (before and after median)
    // 3. insert median into parent (may cause split and so on)
    pub fn insert(&mut self, entry: BTreeNodeEntry) {
        match &mut self.root {
            Some(root) => {
                let leaf_to_insert = root.find_insert_leaf(entry.key);
                leaf_to_insert.push_no_children(entry);

                // check number of elements and split if needed
                if !leaf_to_insert.is_full(self.order) {
                    return;
                }

                let leaf_split = leaf_to_insert.split_node(self.order);
                // self.insert_into_parent(leaf_to_insert, leaf_split.median, leaf_split.left, leaf_split.right);
            }
            None => {
                self.root = Some(BTreeNode::new(entry));
            }
        }
    }

    // TODO: return new root?/update self.root
    fn insert_into_parent(&mut self, node: *const BTreeNode, elem: BTreeNodeEntry, left: BTreeNode, right: BTreeNode) {
        let order = self.order;
        let parent = self.find_parent(node);

        // case when parent exists
        parent.push_with_children(elem, left, right);
        if !parent.is_full(order) {
            return;
        }
        
        let parent_split = parent.split_node(order);
        self.insert_into_parent(parent, parent_split.median, parent_split.left, parent_split.right);
    }

    // TODO: make it return an option, None in case node is root
    fn find_parent(&self, node: *const BTreeNode) -> &mut BTreeNode {
        todo!()
    }
}
