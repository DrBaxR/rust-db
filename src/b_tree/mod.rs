use std::fmt::Debug;

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
                leaf_to_insert.push(entry);

                // check number of elements and split if needed
                if !leaf_to_insert.is_full(self.order) {
                    return;
                }

                // todo!("Implement splitting of full leaf node")
            }
            None => {
                self.root = Some(BTreeNode::new(entry));
            }
        }
    }

    // TODO: remove this method
    pub fn split_root(self) {
        let split = self.root.unwrap().split_node(self.order);

        println!("median: {:?}", split.median);
        println!("left: {:?}", split.left);
        println!("right: {:?}", split.right);
    }
}
