use std::fmt::Debug;

use node::{BTreeNode, BTreeNodeEntry};

pub mod node;

pub struct BTree {
    order: usize,
    root: Option<BTreeNode>,
}

enum ParentInsertResult {
    NewParent(BTreeNode),
    NewRoot(BTreeNode),
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

    // TODO: review all insert logic again and make sure it makes sense
    pub fn insert(&mut self, entry: BTreeNodeEntry) {
        match self.root.take() {
            Some(mut root) => {
                let leaf_to_insert = root.find_insert_leaf(entry.key);
                leaf_to_insert.push_no_children(entry);

                // check number of elements and split if needed
                if leaf_to_insert.is_full(self.order) {
                    let leaf_split = leaf_to_insert.split_node(self.order);

                    let new_root = self.insert_into_parent(
                        leaf_to_insert,
                        leaf_split.median,
                        leaf_split.left,
                        leaf_split.right,
                    );

                    // TODO: update code to use new result of the insert_into_parent method
                    // case below represents case when NewRoot is returned
                    // for case when NewParent is returned, you will need to probably update the data held by the enum
                    // to identify which node you need to swap - or maybe not and use some find_parent to a reference
                    // (idk, brain fried)

                    // self.root = new_root.or(Some(root));
                } else {
                    self.root = Some(root);
                }
            }
            None => {
                self.root = Some(BTreeNode::new(entry));
            }
        }
    }

    // return last node that needed to be changed (from leaf to root); OR the new root
    fn insert_into_parent(
        &self,
        node: *const BTreeNode,
        mut elem: BTreeNodeEntry,
        left: BTreeNode,
        right: BTreeNode,
    ) -> ParentInsertResult {
        let order = self.order;
        // unwrap is acceptable, as this method only gets called if the tree has a root
        let parent = BTree::find_parent_of(self.root.as_ref().unwrap(), node);

        if let Some(parent) = parent {
            // has parent
            let mut new_parent = parent.clone();
            new_parent.push_with_children(elem, left, right);

            if new_parent.is_full(order) {
                let parent_split = new_parent.split_node(order);

                return self.insert_into_parent(
                    parent,
                    parent_split.median,
                    parent_split.left,
                    parent_split.right,
                );
            } else {
                return ParentInsertResult::NewParent(new_parent);
            }
        } else {
            // is root
            elem.right = Some(right);
            let mut new_root = BTreeNode::new(elem);
            new_root.left = Some(Box::new(left));

            return ParentInsertResult::NewRoot(new_root);
        }
    }

    // None => node points to tree root; assumes node is always part of the tree
    fn find_parent_of(root: &BTreeNode, node: *const BTreeNode) -> Option<&BTreeNode> {
        todo!()
    }
}
