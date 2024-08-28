use std::fmt::Debug;

use crate::node::{Node, NodeSplit};

pub struct BTree {
    root: Node,
    b: usize,
}

enum NodeReplace<'a> {
    Node(Node, &'a Node), // what node you need to replace with what value
    Root(Node),           // root needs to be replaced with value
}

impl Debug for BTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "BTree with B = {}\nRoot:\n{:?}", self.b, self.root)
    }
}

// TODO: figure out how to do rust docs
impl BTree {
    pub fn new(b: usize) -> Self {
        Self {
            root: Node::new(b),
            b,
        }
    }

    pub fn insert(&mut self, key: usize, value: usize) {
        // find leaf to insert into
        let node_to_insert = self.root.find_leaf_for(key);
        // insert inside it
        // TODO: make push dirrectly return Option<NodeSplit>, if node needs to split
        let new_node_to_insert = node_to_insert.push(key, value);
        if !new_node_to_insert.is_full() {
            self.root = self.get_root_after_replace(NodeReplace::Node(new_node_to_insert, node_to_insert));
            return;
        }

        // if full split in median, left and right
        let split_node = new_node_to_insert.get_split();

        // insert median in parent (potentiallt split again, again, ...)
        // TODO: make insert_split_in_parent return dirrectly new root
        let node_replace = self.insert_split_in_parent(&new_node_to_insert, split_node);
        self.root = self.get_root_after_replace(node_replace);
    }

    // constructs a new node subtree that represents the correct result post insert
    // return value represents instructions of where you need to replace a node to have a correct post-insert tree
    fn insert_split_in_parent(&self, current: &Node, split: NodeSplit) -> NodeReplace {
        // find parent of node
        let parent = self.find_parent(current);

        // insert median in the parent
        if let Some(parent) = parent {
            // regular node
            let new_parent = parent.push_with_children(split.median.0, split.median.1, Some(split.left), Some(split.right));

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
            new_root = new_root.push_with_children(split.median.0, split.median.1, Some(split.left), Some(split.right));

            return NodeReplace::Root(new_root);
        };
    }

    // return None if node is root
    // panic if node is NOT in tree
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

    // returns new root after replace is applied
    fn get_root_after_replace(&self, replace: NodeReplace) -> Node {
        match replace {
            NodeReplace::Node(node, to_replace) => {
                self.root.clone_with_replaced_node(to_replace, &node)
            },
            NodeReplace::Root(root) => root,
        }
    }
}

// TODO: tests
