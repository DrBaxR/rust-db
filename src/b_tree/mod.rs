use node::{BTreeNode, BTreeNodeEntry};

pub mod node;

pub struct BTree {
    order: usize,
    root: Option<BTreeNode>,
}

impl BTree {
    pub fn new(order: usize, root: Option<BTreeNode>) -> Self {
        Self { order, root }
    }

    pub fn print(&self) {
        println!("Tree order: {}", self.order);

        if let Some(root) = &self.root {
            println!("{}", root.to_string())
        } else {
            println!("Tree is empty");
        }
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

                todo!("Implement splitting of full leaf node")
            }
            None => {
                self.root = Some(BTreeNode::new(entry));
            }
        }
    }
}
