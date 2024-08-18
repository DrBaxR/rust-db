use node::{BTreeNode, BTreeNodeEntry, BTreeNodeSearchError};

pub mod node;

pub struct BTree {
    order: i64,
    root: Option<BTreeNode>,
}

impl BTree {
    pub fn new(order: i64, root: Option<BTreeNode>) -> Self {
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
                // find leaf to insert into
                let mut current_node = root;

                // TODO: (2) extract and make recursive
                while !current_node.is_leaf() {
                    current_node = match current_node.get_greater_than_index(entry.key) {
                        Ok(greater_index) => {
                            if greater_index == 0 {
                                current_node.left.as_mut().unwrap().as_mut()
                            } else {
                                let left_element =
                                    current_node.data.get_mut(greater_index - 1).unwrap();

                                left_element.right.as_mut().unwrap()
                            }
                        }
                        Err(BTreeNodeSearchError::GreaterThanAll) => {
                            let left_element = current_node.data.last_mut().unwrap();

                            left_element.right.as_mut().unwrap()
                        }
                    }
                }

                // push in leaf
                current_node.push(entry);
            }
            None => {
                self.root = Some(BTreeNode::new(entry));
            }
        }
    }
}
