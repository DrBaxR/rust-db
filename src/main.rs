struct BTree {
    order: i64,
    root: Option<BTreeNode>,
}

impl BTree {
    fn new(order: i64, root: Option<BTreeNode>) -> Self {
        Self { order, root }
    }

    fn print(&self) {
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
    fn insert(&mut self, entry: BTreeNodeEntry) {
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
                self.root = Some(BTreeNode::from_entry(entry));
            }
        }
    }
}

struct BTreeNode {
    left: Option<Box<BTreeNode>>,
    data: Vec<BTreeNodeEntry>,
}

enum BTreeNodeSearchError {
    GreaterThanAll,
}

impl BTreeNode {
    fn new() -> Self {
        Self {
            left: None,
            data: vec![],
        }
    }

    fn from_entry(entry: BTreeNodeEntry) -> Self {
        Self {
            left: None,
            data: vec![entry],
        }
    }

    fn to_string(&self) -> String {
        let mut res = String::new();
        // current node
        res.push_str("[ ");
        for entry in &self.data {
            res.push_str(&entry.to_string());
            res.push(' ');
        }
        res.push_str(" ]\n\t");

        // children
        if let Some(left) = &self.left {
            res.push_str(&left.to_string());
        }

        for entry in &self.data {
            if let Some(right) = &entry.right {
                res.push_str(&right.to_string());
            }
        }

        res
    }

    fn is_leaf(&self) -> bool {
        if let Some(_) = &self.left {
            return false;
        }

        for entry in &self.data {
            if let Some(_) = &entry.right {
                return false;
            }
        }

        true
    }

    fn get_greater_than_index(&self, search_key: i64) -> Result<usize, BTreeNodeSearchError> {
        for (i, entry) in self.data.iter().enumerate() {
            if entry.key > search_key {
                return Ok(i);
            }
        }

        Err(BTreeNodeSearchError::GreaterThanAll)
    }

    fn push(&mut self, entry: BTreeNodeEntry) {
        let greater_index = match self.get_greater_than_index(entry.key) {
            Ok(index) => index,
            Err(BTreeNodeSearchError::GreaterThanAll) => self.data.len() - 1,
        };

        self.data.insert(greater_index, entry);
    }
}

struct BTreeNodeEntry {
    key: i64,
    value: i64,
    right: Option<BTreeNode>,
}

impl BTreeNodeEntry {
    fn new(key: i64, value: i64, right: Option<BTreeNode>) -> Self {
        Self { key, value, right }
    }

    fn to_string(&self) -> String {
        format!("|{} -> {}|", self.key, self.value)
    }
}

fn main() {
    // entries
    let entry1 = BTreeNodeEntry::new(1, 1, None);
    let entry2 = BTreeNodeEntry::new(2, 2, None);
    let entry3 = BTreeNodeEntry::new(3, 3, None);

    // tree init
    let mut tree = BTree::new(3, None);
    tree.insert(entry3);
    tree.insert(entry1);
    tree.insert(entry2); // SHOULD SPLIT

    tree.print();
}
