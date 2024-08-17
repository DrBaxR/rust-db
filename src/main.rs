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
}

struct BTreeNode {
    left: Option<Box<BTreeNode>>,
    data: Vec<BTreeNodeEntry>,
}

impl BTreeNode {
    fn new() -> Self {
        Self {
            left: None,
            data: vec![],
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
    // node 1 init
    let mut node1 = BTreeNode::new();
    let entry1 = BTreeNodeEntry::new(1, 1, None);
    let entry3 = BTreeNodeEntry::new(3, 3, None);
    node1.data.push(entry1);
    node1.data.push(entry3);

    // node 2 init
    let mut node2 = BTreeNode::new();
    let entry2 = BTreeNodeEntry::new(2, 2, None);
    node2.data.push(entry2);
    node1.left = Some(Box::new(node2));

    // tree init
    let mut tree = BTree::new(3, None);
    tree.root = Some(node1);


    tree.print();
}
