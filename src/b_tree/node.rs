use std::fmt::Debug;

#[derive(Clone)]
pub struct BTreeNode {
    pub left: Option<Box<BTreeNode>>,
    pub data: Vec<BTreeNodeEntry>,
}

pub struct BTreeNodeSplit {
    pub median: BTreeNodeEntry,
    pub left: BTreeNode,
    pub right: BTreeNode,
}

pub enum BTreeNodeSearchError {
    GreaterThanAll,
}

impl Debug for BTreeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl BTreeNode {
    pub fn new(entry: BTreeNodeEntry) -> Self {
        Self {
            left: None,
            data: vec![entry],
        }
    }

    fn from_vec(entries: &[BTreeNodeEntry]) -> Self {
        Self {
            left: None,
            data: entries.to_vec(),
        }
    }

    pub fn to_string(&self) -> String {
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

    pub fn is_full(&self, order: usize) -> bool {
        self.data.len() == order
    }

    pub fn is_leaf(&self) -> bool {
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

    pub fn get_greater_than_index(&self, search_key: i64) -> Result<usize, BTreeNodeSearchError> {
        for (i, entry) in self.data.iter().enumerate() {
            if entry.key > search_key {
                return Ok(i);
            }
        }

        Err(BTreeNodeSearchError::GreaterThanAll)
    }

    pub fn push(&mut self, entry: BTreeNodeEntry) {
        let greater_index = match self.get_greater_than_index(entry.key) {
            Ok(index) => index,
            Err(BTreeNodeSearchError::GreaterThanAll) => self.data.len() - 1,
        };

        self.data.insert(greater_index, entry);
    }

    pub fn find_insert_leaf<'a>(&'a mut self, search_key: i64) -> &'a mut BTreeNode {
        if self.is_leaf() {
            return self;
        }

        let index = match self.get_greater_than_index(search_key) {
            Ok(i) => {
                if i == 0 {
                    0
                } else {
                    i - 1
                }
            }
            Err(BTreeNodeSearchError::GreaterThanAll) => self.data.len() - 1,
        };

        let next_node = self.data.get_mut(index).unwrap().right.as_mut().unwrap();
        return next_node.find_insert_leaf(search_key);
    }

    // assumes that node is full; doesn't set right of the entry to the right field
    pub fn split_node(&self, order: usize) -> BTreeNodeSplit {
        let median_index = order / 2;
        let median = self.data.get(median_index).unwrap();

        let split_left = &self.data[0..median_index];
        let split_right = &self.data[median_index + 1..];

        BTreeNodeSplit {
            median: BTreeNodeEntry::new(median.key, median.value, None),
            left: BTreeNode::from_vec(split_left),
            right: BTreeNode::from_vec(split_right),
        }
    }
}

#[derive(Clone)]
pub struct BTreeNodeEntry {
    pub key: i64,
    pub value: i64,
    pub right: Option<BTreeNode>,
}

impl Debug for BTreeNodeEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl BTreeNodeEntry {
    pub fn new(key: i64, value: i64, right: Option<BTreeNode>) -> Self {
        Self { key, value, right }
    }

    fn to_string(&self) -> String {
        format!("|{} -> {}|", self.key, self.value)
    }
}
