pub struct BTreeNode {
    pub left: Option<Box<BTreeNode>>,
    pub data: Vec<BTreeNodeEntry>,
}

pub enum BTreeNodeSearchError {
    GreaterThanAll,
}

impl BTreeNode {
    pub fn new(entry: BTreeNodeEntry) -> Self {
        Self {
            left: None,
            data: vec![entry],
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
}

pub struct BTreeNodeEntry {
    pub key: i64,
    pub value: i64,
    pub right: Option<BTreeNode>,
}

impl BTreeNodeEntry {
    pub fn new(key: i64, value: i64, right: Option<BTreeNode>) -> Self {
        Self { key, value, right }
    }

    fn to_string(&self) -> String {
        format!("|{} -> {}|", self.key, self.value)
    }
}
