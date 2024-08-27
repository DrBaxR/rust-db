use std::fmt::Debug;

// maps number to number
// contains (i in 0..2b-1) elements
// edges[i] - child to the left of the ith element
// edges[i + 1] - child to the right of the ith element
#[derive(Clone)]
pub struct Node {
    keys: Vec<usize>,
    values: Vec<usize>,
    edges: Vec<Option<Node>>,
    b: usize,
}

pub struct NodeSplit {
    pub median: (usize, usize),
    pub left: Node,
    pub right: Node,
}

enum KeySearchResult {
    NodeEmpty,
    LessThanAll,
    LessThanIndex(usize),
    GreaterThanAll,
}

impl Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut entries = String::from("[ ");
        for i in 0..self.keys.len() {
            entries.push_str(format!("| {} -> {} | ", self.keys[i], self.values[i]).as_str());
        }
        entries.push_str("]");

        let mut children = String::from("");
        for edge in self.edges.iter() {
            if let Some(edge) = edge {
                children.push_str(format!("{:?}\n", edge).as_str());
            } else {
                children.push_str("None\n");
            }
        }

        write!(f, "{}\n{}", entries, children)
    }
}

impl Node {
    pub fn new(b: usize) -> Self {
        Self {
            keys: vec![],
            values: vec![],
            edges: vec![],
            b,
        }
    }

    fn is_leaf(&self) -> bool {
        self.edges.iter().all(|e| e.is_none())
    }

    // determines if self is the parent of node
    pub fn is_parent_of(&self, node: &Node) -> bool {
        self.edges
            .iter()
            .filter(|op| op.is_some())
            .map(|op| op.as_ref().unwrap())
            .any(|child| std::ptr::addr_eq(child, node))
    }

    // None if parent is not inside tree
    pub fn find_parent_of(&self, node: &Node) -> Option<&Node> {
        if self.is_parent_of(node) {
            return Some(self);
        }

        if self.is_leaf() {
            return None;
        }

        for edge in self
            .edges
            .iter()
            .filter(|o| o.is_some())
            .map(|o| o.as_ref().unwrap())
        {
            if let Some(parent) = edge.find_parent_of(node) {
                return Some(parent);
            }
        }

        None
    }

    pub fn is_full(&self) -> bool {
        self.keys.len() >= 2 * self.b - 1
    }

    // panics if trying to find a key in empty node
    pub fn find_leaf_for(&self, key: usize) -> &Node {
        if self.is_leaf() {
            return self;
        }

        let search_result = self.search_key(key);
        match search_result {
            KeySearchResult::NodeEmpty => self,
            KeySearchResult::LessThanAll => self.edges[0].as_ref().unwrap().find_leaf_for(key),
            KeySearchResult::LessThanIndex(i) => self.edges[i].as_ref().unwrap().find_leaf_for(key),
            KeySearchResult::GreaterThanAll => self
                .edges
                .last()
                .unwrap()
                .as_ref()
                .unwrap()
                .find_leaf_for(key),
        }
    }

    // only takes care of the value, setting left and right to None
    // creates a new node that represents current node post-split
    pub fn push(&self, key: usize, value: usize) -> Node {
        self.push_with_children(key, value, None, None)
    }

    // return new node - how current node would look like if you insert
    // should be able to handle case where self is empty
    pub fn push_with_children(
        &self,
        key: usize,
        value: usize,
        left: Option<Node>,
        right: Option<Node>,
    ) -> Node {
        let search_result = self.search_key(key);
        let mut new_node = self.clone();

        match search_result {
            KeySearchResult::NodeEmpty => {
                new_node.keys.push(key);
                new_node.values.push(value);
                new_node.edges.push(left);
                new_node.edges.push(right);
            }
            KeySearchResult::LessThanAll => {
                new_node.keys.insert(0, key);
                new_node.values.insert(0, value);
                new_node.edges.insert(0, left);
                new_node.edges[1] = right;
            }
            KeySearchResult::LessThanIndex(i) => {
                new_node.keys.insert(i, key);
                new_node.values.insert(i, value);
                new_node.edges.insert(i, left);
                new_node.edges[i + 1] = right;
            }
            KeySearchResult::GreaterThanAll => {
                new_node.keys.push(key);
                new_node.values.push(value);
                new_node.edges.push(right);
                let second_to_last = new_node.edges.len() - 2;
                new_node.edges[second_to_last] = left;
            }
        }

        new_node
    }

    // return what would happen if you split
    // panic if node is not full or has too many things inside
    pub fn get_split(&self) -> NodeSplit {
        if !self.is_full() {
            panic!("Can't split, node is not full");
        }

        // find median
        let median = (
            *self.keys.get(self.b - 1).unwrap(),
            *self.values.get(self.b - 1).unwrap(),
        );

        // find left and right
        let mut left = Node::new(self.b);
        let mut right = Node::new(self.b);
        for i in 0..self.b - 1 {
            // left
            left.keys[i] = self.keys[i];
            left.values[i] = self.values[i];
            left.edges[i + 1] = self.edges[i + 1].clone();

            // right
            right.keys[i] = self.keys[self.b + i];
            right.values[i] = self.values[self.b + i];
            right.edges[i + 1] = self.edges[self.b + i + 1].clone();
        }
        left.edges[0] = self.edges[0].clone();
        right.edges[0] = self.edges[self.b].clone();

        NodeSplit {
            median,
            left,
            right,
        }
    }

    // search where key should be inserted in self
    fn search_key(&self, key: usize) -> KeySearchResult {
        if self.edges.is_empty() {
            return KeySearchResult::NodeEmpty;
        }

        if key < *self.keys.get(0).unwrap() {
            return KeySearchResult::LessThanAll;
        }

        for (i, self_key) in self.keys.iter().enumerate() {
            if key < *self_key {
                return KeySearchResult::LessThanIndex(i);
            }
        }

        KeySearchResult::GreaterThanAll
    }

    // returns clone of self, but instead of node to_replace (as child somewhere), use replace_with
    // if not in tree, clone without replacing
    pub fn clone_with_replaced_node(&self, to_replace: &Node, replace_with: &Node) -> Node {
        if std::ptr::addr_eq(self, to_replace) {
            return replace_with.clone();
        }

        let mut new_self = self.clone_without_edges();
        for (i, edge) in self.edges.iter().enumerate() {
            if let Some(edge) = edge {
                new_self.edges[i] = Some(edge.clone_with_replaced_node(to_replace, replace_with));
            } else {
                new_self.edges[i] = None;
            }
        }

        new_self
    }

    // clones keys and values, all edges of original are set to None
    fn clone_without_edges(&self) -> Node {
        let mut new_edges = vec![];
        for _ in self.edges.iter() {
            new_edges.push(None);
        }

        Node {
            keys: self.keys.clone(),
            values: self.values.clone(),
            edges: new_edges,
            b: self.b,
        }
    }
}

// TODO: tests
