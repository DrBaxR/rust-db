// maps number to number
// contains (i in 0..2b-1) elements
// edges[i] - child to the left of the ith element
// edges[i + 1] - child to the right of the ith element
pub struct Node {
    keys: Vec<usize>,
    values: Vec<usize>,
    edges: Vec<Node>,
    b: usize,
}

pub struct NodeSplit {
    median: (usize, usize),
    left: Node,
    right: Node,
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
        self.edges.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.keys.len() >= 2 * self.b - 1
    }

    pub fn find_leaf_for(&self, key: usize) -> &Node {
        if self.is_leaf() {
            return self;
        }

        for (i, node_key) in self.keys.iter().enumerate() {
            if key < *node_key {
                return self.edges.get(i).unwrap().find_leaf_for(key);
            }
        }

        return self.edges.last().unwrap().find_leaf_for(key);
    }

    // creates a new node that represents current node post-split
    pub fn push(&self, key: usize, value: usize) -> Node {
        todo!()
    }

    pub fn get_split(&self) -> NodeSplit {
        todo!()
    }
}