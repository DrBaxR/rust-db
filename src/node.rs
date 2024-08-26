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
        todo!()
    }

    // determines if self is the parent of node
    pub fn is_parent_of(&self, node: &Node) -> bool {
        self.edges
            .iter()
            .filter(|op| op.is_some())
            .map(|op| op.as_ref().unwrap())
            .any(|child| std::ptr::eq(child, node))
    }

    pub fn is_full(&self) -> bool {
        self.keys.len() >= 2 * self.b - 1
    }

    // panics if trying to find a key in empty node
    pub fn find_leaf_for(&self, key: usize) -> &Node {
        todo!("implementation below is not correct according to the updated struct")
        // if self.is_leaf() {
        //     return self;
        // }

        // for (i, node_key) in self.keys.iter().enumerate() {
        //     if key < *node_key {
        //         return self.edges.get(i).unwrap().find_leaf_for(key);
        //     }
        // }

        // return self.edges.last().unwrap().find_leaf_for(key);
    }

    // only takes care of the value, setting left and right to None
    // creates a new node that represents current node post-split
    // keep in mind case where self is empty
    pub fn push(&self, key: usize, value: usize) -> Node {
        todo!()
    }

    // push to current node, also taking care of the
    // should be able to handle case where self is empty
    pub fn push_with_children(&mut self, split: NodeSplit) -> Node {
        todo!()
    }

    // return what would happen if you split
    // panic if node is not full or has too many things inside
    pub fn get_split(&self) -> NodeSplit {
        todo!()
    }
}
