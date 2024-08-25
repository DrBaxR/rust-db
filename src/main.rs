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

    fn find_leaf_for(&self, key: usize) -> &Node {
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

pub struct BTree {
    root: Node,
    b: usize,
}

enum NodeReplace<'a> {
    Node(Node, &'a Node), // what node you need to replace with what value
    Root(Node) // root needs to be replaced with value
}

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
        node_to_insert.push(key, value);
        if !node_to_insert.is_full() {
            return;
        }

        // if full split in median, left and right
        let split_node = node_to_insert.get_split();
        
        // insert median in parent (potentiallt split again, again, ...)
        // TODO: make insert_split_in_parent return dirrectly new root
        let node_replace = self.insert_split_in_parent(node_to_insert, split_node);
        self.root = self.get_root_after_replace(node_replace);
    }

    // constructs a new node subtree that represents the correct result post insert
    // return value represents instructions of where you need to replace a node to have a correct post-insert tree
    fn insert_split_in_parent(&self, parent_of: &Node, split: NodeSplit) -> NodeReplace {
        todo!()
    }

    // returns new root after replace is applied
    fn get_root_after_replace(&self, replace: NodeReplace) -> Node {
        todo!()
    }
}

fn main() {
    todo!()
}
