#[cfg(test)]
mod tests;

/// Node of a key-value pair that maps a number to a number.
///
/// ### Constraints
/// Contains a maximum of `2 * b - 1` elements. This affects the coundaries of the vectors as follows:
/// - `keys`: contains `i` in `0..2 * b - 1`
/// - `values`: contains `i` in `0..2 * b - 1`
/// - `edges`: contains `i` in `0..2 * b`
///
/// ### Semantics
/// - `key[i]`: the key of the *ith* element in the node
/// - `value[i]`: the value of the *ith* element in the node
/// - `edges[i]`: the left child of the *ith* element in the node
/// - `edges[i + 1]`: the right child of the *ith* element in the node
#[derive(Clone)]
pub struct Node {
    pub keys: Vec<usize>,
    pub values: Vec<usize>,
    pub edges: Vec<Option<Node>>,
    pub b: usize,
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

impl Node {
    pub fn new(b: usize) -> Self {
        Self {
            keys: vec![],
            values: vec![],
            edges: vec![],
            b,
        }
    }

    // allocates memory for node with elems key value pairs
    fn new_of_size(b: usize, elems: usize) -> Self {
        Self {
            keys: vec![0; elems],
            values: vec![0; elems],
            edges: vec![None; elems + 1],
            b,
        }
    }

    /// Returns `true` if the node is a leaf (has no children).
    pub fn is_leaf(&self) -> bool {
        self.edges.is_empty() || self.edges.iter().all(|e| e.is_none())
    }

    /// Returns `true` if the `node` reference can be found in the children of `self`.
    pub fn is_parent_of(&self, node: &Node) -> bool {
        self.edges
            .iter()
            .filter(|op| op.is_some())
            .map(|op| op.as_ref().unwrap())
            .any(|child| std::ptr::addr_eq(child, node))
    }

    /// Returns the parent of `node`, if it can be found in the children of `self` (including itself). Otherwise returns `None`.
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

    /// Returns `true` if node is full.
    pub fn is_full(&self) -> bool {
        self.keys.len() >= 2 * self.b - 1
    }

    /// Returns a reference to a *leaf* in `self`'s *subtree* (including itself) that is fit for inserting a node with a key of `key`.
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

    /// Return a new `Node` that represents how `self` looks like after inserting a `key` -> `value` pair that has `None` as left and right children.
    ///
    /// **Note:** This method does not check whether the node is full or not before inserting into it.
    pub fn push(&self, key: usize, value: usize) -> Node {
        self.push_with_children(key, value, None, None)
    }

    /// Return a new `Node` that represents how `self` looks like after inserting a `key` ->`vaue` pair that has `left` and `right` as children.
    ///
    /// **Note:** This method does not check whether the node is full or not before inserting into it.
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

    /// Return split of current node. Assumes that node is full.
    ///
    /// # Panics
    /// Panics if `self` is not full
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
        let mut left = Node::new_of_size(self.b, self.b - 1);
        let mut right = Node::new_of_size(self.b, self.b - 1);
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

    /// Returns a `KeySearchResult` that indicates where `key` should be inserted in `self`.
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

    /// Returns a clone of `self` that resembles how it looks like with `to_replace` replaced with `replace_with`.
    /// Just acts as a regular clone if `to_replace` can't be found in `self`'s children (including itself).
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

    /// Returns clone of `self`, with all edges set to `None`.
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

    /// Recursively print `self` with all children.
    pub fn print_node(&self, level: usize) {
        // print entries
        let padding = "\t".repeat(level);

        let mut entries = String::from("[ ");
        for i in 0..self.keys.len() {
            entries.push_str(format!("| {} -> {} | ", self.keys[i], self.values[i]).as_str());
        }
        entries.push_str("]");
        println!("{}{}{}", level, padding, entries);

        // print children
        for edge in self.edges.iter() {
            if let Some(edge) = edge {
                edge.print_node(level + 1);
            } else {
                println!("{}{}\tNone", level + 1, padding);
            }
        }
    }

    /// Returns depth of subtree starting at `self` (if node is leaf, depth is `1`).
    pub fn depth(&self) -> usize {
        self.depth_from(1)
    }

    fn depth_from(&self, current_depth: usize) -> usize {
        if self.is_leaf() {
            return current_depth;
        }

        return self.edges[0]
            .as_ref()
            .unwrap()
            .depth_from(current_depth + 1);
    }

    /// Returns a reference to the node in the subtree starting from `self` that contains `key`.
    pub fn find_node_with(&self, key: usize) -> Option<&Node> {
        if self.contains(key) {
            return Some(self);
        }

        for edge in self.edges.iter().flatten() {
            if let Some(found) = edge.find_node_with(key) {
                return Some(found);
            }
        }

        None
    }

    /// Returns `true` if the current node contains `key`.
    fn contains(&self, key: usize) -> bool {
        self.keys.iter().any(|k| *k == key)
    }

    /// Returns new node with key, value and right edge removed from it. Second entry in the returned tuple is the value that would be removed.
    pub fn delete_entry(&self, key: usize) -> (Node, Option<usize>) {
        let index = self.index_of(key);
        let mut new_node = self.clone();

        if index.is_none() {
            return (new_node, None);
        }

        let index = index.unwrap();
        new_node.keys.remove(index);
        let value = new_node.values.remove(index);
        new_node.edges.remove(index + 1);

        return (new_node, Some(value));
    }

    /// Returns index of entry with `key`.
    fn index_of(&self, key: usize) -> Option<usize> {
        self.keys.iter().position(|k| *k == key)
    }

    /// Returns a reference to the right child of entry with `key`.
    ///
    /// # Panics
    /// Panics if node with key has no right child, so *should* only be called on non-leaf nodes.
    pub fn get_right_child(&self, key: usize) -> Option<&Node> {
        self.index_of(key)
            .map(|i| Some(self.edges[i + 1].as_ref().unwrap()))?
    }

    /// Returns the largest key in the node.
    pub fn largest_key(&self) -> usize {
        todo!()
    }

    /// Return new node that has its `key` entry replaced with `new`. Second entry in the tuple is the value what got replaced.
    pub fn replace_entry_with(&self, key: usize, new: (usize, usize)) -> (Node, usize) {
        todo!()
    }
}
