use std::{borrow::BorrowMut, collections::HashMap};

#[cfg(test)]
mod tests;

// TODO: document how these structures work
/// Can match a value to a character sequence. Internally, acts something like a trie, only internally
/// explicitly specifies whether a node represents a match (containing the matched value) or not.
///
/// **Note:** The main disadvantage of this representation is that the tree-like structure means we can have
/// no cycles, which leads to not being capable to have multiple characters transition to the same state.
pub struct ChrSqMatcher<V> {
    root: ChrSqMatcherNode<V>,
}

impl<V> ChrSqMatcher<V> {
    pub fn new() -> Self {
        Self {
            root: ChrSqMatcherNode::empty(),
        }
    }

    pub fn with(matches: Vec<(&str, V)>) -> Self {
        let mut matcher = Self::new();

        for (m, v) in matches {
            matcher.add_match(m, v);
        }

        matcher
    }

    /// Adds `sequence` as a matching sequence of characters that matches "value" in the matcher.
    pub fn add_match(&mut self, sequence: &str, value: V) {
        let mut current_node = &mut self.root;

        for c in sequence.chars() {
            current_node = current_node.get_or_insert_child(c);
        }

        current_node.value = Some(value);
    }

    /// Returns value matched to `sequence` in the matcher. Will return `None` if no value matched to `sequence`.
    pub fn get_match(&self, sequence: &str) -> Option<&V> {
        let mut current_node = &self.root;

        for c in sequence.chars() {
            current_node = current_node.get_child(c)?;
        }

        return current_node.value.as_ref();
    }

    /// Returns a FSM using the root node as start state; and using internal trie data structure for transitioning
    /// to other states.
    pub fn as_fsm(&self) -> ChrSqMatcherFSM<V> {
        ChrSqMatcherFSM::new(&self.root)
    }
}

/// Uses same internal trie-like data structure as the `ChrSqMatcher`, only exposes an FSM-like interface for more
/// efficient use in the context of a character-by-character use.
///
/// # Note
/// The dissadvantage of this interface of the data structure is that it can only be used in read-only mode.
pub struct ChrSqMatcherFSM<'a, V> {
    state: &'a ChrSqMatcherNode<V>,
}

impl<'a, V> ChrSqMatcherFSM<'a, V> {
    fn new(start_state: &'a ChrSqMatcherNode<V>) -> Self {
        Self { state: start_state }
    }

    /// Applies the `c` transition the the FSM.
    ///
    /// # Errors
    /// Will return `Err` if there is no state to transition to by applying `c`.
    pub fn transition(&mut self, c: char) -> Result<(), ()> {
        self.state = self.state.get_child(c).ok_or(())?;

        Ok(())
    }

    /// Returns the value of the current state the machine is in. Will return `None` if the state has no value
    /// associated to it.
    pub fn current_value(&self) -> Option<&'a V> {
        self.state.get_value()
    }
}

/// If the value of the node is not `None`, then it means that it's a matching node.
struct ChrSqMatcherNode<V> {
    value: Option<V>,
    children: HashMap<char, ChrSqMatcherNode<V>>,
}

impl<V> ChrSqMatcherNode<V> {
    fn empty() -> Self {
        Self {
            value: None,
            children: HashMap::new(),
        }
    }

    /// Returns child for `c`, or inserts empty ching for that transition if it doesn't exist and returns it.
    fn get_or_insert_child(&mut self, c: char) -> &mut ChrSqMatcherNode<V> {
        self.children.entry(c).or_insert(Self::empty()).borrow_mut()
    }

    /// Returns child for transition `c`.
    fn get_child(&self, c: char) -> Option<&ChrSqMatcherNode<V>> {
        self.children.get(&c)
    }

    /// Returns the value of the node.
    fn get_value(&self) -> Option<&V> {
        self.value.as_ref()
    }
}
