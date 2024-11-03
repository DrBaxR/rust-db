use std::{borrow::BorrowMut, collections::HashMap};

#[cfg(test)]
mod tests;

/// Acts something like a trie, only explicitly specifies whether a node represents a match (terminal) or not.
struct CharMatcher {
    root: CharMatcherNode,
}

impl CharMatcher {
    fn new() -> Self {
        Self {
            root: CharMatcherNode::empty(),
        }
    }

    fn with(matches: &[&str]) -> Self {
        let mut matcher = Self::new();

        for m in matches {
            matcher.add_match(m);
        }

        matcher
    }

    /// Adds `sequence` as a matching sequence of characters in the matcher.
    fn add_match(&mut self, sequence: &str) {
        let mut current_node = &mut self.root;

        for c in sequence.chars() {
            current_node = current_node.get_or_insert_child(c);
        }

        current_node.terminal = true;
    }

    /// Returns `true` if `sqquence` is a matching character sequence in the matcher.
    fn is_match(&self, sequence: &str) -> bool {
        let mut current_node = &self.root;

        for c in sequence.chars() {
            current_node = match current_node.get_child(c) {
                Some(child) => child,
                None => return false,
            };
        }

        return current_node.terminal
    }
}

struct CharMatcherNode {
    terminal: bool,
    children: HashMap<char, CharMatcherNode>,
}

impl CharMatcherNode {
    fn empty() -> Self {
        Self {
            terminal: false,
            children: HashMap::new(),
        }
    }

    /// Returns child for `c`, or inserts empty ching for that transition if it doesn't exist and returns it.
    fn get_or_insert_child(&mut self, c: char) -> &mut CharMatcherNode {
        self.children.entry(c).or_insert(Self::empty()).borrow_mut()
    }

    /// Returns child for transition `c`.
    fn get_child(&self, c: char) -> Option<&CharMatcherNode> {
        self.children.get(&c)
    }
}