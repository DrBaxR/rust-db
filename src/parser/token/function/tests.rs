use crate::parser::token::function::Function;

use super::FunctionTokenizer;

#[test]
fn matches() {
    let tokenizer = FunctionTokenizer::new();

    assert_eq!(tokenizer.largest_match("count"), Some((Function::Count, 5)));
    assert_eq!(tokenizer.largest_match("AVG"), Some((Function::Avg, 3)));
    assert_eq!(tokenizer.largest_match("Round"), Some((Function::Round, 5)));
}

#[test]
fn no_match() {
    let tokenizer = FunctionTokenizer::new();

    assert_eq!(tokenizer.largest_match(" count"), None);
    assert_eq!(tokenizer.largest_match("SELECT"), None);
}
