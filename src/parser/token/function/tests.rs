use crate::parser::token::{function::Function, Token};

use super::FunctionTokenizer;

#[test]
fn matches() {
    let tokenizer = FunctionTokenizer::new();

    assert_eq!(tokenizer.largest_match("count"), Some((Token::Function(Function::Count), 5)));
    assert_eq!(tokenizer.largest_match("AVG"), Some((Token::Function(Function::Avg), 3)));
    assert_eq!(tokenizer.largest_match("Round"), Some((Token::Function(Function::Round), 5)));
}

#[test]
fn no_match() {
    let tokenizer = FunctionTokenizer::new();

    assert_eq!(tokenizer.largest_match(" count"), None);
    assert_eq!(tokenizer.largest_match("SELECT"), None);
}
