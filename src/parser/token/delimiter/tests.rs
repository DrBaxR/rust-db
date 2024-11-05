use crate::parser::token::{delimiter::Delimiter, Token};

use super::DelimiterTokenizer;

#[test]
fn matches() {
    let tokenizer = DelimiterTokenizer::new();

    assert_eq!(
        tokenizer.largest_match(","),
        Some((Token::Delimiter(Delimiter::Comma), 1))
    );
    assert_eq!(
        tokenizer.largest_match("."),
        Some((Token::Delimiter(Delimiter::Dot), 1))
    );
    assert_eq!(
        tokenizer.largest_match(";"),
        Some((Token::Delimiter(Delimiter::Semicolon), 1))
    );
    assert_eq!(
        tokenizer.largest_match("("),
        Some((Token::Delimiter(Delimiter::OpenParen), 1))
    );
    assert_eq!(
        tokenizer.largest_match("]"),
        Some((Token::Delimiter(Delimiter::CloseBracket), 1))
    );
}

#[test]
fn no_match() {
    let tokenizer = DelimiterTokenizer::new();

    assert_eq!(tokenizer.largest_match(" ,"), None);
    assert_eq!(tokenizer.largest_match("SELECT"), None);
}
