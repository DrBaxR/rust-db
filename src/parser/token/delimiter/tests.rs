use crate::parser::token::delimiter::Delimiter;

use super::DelimiterTokenizer;

#[test]
fn matches() {
    let tokenizer = DelimiterTokenizer::new();

    assert_eq!(tokenizer.largest_match(","), Some((Delimiter::Comma, 1)));
    assert_eq!(tokenizer.largest_match("."), Some((Delimiter::Dot, 1)));
    assert_eq!(tokenizer.largest_match(";"), Some((Delimiter::Semicolon, 1)));
    assert_eq!(tokenizer.largest_match("("), Some((Delimiter::OpenParen, 1)));
    assert_eq!(tokenizer.largest_match("]"), Some((Delimiter::CloseBracket, 1)));
}

#[test]
fn no_match() {
    let tokenizer = DelimiterTokenizer::new();

    assert_eq!(tokenizer.largest_match(" ,"), None);
    assert_eq!(tokenizer.largest_match("SELECT"), None);
}