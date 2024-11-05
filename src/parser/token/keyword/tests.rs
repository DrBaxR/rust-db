use crate::parser::token::{keyword::Keyword, Token};

use super::KeywordTokenizer;

#[test]
fn full_match() {
    let tokenizer = KeywordTokenizer::new();

    assert_eq!(tokenizer.largest_match("ASC"), Some((Token::Keyword(Keyword::Asc), 3)));
    assert_eq!(tokenizer.largest_match("BETWEEN"), Some((Token::Keyword(Keyword::Between), 7)));
    assert_eq!(tokenizer.largest_match("create index"), Some((Token::Keyword(Keyword::CreateIndex), 12)));
    assert_eq!(tokenizer.largest_match("DIStincT"), Some((Token::Keyword(Keyword::Distinct), 8)));
    assert_eq!(tokenizer.largest_match("EXPLAIN"), Some((Token::Keyword(Keyword::Explain), 7)));
    assert_eq!(tokenizer.largest_match("FROM"), Some((Token::Keyword(Keyword::From), 4)));
    assert_eq!(tokenizer.largest_match("INNER JOIN"), Some((Token::Keyword(Keyword::InnerJoin), 10)));
    assert_eq!(tokenizer.largest_match("WHERE"), Some((Token::Keyword(Keyword::Where), 5)));
}

#[test]
fn no_match() {
    let tokenizer = KeywordTokenizer::new();

    assert_eq!(tokenizer.largest_match("test"), None);
    assert_eq!(tokenizer.largest_match(" SELECT"), None);
    assert_eq!(tokenizer.largest_match("123"), None);
}

#[test]
fn partial_match() {
    let tokenizer = KeywordTokenizer::new();

    assert_eq!(tokenizer.largest_match("SELECT * FROM test;"), Some((Token::Keyword(Keyword::Select), 6)));
    assert_eq!(tokenizer.largest_match("INSERT INTOxxxxxxxxxxx"), Some((Token::Keyword(Keyword::InsertInto), 11)));
}
