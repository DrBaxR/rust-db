use crate::parser::token::keyword::Keyword;

use super::KeywordTokenizer;

#[test]
fn full_match() {
    let tokenizer = KeywordTokenizer::new();

    assert_eq!(tokenizer.largest_match("ASC"), Some((Keyword::Asc, 3)));
    assert_eq!(tokenizer.largest_match("BETWEEN"), Some((Keyword::Between, 7)));
    assert_eq!(tokenizer.largest_match("create index"), Some((Keyword::CreateIndex, 12)));
    assert_eq!(tokenizer.largest_match("DIStincT"), Some((Keyword::Distinct, 8)));
    assert_eq!(tokenizer.largest_match("EXPLAIN"), Some((Keyword::Explain, 7)));
    assert_eq!(tokenizer.largest_match("FROM"), Some((Keyword::From, 4)));
    assert_eq!(tokenizer.largest_match("INNER JOIN"), Some((Keyword::InnerJoin, 10)));
    assert_eq!(tokenizer.largest_match("WHERE"), Some((Keyword::Where, 5)));
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

    assert_eq!(tokenizer.largest_match("SELECT * FROM test;"), Some((Keyword::Select, 6)));
    assert_eq!(tokenizer.largest_match("INSERT INTOxxxxxxxxxxx"), Some((Keyword::InsertInto, 11)));
}
