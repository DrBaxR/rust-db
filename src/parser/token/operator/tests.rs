use crate::parser::token::{operator::Operator, Token};

use super::OperatorTokenizer;

#[test]
fn arithmetic() {
    let tokenizer = OperatorTokenizer::new();

    assert_eq!(tokenizer.largest_match("+"), Some((Token::Operator(Operator::Plus), 1)));
    assert_eq!(tokenizer.largest_match("-"), Some((Token::Operator(Operator::Minus), 1)));
    assert_eq!(tokenizer.largest_match("*"), Some((Token::Operator(Operator::Multiply), 1)));
    assert_eq!(tokenizer.largest_match("/"), Some((Token::Operator(Operator::Divide), 1)));
    assert_eq!(tokenizer.largest_match("%"), Some((Token::Operator(Operator::Modulus), 1)));

    assert_eq!(tokenizer.largest_match("%sample"), Some((Token::Operator(Operator::Modulus), 1)));
}

#[test]
fn comparison() {
    let tokenizer = OperatorTokenizer::new();

    assert_eq!(tokenizer.largest_match("="), Some((Token::Operator(Operator::Equal), 1)));
    assert_eq!(tokenizer.largest_match("!="), Some((Token::Operator(Operator::NotEqual), 2)));
    assert_eq!(tokenizer.largest_match("<>"), Some((Token::Operator(Operator::NotEqual), 2)));
    assert_eq!(tokenizer.largest_match(">"), Some((Token::Operator(Operator::GreaterThan), 1)));
    assert_eq!(tokenizer.largest_match(">="), Some((Token::Operator(Operator::GreaterThanOrEqual), 2)));
    assert_eq!(tokenizer.largest_match("<"), Some((Token::Operator(Operator::LessThan), 1)));
    assert_eq!(tokenizer.largest_match("<="), Some((Token::Operator(Operator::LessThanOrEqual), 2)));

    assert_eq!(tokenizer.largest_match("<=123"), Some((Token::Operator(Operator::LessThanOrEqual), 2)));
}

#[test]
fn word() {
    let tokenizer = OperatorTokenizer::new();

    assert_eq!(tokenizer.largest_match("AND"), Some((Token::Operator(Operator::And), 3)));
    assert_eq!(tokenizer.largest_match("OR"), Some((Token::Operator(Operator::Or), 2)));
    assert_eq!(tokenizer.largest_match("NOT"), Some((Token::Operator(Operator::Not), 3)));
    assert_eq!(tokenizer.largest_match("LIKE"), Some((Token::Operator(Operator::Like), 4)));
    assert_eq!(tokenizer.largest_match("lIKe"), Some((Token::Operator(Operator::Like), 4)));
    assert_eq!(tokenizer.largest_match("IN"), Some((Token::Operator(Operator::In), 2)));
    assert_eq!(tokenizer.largest_match("IS"), Some((Token::Operator(Operator::Is), 2)));

    assert_eq!(tokenizer.largest_match("LIKE '...'"), Some((Token::Operator(Operator::Like), 4)));
}

#[test]
fn no_match() {
    let tokenizer = OperatorTokenizer::new();

    assert_eq!(tokenizer.largest_match("nANd"), None);
    assert_eq!(tokenizer.largest_match("test"), None);
    assert_eq!(tokenizer.largest_match("&&"), None);
}
