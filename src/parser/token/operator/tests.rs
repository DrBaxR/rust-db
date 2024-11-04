use crate::parser::token::operator::Operator;

use super::OperatorTokenizer;

#[test]
fn arithmetic() {
    let tokenizer = OperatorTokenizer::new();

    assert_eq!(tokenizer.largest_match("+"), Some((Operator::Plus, 1)));
    assert_eq!(tokenizer.largest_match("-"), Some((Operator::Minus, 1)));
    assert_eq!(tokenizer.largest_match("*"), Some((Operator::Multiply, 1)));
    assert_eq!(tokenizer.largest_match("/"), Some((Operator::Divide, 1)));
    assert_eq!(tokenizer.largest_match("%"), Some((Operator::Modulus, 1)));

    assert_eq!(tokenizer.largest_match("%sample"), Some((Operator::Modulus, 1)));
}

#[test]
fn comparison() {
    let tokenizer = OperatorTokenizer::new();

    assert_eq!(tokenizer.largest_match("="), Some((Operator::Equal, 1)));
    assert_eq!(tokenizer.largest_match("!="), Some((Operator::NotEqual, 2)));
    assert_eq!(tokenizer.largest_match("<>"), Some((Operator::NotEqual, 2)));
    assert_eq!(tokenizer.largest_match(">"), Some((Operator::GreaterThan, 1)));
    assert_eq!(tokenizer.largest_match(">="), Some((Operator::GreaterThanOrEqual, 2)));
    assert_eq!(tokenizer.largest_match("<"), Some((Operator::LessThan, 1)));
    assert_eq!(tokenizer.largest_match("<="), Some((Operator::LessThanOrEqual, 2)));

    assert_eq!(tokenizer.largest_match("<=123"), Some((Operator::LessThanOrEqual, 2)));
}

#[test]
fn word() {
    let tokenizer = OperatorTokenizer::new();

    assert_eq!(tokenizer.largest_match("AND"), Some((Operator::And, 3)));
    assert_eq!(tokenizer.largest_match("OR"), Some((Operator::Or, 2)));
    assert_eq!(tokenizer.largest_match("NOT"), Some((Operator::Not, 3)));
    assert_eq!(tokenizer.largest_match("LIKE"), Some((Operator::Like, 4)));
    assert_eq!(tokenizer.largest_match("lIKe"), Some((Operator::Like, 4)));
    assert_eq!(tokenizer.largest_match("IN"), Some((Operator::In, 2)));
    assert_eq!(tokenizer.largest_match("IS"), Some((Operator::Is, 2)));

    assert_eq!(tokenizer.largest_match("LIKE '...'"), Some((Operator::Like, 4)));
}

#[test]
fn no_match() {
    let tokenizer = OperatorTokenizer::new();

    assert_eq!(tokenizer.largest_match("nANd"), None);
    assert_eq!(tokenizer.largest_match("test"), None);
    assert_eq!(tokenizer.largest_match("&&"), None);
}
