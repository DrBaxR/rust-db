use crate::parser::token::{value::Value, Token};

use super::ValueTokenizer;

#[test]
fn integer() {
    let t = ValueTokenizer::new();

    assert_eq!(t.largest_match("123"), Some((Token::Value(Value::Integer(123)), 3)));
    assert_eq!(t.largest_match("99999asd"), Some((Token::Value(Value::Integer(99999)), 5)));
    assert_eq!(t.largest_match("99999!"), Some((Token::Value(Value::Integer(99999)), 5)));
    assert_eq!(t.largest_match("-99999"), None);
}

#[test]
fn float() {
    let t = ValueTokenizer::new();

    assert_eq!(t.largest_match("123.1"), Some((Token::Value(Value::Float(123.1)), 5)));
    assert_eq!(t.largest_match("999.342"), Some((Token::Value(Value::Float(999.342)), 7)));
    assert_eq!(t.largest_match("123."), None);
    assert_eq!(t.largest_match("+123.1"), None);
}

#[test]
fn string() {
    let t = ValueTokenizer::new();

    assert_eq!(
        t.largest_match("'this is a string :)'"),
        Some((Token::Value(Value::String(String::from("this is a string :)"))), 21)) // 19 + 2 (the 's that are not included in the string)
    );
    assert_eq!(
        t.largest_match("'_!()SELECT123'"),
        Some((Token::Value(Value::String(String::from("_!()SELECT123"))), 15))
    );
    assert_eq!(
        t.largest_match("'i'test"),
        Some((Token::Value(Value::String(String::from("i"))), 3))
    );
    assert_eq!(t.largest_match("'this is not a valid string :("), None);
    assert_eq!(t.largest_match("_'this is not a valid string :('"), None);
}

#[test]
fn boolean() {
    let t = ValueTokenizer::new();

    assert_eq!(t.largest_match("TRUE"), Some((Token::Value(Value::Boolean(true)), 4)));
    assert_eq!(t.largest_match("trUe"), Some((Token::Value(Value::Boolean(true)), 4)));
    assert_eq!(t.largest_match("TRUEasdasd"), Some((Token::Value(Value::Boolean(true)), 4)));
    assert_eq!(t.largest_match("false"), Some((Token::Value(Value::Boolean(false)), 5)));
}

#[test]
fn null() {
    let t = ValueTokenizer::new();

    assert_eq!(t.largest_match("null"), Some((Token::Value(Value::Null), 4)));
    assert_eq!(t.largest_match("NuLL"), Some((Token::Value(Value::Null), 4)));
    assert_eq!(t.largest_match("null"), Some((Token::Value(Value::Null), 4)));
}

#[test]
fn no_match() {
    let t = ValueTokenizer::new();

    assert_eq!(t.largest_match("no match"), None);
    assert_eq!(t.largest_match("SELECT * FROM table;"), None);
    assert_eq!(t.largest_match("12."), None);
    assert_eq!(t.largest_match("'not right"), None);
    assert_eq!(t.largest_match("nul"), None);
    assert_eq!(t.largest_match("faL"), None);
    assert_eq!(t.largest_match("T"), None);
    assert_eq!(t.largest_match("Tr"), None);
}
