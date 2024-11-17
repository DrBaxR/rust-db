use crate::parser::token::{function::Function, value::Value};

use super::{
    token::{keyword::Keyword, Token},
    SqlParser,
};

#[test]
fn match_next_option() {
    let mut p = SqlParser::new(vec![Token::Keyword(Keyword::Select)]);
    assert_eq!(p.match_next_option(&vec![]), Ok(None));
    assert_eq!(
        p.match_next_option(&vec![Token::Keyword(Keyword::Select)]),
        Ok(Some(&Token::Keyword(Keyword::Select)))
    );

    let mut p = SqlParser::new(vec![Token::Keyword(Keyword::Select)]);
    assert_eq!(
        p.match_next_option(&vec![
            Token::Keyword(Keyword::Select),
            Token::Keyword(Keyword::Delete)
        ]),
        Ok(Some(&Token::Keyword(Keyword::Select)))
    );
    // cursor advanced and no more tokens
    assert!(p
        .match_next_option(&vec![Token::Keyword(Keyword::Select),])
        .is_err());

    let mut p = SqlParser::new(vec![]);
    assert!(p
        .match_next_option(&vec![
            Token::Keyword(Keyword::Select),
            Token::Keyword(Keyword::Delete)
        ])
        .is_err());
}

#[test]
fn match_next() {
    let mut p = SqlParser::new(vec![Token::Keyword(Keyword::Select)]);
    assert!(p.match_next(Token::Keyword(Keyword::Select)).is_ok());
    assert!(p.match_next(Token::Keyword(Keyword::Select)).is_err());

    let mut p = SqlParser::new(vec![Token::Keyword(Keyword::As)]);
    assert!(p.match_next(Token::Keyword(Keyword::Select)).is_err());
}

#[test]
fn match_next_identifier() {
    let mut p = SqlParser::new(vec![Token::Identifier("my_table".to_string())]);
    assert_eq!(p.match_next_identifier().unwrap(), "my_table".to_string());
    assert!(p.match_next_identifier().is_err());
}

#[test]
fn match_next_value() {
    let mut p = SqlParser::new(vec![Token::Identifier("my_table".to_string())]);
    assert!(p.match_next_value().is_err());

    let mut p = SqlParser::new(vec![Token::Value(Value::Integer(12))]);
    assert_eq!(p.match_next_value().unwrap(), Value::Integer(12));
    assert!(p.match_next_value().is_err());
}

#[test]
fn match_next_function() {
    let mut p = SqlParser::new(vec![Token::Identifier("my_table".to_string())]);
    assert!(p.match_next_function().is_err());

    let mut p = SqlParser::new(vec![Token::Function(Function::Min)]);
    assert_eq!(p.match_next_function().unwrap(), Function::Min);
    assert!(p.match_next_function().is_err());
}