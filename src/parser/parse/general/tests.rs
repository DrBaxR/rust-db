use crate::parser::{
    ast::general::{TableExpression, Term},
    parse::general::parse_paren_term,
    token::{value::Value, Tokenizer},
    SqlParser,
};

use super::parse_table_expression;

#[test]
fn parse_table_expression_test() {
    let tokens = Tokenizer::new().tokenize("my_table AS mt").unwrap();
    let mut p = SqlParser::new(tokens);

    assert_eq!(
        parse_table_expression(&mut p).unwrap(),
        TableExpression {
            table_name: "my_table".to_string(),
            alias: "mt".to_string()
        }
    );
}

#[test]
fn parse_paren_term_test() {
    let tokens = Tokenizer::new().tokenize("(1.12)").unwrap();
    let mut p = SqlParser::new(tokens);

    assert_eq!(
        parse_paren_term(&mut p).unwrap(),
        Term::Value(Value::Float(1.12))
    );

    let tokens = Tokenizer::new().tokenize("('test')").unwrap();
    let mut p = SqlParser::new(tokens);

    assert_eq!(
        parse_paren_term(&mut p).unwrap(),
        Term::Value(Value::String("test".to_string()))
    );

    let tokens = Tokenizer::new().tokenize("(*)").unwrap();
    let mut p = SqlParser::new(tokens);

    // TODO: uncomment once all is implemented
    // assert!(parse_paren_term(&mut p).is_err());
}
