use crate::parser::{
    ast::general::{Factor, Function, Operand, TableExpression, Term},
    parse::general::{parse_paren_term, parse_term},
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

    assert!(parse_paren_term(&mut p).is_err());
}

fn get_parser(raw: &str) -> SqlParser {
    let tokens = Tokenizer::new().tokenize(raw).unwrap();
    SqlParser::new(tokens)
}

#[test]
fn parse_term_test() {
    let mut p = get_parser("12");
    assert_eq!(parse_term(&mut p).unwrap(), Term::Value(Value::Integer(12)));

    let mut p = get_parser("MAX(12)");
    assert_eq!(
        parse_term(&mut p).unwrap(),
        Term::Function(Function::Max(Box::new(Term::Value(Value::Integer(12)))))
    );

    let mut p = get_parser("(1)");
    assert_eq!(
        parse_term(&mut p).unwrap(),
        Term::Operand(Operand {
            left: Factor {
                left: Box::new(Term::Value(Value::Integer(1))),
                right: vec![]
            },
            right: vec![]
        })
    );

    let mut p = get_parser("column");
    assert_eq!(
        parse_term(&mut p).unwrap(),
        Term::Column {
            table_alias: None,
            name: "column".to_string()
        }
    );

    let mut p = get_parser("(1, 2, 3)");
    assert_eq!(
        parse_term(&mut p).unwrap(),
        Term::RowValueConstructor(vec![
            Term::Value(Value::Integer(1)),
            Term::Value(Value::Integer(2)),
            Term::Value(Value::Integer(3))
        ])
    );
}
