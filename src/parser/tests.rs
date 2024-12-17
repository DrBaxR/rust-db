use crate::parser::{
    ast::{general::Term, DeleteStatement, InsertStatement},
    token::{function::Function, value::Value},
    SqlStatement,
};

use super::{
    ast::general::{AndCondition, Condition, Expression, Factor, Operand},
    token::{keyword::Keyword, Token, Tokenizer},
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

fn get_bool_expression(value: bool) -> Expression {
    Expression {
        and_conditions: vec![AndCondition {
            conditions: vec![Condition::Operation {
                operand: Operand {
                    left: Factor {
                        left: Box::new(Term::Value(Value::Boolean(value))),
                        right: vec![],
                    },
                    right: vec![],
                },
                operation: None,
            }],
        }],
    }
}

#[test]
fn parse() {
    let tokens = Tokenizer::new()
        .tokenize("INSERT INTO my_table (a, b) VALUES (1, 2)")
        .unwrap();
    let mut parser = SqlParser::new(tokens);
    assert_eq!(
        parser.parse().unwrap(),
        SqlStatement::Insert(InsertStatement {
            table_name: "my_table".to_string(),
            columns: vec!["a".to_string(), "b".to_string()],
            values: vec![
                Term::Value(Value::Integer(1)),
                Term::Value(Value::Integer(2)),
            ],
        })
    );

    let tokens = Tokenizer::new()
        .tokenize("DELETE FROM my_table WHERE true LIMIT 100")
        .unwrap();
    let mut parser = SqlParser::new(tokens);
    assert_eq!(
        parser.parse().unwrap(),
        SqlStatement::Delete(DeleteStatement {
            table_name: "my_table".to_string(),
            where_expression: Some(get_bool_expression(true)),
            limit: Some(100),
        })
    );
}
