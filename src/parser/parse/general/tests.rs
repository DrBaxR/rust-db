use crate::parser::{ast::general::TableExpression, token::Tokenizer, SqlParser};

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
