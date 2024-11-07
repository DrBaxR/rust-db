use crate::parser::token::delimiter::Delimiter;
use crate::parser::token::keyword::Keyword;
use crate::parser::token::operator::Operator;

use super::Token;
use super::Tokenizer;

#[test]
fn simple_statement() {
    let t = Tokenizer::new();

    assert_eq!(
        t.tokenize("select * from my_table;").unwrap(),
        vec![
            Token::Keyword(Keyword::Select),
            Token::Operator(Operator::Multiply),
            Token::Keyword(Keyword::From),
            Token::Identifier(String::from("my_table")),
            Token::Delimiter(Delimiter::Semicolon),
        ]
    );
}

#[test]
fn simple_statement_whitespace() {
    let t = Tokenizer::new();
    let expected = vec![
        Token::Keyword(Keyword::Select),
        Token::Operator(Operator::Multiply),
        Token::Keyword(Keyword::From),
        Token::Identifier(String::from("my_table")),
        Token::Delimiter(Delimiter::Semicolon),
    ];

    assert_eq!(t.tokenize("select * from                my_table;").unwrap(), expected);
    assert_eq!(t.tokenize("select \t*   from my_table\n;").unwrap(), expected);
}

#[test]
fn simple_statement_case_insensitive() {
    let t = Tokenizer::new();
    let expected = vec![
        Token::Keyword(Keyword::Select),
        Token::Operator(Operator::Multiply),
        Token::Keyword(Keyword::From),
        Token::Identifier(String::from("my_table")),
        Token::Delimiter(Delimiter::Semicolon),
    ];

    assert_eq!(t.tokenize("select * from my_table;").unwrap(), expected);
    assert_eq!(t.tokenize("SELECT * FROM my_table;").unwrap(), expected);
    assert_eq!(t.tokenize("SeLEcT * fROm my_table;").unwrap(), expected);
}
// TODO: more tests
