use crate::parser::token::delimiter::Delimiter;
use crate::parser::token::function::Function;
use crate::parser::token::keyword::Keyword;
use crate::parser::token::operator::Operator;
use crate::parser::token::value::Value;

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

    assert_eq!(
        t.tokenize("select * from                my_table;")
            .unwrap(),
        expected
    );
    assert_eq!(
        t.tokenize("select \t*   from my_table\n;").unwrap(),
        expected
    );
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

#[test]
fn complex_statement() {
    let t = Tokenizer::new();
    let sql = "SELECT\n
                        p.product_id,\n
                        p.product_name,\n
                        COUNT(*) AS total_sales,\n
                        AVG(s.amount) AS avg_sale_amount,\n
                        MAX(s.sale_date) AS last_sold_date\n
                    FROM\n
                        products p\n
                        LEFT JOIN sales s ON p.product_id = s.product_id\n
                    WHERE\n
                        p.is_active = TRUE\n
                    GROUP BY\n
                        p.product_id, p.product_name;";
    let expected = vec![
        // line 1: SELECT
        Token::Keyword(Keyword::Select),
        // line 2: p.product_id,
        Token::Identifier("p".to_string()),
        Token::Delimiter(Delimiter::Dot),
        Token::Identifier("product_id".to_string()),
        Token::Delimiter(Delimiter::Comma),
        // line 3: p.product_name,
        Token::Identifier("p".to_string()),
        Token::Delimiter(Delimiter::Dot),
        Token::Identifier("product_name".to_string()),
        Token::Delimiter(Delimiter::Comma),
        // line 4: COUNT(*) AS total_sales,
        Token::Function(Function::Count),
        Token::Delimiter(Delimiter::OpenParen),
        Token::Operator(Operator::Multiply),
        Token::Delimiter(Delimiter::CloseParen),
        Token::Keyword(Keyword::As),
        Token::Identifier("total_sales".to_string()),
        Token::Delimiter(Delimiter::Comma),
        // line 5: AVG(s.amount) AS avg_sale_amount,
        Token::Function(Function::Avg),
        Token::Delimiter(Delimiter::OpenParen),
        Token::Identifier("s".to_string()),
        Token::Delimiter(Delimiter::Dot),
        Token::Identifier("amount".to_string()),
        Token::Delimiter(Delimiter::CloseParen),
        Token::Keyword(Keyword::As),
        Token::Identifier("avg_sale_amount".to_string()),
        Token::Delimiter(Delimiter::Comma),
        // line 6: MAX(s.sale_date) AS last_sold_date
        Token::Function(Function::Max),
        Token::Delimiter(Delimiter::OpenParen),
        Token::Identifier("s".to_string()),
        Token::Delimiter(Delimiter::Dot),
        Token::Identifier("sale_date".to_string()),
        Token::Delimiter(Delimiter::CloseParen),
        Token::Keyword(Keyword::As),
        Token::Identifier("last_sold_date".to_string()),
        // line 7: FROM
        Token::Keyword(Keyword::From),
        // line 8: products p
        Token::Identifier("products".to_string()),
        Token::Identifier("p".to_string()),
        // line 9: LEFT JOIN sales s ON p.product_id = s.product_id
        Token::Keyword(Keyword::LeftJoin),
        Token::Identifier("sales".to_string()),
        Token::Identifier("s".to_string()),
        Token::Keyword(Keyword::On),
        Token::Identifier("p".to_string()),
        Token::Delimiter(Delimiter::Dot),
        Token::Identifier("product_id".to_string()),
        Token::Operator(Operator::Equal),
        Token::Identifier("s".to_string()),
        Token::Delimiter(Delimiter::Dot),
        Token::Identifier("product_id".to_string()),
        // line 10: WHERE
        Token::Keyword(Keyword::Where),
        // line 11: p.is_active = TRUE
        Token::Identifier("p".to_string()),
        Token::Delimiter(Delimiter::Dot),
        Token::Identifier("is_active".to_string()),
        Token::Operator(Operator::Equal),
        Token::Value(Value::Boolean(true)),
        // line 12: GROUP BY
        Token::Keyword(Keyword::GroupBy),
        // line 13: p.product_id, p.product_name;
        Token::Identifier("p".to_string()),
        Token::Delimiter(Delimiter::Dot),
        Token::Identifier("product_id".to_string()),
        Token::Delimiter(Delimiter::Comma),
        Token::Identifier("p".to_string()),
        Token::Delimiter(Delimiter::Dot),
        Token::Identifier("product_name".to_string()),
        Token::Delimiter(Delimiter::Semicolon),
    ];

    assert_eq!(t.tokenize(sql).unwrap(), expected);
}

#[test]
fn invalid_statements() {
    let t = Tokenizer::new();

    assert!(t.tokenize("SELECT * FROM test WHERE t.id = 1.").is_err());
    assert!(t.tokenize("SELECT 1 + 2 + 3.").is_err());
    assert!(t.tokenize("SELECT 'this string is invalid;").is_err());
}
