use general::{
    parse_expression, parse_expressions, parse_join_expression, parse_order_by_expression,
    parse_select_expressions, parse_table_expression,
};

use super::{
    ast::SelectStatement,
    token::{keyword::Keyword, value::Value, Token},
    SqlParser,
};

mod general;

fn parse_select_statement(parser: &mut SqlParser) -> Result<SelectStatement, String> {
    // SELECT
    parser
        .match_next_option(&vec![Token::Keyword(Keyword::Select)])?
        .ok_or("STX: Expected SELECT keyword".to_string())?;

    // [ "DISTINCT" | "ALL" ]
    let is_distinct = match parser.match_next_option(&vec![
        Token::Keyword(Keyword::Distinct),
        Token::Keyword(Keyword::All),
    ])? {
        Some(Token::Keyword(keyword)) => *keyword == Keyword::Distinct,
        None => false,
        _ => panic!("STX: Anomaly"),
    };

    // select_expression , { "," , select_expression }
    let select_expressions = parse_select_expressions(parser)?;

    // FROM
    parser
        .match_next_option(&vec![Token::Keyword(Keyword::From)])?
        .ok_or("STX: Expected FROM keyword".to_string())?;

    // table_expression
    let from_expression = parse_table_expression(parser)?;

    // [ "WHERE" , expression ]
    let where_expression =
        if let Some(_) = parser.match_next_option(&vec![Token::Keyword(Keyword::Where)])? {
            Some(parse_expression(parser)?)
        } else {
            None
        };

    // [ "GROUP BY" , expression , { "," , expression } ]
    let group_by_expressions =
        if let Some(_) = parser.match_next_option(&vec![Token::Keyword(Keyword::GroupBy)])? {
            parse_expressions(parser)?
        } else {
            vec![]
        };

    // [ "HAVING" , expression ]
    let having_expression =
        if let Some(_) = parser.match_next_option(&vec![Token::Keyword(Keyword::GroupBy)])? {
            Some(parse_expression(parser)?)
        } else {
            None
        };

    // [ "ORDER BY" , expression , { "," , expression } , order ]
    let order_by_expression = parse_order_by_expression(parser).ok();

    // [ "LIMIT" , number ]
    let limit = if let Some(_) = parser.match_next_option(&vec![Token::Keyword(Keyword::Limit)])? {
        let next = parser.pop()?;

        match next {
            Token::Value(Value::Integer(value)) => Some(*value as usize),
            _ => return Err("STX: Expected integer after LIMIT".to_string()),
        }
    } else {
        None
    };

    // [ ( "INNER JOIN" | "JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "OUTER JOIN" ) , table_expression , "ON" , expression ]
    let join_expression = parse_join_expression(parser).ok();

    Ok(SelectStatement {
        is_distinct,
        select_expressions,
        from_expression,
        where_expression,
        group_by_expressions,
        having_expression,
        order_by_expression,
        join_expression,
        limit,
    })
}
