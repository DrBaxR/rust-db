use general::{
    parse_column_defs, parse_expression, parse_expressions, parse_identifiers,
    parse_join_expression, parse_order_by_expression, parse_select_expressions,
    parse_table_expression, parse_terms,
};

use super::{
    ast::{
        CreateIndexStatement, CreateTableStatement, DeleteStatement, InsertStatement,
        SelectStatement,
    },
    token::{identifier, keyword::Keyword, value::Value, Token},
    SqlParser,
};

mod general;

// TODO: test
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

// TODO: test
fn parse_create_table_statement(parser: &mut SqlParser) -> Result<CreateTableStatement, String> {
    parser.match_next(Token::Keyword(Keyword::CreateTable))?;
    let table_name = parser.match_next_identifier()?;
    let columns = parse_column_defs(parser)?;

    return Ok(CreateTableStatement {
        table_name,
        columns,
    });
}

// TODO: test
fn parse_create_index_statement(parser: &mut SqlParser) -> Result<CreateIndexStatement, String> {
    parser.match_next(Token::Keyword(Keyword::CreateIndex))?;
    let index_name = parser.match_next_identifier()?;
    parser.match_next(Token::Keyword(Keyword::On))?;
    let table_name = parser.match_next_identifier()?;
    let columns = parse_identifiers(parser)?;

    Ok(CreateIndexStatement {
        index_name,
        table_name,
        columns,
    })
}

// TODO: test
fn parse_delete_statement(parser: &mut SqlParser) -> Result<DeleteStatement, String> {
    parser.match_next(Token::Keyword(Keyword::Delete))?;
    parser.match_next(Token::Keyword(Keyword::From))?;
    let table_name = parser.match_next_identifier()?;

    let where_expression = if parser.match_next(Token::Keyword(Keyword::Where)).is_ok() {
        Some(parse_expression(parser)?)
    } else {
        None
    };

    let limit = if let Some(_) = parser.match_next_option(&vec![Token::Keyword(Keyword::Limit)])? {
        let next = parser.pop()?;

        match next {
            Token::Value(Value::Integer(value)) => Some(*value as usize),
            _ => return Err("STX: Expected integer after LIMIT".to_string()),
        }
    } else {
        None
    };

    Ok(DeleteStatement {
        table_name,
        where_expression,
        limit,
    })
}

// TODO: test
fn parse_insert_statement(parser: &mut SqlParser) -> Result<InsertStatement, String> {
    parser.match_next(Token::Keyword(Keyword::InsertInto))?;
    let table_name = parser.match_next_identifier()?;

    let slot = parser.save();
    let columns = if let Ok(identifiers) = parse_identifiers(parser) {
        identifiers
    } else {
        parser.load(slot);
        vec![]
    };

    parser.match_next(Token::Keyword(Keyword::Values))?;
    let values = parse_terms(parser)?;

    Ok(InsertStatement {
        table_name,
        columns,
        values,
    })
}
