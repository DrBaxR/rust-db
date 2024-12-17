use general::{
    parse_column_defs, parse_expression, parse_expressions, parse_identifiers,
    parse_join_expression, parse_order_by_expression, parse_select_expressions, parse_set_values,
    parse_table_expression, parse_terms,
};

use super::{
    ast::{
        CreateIndexStatement, CreateTableStatement, DeleteStatement, ExplainStatement,
        InsertStatement, SelectStatement, TransactionStatement, UpdateStatement,
    },
    token::{keyword::Keyword, value::Value, Token},
    SqlParser,
};

mod general;
mod tests;

pub fn parse_select_statement(parser: &mut SqlParser) -> Result<SelectStatement, String> {
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

pub fn parse_create_table_statement(parser: &mut SqlParser) -> Result<CreateTableStatement, String> {
    parser.match_next(Token::Keyword(Keyword::CreateTable))?;
    let table_name = parser.match_next_identifier()?;
    let columns = parse_column_defs(parser)?;

    return Ok(CreateTableStatement {
        table_name,
        columns,
    });
}

pub fn parse_create_index_statement(parser: &mut SqlParser) -> Result<CreateIndexStatement, String> {
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

pub fn parse_delete_statement(parser: &mut SqlParser) -> Result<DeleteStatement, String> {
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

pub fn parse_insert_statement(parser: &mut SqlParser) -> Result<InsertStatement, String> {
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

pub fn parse_update_statement(parser: &mut SqlParser) -> Result<UpdateStatement, String> {
    parser.match_next(Token::Keyword(Keyword::Update))?;
    let table_name = parser.match_next_identifier()?;

    parser.match_next(Token::Keyword(Keyword::Set))?;
    let values = parse_set_values(parser)?;

    parser.match_next(Token::Keyword(Keyword::Where))?;
    let where_expression = parse_expression(parser)?;

    Ok(UpdateStatement {
        table_name,
        values,
        where_expression,
    })
}

pub fn parse_explain_statement(parser: &mut SqlParser) -> Result<ExplainStatement, String> {
    parser.match_next(Token::Keyword(Keyword::Explain))?;

    if let Ok(select_statement) = parse_select_statement(parser) {
        return Ok(ExplainStatement::Select(select_statement));
    }

    if let Ok(update_statement) = parse_update_statement(parser) {
        return Ok(ExplainStatement::Update(update_statement));
    }

    if let Ok(delete_statement) = parse_delete_statement(parser) {
        return Ok(ExplainStatement::Delete(delete_statement));
    }

    Err("STX: Expected select, update or delete statement".to_string())
}

pub fn parse_transaction_statement(parser: &mut SqlParser) -> Result<TransactionStatement, String> {
    if parser.match_next(Token::Keyword(Keyword::Begin)).is_ok() {
        return Ok(TransactionStatement::Begin);
    }

    if parser.match_next(Token::Keyword(Keyword::Commit)).is_ok() {
        return Ok(TransactionStatement::Commit);
    }

    if parser.match_next(Token::Keyword(Keyword::Rollback)).is_ok() {
        return Ok(TransactionStatement::Rollback);
    }

    Err("STX: Expected BEGIN, COMMIT or ROLLBACK".to_string())
}
