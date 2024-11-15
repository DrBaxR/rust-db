use crate::parser::{
    ast::{
        general::{Expression, TableExpression, Term},
        JoinExpression, OrderByExpression, SelectExpression,
    },
    token::{delimiter::Delimiter, keyword::Keyword, operator::Operator, Token},
    SqlParser,
};

#[cfg(test)]
mod tests;

/// Parse expression matching `select_expression , { "," , select_expression }`.
// TODO: test
pub fn parse_select_expressions(parser: &mut SqlParser) -> Result<Vec<SelectExpression>, String> {
    let mut select_expressions = vec![parse_select_expression(parser)?];

    loop {
        if parser
            .match_next(Token::Delimiter(Delimiter::Comma))
            .is_err()
        {
            // expressions list is done
            break;
        }

        select_expressions.push(parse_select_expression(parser)?);
    }

    Ok(select_expressions)
}

/// Parse expression matching `"*" | term , [ "AS" , column_alias ]`.
// TODO: test
fn parse_select_expression(parser: &mut SqlParser) -> Result<SelectExpression, String> {
    if parser
        .match_next(Token::Operator(Operator::Multiply))
        .is_ok()
    {
        return Ok(SelectExpression::All);
    }

    let term = parse_term(parser).map_err(|_| "STX: Expected either '*' or a term".to_string())?;
    let alias = if parser.match_next(Token::Keyword(Keyword::As)).is_ok() {
        Some(parser.match_next_identifier()?)
    } else {
        None
    };

    Ok(SelectExpression::As { term, alias })
}

fn parse_term(parser: &mut SqlParser) -> Result<Term, String> {
    todo!()
}

/// Parse expression matching `table_name , [ "AS" , table_alias ]`.
pub fn parse_table_expression(parser: &mut SqlParser) -> Result<TableExpression, String> {
    let table_name = parser.match_next_identifier()?;
    parser.match_next(Token::Keyword(Keyword::As))?;
    let alias = parser.match_next_identifier()?;

    Ok(TableExpression { table_name, alias })
}

pub fn parse_expression(parser: &mut SqlParser) -> Result<Expression, String> {
    todo!()
}

pub fn parse_expressions(parser: &mut SqlParser) -> Result<Vec<Expression>, String> {
    todo!()
}

pub fn parse_order_by_expression(
    parser: &mut SqlParser,
) -> Result<Option<OrderByExpression>, String> {
    todo!()
}

pub fn parse_join_expression(parser: &mut SqlParser) -> Result<Option<JoinExpression>, String> {
    todo!()
}
