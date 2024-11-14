use crate::parser::{
    ast::{
        general::{Expression, TableExpression},
        JoinExpression, OrderByExpression, SelectExpression,
    }, token::{keyword::Keyword, Token}, SqlParser
};

#[cfg(test)]
mod tests;

pub fn parse_select_expressions(parser: &mut SqlParser) -> Result<Vec<SelectExpression>, String> {
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
