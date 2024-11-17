use crate::parser::{
    ast::{
        general::{CountType, Expression, Function, Operand, TableExpression, Term},
        JoinExpression, OrderByExpression, SelectExpression,
    },
    token::{delimiter::Delimiter, function, keyword::Keyword, operator::Operator, Token},
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

/// Parse expression matching `value | function | "(" + operand + ")" | ( [ table_alias , "." ] , column_ref ) | row_value_constructor`.
// TODO: test
fn parse_term(parser: &mut SqlParser) -> Result<Term, String> {
    // value
    parser.save();
    if let Ok(value) = parser.match_next_value() {
        return Ok(Term::Value(value));
    }
    parser.load();

    // function
    parser.save();
    if let Ok(function) = parse_function(parser) {
        return Ok(Term::Function(function));
    }
    parser.load();

    // "(" + operand + ")"
    parser.save();
    if let Ok(operand) = parse_paren_operand(parser) {
        return Ok(Term::Operand(operand));
    }
    parser.load();

    // ( [ table_alias , "." ] , column_ref )
    parser.save();
    if let Ok((table_alias, name)) = parse_column_identifier(parser) {
        return Ok(Term::Column { table_alias, name });
    }
    parser.load();

    // row_value_constructor
    if let Ok(terms) = parse_row_value_constructor(parser) {
        return Ok(Term::RowValueConstructor(terms));
    }

    Err("STX: Expected term expression variant".to_string())
}

/// Parse expression matching `function_count | function_sum | function_avg | function_min | function_max | function_now `.
// TODO: test
fn parse_function(parser: &mut SqlParser) -> Result<Function, String> {
    match parser.match_next_function()? {
        function::Function::Count => {
            parser.match_next(Token::Delimiter(Delimiter::OpenParen))?;
            let distinct = parser.match_next(Token::Keyword(Keyword::Distinct)).is_ok();

            if parser
                .match_next(Token::Operator(Operator::Multiply))
                .is_ok()
            {
                // count * case
                parser.match_next(Token::Delimiter(Delimiter::CloseParen))?;
                return Ok(Function::Count {
                    distinct,
                    count_type: CountType::All,
                });
            }

            // count term case
            let term = parse_term(parser)?;
            parser.match_next(Token::Delimiter(Delimiter::CloseParen))?;

            Ok(Function::Count {
                distinct,
                count_type: CountType::Term(Box::new(term)),
            })
        }
        function::Function::Sum => Ok(Function::Sum(Box::new(parse_paren_term(parser)?))),
        function::Function::Avg => Ok(Function::Avg(Box::new(parse_paren_term(parser)?))),
        function::Function::Min => Ok(Function::Min(Box::new(parse_paren_term(parser)?))),
        function::Function::Max => Ok(Function::Max(Box::new(parse_paren_term(parser)?))),
        function::Function::Upper => Err("STX: UPPER function not supported".to_string()),
        function::Function::Lower => Err("STX: LOWER function not supported".to_string()),
        function::Function::Length => Err("STX: LENGTH function not supported".to_string()),
        function::Function::Round => Err("STX: ROUND function not supported".to_string()),
        function::Function::Now => {
            parser.match_next(Token::Delimiter(Delimiter::OpenParen))?;
            parser.match_next(Token::Delimiter(Delimiter::CloseParen))?;

            Ok(Function::Now)
        }
        function::Function::Coalesce => Err("STX: COALESCE function not supported".to_string()),
    }
}

/// Parse expression matching `"(" + term + ")"`.
fn parse_paren_term(parser: &mut SqlParser) -> Result<Term, String> {
    parser.match_next(Token::Delimiter(Delimiter::OpenParen))?;
    let term = parse_term(parser)?;
    parser.match_next(Token::Delimiter(Delimiter::CloseParen))?;

    Ok(term)
}

/// Parse expression matching `"(" + operand + ")"`.
// TODO: test
fn parse_paren_operand(parser: &mut SqlParser) -> Result<Operand, String> {
    parser.match_next(Token::Delimiter(Delimiter::OpenParen))?;
    let operand = parse_operand(parser)?;
    parser.match_next(Token::Delimiter(Delimiter::CloseParen))?;

    Ok(operand)
}

/// Parse expression matching `factor , { "+" | "-" , factor }`.
// TODO: test
fn parse_operand(parser: &mut SqlParser) -> Result<Operand, String> {
    todo!("this")
}

/// Parse expression matching `( [ table_alias , "." ] , column_ref )`.
// TODO: test
fn parse_column_identifier(parser: &mut SqlParser) -> Result<(Option<String>, String), String> {
    todo!("this")
}

/// Parse expression matching `"(" , term , "," , term , { "," , term } , ")"`.
// TODO: test
fn parse_row_value_constructor(parser: &mut SqlParser) -> Result<Vec<Term>, String> {
    todo!("this")
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
