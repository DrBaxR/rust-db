use crate::parser::{
    ast::{
        general::{
            AndCondition, Condition, CountType, Expression, Factor, FactorRight, Function, Operand,
            OperandRight, Operation, TableExpression, Term,
        },
        JoinExpression, OrderByExpression, SelectExpression,
    },
    token::{delimiter::Delimiter, function, keyword::Keyword, operator::Operator, Token},
    SqlParser,
};

#[cfg(test)]
mod tests;

/// Parse expression matching `select_expression , { "," , select_expression }`.
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
fn parse_term(parser: &mut SqlParser) -> Result<Term, String> {
    // value
    let slot = parser.save();
    if let Ok(value) = parser.match_next_value() {
        return Ok(Term::Value(value));
    }
    parser.load(slot);

    // function
    let slot = parser.save();
    if let Ok(function) = parse_function(parser) {
        return Ok(Term::Function(function));
    }
    parser.load(slot);

    // "(" + operand + ")"
    let slot = parser.save();
    if let Ok(operand) = parse_paren_operand(parser) {
        return Ok(Term::Operand(operand));
    }
    parser.load(slot);

    // ( [ table_alias , "." ] , column_ref )
    let slot = parser.save();
    if let Ok((table_alias, name)) = parse_column_identifier(parser) {
        return Ok(Term::Column { table_alias, name });
    }
    dbg!(slot);
    dbg!(&parser.saves);
    parser.load(slot);
    dbg!(parser.cursor);

    // row_value_constructor
    if let Ok(terms) = parse_row_value_constructor(parser) {
        return Ok(Term::RowValueConstructor(terms));
    }

    Err(format!(
        "STX: Expected term expression variant, got {:?}",
        parser.peek()
    ))
}

/// Parse expression matching `function_count | function_sum | function_avg | function_min | function_max | function_now `.
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
fn parse_paren_operand(parser: &mut SqlParser) -> Result<Operand, String> {
    parser.match_next(Token::Delimiter(Delimiter::OpenParen))?;
    let operand = parse_operand(parser)?;
    parser.match_next(Token::Delimiter(Delimiter::CloseParen))?;

    Ok(operand)
}

/// Parse expression matching `factor , { "+" | "-" , factor }`.
fn parse_operand(parser: &mut SqlParser) -> Result<Operand, String> {
    let left = parse_factor(parser)?;

    let mut right = vec![];
    loop {
        let is_plus = parser.match_next(Token::Operator(Operator::Plus)).is_ok();
        let is_minus = parser.match_next(Token::Operator(Operator::Minus)).is_ok();
        if !is_plus && !is_minus {
            break;
        }

        let factor = parse_factor(parser)?;
        if is_plus {
            right.push(OperandRight::Plus(factor));
        } else if is_minus {
            right.push(OperandRight::Minus(factor));
        }
    }

    Ok(Operand { left, right })
}

/// Parse expression matching `term , { "*" | "/" , term }`.
fn parse_factor(parser: &mut SqlParser) -> Result<Factor, String> {
    let left = Box::new(parse_term(parser)?);

    let mut right = vec![];
    loop {
        let is_mul = parser
            .match_next(Token::Operator(Operator::Multiply))
            .is_ok();
        let is_div = parser.match_next(Token::Operator(Operator::Divide)).is_ok();
        if !is_mul && !is_div {
            break;
        }

        let term = parse_term(parser)?;
        if is_mul {
            right.push(FactorRight::Mult(term));
        } else if is_div {
            right.push(FactorRight::Div(term));
        }
    }

    Ok(Factor { left, right })
}

/// Parse expression matching `( [ table_alias , "." ] , column_ref )`.
fn parse_column_identifier(parser: &mut SqlParser) -> Result<(Option<String>, String), String> {
    let first = parser.match_next_identifier()?;

    let second = if parser.match_next(Token::Delimiter(Delimiter::Dot)).is_ok() {
        Some(parser.match_next_identifier()?)
    } else {
        None
    };

    if let Some(second) = second {
        Ok((Some(first), second))
    } else {
        Ok((None, first))
    }
}

/// Parse expression matching `"(" , term , "," , term , { "," , term } , ")"`.
fn parse_row_value_constructor(parser: &mut SqlParser) -> Result<Vec<Term>, String> {
    parser.match_next(Token::Delimiter(Delimiter::OpenParen))?;

    let mut terms = vec![];
    terms.push(parse_term(parser)?);
    parser.match_next(Token::Delimiter(Delimiter::Comma))?;
    terms.push(parse_term(parser)?);

    loop {
        if parser
            .match_next(Token::Delimiter(Delimiter::Comma))
            .is_err()
        {
            break;
        }

        terms.push(parse_term(parser)?);
    }
    parser.match_next(Token::Delimiter(Delimiter::CloseParen))?;

    Ok(terms)
}

/// Parse expression matching `table_name , [ "AS" , table_alias ]`.
pub fn parse_table_expression(parser: &mut SqlParser) -> Result<TableExpression, String> {
    let table_name = parser.match_next_identifier()?;
    parser.match_next(Token::Keyword(Keyword::As))?;
    let alias = parser.match_next_identifier()?;

    Ok(TableExpression { table_name, alias })
}

/// Parse expression matching `and_condition , { "OR" , and_condition }`.
// TODO: test
pub fn parse_expression(parser: &mut SqlParser) -> Result<Expression, String> {
    let mut and_conditions = vec![parse_and_condition(parser)?];

    loop {
        if parser.match_next(Token::Operator(Operator::Or)).is_err() {
            break;
        }

        and_conditions.push(parse_and_condition(parser)?);
    }

    Ok(Expression { and_conditions })
}

/// Parse expression matching `condition , { "AND" , condition }`.
// TODO: test
fn parse_and_condition(parser: &mut SqlParser) -> Result<AndCondition, String> {
    let mut conditions = vec![parse_condition(parser)?];

    loop {
        if parser.match_next(Token::Operator(Operator::And)).is_err() {
            break;
        }

        conditions.push(parse_condition(parser)?);
    }

    Ok(AndCondition { conditions })
}

/// Parse expression matching
/// ```
/// ( operand , [
///   ( compare , operand )
///   | ( [ "NOT" ] , "IN" , "(" , constant_operand , { "," , constant_operand } , ")" )
///   | ( [ "NOT" ] , "LIKE" , string )
///   | ( [ "NOT" ] , "BETWEEN" , operand , "AND" , operand )
///   | ( "IS" , [ "NOT" ] , "NULL" )
///   ] )
/// |   "NOT" , expression
/// |   "(" , expression , ")"
/// ```
// TODO: test
fn parse_condition(parser: &mut SqlParser) -> Result<Condition, String> {
    if let Ok(operand) = parse_operand(parser) {
        return Ok(Condition::Operation {
            operand,
            operation: parse_operation(parser)?,
        });
    }

    if parser.match_next(Token::Operator(Operator::Not)).is_ok() {
        return Ok(Condition::Negative(parse_expression(parser)?));
    }

    if parser
        .match_next(Token::Delimiter(Delimiter::OpenParen))
        .is_ok()
    {
        let expression = parse_expression(parser)?;
        parser.match_next(Token::Delimiter(Delimiter::CloseParen))?;

        return Ok(Condition::Positive(expression));
    }

    Err("STX: Expected condition".to_string())
}

/// Parse expression matching
/// ```
/// [
///   ( compare , operand )
///   | ( [ "NOT" ] , "IN" , "(" , constant_operand , { "," , constant_operand } , ")" )
///   | ( [ "NOT" ] , "LIKE" , string )
///   | ( [ "NOT" ] , "BETWEEN" , operand , "AND" , operand )
///   | ( "IS" , [ "NOT" ] , "NULL" )
/// ]
/// ```
// TODO: test
pub fn parse_operation(parser: &mut SqlParser) -> Result<Option<Operation>, String> {
    todo!("this");
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
