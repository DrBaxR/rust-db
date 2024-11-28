use crate::parser::{
    ast::{
        general::{
            AndCondition, Condition, CountType, Expression, Factor, FactorRight, Function, Operand,
            OperandRight, Operation, TableExpression, Term,
        },
        JoinExpression, JoinType, OrderByExpression, SelectExpression,
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
fn parse_condition(parser: &mut SqlParser) -> Result<Condition, String> {
    if let Ok(operand) = parse_operand(parser) {
        return Ok(Condition::Operation {
            operand,
            operation: parse_operation(parser).ok(),
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
pub fn parse_operation(parser: &mut SqlParser) -> Result<Operation, String> {
    let slot = parser.save();
    if let Ok(in_operation) = parse_in_operation(parser) {
        return Ok(in_operation);
    }
    parser.load(slot);

    let slot = parser.save();
    if let Ok(like_operation) = parse_like_operation(parser) {
        return Ok(like_operation);
    }
    parser.load(slot);

    let slot = parser.save();
    if let Ok(between_operation) = parse_between_operation(parser) {
        return Ok(between_operation);
    }
    parser.load(slot);

    let slot = parser.save();
    if let Ok(null_operation) = parse_null_operation(parser) {
        return Ok(null_operation);
    }
    parser.load(slot);

    let slot = parser.save();
    if let Ok(compare) = parser.match_next_comparison() {
        if let Ok(operand) = parse_operand(parser) {
            return Ok(Operation::Comparison {
                cmp_type: compare,
                operand,
            });
        }
    }
    parser.load(slot);

    Err("STX: Expected operation".to_string())
}

/// Parse expression matching `[ "NOT" ] , "IN" , "(" , constant_operand , { "," , constant_operand } , ")"`.
fn parse_in_operation(parser: &mut SqlParser) -> Result<Operation, String> {
    let not = parser.match_next(Token::Operator(Operator::Not)).is_ok();

    parser.match_next(Token::Operator(Operator::In))?;
    parser.match_next(Token::Delimiter(Delimiter::OpenParen))?;

    let mut operands = vec![parse_operand(parser)?];
    loop {
        if parser
            .match_next(Token::Delimiter(Delimiter::Comma))
            .is_err()
        {
            break;
        }

        operands.push(parse_operand(parser)?);
    }

    parser.match_next(Token::Delimiter(Delimiter::CloseParen))?;

    Ok(Operation::In { not, operands })
}

/// Parse expression matching `[ "NOT" ] , "LIKE" , string`.
fn parse_like_operation(parser: &mut SqlParser) -> Result<Operation, String> {
    let not = parser.match_next(Token::Operator(Operator::Not)).is_ok();

    parser.match_next(Token::Operator(Operator::Like))?;
    let value = parser.match_next_value()?;

    match value {
        crate::parser::token::value::Value::String(template) => {
            Ok(Operation::Like { not, template })
        }
        _ => Err("STX: Expected string value".to_string()),
    }
}

/// Parse expression matching `[ "NOT" ] , "BETWEEN" , operand , "AND" , operand`.
fn parse_between_operation(parser: &mut SqlParser) -> Result<Operation, String> {
    let not = parser.match_next(Token::Operator(Operator::Not)).is_ok();

    parser.match_next(Token::Keyword(Keyword::Between))?; // don't ask why BETWEEN is a keyword while the others are operators, I can't be bothered to change it
    let start = parse_operand(parser)?;
    parser.match_next(Token::Operator(Operator::And))?;
    let end = parse_operand(parser)?;

    Ok(Operation::Between { not, start, end })
}

/// Parse expression matching `"IS" , [ "NOT" ] , "NULL"`.
fn parse_null_operation(parser: &mut SqlParser) -> Result<Operation, String> {
    if parser.match_next(Token::Keyword(Keyword::IsNull)).is_ok() {
        return Ok(Operation::IsNull { not: false });
    }

    if parser
        .match_next(Token::Keyword(Keyword::IsNotNull))
        .is_ok()
    {
        return Ok(Operation::IsNull { not: true });
    }

    Err("STX: Expected null operation".to_string())
}

/// Parse expression matching `expression , { "," , expression }`.
pub fn parse_expressions(parser: &mut SqlParser) -> Result<Vec<Expression>, String> {
    let mut expressions = vec![parse_expression(parser)?];

    loop {
        if parser
            .match_next(Token::Delimiter(Delimiter::Comma))
            .is_err()
        {
            break;
        }

        expressions.push(parse_expression(parser)?);
    }

    Ok(expressions)
}

/// Parse expression matching `[ "ORDER BY" , expression , { "," , expression } , order ]`.
pub fn parse_order_by_expression(parser: &mut SqlParser) -> Result<OrderByExpression, String> {
    parser.match_next(Token::Keyword(Keyword::OrderBy))?;

    let expressions = parse_expressions(parser)?;
    match parser.match_next_keyword() {
        Ok(kw) => match kw {
            Keyword::Asc => Ok(OrderByExpression {
                expressions,
                asc: true,
            }),
            Keyword::Desc => Ok(OrderByExpression {
                expressions,
                asc: false,
            }),
            _ => Err("STX: Expected either ASC or DESC".to_string()),
        },
        Err(_) => Err("STX: Expected a keyword".to_string()),
    }
}

/// Parse expression matching `[ ( "INNER JOIN" | "JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "OUTER JOIN" ) , table_expression , "ON" , expression ]`.
// TODO: test
pub fn parse_join_expression(parser: &mut SqlParser) -> Result<JoinExpression, String> {
    let join_type = match parser.match_next_keyword()? {
        Keyword::InnerJoin => Ok(JoinType::Inner),
        Keyword::Join => Ok(JoinType::Inner),
        Keyword::LeftJoin => Ok(JoinType::Left),
        Keyword::OuterJoin => Ok(JoinType::Outer),
        Keyword::RightJoin => Ok(JoinType::Right),
        _ => Err("STX: Expected join type".to_string()),
    }?;

    let table = parse_table_expression(parser)?;

    parser.match_next(Token::Keyword(Keyword::On))?;
    let on = parse_expression(parser)?;

    Ok(JoinExpression {
        join_type,
        table,
        on,
    })
}
