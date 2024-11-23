use crate::parser::{
    self,
    ast::{
        general::{
            AndCondition, CompareType, Condition, CountType, Expression, Factor, FactorRight,
            Function, Operand, OperandRight, Operation, TableExpression, Term,
        },
        SelectExpression,
    },
    parse::general::{
        parse_and_condition, parse_between_operation, parse_column_identifier, parse_condition,
        parse_expression, parse_factor, parse_function, parse_in_operation, parse_like_operation,
        parse_null_operation, parse_operand, parse_operation, parse_paren_term,
        parse_row_value_constructor, parse_select_expression, parse_select_expressions, parse_term,
    },
    token::{value::Value, Token, Tokenizer},
    SqlParser,
};

use super::parse_table_expression;

#[test]
fn parse_table_expression_test() {
    let tokens = Tokenizer::new().tokenize("my_table AS mt").unwrap();
    let mut p = SqlParser::new(tokens);

    assert_eq!(
        parse_table_expression(&mut p).unwrap(),
        TableExpression {
            table_name: "my_table".to_string(),
            alias: "mt".to_string()
        }
    );
}

#[test]
fn parse_paren_term_test() {
    let tokens = Tokenizer::new().tokenize("(1.12)").unwrap();
    let mut p = SqlParser::new(tokens);

    assert_eq!(
        parse_paren_term(&mut p).unwrap(),
        Term::Value(Value::Float(1.12))
    );

    let tokens = Tokenizer::new().tokenize("('test')").unwrap();
    let mut p = SqlParser::new(tokens);

    assert_eq!(
        parse_paren_term(&mut p).unwrap(),
        Term::Value(Value::String("test".to_string()))
    );

    let tokens = Tokenizer::new().tokenize("(*)").unwrap();
    let mut p = SqlParser::new(tokens);

    assert!(parse_paren_term(&mut p).is_err());
}

fn get_parser(raw: &str) -> SqlParser {
    let tokens = Tokenizer::new().tokenize(raw).unwrap();
    SqlParser::new(tokens)
}

#[test]
fn parse_term_test() {
    let mut p = get_parser("12");
    assert_eq!(parse_term(&mut p).unwrap(), Term::Value(Value::Integer(12)));

    let mut p = get_parser("MAX(12)");
    assert_eq!(
        parse_term(&mut p).unwrap(),
        Term::Function(Function::Max(Box::new(Term::Value(Value::Integer(12)))))
    );

    let mut p = get_parser("(1)");
    assert_eq!(
        parse_term(&mut p).unwrap(),
        Term::Operand(Operand {
            left: Factor {
                left: Box::new(Term::Value(Value::Integer(1))),
                right: vec![]
            },
            right: vec![]
        })
    );

    let mut p = get_parser("column");
    assert_eq!(
        parse_term(&mut p).unwrap(),
        Term::Column {
            table_alias: None,
            name: "column".to_string()
        }
    );

    let mut p = get_parser("(1, 2, 3)");
    assert_eq!(
        parse_term(&mut p).unwrap(),
        Term::RowValueConstructor(vec![
            Term::Value(Value::Integer(1)),
            Term::Value(Value::Integer(2)),
            Term::Value(Value::Integer(3))
        ])
    );
}

#[test]
fn parse_function_count() {
    let mut parser = get_parser("COUNT(*)");
    assert_eq!(
        parse_function(&mut parser).unwrap(),
        Function::Count {
            distinct: false,
            count_type: CountType::All
        }
    );

    let mut parser = get_parser("COUNT(column)");
    assert_eq!(
        parse_function(&mut parser).unwrap(),
        Function::Count {
            distinct: false,
            count_type: CountType::Term(Box::new(Term::Column {
                table_alias: None,
                name: "column".to_string()
            }))
        }
    );

    let mut parser = get_parser("COUNT( DISTINCT column )");
    assert_eq!(
        parse_function(&mut parser).unwrap(),
        Function::Count {
            distinct: true,
            count_type: CountType::Term(Box::new(Term::Column {
                table_alias: None,
                name: "column".to_string()
            }))
        }
    );
}

#[test]
fn parse_function_sum() {
    let mut parser = get_parser("SUM(column)");
    assert_eq!(
        parse_function(&mut parser).unwrap(),
        Function::Sum(Box::new(Term::Column {
            table_alias: None,
            name: "column".to_string()
        }))
    );

    let mut parser = get_parser("SUM(column");
    assert!(parse_function(&mut parser).is_err());
}

#[test]
fn parse_function_now() {
    let mut parser = get_parser("NOW()");
    assert_eq!(parse_function(&mut parser).unwrap(), Function::Now);
}

#[test]
fn parse_factor_test() {
    let mut parser = get_parser("1");
    assert_eq!(
        parse_factor(&mut parser).unwrap(),
        Factor {
            left: Box::new(Term::Value(Value::Integer(1))),
            right: vec![],
        }
    );

    let mut parser = get_parser("1*2/99");
    assert_eq!(
        parse_factor(&mut parser).unwrap(),
        Factor {
            left: Box::new(Term::Value(Value::Integer(1))),
            right: vec![
                FactorRight::Mult(Term::Value(Value::Integer(2))),
                FactorRight::Div(Term::Value(Value::Integer(99)))
            ],
        }
    );
}

#[test]
fn parse_operand_test() {
    let mut parser = get_parser("1");
    assert_eq!(
        parse_operand(&mut parser).unwrap(),
        Operand {
            left: Factor {
                left: Box::new(Term::Value(Value::Integer(1))),
                right: vec![]
            },
            right: vec![]
        }
    );

    let mut parser = get_parser("1 + 1 - 1");
    assert_eq!(
        parse_operand(&mut parser).unwrap(),
        Operand {
            left: Factor {
                left: Box::new(Term::Value(Value::Integer(1))),
                right: vec![]
            },
            right: vec![
                OperandRight::Plus(Factor {
                    left: Box::new(Term::Value(Value::Integer(1))),
                    right: vec![]
                }),
                OperandRight::Minus(Factor {
                    left: Box::new(Term::Value(Value::Integer(1))),
                    right: vec![]
                })
            ]
        }
    );
}

#[test]
fn parse_column_identifier_test() {
    let mut parser = get_parser("my_table.my_column");
    assert_eq!(
        parse_column_identifier(&mut parser).unwrap(),
        (Some("my_table".to_string()), "my_column".to_string())
    );

    let mut parser = get_parser("my_column");
    assert_eq!(
        parse_column_identifier(&mut parser).unwrap(),
        (None, "my_column".to_string())
    );
}

#[test]
fn parse_row_value_constructor_test() {
    let mut parser = get_parser("(1, 1)");
    assert_eq!(
        parse_row_value_constructor(&mut parser).unwrap(),
        vec![
            Term::Value(Value::Integer(1)),
            Term::Value(Value::Integer(1))
        ]
    );

    let mut parser = get_parser("(1, 1, 1)");
    assert_eq!(
        parse_row_value_constructor(&mut parser).unwrap(),
        vec![
            Term::Value(Value::Integer(1)),
            Term::Value(Value::Integer(1)),
            Term::Value(Value::Integer(1))
        ]
    );

    let mut parser = get_parser("(1)");
    assert!(parse_row_value_constructor(&mut parser).is_err());
}

#[test]
fn parse_select_expression_test() {
    let mut parser = get_parser("*");
    assert_eq!(
        parse_select_expression(&mut parser).unwrap(),
        SelectExpression::All
    );

    let mut parser = get_parser("1");
    assert_eq!(
        parse_select_expression(&mut parser).unwrap(),
        SelectExpression::As {
            term: Term::Value(Value::Integer(1)),
            alias: None
        }
    );

    let mut parser = get_parser("1 as my_column");
    assert_eq!(
        parse_select_expression(&mut parser).unwrap(),
        SelectExpression::As {
            term: Term::Value(Value::Integer(1)),
            alias: Some("my_column".to_string())
        }
    );
}

#[test]
fn parse_select_expressions_test() {
    let mut parser = get_parser("*");
    assert_eq!(
        parse_select_expressions(&mut parser).unwrap(),
        vec![SelectExpression::All]
    );

    let mut parser = get_parser("*, *, *");
    assert_eq!(
        parse_select_expressions(&mut parser).unwrap(),
        vec![
            SelectExpression::All,
            SelectExpression::All,
            SelectExpression::All
        ]
    );
}

#[test]
fn parse_null_operation_test() {
    let mut parser = get_parser("IS NULL");
    assert_eq!(
        parse_null_operation(&mut parser).unwrap(),
        Operation::IsNull { not: false }
    );

    let mut parser = get_parser("IS NOT NULL");
    assert_eq!(
        parse_null_operation(&mut parser).unwrap(),
        Operation::IsNull { not: true }
    );

    let mut parser = get_parser("IS");
    assert!(parse_null_operation(&mut parser).is_err());
}

fn get_number_operand(number: i64) -> Operand {
    Operand {
        left: Factor {
            left: Box::new(Term::Value(Value::Integer(number))),
            right: vec![],
        },
        right: vec![],
    }
}

#[test]
fn parse_between_operation_test() {
    let mut parser = get_parser("BETWEEN 1 AND 1");
    assert_eq!(
        parse_between_operation(&mut parser).unwrap(),
        Operation::Between {
            not: false,
            start: get_number_operand(1),
            end: get_number_operand(1)
        }
    );

    let mut parser = get_parser("NOT BETWEEN 1 AND 1");
    assert_eq!(
        parse_between_operation(&mut parser).unwrap(),
        Operation::Between {
            not: true,
            start: get_number_operand(1),
            end: get_number_operand(1)
        }
    );
    let mut parser = get_parser("NOT BETWEEN 1 1");
    assert!(parse_between_operation(&mut parser).is_err(),);
}

#[test]
fn parse_like_operation_test() {
    let mut parser = get_parser("LIKE 'test'");
    assert_eq!(
        parse_like_operation(&mut parser).unwrap(),
        Operation::Like {
            not: false,
            template: "test".to_string()
        }
    );

    let mut parser = get_parser("NOT LIKE 'test'");
    assert_eq!(
        parse_like_operation(&mut parser).unwrap(),
        Operation::Like {
            not: true,
            template: "test".to_string()
        }
    );

    let mut parser = get_parser("NOT LIKE 1");
    assert!(parse_like_operation(&mut parser).is_err(),);
}

#[test]
fn parse_in_operation_test() {
    let mut parser = get_parser("IN (1)");
    assert_eq!(
        parse_in_operation(&mut parser).unwrap(),
        Operation::In {
            not: false,
            operands: vec![get_number_operand(1)]
        }
    );

    let mut parser = get_parser("NOT IN (1, 1)");
    assert_eq!(
        parse_in_operation(&mut parser).unwrap(),
        Operation::In {
            not: true,
            operands: vec![get_number_operand(1), get_number_operand(1)]
        }
    );

    let mut parser = get_parser("IN 1");
    assert!(parse_in_operation(&mut parser).is_err(),);
}

#[test]
fn parse_operation_test() {
    let mut parser = get_parser("IS NULL");
    assert_eq!(
        parse_operation(&mut parser).unwrap(),
        Operation::IsNull { not: false }
    );

    let mut parser = get_parser("BETWEEN 1 AND 1");
    assert_eq!(
        parse_operation(&mut parser).unwrap(),
        Operation::Between {
            not: false,
            start: get_number_operand(1),
            end: get_number_operand(1)
        }
    );

    let mut parser = get_parser("LIKE 'test'");
    assert_eq!(
        parse_operation(&mut parser).unwrap(),
        Operation::Like {
            not: false,
            template: "test".to_string()
        }
    );

    let mut parser = get_parser("IN (1)");
    assert_eq!(
        parse_operation(&mut parser).unwrap(),
        Operation::In {
            not: false,
            operands: vec![get_number_operand(1)]
        }
    );

    let mut parser = get_parser("<= 1");
    assert_eq!(
        parse_operation(&mut parser).unwrap(),
        Operation::Comparison {
            cmp_type: CompareType::LTE,
            operand: get_number_operand(1)
        }
    );
}

fn get_bool_expression(value: bool) -> Expression {
    Expression {
        and_conditions: vec![AndCondition {
            conditions: vec![Condition::Operation {
                operand: Operand {
                    left: Factor {
                        left: Box::new(Term::Value(Value::Boolean(value))),
                        right: vec![],
                    },
                    right: vec![],
                },
                operation: None,
            }],
        }],
    }
}

#[test]
fn parse_condition_test() {
    let mut parser = get_parser("1");
    assert_eq!(
        parse_condition(&mut parser).unwrap(),
        Condition::Operation {
            operand: get_number_operand(1),
            operation: None,
        }
    );

    let mut parser = get_parser("1 < 2");
    assert_eq!(
        parse_condition(&mut parser).unwrap(),
        Condition::Operation {
            operand: get_number_operand(1),
            operation: Some(Operation::Comparison {
                cmp_type: CompareType::LT,
                operand: get_number_operand(2)
            })
        }
    );

    let mut parser = get_parser("NOT true");
    assert_eq!(
        parse_condition(&mut parser).unwrap(),
        Condition::Negative(get_bool_expression(true))
    );
}

fn get_bool_operand(value: bool) -> Operand {
    Operand {
        left: Factor {
            left: Box::new(Term::Value(Value::Boolean(value))),
            right: vec![],
        },
        right: vec![],
    }
}

#[test]
fn parse_and_condition_test() {
    let mut parser = get_parser("true");
    assert_eq!(
        parse_and_condition(&mut parser).unwrap(),
        AndCondition {
            conditions: vec![Condition::Operation {
                operand: get_bool_operand(true),
                operation: None
            },]
        }
    );

    let mut parser = get_parser("true AND true AND true");
    assert_eq!(
        parse_and_condition(&mut parser).unwrap(),
        AndCondition {
            conditions: vec![
                Condition::Operation {
                    operand: get_bool_operand(true),
                    operation: None
                },
                Condition::Operation {
                    operand: get_bool_operand(true),
                    operation: None
                },
                Condition::Operation {
                    operand: get_bool_operand(true),
                    operation: None
                }
            ]
        }
    );
}

#[test]
fn parse_expression_test() {
    let mut parser = get_parser("true");
    assert_eq!(
        parse_expression(&mut parser).unwrap(),
        get_bool_expression(true)
    );

    let mut parser = get_parser("true OR true");
    assert_eq!(
        parse_expression(&mut parser).unwrap(),
        Expression {
            and_conditions: vec![
                AndCondition {
                    conditions: vec![Condition::Operation {
                        operand: Operand {
                            left: Factor {
                                left: Box::new(Term::Value(Value::Boolean(true))),
                                right: vec![],
                            },
                            right: vec![],
                        },
                        operation: None,
                    }],
                },
                AndCondition {
                    conditions: vec![Condition::Operation {
                        operand: Operand {
                            left: Factor {
                                left: Box::new(Term::Value(Value::Boolean(true))),
                                right: vec![],
                            },
                            right: vec![],
                        },
                        operation: None,
                    }],
                }
            ],
        }
    );
}
