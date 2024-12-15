use crate::parser::{
    ast::{
        general::{
            AndCondition, CompareType, Condition, Expression, Factor, Operand, Operation,
            TableExpression, Term,
        },
        JoinExpression, JoinType, OrderByExpression, SelectExpression, SelectStatement,
    },
    parse::parse_select_statement,
    token::{value::Value, Tokenizer},
    SqlParser,
};

fn get_parser(raw: &str) -> SqlParser {
    let tokens = Tokenizer::new().tokenize(raw).unwrap();
    SqlParser::new(tokens)
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
fn parse_select_statement_test() {
    let mut parser = get_parser("SELECT c.col_a AS a, d.col_b AS b FROM table_c AS c WHERE true ORDER BY true ASC LIMIT 100 JOIN table_d AS d ON c.id = d.id");
    let expected = SelectStatement {
        is_distinct: false,
        select_expressions: vec![
            SelectExpression::As {
                term: Term::Column {
                    table_alias: Some("c".to_string()),
                    name: "col_a".to_string(),
                },
                alias: Some("a".to_string()),
            },
            SelectExpression::As {
                term: Term::Column {
                    table_alias: Some("d".to_string()),
                    name: "col_b".to_string(),
                },
                alias: Some("b".to_string()),
            },
        ],
        from_expression: TableExpression {
            table_name: "table_c".to_string(),
            alias: Some("c".to_string()),
        },
        where_expression: Some(get_bool_expression(true)),
        group_by_expressions: vec![],
        having_expression: None,
        order_by_expression: Some(OrderByExpression {
            expressions: vec![get_bool_expression(true)],
            asc: true,
        }),
        limit: Some(100),
        join_expression: Some(JoinExpression {
            join_type: JoinType::Inner,
            table: TableExpression {
                table_name: "table_d".to_string(),
                alias: Some("d".to_string()),
            },
            on: Expression {
                and_conditions: vec![AndCondition {
                    conditions: vec![Condition::Operation {
                        operand: Operand {
                            left: Factor {
                                left: Box::new(Term::Column {
                                    table_alias: Some("c".to_string()),
                                    name: "id".to_string(),
                                }),
                                right: vec![],
                            },
                            right: vec![],
                        },
                        operation: Some(Operation::Comparison {
                            cmp_type: CompareType::EQ,
                            operand: Operand {
                                left: Factor {
                                    left: Box::new(Term::Column {
                                        table_alias: Some("d".to_string()),
                                        name: "id".to_string(),
                                    }),
                                    right: vec![],
                                },
                                right: vec![],
                            },
                        }),
                    }],
                }],
            },
        }),
    };

    assert_eq!(parse_select_statement(&mut parser).unwrap(), expected);
}