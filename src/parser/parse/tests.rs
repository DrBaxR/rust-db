use crate::parser::{
    ast::{
        general::{
            AndCondition, ColumnDef, CompareType, Condition, Expression, Factor, Operand,
            Operation, TableExpression, Term,
        }, CreateIndexStatement, CreateTableStatement, DeleteStatement, ExplainStatement, InsertStatement, JoinExpression, JoinType, OrderByExpression, SelectExpression, SelectStatement, TransactionStatement, UpdateStatement
    },
    parse::{
        parse_create_index_statement, parse_create_table_statement, parse_delete_statement, parse_explain_statement, parse_insert_statement, parse_select_statement, parse_transaction_statement, parse_update_statement
    },
    token::{data_type::DataType, value::Value, Tokenizer},
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

#[test]
fn parse_create_table_statement_test() {
    let mut parser = get_parser("CREATE TABLE my_table (a INTEGER, b VARCHAR)");
    let expected = CreateTableStatement {
        table_name: "my_table".to_string(),
        columns: vec![
            ColumnDef {
                name: "a".to_string(),
                data_type: DataType::Integer,
            },
            ColumnDef {
                name: "b".to_string(),
                data_type: DataType::Varchar,
            },
        ],
    };

    assert_eq!(parse_create_table_statement(&mut parser).unwrap(), expected);
}

#[test]
fn parse_create_index_statement_test() {
    let mut parser = get_parser("CREATE INDEX my_index ON my_table (a, b)");
    let expected = CreateIndexStatement {
        index_name: "my_index".to_string(),
        table_name: "my_table".to_string(),
        columns: vec!["a".to_string(), "b".to_string()],
    };

    assert_eq!(parse_create_index_statement(&mut parser).unwrap(), expected)
}

#[test]
fn parse_delete_statement_test() {
    let mut parser = get_parser("DELETE FROM my_table WHERE true LIMIT 100");
    let expected = DeleteStatement {
        table_name: "my_table".to_string(),
        where_expression: Some(get_bool_expression(true)),
        limit: Some(100),
    };

    assert_eq!(parse_delete_statement(&mut parser).unwrap(), expected);
}

#[test]
fn parse_insert_statement_test() {
    let mut parser = get_parser("INSERT INTO my_table (a, b) VALUES (1, 2)");
    let expected = InsertStatement {
        table_name: "my_table".to_string(),
        columns: vec!["a".to_string(), "b".to_string()],
        values: vec![
            Term::Value(Value::Integer(1)),
            Term::Value(Value::Integer(2)),
        ],
    };

    assert_eq!(parse_insert_statement(&mut parser).unwrap(), expected);
}

#[test]
fn parse_update_statement_test() {
    let mut parser = get_parser("UPDATE my_table SET a = 1, b = 2 WHERE id = 3");
    let expected = UpdateStatement {
        table_name: "my_table".to_string(),
        values: vec![
            ("a".to_string(), Value::Integer(1)),
            ("b".to_string(), Value::Integer(2)),
        ],
        where_expression: Expression {
            and_conditions: vec![AndCondition {
                conditions: vec![Condition::Operation {
                    operand: Operand {
                        left: Factor {
                            left: Box::new(Term::Column {
                                table_alias: None,
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
                                left: Box::new(Term::Value(Value::Integer(3))),
                                right: vec![],
                            },
                            right: vec![],
                        },
                    }),
                }],
            }],
        },
    };

    assert_eq!(parse_update_statement(&mut parser).unwrap(), expected);
}

#[test]
fn parse_explain_statement_test() {
    let mut parser = get_parser("EXPLAIN DELETE FROM my_table WHERE true LIMIT 100");
    let expected = ExplainStatement::Delete(DeleteStatement {
        table_name: "my_table".to_string(),
        where_expression: Some(get_bool_expression(true)),
        limit: Some(100),
    });

    assert_eq!(parse_explain_statement(&mut parser).unwrap(), expected);
}

#[test]
fn parse_transaction_statement_test() {
    let mut parser = get_parser("COMMIT");
    assert_eq!(parse_transaction_statement(&mut parser).unwrap(), TransactionStatement::Commit);

    let mut parser = get_parser("BEGIN");
    assert_eq!(parse_transaction_statement(&mut parser).unwrap(), TransactionStatement::Begin);

    let mut parser = get_parser("ROLLBACK");
    assert_eq!(parse_transaction_statement(&mut parser).unwrap(), TransactionStatement::Rollback);
}