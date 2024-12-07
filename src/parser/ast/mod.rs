use general::{ColumnDef, Expression, TableExpression, Term};

use super::token::value::Value;

pub mod general;

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub is_distinct: bool,
    /// 1+
    pub select_expressions: Vec<SelectExpression>,
    pub from_expression: TableExpression,     // FROM
    pub where_expression: Option<Expression>, // WHERE
    /// 0+
    pub group_by_expressions: Vec<Expression>, // GROUP BY
    pub having_expression: Option<Expression>, // HAVING
    pub order_by_expression: Option<OrderByExpression>, // ORDER BY
    pub limit: Option<usize>,                 // LIMIT
    pub join_expression: Option<JoinExpression>, // JOIN
}

#[derive(Debug, PartialEq)]
pub struct JoinExpression {
    pub join_type: JoinType,
    pub table: TableExpression,
    pub on: Expression, // ON
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Inner, // JOIN | INNER JOIN
    Left,
    Right,
    Outer,
}

#[derive(Debug, PartialEq)]
pub struct OrderByExpression {
    /// 1+
    pub expressions: Vec<Expression>,
    /// either `ASC` or `DESC`
    pub asc: bool,
}

#[derive(Debug, PartialEq)]
pub enum SelectExpression {
    All,
    As { term: Term, alias: Option<String> },
}

#[derive(Debug, PartialEq)]
pub struct CreateTableStatement {
    pub table_name: String,
    /// 1+
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, PartialEq)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String, // ON
    /// 1+
    pub columns: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub struct DeleteStatement {
    pub table_name: String,                   // FROM
    pub where_expression: Option<Expression>, // WHERE
    pub limit: Option<usize>,                 // LIMIT
}

#[derive(Debug, PartialEq)]
pub struct InsertStatement {
    pub table_name: String, // INTO
    // 0+
    pub columns: Vec<String>,
    // 1+
    pub values: Vec<Term>, // VALUES
}

#[derive(Debug, PartialEq)]
pub struct UpdateStatement {
    pub table_name: String,
    /// 1+; represents (column_name, value)
    pub values: Vec<(String, Value)>, // SET
    pub where_expression: Expression,
}

#[derive(Debug, PartialEq)]
pub enum ExplainStatement {
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

#[derive(Debug, PartialEq)]
pub enum TransactionStatement {
    Begin,
    Commit,
    Rollback,
}
