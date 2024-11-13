use general::{ColumnDef, Expression, TableExpression, Term};

use super::token::value::Value;

pub mod general;

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
    pub join_expression: Option<JoinExpression>,         // JOIN
}

pub struct JoinExpression {
    join_type: JoinType,
    table: TableExpression,
    on_expression: Expression, // ON
}

pub enum JoinType {
    Inner, // JOIN | INNER JOIN
    Left,
    Right,
    Outer,
}

pub struct OrderByExpression {
    /// 1+
    expressions: Vec<Expression>,
    /// either `ASC` or `DESC`
    asc: bool,
}

pub enum SelectExpression {
    All,
    As { term: Term, alias: String },
}

pub struct CreateTableStatement {
    table_name: String,
    /// 1+
    columns: Vec<ColumnDef>,
}

pub struct CreateIndexStatement {
    index_name: String,
    table: String, // ON
    /// 1+
    on_columns: Vec<String>,
}

pub struct DeleteStatement {
    table_name: String,                   // FROM
    where_expression: Option<Expression>, // WHERE
    limit: Option<usize>,                 // LIMIT
}

pub struct InsertStatement {
    table_name: String, // INTO
    // 0+
    columns: Vec<String>,
    // 1+
    values: Vec<Term>, // VALUES
}

pub struct UpdateStatement {
    table_name: String,
    /// 1+; represents (column_name, value)
    values: Vec<(String, Value)>, // SET
    where_expression: Expression,
}

pub enum ExplainStatement {
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

pub enum TransactionStatement {
    Begin,
    Commit,
    Rollback,
}
