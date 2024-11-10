use general::{ColumnDef, Expression, TableExpression, Term};

use super::token::value::Value;

pub mod general;

pub struct SelectStatement {
    is_distinct: bool,
    /// 1+
    select_expressions: Vec<SelectExpression>,
    from_expression: TableExpression,     // FROM
    where_expression: Option<Expression>, // WHERE
    /// 0+
    group_by_expressions: Vec<Expression>, // GROUP BY
    having_expression: Option<Expression>, // HAVING
    order_by_expressions: Option<OrderByExpression>, // ORDER BY
    limit: Option<usize>,                 // LIMIT
    join: Option<JoinExpression>,         // JOIN
}

struct JoinExpression {
    join_type: JoinType,
    table: TableExpression,
    on_expression: Expression, // ON
}

enum JoinType {
    Inner, // JOIN | INNER JOIN
    Left,
    Right,
    Outer,
}

struct OrderByExpression {
    /// 1+
    expressions: Vec<Expression>,
    /// either `ASC` or `DESC`
    asc: bool,
}

enum SelectExpression {
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
