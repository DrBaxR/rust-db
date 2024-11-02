// grammar: https://forcedotcom.github.io/phoenix/index.html

// TODO: character by character FSM???
/// A subset of all the SQL spec keywords (didn't include the ones I don't feel are that important). Got
/// them from [here](https://www.w3schools.com/sql/sql_ref_keywords.asp).
enum Keyword {
    Any,
    As,
    Asc,
    Between,
    Create,
    CreateIndex,
    CreateTable,
    Delete,
    Desc,
    Distinct,
    Explain,
    From,
    GroupBy,
    Having,
    Index,
    InnerJoin,
    InsertInto,
    IsNull,
    IsNotNull,
    Join,
    LeftJoin,
    Limit,
    NotNull,
    OrderBy,
    OuterJoin,
    RightJoin,
    Rownum,
    Select,
    SelectDistinct,
    Set,
    Table,
    TruncateTable,
    Update,
    Values,
    Where,
}

/// A value literal used in a SQL statement.
enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

/// An operator used in a SQL statement.
enum Operator {
    Plus,     // +
    Minus,    // -
    Multiply, // *
    Divide,   // /
    Modulus,  // %

    Equal,              // =
    NotEqual,           // != or <>
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    LessThan,           // <
    LessThanOrEqual,    // <=

    And,  // AND
    Or,   // OR
    Not,  // NOT
    Like, // LIKE
    In,   // IN
    Is,   // IS
}

/// A delimiter/punctuation used in a SQL statement.
enum Delimiter {
    Comma,        // ,
    Semicolon,    // ;
    Dot,          // .
    OpenParen,    // (
    CloseParen,   // )
    OpenBracket,  // [
    CloseBracket, // ]
}

/// A data type used in SQL when defining the schema.
enum DataType {
    Integer,
    BigInt,
    Float,
    Double,
    Decimal,
    Varchar,
    Char,
    Text,
    Boolean,
    Date,
    Time,
    Timestamp,
    Binary,
}

/// A type of function used in SQL.
enum Function {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Upper,
    Lower,
    Length,
    Round,
    Now,
    Coalesce,
}

/// A token that represents a single unit of a SQL statement.
enum Token {
    Keyword(Keyword),
    Identifier(String),
    Operator(Operator),
    Value(Value),
    Delimiter(Delimiter),
    Function(Function),
    DataType(DataType),
    EndOfStatement,
}

// TODO: token-by-token FSM
/// A parser that can interpret some raw SQL string.
struct SqlParser {
    raw: String,
    cursor: usize,
    // state: ?
}

impl SqlParser {
    fn new(sql: String) -> Self {
        Self {
            raw: sql,
            cursor: 0,
        }
    }

    fn parse(&mut self) {
        todo!()
    }

    fn peek(&self) -> Self {
        todo!()
    }

    fn pop(&mut self) -> String {
        todo!()
    }
}
