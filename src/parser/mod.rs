// grammar: https://forcedotcom.github.io/phoenix/index.html
// keywords: https://www.w3schools.com/sql/sql_ref_keywords.asp
enum Keyword {
    Create,
    CreateIndex,
    CreateTable,
    Delete,
    Desc,
    Distinct,
    From,
    GroupBy,
    Having,
    In,
    Index,
    InnerJoin,
    InsertInto,
    IsNull,
    IsNotNull,
    Join,
    LeftJoin
    // TODO
}

enum Value {
    // TODO
}

enum Operator {
    // TODO
}

enum Delimiter {
    // TODO
}

enum FunctionToken {
    // TODO
}

enum DataType {
    // TODO
}

enum Token {
    Keyword(Keyword),
    Identifier(String),
    Operator(Operator),
    Value(Value),
    Delimiter(Delimiter),
    Function(String),
    DataType(DataType)
}