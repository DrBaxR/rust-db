use data_type::DataType;
use delimiter::Delimiter;
use function::Function;
use keyword::Keyword;
use operator::Operator;
use value::Value;

// TODO: character by character FSM??? (start with operator, i think it's the most indicative)
mod data_type;
mod delimiter;
mod function;
mod keyword;
mod operator;
mod value;

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
