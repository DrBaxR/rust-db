use data_type::DataType;
use delimiter::Delimiter;
use function::Function;
use keyword::Keyword;
use operator::Operator;
use value::Value;

mod char_matcher;

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
