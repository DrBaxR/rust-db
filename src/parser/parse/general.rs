use crate::parser::token::{data_type::DataType, value::Value};

pub enum Term {
    Value(Value),
    Function(Function),
    Operand(Operand),
    Column {
        table_alias: Option<String>,
        name: String,
    },
    /// 2+
    RowValueConstructor(Vec<Term>),
}

enum Function {
    Count {
        distinct: bool,
        count_type: CountType,
    },
    Sum(Box<Term>),
    Avg(Box<Term>),
    Min(Box<Term>),
    Max(Box<Term>),
    Now,
}

enum CountType {
    All,
    Term(Box<Term>),
}

/// Represents a summand for arithmetic operations.
struct Operand {
    /// either `+` or `-`
    addition: bool,
    /// 1+
    factors: Vec<Factor>,
}

struct Factor {
    /// either `*` or `/`
    multiplication: bool,
    /// 1+
    terms: Vec<Term>,
}

pub struct TableExpression {
    table_name: String,
    alias: String,
}

/// Represents an `OR` expression for boolean operations.
pub struct Expression {
    /// 1+
    and_conditions: Vec<AndCondition>,
}

struct AndCondition {
    /// 1+
    conditions: Vec<Condition>,
}

enum Condition {
    Operation {
        operand: Operand,
        operation: Option<Operation>,
    },
    Negative(Expression),
    Positive(Expression),
}

enum Operation {
    Comparison {
        cmp_type: CompareType,
        operand: Operand,
    },
    In {
        not: bool,
        /// 1+
        operands: Vec<Operand>,
    },
    Like {
        not: bool,
        template: String,
    },
    Between {
        not: bool,
        start: Operand,
        end: Operand,
    },
    IsNull {
        not: bool,
    },
}

enum CompareType {
    EQ,  // =
    NE,  // != or <>
    GT,  // >
    GTE, // >=
    LT,  // <
    LTE, // <=
}

pub struct ColumnDef {
    name: String,
    /// At the moment varchar is of set size (255)
    data_type: DataType,
}
