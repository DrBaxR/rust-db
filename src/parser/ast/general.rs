use crate::parser::token::{data_type::DataType, value::Value};

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub enum Function {
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

#[derive(Debug, PartialEq)]
pub enum CountType {
    All,
    Term(Box<Term>),
}

/// Represents a summand for arithmetic operations.
#[derive(Debug, PartialEq)]
pub struct Operand {
    pub left: Factor,
    /// 0+
    pub right: Vec<OperandRight>,
}

#[derive(Debug, PartialEq)]
pub enum OperandRight {
    Plus(Factor),
    Minus(Factor),
}

#[derive(Debug, PartialEq)]
pub struct Factor {
    pub left: Box<Term>,
    pub right: Vec<FactorRight>,
}

#[derive(Debug, PartialEq)]
pub enum FactorRight {
    Mult(Term),
    Div(Term),
}

#[derive(Debug, PartialEq)]
pub struct TableExpression {
    pub table_name: String,
    pub alias: Option<String>,
}

/// Represents an `OR` expression for boolean operations.
#[derive(Debug, PartialEq)]
pub struct Expression {
    /// 1+ (separated by OR)
    pub and_conditions: Vec<AndCondition>,
}

#[derive(Debug, PartialEq)]
pub struct AndCondition {
    /// 1+ (separated by AND)
    pub conditions: Vec<Condition>,
}

#[derive(Debug, PartialEq)]
pub enum Condition {
    Operation {
        operand: Operand,
        operation: Option<Operation>,
    },
    Negative(Expression),
    Positive(Expression),
}

#[derive(Debug, PartialEq)]
pub enum Operation {
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

#[derive(Debug, PartialEq)]
pub enum CompareType {
    EQ,  // =
    NE,  // != or <>
    GT,  // >
    GTE, // >=
    LT,  // <
    LTE, // <=
}

#[derive(Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    /// At the moment varchar is of set size (255)
    pub data_type: DataType,
}
