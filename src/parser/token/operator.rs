/// An operator used in a SQL statement.
pub enum Operator {
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
