use super::{char_matcher::ChrSqMatcher, Token};

#[cfg(test)]
mod tests;

/// An operator used in a SQL statement.
#[derive(Clone, PartialEq, Debug)]
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

pub struct OperatorTokenizer {
    matcher: ChrSqMatcher<Operator>,
}

impl OperatorTokenizer {
    pub fn new() -> Self {
        Self {
            matcher: ChrSqMatcher::with(vec![
                ("+", Operator::Plus),
                ("-", Operator::Minus),
                ("*", Operator::Multiply),
                ("/", Operator::Divide),
                ("%", Operator::Modulus),
                ("=", Operator::Equal),
                ("!=", Operator::NotEqual),
                ("<>", Operator::NotEqual),
                (">", Operator::GreaterThan),
                (">=", Operator::GreaterThanOrEqual),
                ("<", Operator::LessThan),
                ("<=", Operator::LessThanOrEqual),
                ("AND", Operator::And),
                ("OR", Operator::Or),
                ("NOT", Operator::Not),
                ("LIKE", Operator::Like),
                ("IN", Operator::In),
                ("IS", Operator::Is),
            ]),
        }
    }

    /// Returns the longest matching operator in `raw` and the size of the characters that have been matched.
    pub fn largest_match(&self, raw: &str) -> Option<(Token, usize)> {
        let mut fsm = self.matcher.as_fsm();

        let mut largest = None;
        for (i, c) in raw.chars().enumerate() {
            if fsm.transition(c.to_ascii_uppercase()).is_err() {
                return largest;
            }

            if let Some(value) = fsm.current_value() {
                largest = Some((Token::Operator(value.clone()), i + 1))
            }
        }

        largest
    }
}
