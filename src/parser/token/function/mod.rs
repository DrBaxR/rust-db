use super::{char_matcher::ChrSqMatcher, Token};

#[cfg(test)]
mod tests;

/// A type of function used in SQL.
#[derive(Debug, Clone, PartialEq)]
pub enum Function {
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

pub struct FunctionTokenizer {
    matcher: ChrSqMatcher<Function>,
}

impl FunctionTokenizer {
    pub fn new() -> Self {
        Self {
            matcher: ChrSqMatcher::with(vec![
                ("COUNT", Function::Count),
                ("SUM", Function::Sum),
                ("AVG", Function::Avg),
                ("MIN", Function::Min),
                ("MAX", Function::Max),
                ("UPPER", Function::Upper),
                ("LOWER", Function::Lower),
                ("LENGTH", Function::Length),
                ("ROUND", Function::Round),
                ("NOW", Function::Now),
                ("COALESCE", Function::Coalesce),
            ])
        }
    }

    pub fn largest_match(&self, raw: &str) -> Option<(Token, usize)> {
        let mut fsm = self.matcher.as_fsm();

        let mut largest = None;
        for (i, c) in raw.chars().enumerate() {
            if fsm.transition(c.to_ascii_uppercase()).is_err() {
                return largest;
            }

            if let Some(value) = fsm.current_value() {
                largest = Some((Token::Function(value.clone()), i + 1))
            }
        }

        largest
    }
}