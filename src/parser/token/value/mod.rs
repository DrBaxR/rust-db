use super::char_matcher::ChrSqMatcher;

#[cfg(test)]
mod tests;

/// A value literal used in a SQL statement.
#[derive(PartialEq, Clone)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

#[derive(PartialEq)]
enum ValueFsmState {
    Start,
    Integer,
    FractionalStart,
    Fractional,
    String,
    StringEnd,
    /// `None` if in non-terminal state
    Keyword(Option<Value>),
    Error,
}

impl ValueFsmState {
    pub fn transition(&self, c: char) -> Self {
        match self {
            Self::Start => Self::transition_start(c),
            Self::Integer => Self::transition_integer(c),
            Self::FractionalStart => Self::transition_fractional_start(c),
            Self::Fractional => Self::transition_fractional(c),
            Self::String => Self::transition_string(c),
            Self::StringEnd => Self::Error,
            Self::Keyword(_) => {
                panic!("Transition from keyword start should be handled externally")
            }
            Self::Error => Self::Error,
        }
    }

    fn transition_start(c: char) -> Self {
        if c.is_numeric() {
            Self::Integer
        } else if c == '.' {
            Self::FractionalStart
        } else if c == '\'' {
            Self::String
        } else if c.to_ascii_uppercase() == 'T'
            || c.to_ascii_uppercase() == 'F'
            || c.to_ascii_uppercase() == 'N'
        {
            Self::Keyword(None)
        } else {
            Self::Error
        }
    }

    fn transition_integer(c: char) -> Self {
        if c.is_numeric() {
            Self::Integer
        } else if c == '.' {
            Self::FractionalStart
        } else {
            Self::Error
        }
    }

    fn transition_fractional_start(c: char) -> Self {
        if c.is_numeric() {
            Self::Fractional
        } else {
            Self::Error
        }
    }

    fn transition_fractional(c: char) -> Self {
        if c.is_numeric() {
            Self::Fractional
        } else {
            Self::Error
        }
    }

    fn transition_string(c: char) -> Self {
        if c == '\'' {
            Self::StringEnd
        } else {
            Self::String
        }
    }
}

struct ValueTokenizer {
    matcher: ChrSqMatcher<Value>,
}

impl ValueTokenizer {
    pub fn new() -> Self {
        // first character is used to transition to the parent FSM's delegating state
        Self {
            matcher: ChrSqMatcher::with(vec![
                ("RUE", Value::Boolean(true)),
                ("ALSE", Value::Boolean(false)),
                ("ULL", Value::Null),
            ]),
        }
    }

    /// Returns the longest matching value in `raw` and the size of the characters that have been matched. Will
    /// return `None` if there is no value matching the start of the string.
    pub fn largest_match(&self, raw: &str) -> Option<(Value, usize)> {
        let mut keyword_fsm = self.matcher.as_fsm();
        let mut state = ValueFsmState::Start;
        let mut cursor = 0;

        // scanner
        for c in raw.chars() {
            if let ValueFsmState::Keyword(_) = state {
                let is_err = keyword_fsm.transition(c.to_ascii_uppercase()).is_err();
                state = ValueFsmState::Keyword(keyword_fsm.current_value().cloned());

                if is_err {
                    break;
                }

                cursor += 1;
            } else {
                state = state.transition(c);

                if state == ValueFsmState::Error {
                    break;
                }

                cursor += 1;
            }
        }

        // evaluator
        let raw_value = &raw[0..cursor];

        match state {
            ValueFsmState::Start => None,
            ValueFsmState::Integer => Some((
                Value::Integer(
                    raw_value
                        .parse()
                        .expect("Integer value expected in Integer state"),
                ),
                cursor,
            )),
            ValueFsmState::FractionalStart => None,
            ValueFsmState::Fractional => Some((
                Value::Float(
                    raw_value
                        .parse()
                        .expect("Float value expected in Float state"),
                ),
                cursor,
            )),
            ValueFsmState::String => None,
            ValueFsmState::StringEnd => Some((
                Value::String(raw_value[1..raw_value.len() - 1].to_string()),
                cursor,
            )),
            ValueFsmState::Keyword(value) => {
                if let Some(value) = value {
                    Some((value, cursor))
                } else {
                    None
                }
            }
            ValueFsmState::Error => None,
        }
    }
}
