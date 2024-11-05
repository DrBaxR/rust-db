use super::Token;

#[cfg(test)]
mod tests;

#[derive(PartialEq)]
enum IdentifierFsmState {
    Start,
    ValidStart,
    ValidContinue,
    Error,
}

impl IdentifierFsmState {
    pub fn transition(&self, c: char) -> IdentifierFsmState {
        match self {
            IdentifierFsmState::Start => Self::transition_start(c),
            IdentifierFsmState::ValidStart => Self::transition_valid_start(c),
            IdentifierFsmState::ValidContinue => Self::transition_valid_continue(c),
            IdentifierFsmState::Error => Self::transition_error(c),
        }
    }

    fn transition_start(c: char) -> IdentifierFsmState {
        if c.is_alphabetic() || c == '_' {
            IdentifierFsmState::ValidStart
        } else {
            IdentifierFsmState::Error
        }
    }

    fn transition_valid_start(c: char) -> IdentifierFsmState {
        if c.is_alphanumeric() || c == '_' || c == '$' || c == '#' {
            IdentifierFsmState::ValidContinue
        } else {
            IdentifierFsmState::Error
        }
    }

    fn transition_valid_continue(c: char) -> IdentifierFsmState {
        Self::transition_valid_start(c)
    }

    fn transition_error(c: char) -> IdentifierFsmState {
        IdentifierFsmState::Error
    }
}

/// Returns the largest matching identifier from the start of `raw` and the length of the match. Will return
/// `None` if there is no matching identifier at the start of the string (equivalent to returning a 0-length 
/// match).
pub fn largest_match(raw: &str) -> Option<(Token, usize)> {
    let mut state = IdentifierFsmState::Start;
    let mut cursor = 0;

    for c in raw.chars() {
        state = state.transition(c);
        if state == IdentifierFsmState::Error {
            break;
        }

        cursor += 1;
    }

    if cursor == 0 {
        return None;
    }

    Some((Token::Identifier(raw[0..cursor].to_string()), cursor))
}