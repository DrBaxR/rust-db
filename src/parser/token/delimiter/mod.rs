use super::{char_matcher::ChrSqMatcher, Token};

mod tests;

/// A delimiter/punctuation used in a SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Delimiter {
    Comma,        // ,
    Semicolon,    // ;
    Dot,          // .
    OpenParen,    // (
    CloseParen,   // )
    OpenBracket,  // [
    CloseBracket, // ]
}

pub struct DelimiterTokenizer {
    matcher: ChrSqMatcher<Delimiter>,
}

impl DelimiterTokenizer {
    pub fn new() -> Self {
        Self {
            matcher: ChrSqMatcher::with(vec![
                (",", Delimiter::Comma),
                (";", Delimiter::Semicolon),
                (".", Delimiter::Dot),
                ("(", Delimiter::OpenParen),
                (")", Delimiter::CloseParen),
                ("[", Delimiter::OpenBracket),
                ("]", Delimiter::CloseBracket),
            ]),
        }
    }

    pub fn largest_match(&self, raw: &str) -> Option<(Token, usize)> {
        let mut fsm = self.matcher.as_fsm();

        let mut largest = None;
        for (i, c) in raw.chars().enumerate() {
            if fsm.transition(c).is_err() {
                return largest;
            }

            if let Some(value) = fsm.current_value() {
                largest = Some((Token::Delimiter(value.clone()), i + 1))
            }
        }

        largest
    }
}
