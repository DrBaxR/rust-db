use super::char_matcher::ChrSqMatcher;

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

struct DelimiterTokenizer {
    matcher: ChrSqMatcher<Delimiter>,
}

impl DelimiterTokenizer {
    fn new() -> Self {
        Self {
            matcher: ChrSqMatcher::with(vec![
                (",", Delimiter::Comma),
                (";", Delimiter::Semicolon),
                (".", Delimiter::Dot),
                ("(", Delimiter::OpenParen),
                (")", Delimiter::CloseParen),
                ("[", Delimiter::OpenBracket),
                ("]", Delimiter::CloseBracket),
            ])
        }
    }

    fn largest_match(&self, raw: &str) -> Option<(Delimiter, usize)> {
        let mut fsm = self.matcher.as_fsm();

        let mut largest = None;
        for (i, c) in raw.chars().enumerate() {
            if fsm.transition(c).is_err() {
                return largest;
            }

            if let Some(value) = fsm.current_value() {
                largest = Some((value.clone(), i + 1))
            }
        }

        largest
    }
}