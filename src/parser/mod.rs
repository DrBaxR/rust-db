use token::Token;

mod token;
mod parse;

struct SqlParser {
    tokens: Vec<Token>,
    cursor: usize,
}

impl SqlParser {
    /// Parses `sql` string and generates AST representation of it.
    ///
    /// # Errors
    /// Will return an `Err` if there was a lexing error, or if there was a syntax error.
    pub fn parse() -> Result<(), ()> {
        todo!("Use AST terminal nodes defined in the parse module and implement the rules in the grammar")
    }
}
