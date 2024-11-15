use ast::{
    general::{Expression, TableExpression},
    JoinExpression, OrderByExpression, SelectExpression, SelectStatement,
};
use token::{keyword::Keyword, value::Value, Token};

#[cfg(test)]
mod tests;

mod ast;
mod parse;
mod token;

struct SqlParser {
    tokens: Vec<Token>,
    cursor: usize,
    saved_cursor: usize,
}

impl SqlParser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, cursor: 0, saved_cursor: 0 }
    }

    /// Parses `sql` string and generates AST representation of it.
    ///
    /// # Errors
    /// Will return an `Err` if there was a lexing error, or if there was a syntax error.
    pub fn parse(&mut self) -> Result<(), ()> {
        todo!("Use AST terminal nodes defined in the parse module and implement the rules in the grammar")
    }

    fn pop(&mut self) -> Result<&Token, String> {
        let token = self
            .tokens
            .get(self.cursor)
            .ok_or("STX: Expected more tokens".to_string());
        self.cursor += 1;

        token
    }

    fn peek(&self) -> Result<&Token, String> {
        self.tokens
            .get(self.cursor)
            .ok_or("STX: Expected more tokens".to_string())
    }

    /// Marks current position as a checkpoint in token stream, which can be gone to with `load()`.
    fn save(&mut self) {
        self.saved_cursor = self.cursor;
    }

    /// Go to last marked checkpoint with `save()`. Will return to start of stream if no `save()` was called before calling this.
    fn load(&mut self) {
        self.cursor = self.saved_cursor;
    }

    /// Returns the next token if it matches any of the `expected_options`. Will return `None` if there is still a token but it matches none of the
    /// options. Also advances the internal token `cursor`.
    ///
    /// # Errors
    /// Will return `Err` if there are no more tokens in the stream.
    fn match_next_option(&mut self, expected_options: &[Token]) -> Result<Option<&Token>, String> {
        let next_token = self.peek()?;

        if expected_options.contains(next_token) {
            return Ok(Some(self.pop()?));
        }

        Ok(None)
    }

    /// Matches next token and moves cursor if matches `expected`.
    ///
    /// # Errors
    /// Will return `Err` if there are no more tokens in the stream OR if next token doesn't match `expected`
    fn match_next(&mut self, expected: Token) -> Result<(), String> {
        let next_token = self.peek()?;

        if expected != *next_token {
            return Err(format!(
                "STX: Expected {:?}, but got {:?}",
                expected, next_token
            ));
        }

        self.pop()?;
        Ok(())
    }

    /// Expects next token to be an identifier and advances cursor if matches.
    /// 
    /// # Errors
    /// Will return `Err` if tokens empty or no match.
    fn match_next_identifier(&mut self) -> Result<String, String> {
        let idenfier = match self.peek()? {
            Token::Identifier(identifier) => {
                Ok(identifier.clone())
            }
            _ => Err("STX: Expected an identifier".to_string()),
        };

        self.pop()?;
        idenfier
    }
}
