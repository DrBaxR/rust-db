use ast::SelectStatement;
use token::{keyword::Keyword, Token};

mod token;
mod ast;

struct SqlParser {
    tokens: Vec<Token>,
    cursor: usize,
}

impl SqlParser {
    /// Parses `sql` string and generates AST representation of it.
    ///
    /// # Errors
    /// Will return an `Err` if there was a lexing error, or if there was a syntax error.
    pub fn parse(&mut self) -> Result<(), ()> {
        todo!("Use AST terminal nodes defined in the parse module and implement the rules in the grammar")
    }

    fn parse_select_statement(&mut self) -> Result<SelectStatement, String> {
        let token = self.pop()?;
        if *token != Token::Keyword(Keyword::Select) {
            return Err("STX: Expected SELECT keyword".to_string());
        }

        let token = self.pop()?;
        let mut is_distinct = false;
        if *token == Token::Keyword(Keyword::Distinct) {
            is_distinct = true;
        } else if *token == Token::Keyword(Keyword::All) {
            is_distinct = false;
        } 

        todo!()
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.cursor)
    }

    fn advance(&mut self) {
        self.cursor += 1;
    }

    fn reset(&mut self) {
        self.cursor = 0;
    }

    fn pop(&mut self) -> Result<&Token, String> {
        let token = self.tokens.get(self.cursor).ok_or("STX: Expected more tokens".to_string());
        self.cursor += 1;

        token
    }
}
