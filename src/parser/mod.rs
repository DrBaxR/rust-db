use ast::{
    general::{Expression, TableExpression},
    SelectExpression, SelectStatement,
};
use token::{keyword::Keyword, Token};

#[cfg(test)]
mod tests;

mod ast;
mod token;

struct SqlParser {
    tokens: Vec<Token>,
    cursor: usize,
}

impl SqlParser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, cursor: 0 }
    }

    /// Parses `sql` string and generates AST representation of it.
    ///
    /// # Errors
    /// Will return an `Err` if there was a lexing error, or if there was a syntax error.
    pub fn parse(&mut self) -> Result<(), ()> {
        todo!("Use AST terminal nodes defined in the parse module and implement the rules in the grammar")
    }

    fn parse_select_statement(&mut self) -> Result<SelectStatement, String> {
        // SELECT
        self.match_next_option(&vec![Token::Keyword(Keyword::Select)])?
            .ok_or("STX: Expected SELECT keyword".to_string())?;

        // [ "DISTINCT" | "ALL" ]
        let is_distinct = match self.match_next_option(&vec![
            Token::Keyword(Keyword::Distinct),
            Token::Keyword(Keyword::All),
        ])? {
            Some(Token::Keyword(keyword)) => *keyword == Keyword::Distinct,
            None => false,
            _ => panic!("STX: Anomaly"),
        };

        // select_expression , { "," , select_expression }
        let select_expressions = self.parse_select_expressions()?;

        // FROM
        self.match_next_option(&vec![Token::Keyword(Keyword::From)])?
            .ok_or("STX: Expected FROM keyword".to_string())?;

        // table_expression
        let from_expression = self.parse_table_expression()?;

        // [ "WHERE" , expression ]
        let where_expression =
            if let Some(_) = self.match_next_option(&vec![Token::Keyword(Keyword::Where)])? {
                Some(self.parse_expression()?)
            } else {
                None
            };
        
        // [ "GROUP BY" , expression , { "," , expression } ]
        todo!()
    }

    fn parse_select_expressions(&mut self) -> Result<Vec<SelectExpression>, String> {
        todo!()
    }

    fn parse_table_expression(&mut self) -> Result<TableExpression, String> {
        todo!()
    }

    fn parse_expression(&mut self) -> Result<Expression, String> {
        todo!()
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

    /// Returns the next token if it matches any of the `expected_options`. Will return `None` if there is still a token but it matches none of the
    /// options.
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
}
