use std::collections::HashMap;

use ast::{
    general::{CompareType, Expression, TableExpression},
    JoinExpression, OrderByExpression, SelectExpression, SelectStatement,
};
use token::{
    data_type::DataType,
    function::{self, Function},
    keyword::Keyword,
    value::Value,
    Token,
};

#[cfg(test)]
mod tests;

mod ast;
mod parse;
mod token;

struct SqlParser {
    tokens: Vec<Token>,
    cursor: usize,
    /// slot -> cursor_pos
    saves: HashMap<usize, usize>,
}

impl SqlParser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            cursor: 0,
            saves: HashMap::new(),
        }
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
    fn save(&mut self) -> usize {
        let next_slot = self.saves.keys().max().unwrap_or(&0) + 1;
        self.saves.insert(next_slot, self.cursor);

        next_slot
    }

    /// Go to last marked checkpoint with `save()`. Will return to start of stream if no `save()` was called before calling this.
    fn load(&mut self, slot: usize) {
        self.cursor = self.saves.remove(&slot).expect("Invalid save slot ID");
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
            Token::Identifier(identifier) => Ok(identifier.clone()),
            _ => Err("STX: Expected an identifier".to_string()),
        };

        self.pop()?;
        idenfier
    }

    /// Expects next token to be a value and advances cursor if matches. Will return the matched `Value` in case
    /// of match.
    ///
    /// # Errors
    /// Will return `Err` if tokens empty or no match.
    fn match_next_value(&mut self) -> Result<Value, String> {
        let value = match self.peek()? {
            Token::Value(value) => Ok(value.clone()),
            _ => Err("STX: Expected a value".to_string()),
        };

        self.pop()?;
        value
    }

    /// Expects next token to be a function token and advances cursor if matches. Will return the matched `Function`
    /// token in case of match.
    ///
    /// # Errors
    /// Will return `Err` if tokens empty or no match.
    fn match_next_function(&mut self) -> Result<Function, String> {
        let function = match self.peek()? {
            Token::Function(function) => Ok(function.clone()),
            _ => Err("STX: Expected function token".to_string()),
        };

        self.pop()?;
        function
    }

    /// Expects next token to be an operator token and advances cursor if matches. Besides being an operator, it also expects it
    /// to be a comparison operator and will return the casted `CompareType` representation of that operator.
    ///
    /// # Errors
    /// Will return `Err` if tokens empty or no match.
    fn match_next_comparison(&mut self) -> Result<CompareType, String> {
        let compare = match self.peek()? {
            Token::Operator(operator) => match operator {
                token::operator::Operator::Equal => Ok(CompareType::EQ),
                token::operator::Operator::NotEqual => Ok(CompareType::NE),
                token::operator::Operator::GreaterThan => Ok(CompareType::GT),
                token::operator::Operator::GreaterThanOrEqual => Ok(CompareType::GTE),
                token::operator::Operator::LessThan => Ok(CompareType::LT),
                token::operator::Operator::LessThanOrEqual => Ok(CompareType::LTE),
                _ => Err("STX: Expected compare operator".to_string()),
            },
            _ => Err("STX: Expected compare operator".to_string()),
        };

        self.pop()?;
        compare
    }

    /// Expects next token to be a keyword and returns the keyword in case of a match (also moves cursor in case of match).
    ///
    /// # Errors
    /// Will return `Err` if tokens empty or no match.
    fn match_next_keyword(&mut self) -> Result<Keyword, String> {
        let keyword = match self.peek()? {
            Token::Keyword(keyword) => Ok(keyword.clone()),
            _ => Err("STX: Expected a keyword".to_string()),
        };

        self.pop()?;
        keyword
    }

    /// Expects next token to be a data type and returns the data type in case of a match (also moves cursor in case of match).
    ///
    /// # Errors
    /// Will return `Err` if tokens empty or no match.
    fn match_next_data_type(&mut self) -> Result<DataType, String> {
        let data_type = match self.peek()? {
            Token::DataType(data_type) => Ok(data_type.clone()),
            _ => Err("STX: Expected a data type".to_string()),
        };

        self.pop()?;
        data_type
    }
}
