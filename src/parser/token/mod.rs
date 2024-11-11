use data_type::{DataType, DataTypeTokenizer};
use delimiter::{Delimiter, DelimiterTokenizer};
use function::{Function, FunctionTokenizer};
use keyword::{Keyword, KeywordTokenizer};
use operator::{Operator, OperatorTokenizer};
use value::{Value, ValueTokenizer};

#[cfg(test)]
mod tests;

mod char_matcher;

pub mod data_type;
pub mod delimiter;
pub mod function;
pub mod identifier;
pub mod keyword;
pub mod operator;
pub mod value;

/// A token that represents a single unit of a SQL statement.
#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    Keyword(Keyword),
    Identifier(String),
    Operator(Operator),
    Value(Value),
    Delimiter(Delimiter),
    Function(Function),
    DataType(DataType),
}

/// Takes in a raw string as input and outputs a list of tokens.
pub struct Tokenizer {
    // identifier tokenizer is just a function
    data_type_tokenizer: DataTypeTokenizer,
    delimiter_tokenizer: DelimiterTokenizer,
    function_tokenizer: FunctionTokenizer,
    keyword_wokenizer: KeywordTokenizer,
    operator_tokenizer: OperatorTokenizer,
    value_tokenizer: ValueTokenizer,
}

impl Tokenizer {
    pub fn new() -> Self {
        Self {
            data_type_tokenizer: DataTypeTokenizer::new(),
            delimiter_tokenizer: DelimiterTokenizer::new(),
            function_tokenizer: FunctionTokenizer::new(),
            keyword_wokenizer: KeywordTokenizer::new(),
            operator_tokenizer: OperatorTokenizer::new(),
            value_tokenizer: ValueTokenizer::new(),
        }
    }

    /// Tokenizes the `raw` input string.
    ///
    /// # Errors
    /// Will return `Err` if there are any portions of the `raw` string that don't fit
    /// in any of the tokens categories.
    pub fn tokenize(&self, raw: &str) -> Result<Vec<Token>, ()> {
        let mut tokens = vec![];

        let mut raw = raw;
        loop {
            let token = self.next_token(raw).ok_or(())?;

            tokens.push(token.0);
            raw = &raw[token.1..];

            if raw.len() == 0 {
                break;
            }

            while raw.chars().next().unwrap().is_whitespace() {
                raw = &raw[1..];
            }
        }

        Ok(tokens)
    }

    /// Returns the next token that `raw` starts with. Will return `None` if there is no matching token.
    fn next_token(&self, raw: &str) -> Option<(Token, usize)> {
        let token_matches = vec![
            identifier::largest_match(raw), // identifier should be FIRST, because max returns last of largest value (important for keywords)
            self.data_type_tokenizer.largest_match(raw),
            self.delimiter_tokenizer.largest_match(raw),
            self.function_tokenizer.largest_match(raw),
            self.keyword_wokenizer.largest_match(raw),
            self.operator_tokenizer.largest_match(raw),
            self.value_tokenizer.largest_match(raw),
        ];

        token_matches
            .iter()
            .filter(|t| t.is_some())
            .map(|t| t.clone().unwrap())
            .max_by(|x, y| x.1.cmp(&y.1))
    }
}
