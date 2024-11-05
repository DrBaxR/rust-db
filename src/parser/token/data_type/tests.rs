use crate::parser::token::{data_type::DataType, Token};

use super::DataTypeTokenizer;

#[test]
fn matches() {
    let tokenizer = DataTypeTokenizer::new();

    assert_eq!(
        tokenizer.largest_match("INTEGER"),
        Some((Token::DataType(DataType::Integer), 7))
    );
    assert_eq!(
        tokenizer.largest_match("VARCHAR"),
        Some((Token::DataType(DataType::Varchar), 7))
    );
    assert_eq!(
        tokenizer.largest_match("teXt"),
        Some((Token::DataType(DataType::Text), 4))
    );
    assert_eq!(
        tokenizer.largest_match("date"),
        Some((Token::DataType(DataType::Date), 4))
    );
    assert_eq!(
        tokenizer.largest_match("TIMESTAMPs"),
        Some((Token::DataType(DataType::Timestamp), 9))
    );
}

#[test]
fn no_match() {
    let tokenizer = DataTypeTokenizer::new();

    assert_eq!(tokenizer.largest_match(" integer"), None);
    assert_eq!(tokenizer.largest_match("test"), None);
}
