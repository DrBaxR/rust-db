use crate::parser::token::data_type::DataType;

use super::DataTypeTokenizer;

#[test]
fn matches() {
    let tokenizer = DataTypeTokenizer::new();

    assert_eq!(tokenizer.largest_match("INTEGER"), Some((DataType::Integer, 7)));
    assert_eq!(tokenizer.largest_match("VARCHAR"), Some((DataType::Varchar, 7)));
    assert_eq!(tokenizer.largest_match("teXt"), Some((DataType::Text, 4)));
    assert_eq!(tokenizer.largest_match("date"), Some((DataType::Date, 4)));
    assert_eq!(tokenizer.largest_match("TIMESTAMPs"), Some((DataType::Timestamp, 9)));
}

#[test]
fn no_match() {
    let tokenizer = DataTypeTokenizer::new();

    assert_eq!(tokenizer.largest_match(" integer"), None);
    assert_eq!(tokenizer.largest_match("test"), None);
}