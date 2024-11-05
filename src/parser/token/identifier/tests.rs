use crate::parser::token::Token;

use super::largest_match;

#[test]
fn matches() {
    assert_eq!(largest_match("sample(test)"), Some((Token::Identifier("sample".to_string()), 6)));
    assert_eq!(largest_match("thisMaTcheS"), Some((Token::Identifier("thisMaTcheS".to_string()), 11)));
    assert_eq!(largest_match("sample_underscore"), Some((Token::Identifier("sample_underscore".to_string()), 17)));
    assert_eq!(largest_match("With1"), Some((Token::Identifier("With1".to_string()), 5)));
    assert_eq!(largest_match("_test"), Some((Token::Identifier("_test".to_string()), 5)));
    assert_eq!(largest_match("_test another"), Some((Token::Identifier("_test".to_string()), 5)));
}

#[test]
fn no_match() {
    assert_eq!(largest_match("1invalid"), None);
    assert_eq!(largest_match(" invalid"), None);
    assert_eq!(largest_match("(invalid"), None);
}