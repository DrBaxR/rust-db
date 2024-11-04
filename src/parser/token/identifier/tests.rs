use super::largest_match;

#[test]
fn matches() {
    assert_eq!(largest_match("sample(test)"), Some(("sample".to_string(), 6)));
    assert_eq!(largest_match("thisMaTcheS"), Some(("thisMaTcheS".to_string(), 11)));
    assert_eq!(largest_match("sample_underscore"), Some(("sample_underscore".to_string(), 17)));
    assert_eq!(largest_match("With1"), Some(("With1".to_string(), 5)));
    assert_eq!(largest_match("_test"), Some(("_test".to_string(), 5)));
    assert_eq!(largest_match("_test another"), Some(("_test".to_string(), 5)));
}

#[test]
fn no_match() {
    assert_eq!(largest_match("1invalid"), None);
    assert_eq!(largest_match(" invalid"), None);
    assert_eq!(largest_match("(invalid"), None);
}