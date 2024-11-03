use super::CharMatcher;

#[test]
fn empty () {
    let matcher = CharMatcher::new();

    assert!(!matcher.is_match("abc"));
    assert!(!matcher.is_match("test123 wow"));
    assert!(!matcher.is_match("this will never match you idiot"));
}

#[test]
fn full_distinct_matches() {
    let matcher = CharMatcher::with(&vec!["john", "doe", "123"]);

    assert!(matcher.is_match("john"));
    assert!(matcher.is_match("doe"));
    assert!(matcher.is_match("123"));

    assert!(!matcher.is_match("andi"));
    assert!(!matcher.is_match("sql"));
}

#[test]
fn add_post_create() {
    let mut matcher = CharMatcher::new();
    assert!(!matcher.is_match("test"));

    matcher.add_match("test");
    assert!(matcher.is_match("test"));
}

#[test]
fn partial_matching() {
    let mut matcher = CharMatcher::with(&vec!["in", "is", "instant", "instance"]);

    assert!(matcher.is_match("in"));
    assert!(matcher.is_match("instant"));

    assert!(!matcher.is_match("inst"));
    assert!(!matcher.is_match("insta"));
    assert!(!matcher.is_match("i"));

    matcher.add_match("i");
    assert!(matcher.is_match("i"));
}