use super::ChrSqMatcher;

#[test]
fn empty () {
    let matcher = ChrSqMatcher::<usize>::new();

    assert!(matcher.get_match("abc").is_none());
    assert!(matcher.get_match("test123 wow").is_none());
    assert!(matcher.get_match("this will never match you idiot").is_none());
}

#[test]
fn full_distinct_matches() {
    let matcher = ChrSqMatcher::with(vec![("john", 1), ("doe", 2), ("123", 3)]);

    assert!(matcher.get_match("john").is_some());
    assert!(matcher.get_match("doe").is_some());
    assert!(matcher.get_match("123").is_some());

    assert!(matcher.get_match("andi").is_none());
    assert!(matcher.get_match("sql").is_none());
}

#[test]
fn add_post_create() {
    let mut matcher = ChrSqMatcher::new();
    assert!(matcher.get_match("test").is_none());

    matcher.add_match("test", 1);
    assert!(matcher.get_match("test").is_some());
}

#[test]
fn partial_matching() {
    let mut matcher = ChrSqMatcher::with(vec![("in", 1), ("is", 2), ("instant", 3), ("instance", 4)]);

    assert!(matcher.get_match("in").is_some());
    assert!(matcher.get_match("instant").is_some());

    assert!(matcher.get_match("inst").is_none());
    assert!(matcher.get_match("insta").is_none());
    assert!(matcher.get_match("i").is_none());

    matcher.add_match("i", 3);
    assert!(matcher.get_match("i").is_some());
}

#[test]
fn matching_value() {
    let matcher = ChrSqMatcher::with(vec![("one", 1), ("two", 2), ("three", 3)]);
    
    assert_eq!(matcher.get_match("one"), Some(&1));
    assert_eq!(matcher.get_match("two"), Some(&2));
    assert_eq!(matcher.get_match("three"), Some(&3));
    assert_eq!(matcher.get_match("four"), None);
}