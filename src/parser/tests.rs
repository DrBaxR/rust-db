use super::{
    token::{keyword::Keyword, Token},
    SqlParser,
};

#[test]
fn match_next_option() {
    let mut p = SqlParser::new(vec![Token::Keyword(Keyword::Select)]);
    assert_eq!(p.match_next_option(&vec![]), Ok(None));
    assert_eq!(
        p.match_next_option(&vec![Token::Keyword(Keyword::Select)]),
        Ok(Some(&Token::Keyword(Keyword::Select)))
    );

    let mut p = SqlParser::new(vec![Token::Keyword(Keyword::Select)]);
    assert_eq!(
        p.match_next_option(&vec![
            Token::Keyword(Keyword::Select),
            Token::Keyword(Keyword::Delete)
        ]),
        Ok(Some(&Token::Keyword(Keyword::Select)))
    );
    // cursor advanced and no more tokens
    assert!(p
        .match_next_option(&vec![Token::Keyword(Keyword::Select),])
        .is_err());

    let mut p = SqlParser::new(vec![]);
    assert!(p
        .match_next_option(&vec![
            Token::Keyword(Keyword::Select),
            Token::Keyword(Keyword::Delete)
        ])
        .is_err());
}
