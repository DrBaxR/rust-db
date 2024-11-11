use super::{char_matcher::ChrSqMatcher, Token};

mod tests;

/// A subset of all the SQL spec keywords (didn't include the ones I don't feel are that important). Got
/// them from [here](https://www.w3schools.com/sql/sql_ref_keywords.asp).
#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    All,
    Any,
    As,
    Asc,
    Begin,
    Between,
    Commit,
    Create,
    CreateIndex,
    CreateTable,
    Delete,
    Desc,
    Distinct,
    Explain,
    From,
    GroupBy,
    Having,
    Index,
    InnerJoin,
    InsertInto,
    IsNull,
    IsNotNull,
    Join,
    LeftJoin,
    Limit,
    NotNull,
    On,
    OrderBy,
    OuterJoin,
    RightJoin,
    Rollback,
    Rownum,
    Select,
    SelectDistinct,
    Set,
    Table,
    TruncateTable,
    Update,
    Values,
    Where,
}

pub struct KeywordTokenizer {
    matcher: ChrSqMatcher<Keyword>,
}

impl KeywordTokenizer {
    pub fn new() -> Self {
        Self {
            matcher: ChrSqMatcher::with(vec![
                ("ALL", Keyword::All),
                ("ANY", Keyword::Any),
                ("AS", Keyword::As),
                ("ASC", Keyword::Asc),
                ("BEGIN", Keyword::Begin),
                ("BETWEEN", Keyword::Between),
                ("COMMIT", Keyword::Commit),
                ("CREATE", Keyword::Create),
                ("CREATE INDEX", Keyword::CreateIndex),
                ("CREATE TABLE", Keyword::CreateTable),
                ("DELETE", Keyword::Delete),
                ("DESC", Keyword::Desc),
                ("DISTINCT", Keyword::Distinct),
                ("EXPLAIN", Keyword::Explain),
                ("FROM", Keyword::From),
                ("GROUP BY", Keyword::GroupBy),
                ("HAVING", Keyword::Having),
                ("INDEX", Keyword::Index),
                ("INNER JOIN", Keyword::InnerJoin),
                ("INSERT INTO", Keyword::InsertInto),
                ("IS NULL", Keyword::IsNull),
                ("IS NOT NULL", Keyword::IsNotNull),
                ("JOIN", Keyword::Join),
                ("LEFT JOIN", Keyword::LeftJoin),
                ("LIMIT", Keyword::Limit),
                ("NOT NULL", Keyword::NotNull),
                ("ON", Keyword::On),
                ("ORDER BY", Keyword::OrderBy),
                ("OUTER JOIN", Keyword::OuterJoin),
                ("RIGHT JOIN", Keyword::RightJoin),
                ("ROLLBACK", Keyword::Rollback),
                ("ROWNUM", Keyword::Rownum),
                ("SELECT", Keyword::Select),
                ("SELECT DISTINCT", Keyword::SelectDistinct),
                ("SET", Keyword::Set),
                ("TABLE", Keyword::Table),
                ("TRUNCATE TABLE", Keyword::TruncateTable),
                ("UPDATE", Keyword::Update),
                ("VALUES", Keyword::Values),
                ("WHERE", Keyword::Where),
            ]),
        }
    }

    /// Returns the longest matching keyword in `raw` and the size of the characters that have been matched.
    pub fn largest_match(&self, raw: &str) -> Option<(Token, usize)> {
        let mut fsm = self.matcher.as_fsm();

        let mut largest = None;
        for (i, c) in raw.chars().enumerate() {
            if fsm.transition(c.to_ascii_uppercase()).is_err() {
                return largest;
            }

            if let Some(value) = fsm.current_value() {
                largest = Some((Token::Keyword(value.clone()), i + 1))
            }
        }

        largest
    }
}
