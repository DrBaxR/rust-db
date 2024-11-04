/// A value literal used in a SQL statement.
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

struct ValueTokenizer {
    // TODO
}