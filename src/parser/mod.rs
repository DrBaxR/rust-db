// grammar: https://forcedotcom.github.io/phoenix/index.html
mod token;

// TODO: token-by-token FSM
/// A parser that can interpret some raw SQL string.
struct SqlParser {
    raw: String,
    cursor: usize,
    // state: ?
}

impl SqlParser {
    fn new(sql: String) -> Self {
        Self {
            raw: sql,
            cursor: 0,
        }
    }

    fn parse(&mut self) {
        todo!()
    }

    fn peek(&self) -> Self {
        todo!()
    }

    fn pop(&mut self) -> String {
        todo!()
    }
}
