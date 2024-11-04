use super::char_matcher::ChrSqMatcher;

#[cfg(test)]
mod tests;

/// A data type used in SQL when defining the schema.
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    BigInt,
    Float,
    Double,
    Decimal,
    Varchar,
    Char,
    Text,
    Boolean,
    Date,
    Time,
    Timestamp,
    Binary,
}

struct DataTypeTokenizer {
    matcher: ChrSqMatcher<DataType>,
}

impl DataTypeTokenizer {
    fn new() -> Self {
        Self {
            matcher: ChrSqMatcher::with(vec![
                ("INTEGER", DataType::Integer),
                ("BIGINT", DataType::BigInt),
                ("FLOAT", DataType::Float),
                ("DOUBLE", DataType::Double),
                ("DECIMAL", DataType::Decimal),
                ("VARCHAR", DataType::Varchar),
                ("CHAR", DataType::Char),
                ("TEXT", DataType::Text),
                ("BOOLEAN", DataType::Boolean),
                ("DATE", DataType::Date),
                ("TIME", DataType::Time),
                ("TIMESTAMP", DataType::Timestamp),
                ("BINARY", DataType::Binary),
            ])
        }
    }

    fn largest_match(&self, raw: &str) -> Option<(DataType, usize)> {
        let mut fsm = self.matcher.as_fsm();

        let mut largest = None;
        for (i, c) in raw.chars().enumerate() {
            if fsm.transition(c.to_ascii_uppercase()).is_err() {
                return largest;
            }

            if let Some(value) = fsm.current_value() {
                largest = Some((value.clone(), i + 1))
            }
        }

        largest
    }
}
