#[derive(Clone)]
pub struct Schema {
    columns: Vec<Column>,
    offsets: Vec<usize>,
    tuple_length: usize,
}

impl Schema {
    /// Creates a new schema with `columns`.
    ///
    /// # Panics
    /// Will panic when `columns` is empty.
    pub fn new(columns: Vec<Column>) -> Self {
        assert!(columns.len() >= 1);

        let mut offsets = vec![0];
        for i in 1..columns.len() {
            offsets.push(offsets[i - 1] + columns[i - 1].size());
        }

        let tuple_length = offsets.last().unwrap() + columns.last().unwrap().size();

        Self {
            columns,
            offsets,
            tuple_length,
        }
    }

    /// Less flexible (but more convenient) constructor for creating a schema with columns of the same type.
    pub fn with_types(types: Vec<ColumnType>) -> Self {
        let columns = types
            .into_iter()
            .enumerate()
            .map(|(i, t)| Column::new_named(i.to_string(), t))
            .collect();

        Self::new(columns)
    }

    /// Returns the offset at which data of the column with the index `col_index` starts relative to the start of the tuple. `None` if out of bounds.
    pub fn get_offset(&self, col_index: usize) -> Option<usize> {
        self.offsets.get(col_index).map(|o| *o)
    }

    /// Returns the size (in bytes) of the type in the column at `col_index`.
    pub fn get_length(&self, col_index: usize) -> Option<usize> {
        self.columns.get(col_index).map(|c| c.col_type.size())
    }

    /// Returns the length of the tuple.
    pub fn get_tuple_len(&self) -> usize {
        self.tuple_length
    }

    pub fn get_cols_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_col_type(&self, index: usize) -> ColumnType {
        self.columns[index].col_type.clone()
    }
}

#[derive(Clone)]
pub struct Column {
    name: String,
    col_type: ColumnType,
}

impl Column {
    /// Create a new column with an empty name and an integer type. In case you want more control, use the other constructor.
    pub fn new(typ: ColumnType) -> Self {
        Self {
            name: "".to_string(),
            col_type: typ,
        }
    }

    pub fn new_named(name: String, typ: ColumnType) -> Self {
        Self {
            name,
            col_type: typ,
        }
    }

    /// Returns the size (in bytes) of the column's data.
    fn size(&self) -> usize {
        self.col_type.size()
    }
}

#[derive(PartialEq, Clone)]
pub enum ColumnType {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Decimal,
    Timestamp,
    Varchar(usize),
}

impl ColumnType {
    pub fn size(&self) -> usize {
        match self {
            ColumnType::Boolean => 1,
            ColumnType::TinyInt => 1,
            ColumnType::SmallInt => 2,
            ColumnType::Integer => 4,
            ColumnType::BigInt => 8,
            ColumnType::Decimal => 8,
            ColumnType::Timestamp => 8,
            ColumnType::Varchar(length) => *length + 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Column, ColumnType, Schema};

    #[test]
    fn schema_constructor_multi_cols() {
        let schema = Schema::new(vec![
            Column::new_named("tiny".to_string(), ColumnType::TinyInt),
            Column::new_named("small".to_string(), ColumnType::SmallInt),
            Column::new_named("bool".to_string(), ColumnType::Boolean),
        ]);

        // tuple structure: |.|..|.|
        assert_eq!(schema.offsets, vec![0, 1, 3]);
        assert_eq!(schema.tuple_length, 4);

        let schema = Schema::new(vec![
            Column::new_named("varchar".to_string(), ColumnType::Varchar(5)),
            Column::new_named("timestamp".to_string(), ColumnType::Timestamp),
            Column::new_named("int".to_string(), ColumnType::Integer),
            Column::new_named("decimal".to_string(), ColumnType::Decimal),
            Column::new_named("timestamp".to_string(), ColumnType::Timestamp),
        ]);

        // tuple structure: |.....|........|....|........|........|
        assert_eq!(schema.offsets, vec![0, 9, 17, 21, 29]);
        assert_eq!(schema.tuple_length, 37);
    }

    #[test]
    fn schema_constructor_one_col() {
        let schema = Schema::new(vec![Column::new_named(
            "int".to_string(),
            ColumnType::Integer,
        )]);

        assert_eq!(schema.offsets, vec![0]);
        assert_eq!(schema.tuple_length, 4);
    }

    #[test]
    #[should_panic]
    fn schema_constructor_no_cols() {
        let _ = Schema::new(vec![]);
    }
}
