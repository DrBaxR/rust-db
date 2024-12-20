#[cfg(test)]
mod tests;

pub struct Tuple {
    // TODO: implement this
}

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
    fn new(columns: Vec<Column>) -> Self {
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

    /// Returns the offset at which data of the column with the index `col_index` starts relative to the start of the tuple.
    fn get_offset(&self, col_index: usize) -> Option<usize> {
        self.offsets.get(col_index).map(|o| *o)
    }

    /// Returns the length of the tuple.
    fn get_tuple_len(&self) -> usize {
        self.tuple_length
    }
}

pub struct Column {
    name: String,
    col_type: ColumnType,
}

impl Column {
    /// Create fixed-size column.
    fn new_fixed(name: String, col_type: ColumnType) -> Self {
        if let ColumnType::Varchar(_) = col_type {
            panic!("Constructor doesn't support VARCHAR type");
        }

        Self { name, col_type }
    }

    /// Create varchar column.
    fn new_varchar(name: String, length: usize) -> Self {
        Self {
            name,
            col_type: ColumnType::Varchar(length),
        }
    }

    /// Returns the size (in bytes) of the column's data.
    fn size(&self) -> usize {
        match self.col_type {
            ColumnType::Boolean => 1,
            ColumnType::TinyInt => 1,
            ColumnType::SmallInt => 2,
            ColumnType::Integer => 4,
            ColumnType::BigInt => 8,
            ColumnType::Decimal => 8,
            ColumnType::Timestamp => 8,
            ColumnType::Varchar(length) => length,
        }
    }
}

#[derive(PartialEq)]
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
