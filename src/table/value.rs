use super::schema::ColumnType;

pub enum ColumnValue {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Decimal(f64),
    Timestamp(u64),
    Varchar(u32, String),
}

pub trait Value {
    fn serialize(&self) -> Vec<u8>;
    fn storage_size(&self) -> usize;
}

// TODO: for all value structs also implement deserialization...

pub struct BooleanValue {
    value: bool,
}

impl Value for BooleanValue {
    fn serialize(&self) -> Vec<u8> {
        vec![if self.value { 1 as u8 } else { 0 as u8 }]
    }

    fn storage_size(&self) -> usize {
        ColumnType::Boolean.size()
    }
}

pub struct TinyIntValue {
    value: i8,
}

impl Value for TinyIntValue {
    fn serialize(&self) -> Vec<u8> {
        vec![self.value as u8]
    }

    fn storage_size(&self) -> usize {
        ColumnType::TinyInt.size()
    }
}

pub struct SmallIntValue {
    value: i16,
}

impl Value for SmallIntValue {
    fn serialize(&self) -> Vec<u8> {
        vec![(self.value >> 8) as u8, self.value as u8]
    }

    fn storage_size(&self) -> usize {
        ColumnType::SmallInt.size()
    }
}

pub struct IntegerValue {
    value: i32,
}

impl Value for IntegerValue {
    fn serialize(&self) -> Vec<u8> {
        vec![
            (self.value >> 24) as u8,
            (self.value >> 16) as u8,
            (self.value >> 8) as u8,
            self.value as u8,
        ]
    }

    fn storage_size(&self) -> usize {
        ColumnType::Integer.size()
    }
}

pub struct BigIntValue {
    value: u64,
}

impl Value for BigIntValue {
    fn serialize(&self) -> Vec<u8> {
        vec![
            (self.value >> 56) as u8,
            (self.value >> 48) as u8,
            (self.value >> 40) as u8,
            (self.value >> 32) as u8,
            (self.value >> 24) as u8,
            (self.value >> 16) as u8,
            (self.value >> 8) as u8,
            self.value as u8,
        ]
    }

    fn storage_size(&self) -> usize {
        todo!()
    }
}

pub struct DecimalValue {
    value: f64,
}

impl Value for DecimalValue {
    fn serialize(&self) -> Vec<u8> {
        self.value.to_be_bytes().to_vec()
    }

    fn storage_size(&self) -> usize {
        ColumnType::Decimal.size()
    }
}

pub struct TimestampValue {
    value: u64,
}

impl Value for TimestampValue {
    fn serialize(&self) -> Vec<u8> {
        vec![
            (self.value >> 56) as u8,
            (self.value >> 48) as u8,
            (self.value >> 40) as u8,
            (self.value >> 32) as u8,
            (self.value >> 24) as u8,
            (self.value >> 16) as u8,
            (self.value >> 8) as u8,
            self.value as u8,
        ]
    }

    fn storage_size(&self) -> usize {
        ColumnType::Timestamp.size()
    }
}

pub struct VarcharValue {
    value: String,
    length: usize,
}

impl Value for VarcharValue {
    /// Structure: `| len (4) | content (len) |`
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = (self.length as u32).to_be_bytes().to_vec();

        let mut content: Vec<_> = self.value.bytes().collect();
        content.resize(self.length, b'\0');

        bytes.append(&mut self.value.bytes().collect());

        bytes
    }

    fn storage_size(&self) -> usize {
        self.length + 4 // characters + u32
    }
}
