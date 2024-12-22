use core::str;

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
    fn deserialize(data: &[u8]) -> Self;
    fn serialize(&self) -> Vec<u8>;
    fn storage_size(&self) -> usize;
}

#[derive(Debug, PartialEq)]
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

    fn deserialize(data: &[u8]) -> Self {
        assert_eq!(data.len(), ColumnType::Boolean.size());

        if data[0] == 0 {
            Self { value: false }
        } else if data[0] == 1 {
            Self { value: true }
        } else {
            panic!("Invalid boolean value, can only be 0 or 1")
        }
    }
}

#[derive(Debug, PartialEq)]
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

    fn deserialize(data: &[u8]) -> Self {
        assert_eq!(data.len(), ColumnType::TinyInt.size());

        Self {
            value: data[0] as i8,
        }
    }
}

#[derive(Debug, PartialEq)]
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

    fn deserialize(data: &[u8]) -> Self {
        assert_eq!(data.len(), ColumnType::SmallInt.size());

        Self {
            value: i16::from_be_bytes([data[0], data[1]]),
        }
    }
}

#[derive(Debug, PartialEq)]
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

    fn deserialize(data: &[u8]) -> Self {
        assert_eq!(data.len(), ColumnType::Integer.size());

        Self {
            value: i32::from_be_bytes([data[0], data[1], data[2], data[3]]),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct BigIntValue {
    value: i64,
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
        ColumnType::BigInt.size()
    }

    fn deserialize(data: &[u8]) -> Self {
        assert_eq!(data.len(), ColumnType::BigInt.size());

        Self {
            value: i64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]),
        }
    }
}

#[derive(Debug, PartialEq)]
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

    fn deserialize(data: &[u8]) -> Self {
        assert_eq!(data.len(), ColumnType::Decimal.size());

        Self {
            value: f64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]),
        }
    }
}

#[derive(Debug, PartialEq)]
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

    fn deserialize(data: &[u8]) -> Self {
        assert_eq!(data.len(), ColumnType::Timestamp.size());

        Self {
            value: u64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]),
        }
    }
}

#[derive(Debug, PartialEq)]
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

        bytes.append(&mut content);
        dbg!(bytes.len());

        bytes
    }

    fn storage_size(&self) -> usize {
        self.length + 4 // characters + u32
    }

    fn deserialize(data: &[u8]) -> Self {
        assert!(data.len() > 4);
        let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;

        assert_eq!(data.len(), length + 4);
        let value = str::from_utf8(&data[4..])
            .expect("Invalid string payload, potentially wrong cast")
            .split('\0') // is it possible for UTF-8 characters to have some b0000_0000 byte in the middle of them?
            .next()
            .expect("String can't be split at \\0 character")
            .to_string();

        Self { value, length }
    }
}

#[cfg(test)]
mod tests {
    use crate::table::value::{BigIntValue, DecimalValue, IntegerValue, TimestampValue};

    use super::{BooleanValue, SmallIntValue, TinyIntValue, Value, VarcharValue};

    #[test]
    fn boolean_value_serialization_consistency() {
        let value = BooleanValue { value: true };
        let serialized = value.serialize();
        let deserialized = BooleanValue::deserialize(&serialized);

        assert_eq!(value, deserialized);
    }

    #[test]
    fn tiny_int_value_serialization_consistency() {
        let value = TinyIntValue { value: 18 };
        let serialized = value.serialize();
        let deserialized = TinyIntValue::deserialize(&serialized);

        assert_eq!(value, deserialized);
    }

    #[test]
    fn small_int_value_serialization_consistency() {
        let value = SmallIntValue { value: -25639 };
        let serialized = value.serialize();
        let deserialized = SmallIntValue::deserialize(&serialized);

        assert_eq!(value, deserialized);
    }

    #[test]
    fn integer_value_serialization_consistency() {
        let value = IntegerValue { value: 1147483648 };
        let serialized = value.serialize();
        let deserialized = IntegerValue::deserialize(&serialized);

        assert_eq!(value, deserialized);
    }

    #[test]
    fn big_int_value_serialization_consistency() {
        let value = BigIntValue {
            value: -8223372843128043239,
        };
        let serialized = value.serialize();
        let deserialized = BigIntValue::deserialize(&serialized);

        assert_eq!(value, deserialized);
    }

    #[test]
    fn decimal_value_serialization_consistency() {
        let value = DecimalValue {
            value: 234534563.890423,
        };
        let serialized = value.serialize();
        let deserialized = DecimalValue::deserialize(&serialized);

        assert_eq!(value, deserialized);
    }

    #[test]
    fn timestamp_value_serialization_consistency() {
        let value = TimestampValue {
            value: 17446744912301425290,
        };
        let serialized = value.serialize();
        let deserialized = TimestampValue::deserialize(&serialized);

        assert_eq!(value, deserialized);
    }

    #[test]
    fn varchar_value_serialization_consistency() {
        let value = VarcharValue {
            value: "this is some string".to_string(),
            length: 255,
        };
        let serialized = value.serialize();
        let deserialized = VarcharValue::deserialize(&serialized);

        assert_eq!(value, deserialized);

        let value = VarcharValue {
            value: "abc".to_string(),
            length: 3,
        };
        let serialized = value.serialize();
        let deserialized = VarcharValue::deserialize(&serialized);

        assert_eq!(value, deserialized);

        let value = VarcharValue {
            value: "🍆🍆🍆🍆".to_string(),
            length: 255,
        };
        let serialized = value.serialize();
        let deserialized = VarcharValue::deserialize(&serialized);

        assert_eq!(value, deserialized);
    }
}
