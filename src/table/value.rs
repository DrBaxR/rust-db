use core::str;
use std::cmp::Ordering;

use super::schema::ColumnType;

#[derive(Debug, PartialEq, Clone)]
pub enum ColumnValue {
    Boolean(BooleanValue),
    TinyInt(TinyIntValue),
    SmallInt(SmallIntValue),
    Integer(IntegerValue),
    BigInt(BigIntValue),
    Decimal(DecimalValue),
    Timestamp(TimestampValue),
    Varchar(VarcharValue),
}

impl ColumnValue {
    pub fn deserialize(data: &[u8], typ: ColumnType) -> ColumnValue {
        match typ {
            ColumnType::Boolean => ColumnValue::Boolean(BooleanValue::deserialize(data)),
            ColumnType::TinyInt => ColumnValue::TinyInt(TinyIntValue::deserialize(data)),
            ColumnType::SmallInt => ColumnValue::SmallInt(SmallIntValue::deserialize(data)),
            ColumnType::Integer => ColumnValue::Integer(IntegerValue::deserialize(data)),
            ColumnType::BigInt => ColumnValue::BigInt(BigIntValue::deserialize(data)),
            ColumnType::Decimal => ColumnValue::Decimal(DecimalValue::deserialize(data)),
            ColumnType::Timestamp => ColumnValue::Timestamp(TimestampValue::deserialize(data)),
            ColumnType::Varchar(len) => {
                let varchar_val = VarcharValue::deserialize(data);
                assert_eq!(len, varchar_val.length);

                ColumnValue::Varchar(varchar_val)
            }
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            ColumnValue::Boolean(boolean_value) => boolean_value.serialize(),
            ColumnValue::TinyInt(tiny_int_value) => tiny_int_value.serialize(),
            ColumnValue::SmallInt(small_int_value) => small_int_value.serialize(),
            ColumnValue::Integer(integer_value) => integer_value.serialize(),
            ColumnValue::BigInt(big_int_value) => big_int_value.serialize(),
            ColumnValue::Decimal(decimal_value) => decimal_value.serialize(),
            ColumnValue::Timestamp(timestamp_value) => timestamp_value.serialize(),
            ColumnValue::Varchar(varchar_value) => varchar_value.serialize(),
        }
    }

    pub fn is_of_type(&self, typ: ColumnType) -> bool {
        match self {
            ColumnValue::Boolean(boolean_value) => boolean_value.is_of_type(typ),
            ColumnValue::TinyInt(tiny_int_value) => tiny_int_value.is_of_type(typ),
            ColumnValue::SmallInt(small_int_value) => small_int_value.is_of_type(typ),
            ColumnValue::Integer(integer_value) => integer_value.is_of_type(typ),
            ColumnValue::BigInt(big_int_value) => big_int_value.is_of_type(typ),
            ColumnValue::Decimal(decimal_value) => decimal_value.is_of_type(typ),
            ColumnValue::Timestamp(timestamp_value) => timestamp_value.is_of_type(typ),
            ColumnValue::Varchar(varchar_value) => varchar_value.is_of_type(typ),
        }
    }

    pub fn typ(&self) -> ColumnType {
        match self {
            ColumnValue::Boolean(_) => ColumnType::Boolean,
            ColumnValue::TinyInt(_) => ColumnType::TinyInt,
            ColumnValue::SmallInt(_) => ColumnType::SmallInt,
            ColumnValue::Integer(_) => ColumnType::Integer,
            ColumnValue::BigInt(_) => ColumnType::BigInt,
            ColumnValue::Decimal(_) => ColumnType::Decimal,
            ColumnValue::Timestamp(_) => ColumnType::Timestamp,
            ColumnValue::Varchar(varchar_value) => ColumnType::Varchar(varchar_value.length),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            ColumnValue::Boolean(boolean_value) => boolean_value.value.to_string(),
            ColumnValue::TinyInt(tiny_int_value) => tiny_int_value.value.to_string(),
            ColumnValue::SmallInt(small_int_value) => small_int_value.value.to_string(),
            ColumnValue::Integer(integer_value) => integer_value.value.to_string(),
            ColumnValue::BigInt(big_int_value) => big_int_value.value.to_string(),
            ColumnValue::Decimal(decimal_value) => decimal_value.value.to_string(),
            ColumnValue::Timestamp(timestamp_value) => timestamp_value.value.to_string(),
            ColumnValue::Varchar(varchar_value) => varchar_value.value.clone(),
        }
    }

    /// Casts value to decimal `ColumnValue`. Works for all the numeric types.
    ///
    /// # Errors
    /// Will return `Err` if called on boolean or varchar.
    pub fn to_decimal(&self) -> Result<ColumnValue, ()> {
        match self {
            ColumnValue::TinyInt(tiny_int_value) => Ok(ColumnValue::Decimal(DecimalValue {
                value: tiny_int_value.value as f64,
            })),
            ColumnValue::SmallInt(small_int_value) => Ok(ColumnValue::Decimal(DecimalValue {
                value: small_int_value.value as f64,
            })),
            ColumnValue::Integer(integer_value) => Ok(ColumnValue::Decimal(DecimalValue {
                value: integer_value.value as f64,
            })),
            ColumnValue::BigInt(big_int_value) => Ok(ColumnValue::Decimal(DecimalValue {
                value: big_int_value.value as f64,
            })),
            ColumnValue::Decimal(decimal_value) => Ok(ColumnValue::Decimal(decimal_value.clone())),
            ColumnValue::Timestamp(timestamp_value) => Ok(ColumnValue::Decimal(DecimalValue {
                value: timestamp_value.value as f64,
            })),
            _ => Err(()),
        }
    }

    /// Compare two values. Returns `Ok(Ordering)` if the values are of the same type and `Err(())` otherwise.
    pub fn compare(&self, other: &ColumnValue) -> Result<Ordering, ()> {
        if !other.is_of_type(self.typ()) {
            return Err(());
        }

        match (self, other) {
            (ColumnValue::Boolean(left), ColumnValue::Boolean(right)) => {
                Ok(left.value.cmp(&right.value))
            }
            (ColumnValue::TinyInt(left), ColumnValue::TinyInt(right)) => {
                Ok(left.value.cmp(&right.value))
            }
            (ColumnValue::SmallInt(left), ColumnValue::SmallInt(right)) => {
                Ok(left.value.cmp(&right.value))
            }
            (ColumnValue::Integer(left), ColumnValue::Integer(right)) => {
                Ok(left.value.cmp(&right.value))
            }
            (ColumnValue::BigInt(left), ColumnValue::BigInt(right)) => {
                Ok(left.value.cmp(&right.value))
            }
            (ColumnValue::Decimal(left), ColumnValue::Decimal(right)) => Ok(left
                .value
                .partial_cmp(&right.value)
                .expect("Invalid decimal comparison")),
            (ColumnValue::Timestamp(left), ColumnValue::Timestamp(right)) => {
                Ok(left.value.cmp(&right.value))
            }
            (ColumnValue::Varchar(left), ColumnValue::Varchar(right)) => {
                Ok(left.value.cmp(&right.value))
            }
            _ => Err(()),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct BooleanValue {
    pub value: bool,
}

impl BooleanValue {
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

    fn is_of_type(&self, typ: ColumnType) -> bool {
        typ == ColumnType::Boolean
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct TinyIntValue {
    pub value: i8,
}

impl TinyIntValue {
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

    fn is_of_type(&self, typ: ColumnType) -> bool {
        typ == ColumnType::TinyInt
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SmallIntValue {
    pub value: i16,
}

impl SmallIntValue {
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

    fn is_of_type(&self, typ: ColumnType) -> bool {
        typ == ColumnType::SmallInt
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct IntegerValue {
    pub value: i32,
}

impl IntegerValue {
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

    fn is_of_type(&self, typ: ColumnType) -> bool {
        typ == ColumnType::Integer
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct BigIntValue {
    pub value: i64,
}

impl BigIntValue {
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

    fn is_of_type(&self, typ: ColumnType) -> bool {
        typ == ColumnType::BigInt
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct DecimalValue {
    pub value: f64,
}

impl DecimalValue {
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

    fn is_of_type(&self, typ: ColumnType) -> bool {
        typ == ColumnType::Decimal
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct TimestampValue {
    pub value: u64,
}

impl TimestampValue {
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

    fn is_of_type(&self, typ: ColumnType) -> bool {
        typ == ColumnType::Timestamp
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct VarcharValue {
    pub value: String,
    pub length: usize,
}

impl VarcharValue {
    /// Structure: `| len (4) | content (len) |`
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = (self.length as u32).to_be_bytes().to_vec();

        let mut content: Vec<_> = self.value.bytes().collect();
        content.resize(self.length, b'\0');

        bytes.append(&mut content);

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

    fn is_of_type(&self, typ: ColumnType) -> bool {
        typ == ColumnType::Varchar(self.length)
    }
}

#[cfg(test)]
mod tests {
    use crate::table::value::{BigIntValue, DecimalValue, IntegerValue, TimestampValue};

    use super::{BooleanValue, SmallIntValue, TinyIntValue, VarcharValue};

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
