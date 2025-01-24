use crate::disk::disk_manager::PageID;

use super::{schema::Schema, value::ColumnValue};

#[derive(Debug, PartialEq, Clone)]
pub struct Tuple {
    data: Vec<u8>,
}

impl Tuple {
    /// Creates a new tuple from the given values as long as they match the given schema.
    ///
    /// # Panics
    /// Will panic if the values don't match the `schema`.
    pub fn new(values: Vec<ColumnValue>, schema: &Schema) -> Self {
        assert_eq!(values.len(), schema.get_cols_count()); // values don't match schema

        let mut data = vec![];
        for (i, value) in values.iter().enumerate() {
            if !value.is_of_type(schema.get_col_type(i)) {
                panic!("Schema doesn't match values");
            }

            data.append(&mut value.serialize());
        }

        Self { data }
    }

    /// Structure: `| length (4) | data (length) |`
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = (self.data.len() as u32).to_be_bytes().to_vec();
        bytes.append(&mut self.data.clone());

        bytes
    }

    pub fn deserialize(data: &[u8]) -> Self {
        assert!(data.len() > 4);
        let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;

        assert_eq!(data.len(), length + 4);
        let data = data[4..].to_vec();

        Self { data }
    }

    pub fn get_value(&self, schema: &Schema, col_index: usize) -> ColumnValue {
        let offset = schema
            .get_offset(col_index)
            .expect("Column index out of schema bounds");
        let length = schema
            .get_length(col_index)
            .expect("Column index out of schema bounds");

        ColumnValue::deserialize(
            &self.data[offset..offset + length],
            schema.get_col_type(col_index),
        )
    }

    pub fn size(&self) -> usize {
        self.data.len() + 4
    }

    pub fn to_string(&self, schema: &Schema) -> String {
        let mut result = String::from("{ ");
        for i in 0..schema.get_cols_count() {
            let value = self.get_value(schema, i);
            result.push_str(&value.to_string());

            if i < schema.get_cols_count() - 1 {
                result.push_str(" , ");
            }
        }
        result.push_str(" }");

        result
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct RID {
    pub page_id: PageID,
    pub slot_num: u16,
}

impl RID {
    pub fn invalid() -> Self {
        Self {
            page_id: 0,
            slot_num: 0,
        }
    }

    pub fn new(page_id: PageID, slot_num: u16) -> Self {
        Self { page_id, slot_num }
    }

    pub fn from_rid(rid: u64) -> Self {
        Self {
            page_id: (rid >> 32) as PageID,
            slot_num: rid as u16,
        }
    }

    /// Get the number representation of the RID.
    pub fn get(&self) -> u64 {
        (self.page_id as u64) << 32 | self.slot_num as u64
    }
}

#[cfg(test)]
mod tests {
    use crate::table::{
        schema::{Column, ColumnType, Schema},
        tuple::{Tuple, RID},
        value::{
            BigIntValue, BooleanValue, ColumnValue, TimestampValue, TinyIntValue, VarcharValue,
        },
    };

    #[test]
    fn rid_consistency() {
        let rid1 = RID::new(12, 21);
        let rid2 = RID::from_rid(rid1.get());

        assert_eq!(rid1, rid2);
    }

    #[test]
    fn tuple_serialization_consistency() {
        let schema = Schema::new(vec![
            Column::new_fixed("tiny".to_string(), ColumnType::TinyInt),
            Column::new_varchar("varchar".to_string(), 255),
            Column::new_fixed("bool".to_string(), ColumnType::Boolean),
        ]);
        let values: Vec<ColumnValue> = vec![
            ColumnValue::TinyInt(TinyIntValue { value: 8 }),
            ColumnValue::Varchar(VarcharValue {
                value: "test".to_string(),
                length: 255,
            }),
            ColumnValue::Boolean(BooleanValue { value: true }),
        ];

        let tuple = Tuple::new(values, &schema);
        let deserialized = Tuple::deserialize(&tuple.serialize());
        assert_eq!(tuple, deserialized);
    }

    #[test]
    fn tuple_get_values() {
        let schema = Schema::new(vec![
            Column::new_fixed("tiny".to_string(), ColumnType::TinyInt),
            Column::new_varchar("varchar".to_string(), 255),
            Column::new_fixed("bool".to_string(), ColumnType::Boolean),
        ]);
        let values: Vec<ColumnValue> = vec![
            ColumnValue::TinyInt(TinyIntValue { value: 8 }),
            ColumnValue::Varchar(VarcharValue {
                value: "test".to_string(),
                length: 255,
            }),
            ColumnValue::Boolean(BooleanValue { value: true }),
        ];

        let tuple = Tuple::new(values.clone(), &schema);
        assert_eq!(tuple.get_value(&schema, 0), values[0]);
        assert_eq!(tuple.get_value(&schema, 1), values[1]);
        assert_eq!(tuple.get_value(&schema, 2), values[2]);
    }

    #[test]
    fn tuple_get_values_complex() {
        let schema = Schema::new(vec![
            Column::new_fixed("tiny".to_string(), ColumnType::TinyInt),
            Column::new_varchar("varchar".to_string(), 255),
            Column::new_fixed("bool".to_string(), ColumnType::Boolean),
            Column::new_fixed("bigint".to_string(), ColumnType::BigInt),
            Column::new_fixed("timestamp".to_string(), ColumnType::Timestamp),
        ]);
        let values: Vec<ColumnValue> = vec![
            ColumnValue::TinyInt(TinyIntValue { value: 8 }),
            ColumnValue::Varchar(VarcharValue {
                value: "test".to_string(),
                length: 255,
            }),
            ColumnValue::Boolean(BooleanValue { value: true }),
            ColumnValue::BigInt(BigIntValue { value: 1237900123 }),
            ColumnValue::Timestamp(TimestampValue { value: 99912395390 }),
        ];

        let tuple = Tuple::new(values.clone(), &schema);
        assert_eq!(tuple.get_value(&schema, 0), values[0]);
        assert_eq!(tuple.get_value(&schema, 1), values[1]);
        assert_eq!(tuple.get_value(&schema, 2), values[2]);
        assert_eq!(tuple.get_value(&schema, 3), values[3]);
        assert_eq!(tuple.get_value(&schema, 4), values[4]);
    }

    #[test]
    #[should_panic]
    fn tuple_create_wrong_schema() {
        let schema = Schema::new(vec![
            Column::new_fixed("tiny".to_string(), ColumnType::TinyInt),
            Column::new_varchar("varchar".to_string(), 255),
            Column::new_fixed("bool".to_string(), ColumnType::Boolean),
        ]);
        let values: Vec<ColumnValue> = vec![
            ColumnValue::TinyInt(TinyIntValue { value: 8 }),
            ColumnValue::Boolean(BooleanValue { value: true }),
            ColumnValue::Varchar(VarcharValue {
                value: "test".to_string(),
                length: 255,
            }),
        ];

        Tuple::new(values.clone(), &schema);
    }

    #[test]
    #[should_panic]
    fn tuple_get_values_column_overflow() {
        let schema = Schema::new(vec![
            Column::new_fixed("tiny".to_string(), ColumnType::TinyInt),
            Column::new_varchar("varchar".to_string(), 255),
            Column::new_fixed("bool".to_string(), ColumnType::Boolean),
        ]);
        let values: Vec<ColumnValue> = vec![
            ColumnValue::TinyInt(TinyIntValue { value: 8 }),
            ColumnValue::Varchar(VarcharValue {
                value: "test".to_string(),
                length: 255,
            }),
            ColumnValue::Boolean(BooleanValue { value: true }),
        ];

        let tuple = Tuple::new(values.clone(), &schema);
        tuple.get_value(&schema, 10);
    }
}
