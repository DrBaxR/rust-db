use schema::Schema;
use value::ColumnValue;

use crate::disk::disk_manager::PageID;

mod schema;
mod value;

pub struct Tuple {
    pub rid: RID, // RID is set externally, after tuple is created
    data: Vec<u8>,
}

impl Tuple {
    /// Creates a new tuple from the given values as long as they match the given schema.
    ///
    /// # Panics
    /// Will panic if the values don't match the `schema`.
    fn new(values: Vec<ColumnValue>, schema: &Schema) -> Self {
        assert_eq!(values.len(), schema.get_cols_count());

        let mut data = vec![];
        for (i, value) in values.iter().enumerate() {
            if !value.is_of_type(schema.get_col_type(i)) {
                panic!("Schema doesn't match values");
            }

            data.append(&mut value.serialize());
        }

        Self {
            rid: RID::invalid(),
            data,
        }
    }

    /// Structure: `| length (4) | data (length) |`
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = (self.data.len() as u32).to_be_bytes().to_vec();
        bytes.append(&mut self.data.clone());

        bytes
    }

    fn deserialize(data: &[u8]) -> Self {
        assert!(data.len() > 4);
        let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;

        assert_eq!(data.len(), length + 4);
        let data = data[4..].to_vec();

        Self {
            rid: RID::invalid(),
            data,
        }
    }

    fn get_value(&self, schema: &Schema, col_index: usize) -> ColumnValue {
        let offset = schema
            .get_offset(col_index)
            .expect("Column index out of schema bounds");
        let length = schema
            .get_length(col_index)
            .expect("Column index out of schema bounds");

        ColumnValue::deserialize(&self.data[offset..offset + length], schema.get_col_type(col_index))
    }
}

#[derive(Debug, PartialEq)]
pub struct RID {
    page_id: PageID,
    slot_num: u32,
}

impl RID {
    fn invalid() -> Self {
        Self {
            page_id: 0,
            slot_num: 0,
        }
    }

    fn new(page_id: PageID, slot_num: u32) -> Self {
        Self { page_id, slot_num }
    }

    fn from_rid(rid: u64) -> Self {
        Self {
            page_id: (rid >> 32) as PageID,
            slot_num: rid as u32,
        }
    }

    /// Get the number representation of the RID.
    fn get(&self) -> u64 {
        (self.page_id as u64) << 32 | self.slot_num as u64
    }
}

#[cfg(test)]
mod tests {
    use super::{schema::{Column, ColumnType, Schema}, value::{BooleanValue, ColumnValue, TinyIntValue, VarcharValue}, Tuple, RID};

    #[test]
    fn rid_consistency() {
        let rid1 = RID::new(12, 21);
        let rid2 = RID::from_rid(rid1.get());

        assert_eq!(rid1, rid2);
    }

    #[test]
    fn tuple_get_values() {
        // TODO: make this shit pass and write some more tests
        let schema = Schema::new(vec![
            Column::new_fixed("tiny".to_string(), ColumnType::TinyInt),
            Column::new_varchar("varchar".to_string(), 255),
            Column::new_fixed("bool".to_string(), ColumnType::Boolean),
        ]);
        let values: Vec<ColumnValue> = vec![
            ColumnValue::TinyInt(TinyIntValue { value: 8 }),
            ColumnValue::Varchar(VarcharValue { value: "test".to_string(), length: 255 }),
            ColumnValue::Boolean(BooleanValue { value: true })
        ];

        let tuple = Tuple::new(values.clone(), &schema);
        assert_eq!(tuple.get_value(&schema, 0), values[0]);
        assert_eq!(tuple.get_value(&schema, 1), values[1]);
        assert_eq!(tuple.get_value(&schema, 2), values[2]);
    }
}
