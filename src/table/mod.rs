use schema::Schema;
use value::{ColumnValue, Value};

use crate::disk::disk_manager::PageID;

mod schema;
mod value;

pub struct Tuple {
    pub rid: RID, // RID is set externally, after tuple is created
    data: Vec<u8>,
}

impl Tuple {
    fn new(values: Vec<impl Value>, schema: &Schema) -> Self {
        todo!()
    }

    /// Structure: `| length (4) | data (length) |`
    fn serialize(&self) -> Vec<u8> {
        todo!()
    }

    fn deserialize(data: &[u8]) -> Self {
        todo!()
    }

    fn get_value(&self, schema: &Schema, col_index: usize) -> ColumnValue {
        todo!()
    }
}

#[derive(Debug, PartialEq)]
pub struct RID {
    page_id: PageID,
    slot_num: u32,
}

impl RID {
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
    use super::RID;

    #[test]
    fn rid_consistency() {
        let rid1 = RID::new(12, 21);
        let rid2 = RID::from_rid(rid1.get());

        assert_eq!(rid1, rid2);
    }
}
