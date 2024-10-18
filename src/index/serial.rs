use super::get_four_bytes_group;

pub trait Serialize {
    fn serialize(&self) -> Vec<u8>;
}

pub trait Deserialize {
    fn deserialize(data: &[u8]) -> Self;
}

// i32
impl Serialize for i32 {
    fn serialize(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl Deserialize for i32 {
    fn deserialize(data: &[u8]) -> Self {
        i32::from_be_bytes(get_four_bytes_group(data, 0))
    }
}

// u32
impl Serialize for u32 {
    fn serialize(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl Deserialize for u32 {
    fn deserialize(data: &[u8]) -> Self {
        u32::from_be_bytes(get_four_bytes_group(data, 0))
    }
}

// u8
impl Serialize for u8 {
    fn serialize(&self) -> Vec<u8> {
        vec![self.clone()]
    }
}

impl Deserialize for u8 {
    fn deserialize(data: &[u8]) -> Self {
        data[0]
    }
}

