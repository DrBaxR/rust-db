use std::sync::Arc;

use page::TupleMeta;
use tuple::{Tuple, RID};

use crate::disk::buffer_pool_manager::BufferPoolManager;

mod page;
mod schema;
mod tuple;
mod value;

pub struct TableHeap {
    bpm: Arc<BufferPoolManager>,
}

impl TableHeap {
    pub fn insert_tuple(meta: TupleMeta, tuple: Tuple) -> Option<RID> {
        todo!()
    }

    pub fn update_tuple_meta(meta: TupleMeta, rid: &RID) {
        todo!()
    }

    pub fn get_tuple(rid: &RID) -> Option<(&TupleMeta, &Tuple)> {
        todo!()
    }
}