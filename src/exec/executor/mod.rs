use std::sync::Arc;

use delete::DeleteExecutor;
use filter::FilterExecutor;
use insert::InsertExecutor;
use projection::ProjectionExecutor;
use seq_scan::SeqScanExecutor;
use update::UpdateExecutor;
use values::ValuesExecutor;

use crate::{
    catalog::Catalog,
    disk::buffer_pool_manager::BufferPoolManager,
    table::{
        schema::Schema,
        tuple::{Tuple, RID},
    },
};

#[cfg(test)]
pub mod tests;

pub mod delete;
pub mod filter;
pub mod insert;
pub mod projection;
pub mod seq_scan;
pub mod update;
pub mod values;

#[derive(Clone)]
pub struct ExecutorContext {
    pub catalog: Arc<Catalog>,
    pub bpm: Arc<BufferPoolManager>,
}

pub trait Execute {
    fn init(&mut self);
    fn next(&mut self) -> Option<(Tuple, RID)>;
    fn output_schema(&self) -> &Schema;
    fn to_string(&self, indent_level: usize) -> String;
}

pub enum Executor {
    Values(ValuesExecutor),
    Projection(ProjectionExecutor),
    Filter(FilterExecutor),
    SeqScan(SeqScanExecutor),
    Insert(InsertExecutor),
    Delete(DeleteExecutor),
    Update(UpdateExecutor),
}

impl Execute for Executor {
    fn init(&mut self) {
        match self {
            Executor::Values(executor) => executor.init(),
            Executor::Projection(executor) => executor.init(),
            Executor::Filter(executor) => executor.init(),
            Executor::SeqScan(executor) => executor.init(),
            Executor::Insert(executor) => executor.init(),
            Executor::Delete(executor) => executor.init(),
            Executor::Update(executor) => executor.init(),
        }
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        match self {
            Executor::Values(executor) => executor.next(),
            Executor::Projection(executor) => executor.next(),
            Executor::Filter(executor) => executor.next(),
            Executor::SeqScan(executor) => executor.next(),
            Executor::Insert(executor) => executor.next(),
            Executor::Delete(executor) => executor.next(),
            Executor::Update(executor) => executor.next(),
        }
    }

    fn output_schema(&self) -> &Schema {
        match self {
            Executor::Values(executor) => executor.output_schema(),
            Executor::Projection(executor) => executor.output_schema(),
            Executor::Filter(executor) => executor.output_schema(),
            Executor::SeqScan(executor) => executor.output_schema(),
            Executor::Insert(executor) => executor.output_schema(),
            Executor::Delete(executor) => executor.output_schema(),
            Executor::Update(executor) => executor.output_schema(),
        }
    }

    fn to_string(&self, indent_level: usize) -> String {
        match self {
            Executor::Values(executor) => executor.to_string(indent_level),
            Executor::Projection(executor) => executor.to_string(indent_level),
            Executor::Filter(executor) => executor.to_string(indent_level),
            Executor::SeqScan(executor) => executor.to_string(indent_level),
            Executor::Insert(executor) => executor.to_string(indent_level),
            Executor::Delete(executor) => executor.to_string(indent_level),
            Executor::Update(executor) => executor.to_string(indent_level),
        }
    }
}
