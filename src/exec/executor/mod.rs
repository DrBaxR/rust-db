use filter::FilterExecutor;
use projection::ProjectionExecutor;
use values::ValuesExecutor;

use crate::table::{
    schema::Schema,
    tuple::{Tuple, RID},
};

#[cfg(test)]
pub mod tests;

pub mod filter;
pub mod projection;
pub mod values;

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
}

impl Execute for Executor {
    fn init(&mut self) {
        match self {
            Executor::Values(executor) => executor.init(),
            Executor::Projection(executor) => executor.init(),
            Executor::Filter(executor) => executor.init(),
        }
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        match self {
            Executor::Values(executor) => executor.next(),
            Executor::Projection(executor) => executor.next(),
            Executor::Filter(executor) => executor.next(),
        }
    }

    fn output_schema(&self) -> &Schema {
        match self {
            Executor::Values(executor) => executor.output_schema(),
            Executor::Projection(executor) => executor.output_schema(),
            Executor::Filter(executor) => executor.output_schema(),
        }
    }

    fn to_string(&self, indent_level: usize) -> String {
        match self {
            Executor::Values(executor) => executor.to_string(indent_level),
            Executor::Projection(executor) => executor.to_string(indent_level),
            Executor::Filter(executor) => executor.to_string(indent_level),
        }
    }
}
