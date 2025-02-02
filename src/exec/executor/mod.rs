use filter::FilterExecutor;
use projection::ProjectionExecutor;
use values::ValuesExecutor;

use crate::table::{
    schema::Schema,
    tuple::{Tuple, RID},
};

pub mod filter;
pub mod projection;
pub mod values;

pub trait Execute {
    fn init(&mut self);
    fn next(&mut self) -> Option<(Tuple, RID)>;
    fn output_schema(&self) -> &Schema;
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
}

// TODO: tests for executors
