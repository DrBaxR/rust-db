use values::ValuesExecutor;

use crate::table::{
    schema::Schema,
    tuple::{Tuple, RID},
};

pub mod values;
pub mod projection;
pub mod filter;

pub trait Execute {
    fn init(&mut self);
    fn next(&mut self) -> Option<(Tuple, RID)>;
    fn output_schema(&self) -> &Schema;
}

pub enum Executor {
    Values(ValuesExecutor),
}

impl Execute for Executor {
    fn init(&mut self) {
        match self {
            Executor::Values(executor) => executor.init(),
        }
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        match self {
            Executor::Values(executor) => executor.next(),
        }
    }

    fn output_schema(&self) -> &Schema {
        match self {
            Executor::Values(executor) => executor.output_schema(),
        }
    }
}

// TODO: tests for executors