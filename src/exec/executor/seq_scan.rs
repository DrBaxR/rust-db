use crate::{
    exec::plan::seq_scan::SeqScanPlanNode,
    table::{
        schema::Schema,
        tuple::{Tuple, RID},
    },
};

use super::{Execute, ExecutorContext};

pub struct SeqScanExecutor {
    context: ExecutorContext,
    plan: SeqScanPlanNode,
}

impl Execute for SeqScanExecutor {
    fn init(&mut self) {
        todo!()
    }

    fn next(&mut self) -> Option<(Tuple, RID)> {
        todo!()
    }

    fn output_schema(&self) -> &Schema {
        todo!()
    }

    fn to_string(&self, indent_level: usize) -> String {
        todo!()
    }
}
