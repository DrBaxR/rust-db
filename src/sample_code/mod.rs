use executors::{filter_executor, projection_executor, seq_scan_executor, values_executor};

use crate::exec::{
    executor::{Execute, Executor},
    plan::PlanNode,
};

mod executors;

// runs
pub fn seq_scan_projection() {
    let (seq_scan_executor, _) = seq_scan_executor();
    let (mut projection_executor, schema) = projection_executor(
        PlanNode::SeqScan(seq_scan_executor.plan.clone()),
        Executor::SeqScan(seq_scan_executor),
    );

    println!("{}", projection_executor.to_string(0));

    projection_executor.init();
    while let Some((tuple, _)) = projection_executor.next() {
        println!("{}", tuple.to_string(&schema));
    }
}

pub fn values_projection_filter() {
    let (values_executor, _) = values_executor();
    let (projection_executor, schema) = projection_executor(
        PlanNode::Values(values_executor.plan.clone()),
        Executor::Values(values_executor),
    );
    let (mut filter_executor, _) = filter_executor(
        PlanNode::Projection(projection_executor.plan.clone()),
        Executor::Projection(projection_executor),
    );
    println!("{}", filter_executor.to_string(0));

    while let Some((tuple, _)) = filter_executor.next() {
        println!("{}", tuple.to_string(&schema));
    }
}
