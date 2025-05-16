use executors::{delete_executor, filter_executor, insert_executor, projection_executor, seq_scan_executor, values_executor};

use crate::{
    catalog,
    exec::{
        executor::{Execute, Executor},
        plan::PlanNode,
    },
    table::schema::{ColumnType, Schema},
};

pub mod executors;
pub mod util;

pub fn seq_scan_projection(db_file: String) {
    let (seq_scan_executor, _) = seq_scan_executor(db_file);
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
    let (values_executor, _) = values_executor(vec![1, 2, 3, 4, 5, 6]);
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

pub fn values_insert(db_file: String) {
    let (values_executor, tuples_schema) = values_executor(vec![1, 2, 3, 4, 5, 6]);
    let (mut insert_executor, schema, catalog) = insert_executor(
        db_file,
        PlanNode::Values(values_executor.plan.clone()),
        Executor::Values(values_executor),
    );

    // table before
    println!("Table before:");
    let table = catalog
        .get_table_by_oid(insert_executor.plan.table_oid)
        .unwrap();
    let table = table.lock().unwrap();
    let tuples = table.table.sequencial_dump();
    for (_, tuple) in tuples {
        println!("{}", tuple.to_string(&tuples_schema));
    }
    drop(table);

    // insert
    println!("\nInsert executor:");
    println!("{}", insert_executor.to_string(0));
    insert_executor.init();
    while let Some((tuple, _)) = insert_executor.next() {
        println!("{}", tuple.to_string(&schema));
    }

    // table after
    println!("\nTable after:");
    let table = catalog
        .get_table_by_oid(insert_executor.plan.table_oid)
        .unwrap();
    let table = table.lock().unwrap();
    let tuples = table.table.sequencial_dump();
    for (_, tuple) in tuples {
        println!("{}", tuple.to_string(&tuples_schema));
    }
}

pub fn values_delete(db_file: String) {
    // TODO: this won't work with a values executor, because it doesn't have valid RIDs. Will need to use a seq scan executor instead.
    let (values_executor, tuples_schema) = values_executor(vec![2, 3]); 
    let (mut delete_executor, schema, catalog) = delete_executor(
        db_file,
        PlanNode::Values(values_executor.plan.clone()),
        Executor::Values(values_executor),
    );

    // table before
    println!("Table before:");
    let table = catalog
        .get_table_by_oid(delete_executor.plan.table_oid)
        .unwrap();
    let table = table.lock().unwrap();
    let tuples = table.table.sequencial_dump();
    for (_, tuple) in tuples {
        println!("{}", tuple.to_string(&tuples_schema));
    }
    drop(table);

    // insert
    println!("\nDelete executor:");
    println!("{}", delete_executor.to_string(0));
    delete_executor.init();
    while let Some((tuple, _)) = delete_executor.next() {
        println!("{}", tuple.to_string(&schema));
    }

    // table after
    println!("\nTable after:");
    let table = catalog
        .get_table_by_oid(delete_executor.plan.table_oid)
        .unwrap();
    let table = table.lock().unwrap();
    let tuples = table.table.sequencial_dump();
    for (_, tuple) in tuples {
        println!("{}", tuple.to_string(&tuples_schema));
    }
}
