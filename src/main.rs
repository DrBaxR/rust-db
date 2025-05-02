use sample_code::{seq_scan_projection, values_projection_filter};

// uncomment when there is no more sample code in main.rs
// #[cfg(test)]
mod test_utils;
mod sample_code;

mod b_tree;
mod catalog;
mod config;
mod disk;
mod exec;
mod index;
mod parser;
mod table;

fn main() {
    // TODO: write test for insert executor
    // TODO: test should check that tuples were inserted into the table and that the indexes were updated

    // seq_scan_projection();
    values_projection_filter();
}
