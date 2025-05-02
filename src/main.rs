use sample_code::{seq_scan_projection, values_insert, values_projection_filter};

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
    // seq_scan_projection();
    // values_projection_filter();
    values_insert();
}
