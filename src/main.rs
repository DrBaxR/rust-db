use sample_code::{
    seq_scan_delete, seq_scan_projection, seq_scan_update, values_insert, values_projection_filter,
};

use crate::sample_code::idx_scan_projection;

// uncomment when there is no more sample code in main.rs
// #[cfg(test)]
mod sample_code;
mod test_utils;

mod b_tree;
mod catalog;
mod config;
mod disk;
mod exec;
mod index;
mod parser;
mod table;

fn main() {
    // seq_scan_projection("db/test.db".to_string());
    // values_projection_filter();
    // values_insert("db/test.db".to_string());
    // seq_scan_delete("db/test.db".to_string());
    // seq_scan_update("db/test.db".to_string());
    idx_scan_projection("db/test.db".to_string());
}
