use sample_code::{
    seq_scan_delete, seq_scan_projection, seq_scan_update, values_insert, values_projection_filter,
};

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
    // TODO: the issue here is that the update executor receives a tuple from the scan executor, deletes is from the table,
    // TODO: then inserts a new one in the table, which will be then read EVENTUALLY by the scan => infinite loop
    // TODO: will need to do some research on this (the halloween problem), but the first thing that came to mind is keep track (a vec of RIDs in update)
    // TODO: of the tuples that you modified - ez
    seq_scan_update("db/test.db".to_string());
}
