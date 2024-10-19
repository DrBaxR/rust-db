pub mod bucket_page;
pub mod directory_page;
pub mod header_page;
pub mod serial;

pub mod disk_extendible_hash_table;

/// Returns the `count` most significant bits of `input`. If value is greater than or equal with `32`, will return `input`.
fn get_msb(input: u32, count: usize) -> u32 {
    if count == 0 {
        return 0;
    }

    let offset = if count > 32 { 32 } else { count };

    input >> (32 - offset)
}

/// Treads `data` as an array of groups of 4 bytes and returns the group that has the index `group_index`.
/// 
/// # Panics
/// Will panic if trying to index outside of the length of `data`, or if accessing one of the bytes would cause that (e.g. `data.len() == 7`
/// and calling function with `group_index == 1` => will try to access `data[7]`).
fn get_four_bytes_group(data: &[u8], group_index: usize) -> [u8; 4] {
    [
        data[group_index * 4],
        data[group_index * 4 + 1],
        data[group_index * 4 + 2],
        data[group_index * 4 + 3],
    ]
}
