pub mod header_page;

/// Returns the `count` most significant bits of `input`. If value is greater than or equal with `32`, will return `input`.
fn get_msb(input: i32, count: usize) -> i32 {
    let offset = if count > 32 { 32 } else { count };
    let result = input >> (32 - offset);

    // masking required because of sign extension
    let mask = (1 << offset) - 1;  // mask with `offset` bits set to 1
    result & mask
}

/// Treads `data` as an array of groups of 4 bytes and returns the group that has the index `group_index`.
fn get_four_bytes_group(data: &[u8], group_index: usize) -> [u8; 4] {
    [
        data[group_index * 4],
        data[group_index * 4 + 1],
        data[group_index * 4 + 2],
        data[group_index * 4 + 3],
    ]
}
