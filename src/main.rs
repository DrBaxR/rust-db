use disk::disk_manager::DiskManager;
use index::page::HashTableHeaderPage;

mod b_tree;
mod config;
mod disk;
mod index;

fn main() {
    // TODO: demo with buffer pool manager of how pages would be used

    // init
    let dm = DiskManager::new(String::from("db/test.db"));

    // write mock page to disk
    let header_data = get_mock_header_data(2);
    let header = HashTableHeaderPage::from_serialized(&header_data);

    let serialized_data = header.serialize();
    dm.write_page(0, &serialized_data);
}

fn get_mock_header_data(max_depth: u8) -> Vec<u8>  {
    let mut header_data = vec![];
    let mut page_id = 0;

    for i in 0..4096 {
        let val = if i % 4 == 3 {
            if i > 2048 {
                max_depth
            } else {
                page_id = if page_id < 255 {
                    page_id + 1
                } else {
                    page_id
                };

                page_id
            }
        } else {
            0
        };

        header_data.push(val);
    }

    header_data
}