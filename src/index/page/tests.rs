use std::{env::temp_dir, fs::remove_file};

use crate::{disk::disk_manager::DiskManager, index::page::HashTableHeaderPage};

#[test]
fn header_deserialization() {
    let header_data = get_mock_header_data(2);
    let header = HashTableHeaderPage::from_serialized(&header_data);

    assert_eq!(header.max_depth, 2);
    assert_eq!(header.get_directory_page_id(0).unwrap(), 1);
}

fn get_mock_header_data(max_depth: u8) -> Vec<u8> {
    let mut header_data = vec![];
    let mut page_id = 0;

    for i in 0..4096 {
        let val = if i % 4 == 3 {
            if i > 2048 {
                max_depth
            } else {
                page_id = if page_id < 255 { page_id + 1 } else { page_id };

                page_id
            }
        } else {
            0
        };

        header_data.push(val);
    }

    header_data
}

#[test]
fn header_serialization() {
    let header_data = get_mock_header_data(2);
    let header = HashTableHeaderPage::from_serialized(&header_data);

    let serialized_data = header.serialize();
    let header_deserialized = HashTableHeaderPage::from_serialized(&serialized_data);

    assert_eq!(
        header.directory_page_ids,
        header_deserialized.directory_page_ids
    );
    assert_eq!(header.max_depth, header_deserialized.max_depth);
}

#[test]
fn header_serialization_disk() {
    // init
    let db_path = temp_dir().join("index_serialization_disk.db");
    let db_file_path = db_path.to_str().unwrap().to_string();
    let dm = DiskManager::new(db_file_path);

    // write mock page to disk
    let header_data = get_mock_header_data(2);
    let header = HashTableHeaderPage::from_serialized(&header_data);

    let serialized_data = header.serialize();
    dm.write_page(0, &serialized_data);

    // deserialize from disk
    let deserialized_data = dm.read_page(0).unwrap();
    let header_deserialized = HashTableHeaderPage::from_serialized(&deserialized_data);

    assert_eq!(
        header.directory_page_ids,
        header_deserialized.directory_page_ids
    );
    assert_eq!(header.max_depth, header_deserialized.max_depth);

    // cleanup
    remove_file(db_path).expect("Couldn't remove test DB file");
}

#[test]
fn header_max_size() {
    let header = HashTableHeaderPage::from_serialized(&get_mock_header_data(1));
    assert_eq!(header.max_size(), 2);

    let header = HashTableHeaderPage::from_serialized(&get_mock_header_data(2));
    assert_eq!(header.max_size(), 4);

    let header = HashTableHeaderPage::from_serialized(&get_mock_header_data(9));
    assert_eq!(header.max_size(), 512);
}

#[test]
fn header_get_directory_page_id() {
    let header = HashTableHeaderPage::from_serialized(&get_mock_header_data(2));

    assert_eq!(header.get_directory_page_id(0).unwrap(), 1);
    assert_eq!(header.get_directory_page_id(1).unwrap(), 2);
    assert_eq!(header.get_directory_page_id(2).unwrap(), 3);
    assert_eq!(header.get_directory_page_id(3).unwrap(), 4);
    assert!(header.get_directory_page_id(4).is_none())
}

#[test]
fn header_set_directory_page_id() {
    let mut header = HashTableHeaderPage::new(vec![1, 2, 3, 4], 2);
    
    let prev = header.set_directory_page_id(0, 12);
    assert_eq!(header.get_directory_page_id(0).unwrap(), 12);
    assert_eq!(prev.unwrap(), 1);

    assert!(header.set_directory_page_id(4, 44).is_err());
}
