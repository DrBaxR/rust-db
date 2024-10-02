use disk::disk_manager::DiskManager;

const DB_PAGE_SIZE: u32 = 4096;
const DB_DEFAULT_SIZE: usize = 16;

mod node;
mod tree;
mod disk;

fn main() {
    let mut dm = DiskManager::new("test.db".to_string());

    let mut page = "pee pee lol".as_bytes().to_vec();
    page.resize(DB_PAGE_SIZE as usize, 0);
    dm.write_page(0, &page);

    let mut page = "poo poo lmao".as_bytes().to_vec();
    page.resize(DB_PAGE_SIZE as usize, 0);
    dm.write_page(1, &page);

    dm.increase_disk_size(17);
}
