use index::page::HashTableHeaderPage;

mod b_tree;
mod config;
mod disk;
mod index;

fn main() {
    let mut header_data = vec![];
    for i in 0..4096 {
        let val = if i % 4 == 3 {
            if i > 2048 {
                2
            } else {
                1
            }
        } else {
            0
        };

        header_data.push(val);
    }

    let header = HashTableHeaderPage::new(&header_data);

    println!("Max size: {}", header.max_size());
}
