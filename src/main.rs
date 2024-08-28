mod node;
mod tree;

fn main() {
    let mut tree = tree::BTree::new(2);
    println!("{:?}", tree);
    tree.insert(1, 1);
    tree.insert(2, 2);
    println!("{:?}", tree);
    // tree.insert(3, 3); // split :)
}
