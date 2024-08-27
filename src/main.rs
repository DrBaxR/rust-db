mod node;
mod tree;

fn main() {
    let mut tree = tree::BTree::new(2);
    println!("{:?}", tree);
    tree.insert(1, 1);
    println!("{:?}", tree);
    tree.insert(2, 2);
    // tree.insert(3, 3); // split :)
}
