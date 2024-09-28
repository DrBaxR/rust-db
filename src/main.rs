mod node;
mod tree;

fn main() {
    // this is an example that removes a node from a non-leaf, causing a leaf to rebalance
    let mut tree = tree::BTree::new(3);

    tree.insert(1, 1);
    tree.insert(2, 2);
    tree.insert(4, 4);
    tree.insert(5, 5);
    tree.insert(6, 6);
    tree.insert(7, 7);
    tree.insert(3, 3);
    tree.insert(8, 8);
    tree.insert(9, 9);
    tree.insert(10, 10);
    tree.insert(11, 11);

    tree.print_tree();

    let _ = tree.remove(4);

    println!();
    tree.print_tree();
}
