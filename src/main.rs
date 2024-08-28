mod node;
mod tree;

fn main() {
    let mut tree = tree::BTree::new(2);

    tree.insert(1, 1);
    tree.insert(10, 10);
    tree.insert(5, 5);
    tree.insert(3, 3);
    tree.insert(7, 7);
    tree.insert(2, 2);
    tree.insert(8, 8);
    tree.insert(2, 2);
    tree.insert(2, 2);
    tree.insert(2, 2);
    tree.insert(2, 2);
    tree.insert(2, 2);
    tree.insert(2, 2);
    tree.insert(2, 2);
    tree.insert(2, 2);

    tree.print_tree();
}
