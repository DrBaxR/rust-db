mod node;
mod tree;

fn main() {
    let mut tree = tree::BTree::new(3);

    tree.insert(1, 1);
    tree.insert(2, 2);
    tree.insert(4, 4);
    tree.insert(5, 5);
    tree.insert(6, 6);
    tree.insert(7, 7);
    tree.insert(3, 3);

    let _ = tree.remove(7);
    let _ = tree.remove(6);
    let _ = tree.remove(1);

    tree.print_tree();
    let _ = tree.remove(2);

    println!();
    tree.print_tree();

    // TODO: test sandwitch as well and other cases except rotate left and right
    // TODO: fix tests that fail now
    // TODO: write automated tests
    // rotate left
    // rotate right
    // sandwitch
    // > remove from non-leaf
}
