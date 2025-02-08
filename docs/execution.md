# Execution Engine

- executor: 
    - uses *executor context*, *plan node*
    - has *children executors*
    - extends *abstract executor* (always has a context)

- plan node:
    - extends *abstract plan node*
    - has *output schema*, *child(ren) plan nodes*
    - can potentially have more attributes based on node type

- projection plan node:
    - has *abstract expression references*
    - a single child
    - evaluates all its expressions and wraps outputs in a *tuple* it emits

- expression reference:
    - has *children expressions*, *return type* (which is a column)
    - can be evaluated
    - can also *evaluate join* on it, not sure what that is TODO

Notes:
- Plan nodes are a tree-like structure that mainly hold data (personal note: I don't really understand why they need to be a tree, since executors already have a tree structure)
- Execotors are a tree-like structure that hold plan nodes and know how to *execute* the nodes

## Executor Factory

Executors can be created by using the executor factory. This entity can create new executors from plan nodes and their children. For example, in order to create a *filter executor*, it will take in a *filter plan node* and create an executor that wraps it; using the **node**'s child plan node to create the **executor**'s child executor.

Implementation flow:
- [x] Expressions
- [x] General structure of plan node
- [x] General structure of executor
- [x] Values
- [x] Projection
- [x] Filter
- [x] Executors tests
- [x] Stringified version of the whole executor tree
- [ ] (WIP) Sequencial scan - will require adding context to the executors (for now BPM and catalog, which will need to be implemented beforehand)
- [ ] Insert
- [ ] Update
- [ ] Delete
- [ ] Index scan