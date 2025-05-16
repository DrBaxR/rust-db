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
- Plan nodes are a tree-like structure that mainly hold data (they don't *necessarily* need to be a tree, the reason why they are is because it's easier for the planner to produce it in that format)
- Execotors are a tree-like structure that hold plan nodes and know how to *execute* the nodes

## Executor Factory

Executors can be created by using the executor factory. This entity can create new executors from plan nodes and their children. For example, in order to create a *filter executor*, it will take in a *filter plan node* and create an executor that wraps it; using the **node**'s child plan node to create the **executor**'s child executor.

## Executor Context

Some executors will require an `ExecutorContext` in order to be constructed. This executor context contains information about the database system that some executors require in order to do their jobs. 

The simplest example for a reason why this is necessary is the sequencial scan executor, which just reads all the tuples in a table one by one. This executor will obviously needaccess to the information of the table for which it does a sequencial scan (the table's `TableInfo`), which can obtained from the `Catalog`, which is part of the `ExecutorContext`.

Implementation flow:
- [x] Expressions
- [x] General structure of plan node
- [x] General structure of executor
- [x] Values
- [x] Projection
- [x] Filter
- [x] Executors tests
- [x] Stringified version of the whole executor tree
- [x] Sequencial scan - will require adding context to the executors (for now BPM and catalog, which will need to be implemented beforehand)
- [x] Insert
- [ ] (WIP) Delete
- [ ] Update
- [ ] Index scan
