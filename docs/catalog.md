# Catalog

A **catalog** is an internal database about *the database's metadata*. It contains things like information about what tables are inside a database, what indexes are inside a database etc. It also keeps track of these metadata by assigning them **OID**s (object IDs).

It can be basically thought of as two maps that map OIDs to either table or index metadata. Its interface can be reduced to: *creating and fetching tables* and *creating and fetching indexes*.

## Table metadata

This metadata contains a table's *schema*, *name* and *OID*. On a more technical view, it also contains a pointer to the table's `TableHeap`.

## Index metadata

This metadata contains a index's *key schema*, *name*, *OID*, *name of the table that the index refers to* and *byte size of the index's key*. Technically, it also contains a pointer to the actual `Index`.

## On Thread Safety

The implementation of the catalog needs to be thread safe, as a single catalog will be shared across multiple transactions which will all individually operate on it.

The granularity of the locks on the catalog are on a table/index level. This means that two transactions will be able to operate on two **different** tables/indexes at
*the same time*, but if two transactions try to access the same table/index at the same time, one of them will **block** until the other one is done with the acquired 
lock.

**Note:** The way this is implemented is conceptually a simplified version of the implementation of locks on buffer pool manager pages (for buffer pool, you can get 
multiple locks on the same page without blocking, as long as they are read-only locks - the only case when the locks block is when you need a write lock when a read 
lock is already present).