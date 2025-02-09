# Catalog

A **catalog** is an internal database about *the database's metadata*. It contains things like information about what tables are inside a database, what indexes are inside a database etc. It also keeps track of these metadata by assigning them **OID**s (object IDs).

It can be basically thought of as two maps that map OIDs to either table or index metadata. Its interface can be reduced to: *creating and fetching tables* and *creating and fetching indexes*.

## Table metadata

This metadata contains a table's *schema*, *name* and *OID*. On a more technical view, it also contains a pointer to the table's `TableHeap`.

## Index metadata

This metadata contains a index's *key schema*, *name*, *OID*, *name of the table that the index refers to* and *byte size of the index's key*. Technically, it also contains a pointer to the actual `Index`.
