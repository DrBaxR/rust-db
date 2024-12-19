# Catalog

A non-persistent "database" that contains meta-data about the database, it handles these: table creation, table lookup, index creation and index lookup.

**Note:** The structure of indexes and how they are represented on disk can be found in the `indexing.md` file.

# Tables

`TableHeap` is a physical representation of a table on disk and is basically a **doubly-linked list of table pages**. The main things that you can do with it is create, read, update and delete tuples; and manipulate the tuples' meta-data.

The main structures that are used within the table heap are as follows:
- `TablePage`: A page that contains a bunch of tuples (also acts like a node in a linked list of pages). With the interface it exposes, you can manage the tuples inside the page. The structure of the page can be seen below:

```
Slotted page format:
 ---------------------------------------------------------
 | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 ---------------------------------------------------------
                               ^
                               free space pointer

 Header format (size in bytes):
 ----------------------------------------------------------------------------
 | NextPageId (4)| NumTuples(2) | NumDeletedTuples(2) |
 ----------------------------------------------------------------------------
 ----------------------------------------------------------------
 | Tuple_1 offset+size (4) | Tuple_2 offset+size (4) | ... |
 ----------------------------------------------------------------

Tuple format:
| meta | data |
 ```


- `TupleMeta`: Meta-data about a tuple
- `Tuple`: Storage for a single tuple's data. The tuple gets created from a list of values and a schema - for more details check `tuple.cpp`
- `RID`: Record identifier, made of 2 parts: `page_id` and `slot_number`. Both of the parts are 32-bit, meaning that the RID is a 64-bit

Other note-worthy concepts:
- Table OID: A 32-bit integer representing the object ID of the table
- `Schema`: The schema of a table
- `Column`: The schema of a column (basically a name and a type)
- `Value`: SQL data stored in some materialized state. There are multiple types of values and each of them implement functionality for comparison