# Catalog

A non-persistent "database" that contains meta-data about the database, it handles these: table creation, table lookup, index creation and index lookup.

**Note:** The structure of indexes and how they are represented on disk can be found in the `indexing.md` file.

# Tables

`TableHeap` is a physical representation of a table on disk and is basically a **doubly-linked list of table pages**. The main things that you can do with it is create, read, update and delete tuples; and manipulate the tuples' meta-data.

The main structures that are used within the table heap are as follows:
- `TablePage`: A page that contains a bunch of tuples (also acts like a node in a linked list of pages). With the interface it exposes, you can manage the tuples inside the page. The structure of the page can be seen below:

```text
| next_page_id (4) | num_tuples (2) | num_deleted_tuples (2) | ... tuples_info ... | ... free ... | ... tuples_data ... |
                                                                   page header end ^              ^ page data start
```

Here is what each of the segments mean:
- `next_page_id`: The PID of the next page in the linked list
- `num_tuples`: The number of tuples stored in this page
- `num_deleted_tuples`: The number of deleted tuples in this page
- `tuples_data`: A list of serialzed tuples
- `tuples_info`: A list of entries where each one of them has this structure:

```text
| tuple_offset (2) | tuple_size (2) | ts (8) | is_deleted (1) |
```

- `TupleMeta`: Meta-data about a tuple
- `Tuple`: Storage for a single tuple's data. The tuple gets created from a list of values and a schema - for more details check `tuple.cpp`
- `RID`: Record identifier, made of 2 parts: `page_id` and `slot_number`. Both of the parts are 32-bit, meaning that the RID is a 64-bit

Other note-worthy concepts:
- Table OID: A 32-bit integer representing the object ID of the table
- `Schema`: The schema of a table
- `Column`: The schema of a column (basically a name and a type)
- `Value`: SQL data stored in some materialized state. There are multiple types of values and each of them implement functionality for comparison