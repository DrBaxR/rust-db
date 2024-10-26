# Disk-Based Hash Index

The index will be used to provide fast data retrieval without needing to search every row in the database, which enables rapid random lookups. The implementation of this structure enables thread-safe search, insertion and deletion (including *growing/shrinking the directory* and *spliting/merging buckets*).

The hashing scheme used is a variant of [extendible hashing](https://en.wikipedia.org/wiki/Extendible_hashing), with an added **non-resizable header page** on top of the directory pages, so that the hash table can hold more values and potentially achieve better multi-threaded performance.

![Extendible Hashing](images/hashing.svg)

## Extendible Hash Table Pages

This section describes the types of pages that will be stored on disk in order to implement the extendible hashing described above.

### Header Page

The structure of this page type's data is as follows:
- `directory_page_ids` (2048 bytes): An array that contains IDs of directory pages (described in the next subsection). Each of the page IDs are a 4 byte integer, which means that this can store up to 512 directory page IDs.
- `max_depth` (4 bytes): The maximum depth the header page can handle.

### Directory Page

The structure of this page type's data is as follows:
- `max_depth` (4 bytes): The maximum depth the directory page can handle.
- `global_depth` (4 bytes): The current directory global depth.
- `local_depths` (512 bytes): An array of local depths of the bucket pages in this directory (1 byte for each page).
- `bucket_page_ids` (2048 bytes): An array of bucket page IDs (described in the next section).

### Bucket Page

The structore of this page type's data is as follows:
- `size` (4 bytes): The number of key-value pairs the bucket is holding.
- `max_size` (4 bytes): The maximum number of key-value pairs the bucket can handle.
- `entries` (<= 4088 bytes): The entries data stored in the bucket (pairs of key-value).

## Extendible Hashing

The implementation details of the extendible hashing scheme used in this project can be found in task #3 of the [CMU Fall 2023 Index Assignment](https://15445.courses.cs.cmu.edu/fall2023/project2/). For the general details of the algorithm, check [this](https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/).

### Interface

The interface that the extendible hash index consists of these methods:
- `insert(key, value)`: Inserts `key`-`value` into the index.
- `remove(key)`: Removes all entries from that index that have `key`.
- `lookup(key)`: Look up all values associated to `key`.
