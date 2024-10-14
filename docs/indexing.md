# Disk-Based Hash Index

The index will be used to provide fast data retrieval without needing to search every row in the database, which enables rapid random lookups. The implementation of this structure enables thread-safe search, insertion and deletion (including *growing/shrinking the directory* and *spliting/merging buckets*).

The hashing scheme used is a variant of [extendible hashing](https://en.wikipedia.org/wiki/Extendible_hashing), with an added **non-resizable header page** on top of the directory pages, so that the hash table can hold more values and potentially achieve better multi-threaded performance.

![Extendible Hashing](images/hashing.svg)

## Extendible Hash Table Pages

TODO

## Extendible Hashing

TODO

## Concurrency Control

TODO