# Buffer Pool Manager

The buffer pool manager is responsible for moving pages back on forth between the memory and disk, as they are needed (requested by other parts of the system, via the pages' *page identifier*). This component is **thread-safe**, as it will be accessed by multiple workers in the system.

It's made up of these three main components:
- LRU-K Replacement Policy
- Disk Scheduler
- Buffer Pool Manager

**Note:** The *Buffer Pool Manager* is built on top of (internally uses in its implementation) the other two components: the *LRU-K Replacement Policy* and the *Disk Scheduler*.

![Buffer Pool Diagram](images/buffer_pool_diagram.png)

## LRU-K Replacement Policy

This component is responsible of tracking page usage in the buffer poola manager. It uses the [LRU-K](https://www.cs.cmu.edu/~natassa/courses/15-721/papers/p297-o_neil.pdf) algorithm to evict frames from the buffer pool manager.

### Characteristics

There are two properties that describe a LRU-K replacement policy:
1. `num_frames: usize`: the maximum number of frames that the replacer will be required to store
2. `k`: How many historical access timestamps get recorded for each frame

### Interface

The interface (i.e. the functionalities that it implements) exposed by the replacement policy looks like this:
- `evict() -> Optional<frame_id>`: Finds the frame with the largest k-distance and evicts that frame. The return value contains the id of the frame that was evicted (or nothing if no frames can be evicted).
- `record_access(frame_id, access_type)`: Records that the given `frame_id` has been accessed at the current timestamp with the `access_type` type of access. Can throw exception if the `frame_id` is invalid.
- `set_evictable(frame_id, set_evictable)`: Set if the frame with `frame_id` is evictable to the value of `set_evictable` (true/false), which controls the the replacer's size. Can throw exception if the `frame_id` is invalid.
- `remove(frame_id)`: Remove and evictable frame with `frame_id` from the replacer, along with its access history, which will decrement the size if removal was successful. Can throw exception if the frame is not evictable. If the frame was not found in replacer, just returns.
- `size() -> usize`: Returns the size of the replacer (which is the number of evictable frames inside it).

## Disk Scheduler

TODO

## Buffer Pool Manager

TODO