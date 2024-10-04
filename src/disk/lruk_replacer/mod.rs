use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
mod tests;

type FrameID = u32;

/// LRU-K frame that tracks the last `k` timestamps when it has been accessed. First entry in the `history` vector represents the *oldest* tracked
/// access and last entry represents the *most recent* access to the frame.
#[derive(Debug)]
struct LRUKFrame {
    id: FrameID,
    k: usize,
    pub is_evictable: bool,
    history: Vec<u128>,
}

impl LRUKFrame {
    fn new(id: FrameID, k: usize) -> Self {
        Self {
            id,
            k,
            is_evictable: false,
            history: Vec::new(),
        }
    }

    /// Records access at the current timestamp for the frame.
    fn record_access(&mut self, timestamp: u128) {
        if self.history.len() >= self.k {
            self.history.remove(0);
        }

        self.history.push(timestamp);
    }

    /// Returns the (backward) k-distance of the frame, which represents the difference between the timestamps of the most recent access and oldest (`k`th) recorded access.
    /// Will return `Err` with the latest access timestamp if there are less than `k` recorded accesses.
    fn k_distance(&self) -> Result<u128, u128> {
        // TODO: make this not crash when it gets called on no recorded values - or just think about this case
        if self.history.len() == self.k && self.k >= 2 {
            return Ok(self.history.last().unwrap() - self.history.first().unwrap());
        }

        Err(*self.history.last().unwrap())
    }
}

pub struct LRUKReplacer {
    max_frames: usize,
    k: usize,
    frames: HashMap<FrameID, LRUKFrame>,
}

impl LRUKReplacer {
    pub fn new(max_frames: usize, k: usize) -> Self {
        Self {
            max_frames,
            k,
            frames: HashMap::new(),
        }
    }

    /// Find and evict the frame with the largest backwar k-distance. Only frames marked as evictable will be candidates for eviction.
    /// Returns the id of the frame that was evicted. Will return `None` if no frames can be evicted.
    pub fn evict(&mut self) -> Option<FrameID> {
        let eviction_candidates_distances: Vec<(&FrameID, Result<u128, u128>)> = self
            .frames
            .iter()
            .filter(|(_, f)| f.is_evictable)
            .map(|(id, frame)| (id, frame.k_distance()))
            .collect();

        let (candidates_with_k, candidates_with_less): (
            Vec<(&u32, Result<u128, u128>)>,
            Vec<(&u32, Result<u128, u128>)>,
        ) = eviction_candidates_distances
            .iter()
            .partition(|(_, k_dist)| k_dist.is_ok());

        // pick evicted from elements with k-distance
        let largest_k_distance = candidates_with_k
            .iter()
            .map(|(id, c)| (*id, c.unwrap()))
            .max_by(|(_, ka), (_, kb)| ka.cmp(kb))
            .map(|(id, _)| id.clone());

        if let Some(id) = largest_k_distance {
            let _ = self.remove(id);
            return Some(id);
        }

        // pick evicted from elements without k-distance (earliest timestamp based on LRU)
        // find element with MIN err value
        let earliest_timestamp = candidates_with_less
            .iter()
            .map(|(id, c)| (*id, c.unwrap_err()))
            .min_by(|(_, ta), (_, tb)| ta.cmp(tb))
            .map(|(id, _)| id.clone());

        if let Some(id) = earliest_timestamp {
            let _ = self.remove(id);
            return Some(id);
        }

        // no frame can be evicted
        None
    }

    /// Wrapper around the `record_access_at` with the current time as `timestamp`.
    pub fn record_access(&mut self, id: FrameID) -> Result<(), ()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time moved backwards?!!")
            .as_millis();

        self.record_access_at(id, now)
    }

    /// Records an access for frame with `id` at `timestamp`. If frame is not already tracked, adds to tracked frames.
    /// 
    /// # Errors
    /// Returns `Err` if trying frame not already recorded and replacer tracked frames already reached the max.
    fn record_access_at(&mut self, id: FrameID, timestamp: u128) -> Result<(), ()> {
        let frame = self.frames.get_mut(&id);

        let frame = match frame {
            Some(f) => f,
            None => {
                if self.frames.len() >= self.max_frames {
                    return Err(());
                }

                let new_frame = LRUKFrame::new(id, self.k);
                self.frames.insert(id, new_frame);

                self.frames.get_mut(&id).unwrap()
            }
        };

        frame.record_access(timestamp);

        Ok(())
    }

    /// Sets frame with `id`'s evictable state to the `evictable` value.
    /// 
    /// # Errors
    /// Returns `Err` if trying to call for a frame id that is not tracked.
    pub fn set_evictable(&mut self, id: FrameID, evictable: bool) -> Result<(), ()> {
        let frame = self
            .frames
            .get_mut(&id)
            .map_or(Err(()), |frame| Ok(frame))?;

        frame.is_evictable = evictable;

        Ok(())
    }

    /// Remove an evictable frame from the replacer. Will do nothing if the frame with `id` doesn't exist in the replacer.
    /// 
    /// # Errors
    /// Returns `Err` if trying to remove a frame that is not evictable.
    pub fn remove(&mut self, id: FrameID) -> Result<(), ()> {
        let frame = match self.frames.get(&id) {
            Some(f) => f,
            None => return Ok(()),
        };

        if !frame.is_evictable {
            return Err(());
        }

        self.frames.remove(&id);
        Ok(())
    }

    /// Returns the size of the replacer, which represents the number of evictable frames.
    pub fn size(&self) -> usize {
        self.frames.iter().filter(|(_, f)| f.is_evictable).count()
    }
}
