//! Tools for tracking event rates for status reporting.
use std::collections::BTreeMap;

/// A counter that tracks the number of events within a given window.
pub struct WindowCounter {
    /// Mapping from timestamp in seconds to number of events
    buffer: BTreeMap<i64, usize>,
    /// Width of event window in seconds
    window: i64,
    /// Current total events across buffer
    count: usize,
}

impl WindowCounter {
    /// Construct a new counter with a window given in seconds.
    pub fn new(window: i64) -> Self {
        Self {
            buffer: Default::default(),
            window,
            count: 0,
        }
    }

    /// Mark that a number of events have occurred
    pub fn increment(&mut self, value: usize) {
        self.clean();
        let time = chrono::Utc::now().timestamp();
        match self.buffer.entry(time) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
            },
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                *entry.get_mut() += value;
            },
        };
        self.count += value;
    }

    /// Get the number of events currently in the window
    pub fn average(&mut self) -> usize {
        self.clean();
        self.count
    }

    /// Flush events that have fallen outside of the window
    fn clean(&mut self) {
        let oldest = chrono::Utc::now().timestamp() - self.window;
        while let Some((&time, &count)) = self.buffer.first_key_value() {
            if time < oldest {
                self.count -= count;
                self.buffer.remove(&time);
            } else {
                break
            }
        }
    }

}

