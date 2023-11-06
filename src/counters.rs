//! Tools for tracking event rates for status reporting.
use std::collections::VecDeque;

/// A counter that tracks the number of events within a given window.
pub struct WindowCounter {
    /// Mapping from timestamp in seconds to number of events
    buffer: VecDeque<(i64, usize)>,
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
        self.count += value;

        let time = chrono::Utc::now().timestamp();

        if let Some((window, counter)) = self.buffer.front_mut() {
            if *window == time {
                *counter += value;
                return;
            }
        }
        self.buffer.push_front((time, value));
    }

    /// Get the number of events currently in the window
    pub fn value(&mut self) -> usize {
        self.clean();
        self.count
    }

    /// Flush events that have fallen outside of the window
    fn clean(&mut self) {
        let oldest = chrono::Utc::now().timestamp() - self.window;
        while let Some((time, value)) = self.buffer.back() {
            if *time < oldest {
                self.count -= *value;
                self.buffer.pop_back();
            } else {
                break
            }
        }
    }

}
