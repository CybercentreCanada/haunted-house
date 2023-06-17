
use std::collections::BTreeMap;


pub struct RateCounter {
    buffer: BTreeMap<i64, usize>,
    window: i64,
    count: usize,
}

impl RateCounter {

    pub fn new(window: i64) -> Self {
        Self {
            buffer: Default::default(),
            window,
            count: 0,
        }
    }

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

    pub fn average(&mut self) -> usize {
        self.clean();
        self.count
    }

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

