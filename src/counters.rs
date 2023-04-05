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


    pub fn tick(&mut self) {
        self.clean();
        let time = chrono::Utc::now().timestamp();
        match self.buffer.entry(time) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(1);
            },
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                *entry.get_mut() += 1;
            },
        };
        self.count += 1;
    }

    pub fn average(&mut self) -> usize {
        self.clean();
        self.count
    }

    fn clean(&mut self) {
        let oldest = chrono::Utc::now().timestamp() - self.window;
        while let Some((&time, &count)) = self.buffer.first_key_value().clone() {
            if time < oldest {
                self.count -= count;
                self.buffer.remove(&time);
            } else {
                break
            }
        }
    }

}


// pub struct TimeAverage {
//     buffer: BTreeMap<u64, (f32, usize)>,
//     window: u64,
//     count: usize,
//     total: f32,
// }

// impl TimeAverage {

//     pub fn new(window: u64) -> Self {
//         Self {
//             buffer: Default::default(),
//             window,
//             count: 0,
//             total: 0.0,
//         }
//     }


//     pub fn push(&mut self, value: f32) {
//         self.clean();
//         let time = std::time::SystemTime::now().elapsed().unwrap().as_secs();
//         match self.buffer.entry(time) {
//             std::collections::btree_map::Entry::Vacant(entry) => {
//                 entry.insert((value, 1));
//             },
//             std::collections::btree_map::Entry::Occupied(mut entry) => {
//                 let tuple = entry.get_mut();
//                 tuple.0 += value;
//                 tuple.1 += 1;
//             },
//         };
//         self.count += 1;
//         self.total += value;
//     }

//     pub fn average(&mut self) -> f32 {
//         self.clean();
//         if self.count == 0 {
//             0.0
//         } else {
//             self.total / (self.count as f32)
//         }
//     }


//     fn clean(&mut self) {
//         let oldest = std::time::SystemTime::now().elapsed().unwrap().as_secs() - self.window;
//         while let Some((&time, &value)) = self.buffer.first_key_value().clone() {
//             let (value, count) = value;
//             if time < oldest {
//                 self.count -= count;
//                 self.total -= value;
//                 self.buffer.remove(&time);
//             } else {
//                 break
//             }
//         }
//     }

// }


// pub struct SimpleAverage {
//     buffer: Vec<f32>,
//     index: usize,
//     size: usize,
//     total: f32,
// }


// impl SimpleAverage {

//     pub fn new(size: usize) -> Self {
//         Self {
//             buffer: vec![],
//             index: 0,
//             size,
//             total: 0.0
//         }
//     }

//     pub fn push(&mut self, value: f32) {
//         if self.buffer.len() < self.size {
//             self.buffer.push(value);
//         } else {
//             self.total -= self.buffer[self.index];
//             self.buffer[self.index] = value;
//             self.index = (self.index + 1) % self.buffer.len();
//         }
//         self.total += value;
//     }

//     pub fn average(&self) -> f32 {
//         self.total / (self.buffer.len() as f32)
//     }

// }