use std::collections::BinaryHeap;
use crate::filter::GenericFilter;



#[derive(PartialEq, Eq)]
struct InsertOption {
    original: usize,
    growth: usize,
    // size: usize,
    id: i64,
}


impl PartialOrd for InsertOption {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some((other.growth, other.original).cmp(&(self.growth, self.original)))
        // if self.growth < other.growth {
        //     return Some(std::cmp::Ordering::Greater);
        // } else if self.original == other.original {
        //     return Some(other.growth.cmp(&self.growth));
        // }
        // return Some(std::cmp::Ordering::Less);
    }
}

impl Ord for InsertOption {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct InsertCursor {
    pub filter: GenericFilter,
    limit: f64,
    options: BinaryHeap<InsertOption>,
}

impl InsertCursor {
    pub fn new(filter: GenericFilter, limit: f64) -> Self {
        Self {
            filter,
            limit,
            options: Default::default()
        }
    }

    pub fn add(&mut self, id: i64, option: &GenericFilter) -> bool {
        let (before, after) = option.overlap_count_ones(&self.filter);
        if after as f64/option.size() as f64 <= self.limit {
            self.options.push(InsertOption {
                original: before,
                growth: after-before,
                // size: (),
                id
            })
        }
        self.can_insert_immediate()
    }

    pub fn can_insert_immediate(&self) -> bool {
        match self.options.peek() {
            Some(item) => item.growth == 0,
            None => false
        }
    }

    pub fn next(&mut self) -> Option<(i64, usize, usize)> {
        let item = self.options.pop()?;
        Some((item.id, item.original, item.growth))
    }
}