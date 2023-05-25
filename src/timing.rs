use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
// use std::sync::Once;
// use std::time::Duration;

use lazy_static::lazy_static;

lazy_static! {
    static ref LABELS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Default::default());
    static ref HERITAGE: std::sync::Mutex<Vec<(usize, usize)>> = std::sync::Mutex::new(Default::default());
}

pub trait TimingCapture {
    type Mark: TimingCapture;

    fn new_id(&self, label: &str) -> usize;
    // fn child_id(&self, parent: usize) -> usize;
    fn add(&self, index: usize, value: f64);
    fn build(&self, index: usize) -> Self::Mark;
}

struct Inner {
    times: Vec<f64>,
    calls: Vec<u64>,
    // heritage: Vec<(usize, usize)>,
    // labels: Vec<String>,
}

pub struct Capture {
    data: RefCell<Inner>
}

impl Capture {
    pub fn new() -> Self {
        Self{data: RefCell::new(Inner {
            times: vec![],
            calls: vec![],
            // heritage: vec![],
            // labels: vec![],
        })}
    }

    fn child_id(&self, parent: usize, label: &str) -> usize {
        let new_index = self.new_id(label);
        let mut heritage = HERITAGE.lock().unwrap();
        heritage.push((parent, new_index));
        new_index
    }

    pub fn print(&self) {
        println!("{}", self.format())
    }

    pub fn format(&self) -> String {
        let mut parents = HashSet::new();
        let mut children = HashSet::new();
        let mut lines = vec![];
        let heritage = HERITAGE.lock().unwrap().clone();
        let labels = LABELS.lock().unwrap().clone();

        for (parent, child) in heritage.iter() {
            parents.insert(*parent);
            children.insert(*child);
        }
        for index in parents {
            if children.contains(&index) {
                continue
            }
            self.format_item(index, "", &heritage, &labels, &mut lines);
        }

        return lines.into_iter().join("\n")
    }

    fn format_item(&self, index: usize, prefix: &str, heritage: &Vec<(usize, usize)>, labels: &Vec<String>, out: &mut Vec<String>) {
        // Gather children
        let mut children = vec![];
        let data = self.data.borrow();
        let mut accounted = 0.0f64;
        for (parent, child) in heritage.iter() {
            if *parent == index {
                children.push((data.times[*child], *child));
                accounted += data.times[*child];
            }
        }
        children.sort_by(|a, b|b.0.partial_cmp(&a.0).unwrap());


        // Print current
        if children.is_empty() {
            out.push(format!("{prefix}{} [{} total, {} calls, {} each]", labels[index], format_time(data.times[index]), data.calls[index], format_time(data.times[index]/data.calls[index] as f64)));
        } else {
            out.push(format!("{prefix}{} [{} total, {} calls, {} each][{} outside children]", labels[index], format_time(data.times[index]), data.calls[index], format_time(data.times[index]/data.calls[index] as f64), format_time(data.times[index]-accounted)));
            for (_, child) in children {
                self.format_item(child, &(prefix.to_owned() + "  "), heritage, labels, out);
            }
        }
    }
}

fn format_time(seconds: f64) -> String {
    if seconds > 0.1 {
        format!("{seconds:.2}s")
    } else if seconds*1000.0 > 1.0 {
        format!("{:.0}ms", (seconds*1000.0))
    } else if seconds*1_000_000.0 > 1.0 {
        format!("{:.0}us", (seconds*1_000_000.0))
    // } else if seconds*1_000_000_000.0 > 1.0 {
    //     format!("{:.0}ns", (seconds*1_000_000_000.0))
    } else {
        format!("0s")
    }
}

// impl<'a> TimingCapture for Capture {
//     type Mark = Mark<'a>;

//     fn new_id(&self, label: &str) -> usize {
//         let mut data = self.data.borrow_mut();
//         let new_index = data.times.len();
//         let new_len = data.times.len() + 1;
//         data.times.resize(new_len, 0.0);
//         data.calls.resize(new_len, 0);
//         data.labels.push(label.to_owned());
//         new_index
//     }

//     fn add(&self, index: usize, value: f64) {
//         let mut data = self.data.borrow_mut();
//         data.times[index] += value;
//         data.calls[index] += 1;
//     }

//     fn build<'a>(&'a self, index: usize) -> Mark<'a> {
//         Mark{time: std::time::Instant::now(), capture: self, index}
//     }
// }

// static mut LABELS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Default::default());



// lazy_static! {
//     static ref HASHMAP: HashMap<u32, &'static str> = {
//         let mut m = HashMap::new();
//         m.insert(0, "foo");
//         m.insert(1, "bar");
//         m.insert(2, "baz");
//         m
//     };
// }

impl<'a> TimingCapture for &'a Capture {
    type Mark = Mark<'a>;

    fn new_id(&self, label: &str) -> usize {
        // static mut SIZE: AtomicUsize = AtomicUsize::new(0);
        let new_index = {
            let mut labels = LABELS.lock().unwrap();
            labels.push(label.to_owned());
            labels.len() - 1
        };
        let mut data = self.data.borrow_mut();
        let new_len = new_index + 1;
        data.times.resize(new_len, 0.0);
        data.calls.resize(new_len, 0);
        new_index
    }

    fn add(&self, index: usize, value: f64) {
        let mut data = self.data.borrow_mut();
        if index >= data.times.len() {
            data.times.resize(index + 1, 0.0);
            data.calls.resize(index + 1, 0);
        }
        data.times[index] += value;
        data.calls[index] += 1;
    }

    fn build(&self, index: usize) -> Mark<'a> {
        Mark{time: std::time::Instant::now(), capture: self, index}
    }
}


pub struct Mark<'a> {
    time: std::time::Instant,
    capture: &'a Capture,
    index: usize,
}

impl<'a> Drop for Mark<'a> {
    fn drop(&mut self) {
        self.capture.add(self.index, self.time.elapsed().as_secs_f64());
    }
}

impl<'a> TimingCapture for Mark<'a> {
    type Mark = Self;

    fn new_id(&self, label: &str) -> usize {
        self.capture.child_id(self.index, label)
    }

    fn add(&self, index: usize, value: f64) {
        self.capture.add(index, value);
    }

    fn build(&self, index: usize) -> Mark<'a> {
        Mark{time: std::time::Instant::now(), capture: self.capture, index}
    }
}

macro_rules! mark {
    ($capture:ident) => {
        {
            unsafe {
                static mut VALUE: usize = 0;
                // The macro will expand into the contents of this block.
                static INIT: std::sync::Once = std::sync::Once::new();
                INIT.call_once(|| {
                    VALUE = $capture.new_id("unnamed");
                });
                $capture.build(VALUE)
            }
        }
    };
    ($capture:ident,$label:expr) => {
        {
            unsafe {
                static mut VALUE: usize = 0;
                // The macro will expand into the contents of this block.
                static INIT: std::sync::Once = std::sync::Once::new();
                INIT.call_once(|| {
                    VALUE = $capture.new_id($label);
                });
                $capture.build(VALUE)
            }
        }
    }
}
use itertools::Itertools;
pub(crate) use mark;

pub struct NullCapture {}

impl NullCapture {
    pub fn new() -> Self {
        Self{}
    }
}

impl TimingCapture for NullCapture {
    type Mark = NullCapture;

    fn new_id(&self, _label: &str) -> usize { 0 }
    fn add(&self, _index: usize, _value: f64) { }
    fn build(&self, _index: usize) -> Self::Mark { Self::new() }
}

// macro_rules! measure {
//     ($capture:ident,$label:expr,$code:block) => {
//         {

//         }
//     }
// }


// fn do_nothing<T: TimingCapture>(capture: T) {
//     measure!(capture, "do_nothing", {
//         for _ii in 0 .. 2 {
//             let _inside = mark!(outside, "iteration");
//             println!("hello");
//         }
//         {
//             let _other = mark!(outside, "slow");
//             std::thread::sleep(std::time::Duration::from_secs_f64(0.4));
//         }
//     })
// }


// fn main() {
//     let capture = Capture::new();
//     do_nothing(&capture);
//     capture.print();
// }

