use std::cell::RefCell;
use std::collections::HashSet;
// use std::sync::Once;
// use std::time::Duration;


pub trait TimingCapture {
    type Mark: TimingCapture;

    fn new_id(&self, label: &str) -> usize;
    // fn child_id(&mut self, parent: usize) -> usize;
    fn add(&self, index: usize, value: f64);
    fn build(&self, index: usize) -> Self::Mark;
}

struct Inner {
    times: Vec<f64>,
    calls: Vec<u64>,
    heritage: Vec<(usize, usize)>,
    labels: Vec<String>,
}

pub struct Capture {
    data: RefCell<Inner>
}

impl Capture {
    pub fn new() -> Self {
        Self{data: RefCell::new(Inner {
            times: vec![],
            calls: vec![],
            heritage: vec![],
            labels: vec![],
        })}
    }
    
    fn child_id(&self, parent: usize, label: &str) -> usize {
        let new_index = self.new_id(label);
        let mut data = self.data.borrow_mut();
        data.heritage.push((parent, new_index));
        new_index
    }

    pub fn print(&self) {
        let mut parents = HashSet::new();
        let mut children = HashSet::new();
        let data = self.data.borrow();
        for (parent, child) in &data.heritage {
            parents.insert(*parent);
            children.insert(*child);
        }
        for index in parents {
            if children.contains(&index) {
                continue
            }
            self.print_item(index, "");
        }
    }

    pub fn print_item(&self, index: usize, prefix: &str) {
        // Gather children
        let mut children = vec![];
        let data = self.data.borrow();
        let mut accounted = 0.0f64;
        for (parent, child) in &data.heritage {
            if *parent == index {
                children.push((data.times[*child], *child));
                accounted += data.times[*child];
            }
        }
        children.sort_by(|a, b|b.0.partial_cmp(&a.0).unwrap());

        // Print current
        if children.is_empty() {
            println!("{prefix}{} [{} total, {} calls, {} each]", data.labels[index], format_time(data.times[index]), data.calls[index], format_time(data.times[index]/data.calls[index] as f64));
        } else {
            println!("{prefix}{} [{} total, {} calls, {} each][{} outside children]", data.labels[index], format_time(data.times[index]), data.calls[index], format_time(data.times[index]/data.calls[index] as f64), format_time(data.times[index]-accounted));
            for (_, child) in children {
                self.print_item(child, &(prefix.to_owned() + "  "));
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

impl<'a> TimingCapture for &'a Capture {
    type Mark = Mark<'a>;

    fn new_id(&self, label: &str) -> usize {
        let mut data = self.data.borrow_mut();
        let new_index = data.times.len();
        let new_len = data.times.len() + 1;
        data.times.resize(new_len, 0.0);
        data.calls.resize(new_len, 0);
        data.labels.push(label.to_owned());
        new_index
    }

    fn add(&self, index: usize, value: f64) {
        let mut data = self.data.borrow_mut();
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

// fn do_nothing<T: TimingCapture>(capture: T) {
//     let outside = mark!(capture, "do_nothing");
//     for _ii in 0 .. 2 {   
//         let _inside = mark!(outside, "iteration");
//         println!("hello");
//     }
//     {
//         let _other = mark!(outside, "slow");
//         std::thread::sleep(Duration::from_secs_f64(0.4));
//     }
// }

    
// fn main() {
//     let capture = Capture::new();
//     do_nothing(&capture);
//     capture.print();
// }

