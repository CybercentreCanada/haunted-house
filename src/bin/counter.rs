use std::cell::RefCell;
use std::sync::Once;


trait TimingCapture {
    type Mark;

    fn new_id(&self, label: &str) -> usize;
    // fn child_id(&mut self, parent: usize) -> usize;
    fn add(&self, index: usize, value: f64);
    fn build(&self, index: usize) -> Mark;
}

struct Inner {
    times: Vec<f64>,
    calls: Vec<u64>,
    heritage: Vec<(usize, usize)>,
    labels: Vec<String>,
}

struct Capture {
    data: RefCell<Inner>
}

impl Capture {
    fn new() -> Self {
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

    fn print(&self) {
        let data = self.data.borrow();
        for ii in 0..data.times.len() {
            println!("{} {} {}", data.times[ii], data.calls[ii], data.times[ii]/data.calls[ii] as f64)
        }
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

    fn build(&self, index: usize) -> Mark {
        Mark{time: std::time::Instant::now(), capture: self, index}
    }
}

struct Mark<'a> {
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

    fn build(&self, index: usize) -> Mark {
        Mark{time: std::time::Instant::now(), capture: self.capture, index}
    }
}

macro_rules! mark {
    ($capture:ident) => {
        {
            unsafe {
                static mut VALUE: usize = 0;
                // The macro will expand into the contents of this block.
                static INIT: Once = Once::new();
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
                static INIT: Once = Once::new();
                INIT.call_once(|| {
                    VALUE = $capture.new_id($label);
                });
                $capture.build(VALUE)
            }
        }
    }
}

fn do_nothing<T: TimingCapture>(capture: T) {
    let outside = mark!(capture, "do_nothing");
    for _ii in 0 .. 2 {   
        let inside = mark!(outside, "iteration");
        println!("hello");
    }
}

    
fn main() {
    let capture = Capture::new();
    do_nothing(&capture);
    capture.print();
}