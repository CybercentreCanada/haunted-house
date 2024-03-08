//!
//! Tools used to capture timing information when profiling.
//!

use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use itertools::Itertools;
use parking_lot::Mutex;

use lazy_static::lazy_static;

use crate::error::Result;

lazy_static! {
    static ref LABELS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Default::default());
    static ref HERITAGE: std::sync::Mutex<Vec<(usize, usize)>> = std::sync::Mutex::new(Default::default());
}

/// Trait that describes an object that can be used to capture timing spans
pub trait TimingCapture {
    /// Type used as a scope guard in the timing capture
    type Mark: TimingCapture;
    /// Create an id identifying a capture span
    fn new_id(&self, label: &str) -> usize;
    /// Add time to the capture for the given span
    fn add(&self, index: usize, value: f64);
    /// Build a scope guard object for the given span
    fn build(&self, index: usize) -> Self::Mark;
}

/// A capture of timing information for single invocation of timed code
pub struct Capture {
    /// runtime checked mutable inner data pointer
    data: RefCell<Inner>
}

/// Data for a capture
struct Inner {
    /// Accumulated time for each span encountered
    times: Vec<f64>,
    /// Invocation count for each span encountered
    calls: Vec<u64>,
}

impl Capture {
    /// Create a new capture object
    pub fn new() -> Self {
        Self{data: RefCell::new(Inner {
            times: vec![],
            calls: vec![],
        })}
    }

    /// Allocate a new span id
    fn child_id(&self, parent: usize, label: &str) -> usize {
        let new_index = self.new_id(label);
        let mut heritage = HERITAGE.lock().unwrap();
        heritage.push((parent, new_index));
        new_index
    }

    #[cfg(test)]
    pub fn print(&self) {
        println!("{}", self.format())
    }

    /// Print the information captured by the span
    pub fn format(&self) -> String {
        let mut parents = HashSet::new();
        let mut children = HashSet::new();
        let mut lines = vec![];
        let heritage = HERITAGE.lock().unwrap().clone();
        let labels = LABELS.lock().unwrap().clone();
        let mut roots = vec![];

        {
            let mut data = self.data.borrow_mut();
            for (parent, child) in heritage.iter() {
                parents.insert(*parent);
                children.insert(*child);
                if *child >= data.times.len() {
                    data.times.resize(*child + 1, 0.0);
                    data.calls.resize(*child + 1, 0);
                }
            }
            for index in parents {
                if children.contains(&index) {
                    continue
                }
                roots.push((data.times[index], index));
            }
        }

        roots.sort_by(|a, b|b.0.partial_cmp(&a.0).unwrap());
        for (_, index) in roots {
            self.format_item(index, "", &heritage, &labels, &mut lines);
        }

        return lines.into_iter().join("\n")
    }

    /// helper function for 'format'
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

/// Format a span in seconds into human readable units
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
        String::from("0s")
    }
}

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

/// A span marker scope guard to capture the time spent in a given span
pub struct Mark<'a> {
    /// Time that the span was entered
    time: std::time::Instant,
    /// The capture to store runtime information into when this span exits
    capture: &'a Capture,
    /// Id of the span
    index: usize,
}

impl Drop for Mark<'_> {
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

/// Helper macro to indicate timing capture sites
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
use serde::{Deserialize, Serialize};

/// A null capture object that doesn't actually do anything
pub struct NullCapture {}

impl NullCapture {
    /// Create a non-capturing drop in
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

const STAT_PATH: &str = "/sys/fs/cgroup/cpu.stat";

async fn load_cgroup_cpu_usage() -> u64 {
    let body = match tokio::fs::read_to_string(&STAT_PATH).await {
        Ok(body) => body,
        Err(_) => return 0,
    };

    for line in body.lines() {
        if line.starts_with("usage_usec") {
            return match &line[11..].parse() {
                Ok(value) => *value,
                Err(_) => 0,
            }
        }
    }
    0
}

async fn load_process_memory(pid: u64) -> u64 {
    let path = format!("/proc/{pid}/status");
    let body = match tokio::fs::read_to_string(&path).await {
        Ok(body) => body,
        Err(_) => return 0,
    };
    let parser = parse_size::Config::new().with_binary();

    for line in body.lines() {
        if line.starts_with("VmRSS:") {
            return parser.parse_size(&line[6..].trim()).unwrap_or_default();
        }
    }
    0
}

async fn load_memory() -> u64 {
    let mut total: u64 = 0;
    let mut iter = match tokio::fs::read_dir("/proc/").await {
        Ok(iter) => iter,
        Err(_) => return 0,
    };

    while let Ok(Some(dir)) = iter.next_entry().await {
        let pid: u64 = match dir.file_name().to_string_lossy().parse() {
            Ok(pid) => pid,
            Err(_) => continue,
        };
        total += load_process_memory(pid).await;
    }
    total
}


#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceReport {
    memory: u64,
    cpu_load_1m: f64,
    cpu_load_5m: f64,
    cpu_load_15m: f64,
}

const TIMESLOTS: usize = 16;
const MINUTE: Duration = Duration::from_secs(60);

#[derive(Default)]
struct Timestamps {
    data: [u64; TIMESLOTS],
    index: usize,
}

impl Timestamps {
    fn read_gap(&self, gap: usize) -> u64 {
        let current = self.data[self.index];
        let previous = self.data[(self.index + TIMESLOTS - gap) % TIMESLOTS];
        current - previous
    } 
}

#[derive(Clone)]
pub struct ResourceTracker {
    cpu: Arc<Mutex<Timestamps>>,
}

impl ResourceTracker {
    pub fn start() -> ResourceTracker {
        let cpu = Arc::new(Mutex::new(Timestamps::default()));
        let stamps = cpu.clone();
        tokio::spawn(async move {
            {
                let initial = load_cgroup_cpu_usage().await;
                let mut data = stamps.lock();
                for index in 0..TIMESLOTS {
                    data.data[index] = initial;
                }
            }

            while Arc::strong_count(&stamps) > 1 {
                tokio::time::sleep(MINUTE).await;
                let count = load_cgroup_cpu_usage().await;
                let mut data = stamps.lock();
                data.index = (data.index + 1) % TIMESLOTS;
                let index = data.index;
                data.data[index] = count;
            }
        });
        ResourceTracker { cpu }
    }

    pub async fn read(&self) -> ResourceReport {
        let memory = load_memory().await;
        let data = self.cpu.lock();
        let min_1 = MINUTE.as_micros() as f64;
        let min_5 = min_1 * 5.0;
        let min_15 = min_1 * 15.0;
        ResourceReport {
            memory,
            cpu_load_1m: data.read_gap(1) as f64/min_1,
            cpu_load_5m: data.read_gap(5) as f64/min_5,
            cpu_load_15m: data.read_gap(15) as f64/min_15,
        }
    }
}


#[tokio::test]
async fn test_load_cgroup_cpu_usage() {
    let t1 = load_cgroup_cpu_usage().await;
    assert!(t1 > 0);
    for i in 0..100000 {
        i.to_string();
    }
    assert!(load_cgroup_cpu_usage().await > t1);
}

#[tokio::test]
async fn test_load_memory() {
    assert!(load_memory().await > 0);
}