use std::cell::{Cell, RefCell};
use std::collections::{HashSet, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use bitvec::vec::BitVec;
use anyhow::{anyhow, Result, Context};
use plotters::prelude::{IntoDrawingArea, ChartBuilder, LabelAreaPosition, IntoLogRange, Rectangle, IntoLinspace};
use plotters::series::{Histogram, LineSeries};
use plotters::style::{Color, HSLColor};
use sha2::Digest;
use sqlx::SqlitePool;
use sqlx::pool::PoolOptions;

use plotters::backend::SVGBackend;


pub trait Filter {

    fn label(&self) -> String;
    fn data<'a>(&'a self) -> &'a BitVec;

    fn search(&self, target: &Vec<u8>) -> Result<bool>;
}

fn load(label: &str, data: BitVec) -> Result<Box<dyn Filter>> {
    if label.starts_with("simple:") {
        Ok(Box::new(SimpleFilter::load(label, data)?))
    } else if label.starts_with("kin:") {
        Ok(Box::new(KinFilter::load(label, data)?))
    } else {
        Err(anyhow!("Unknown label format: {label}"))
    }
}

pub struct SimpleFilter {
    data: BitVec
}

impl Filter for SimpleFilter {
    fn label(&self) -> String {
        SimpleFilter::label_as(self.data.len() as u32)
    }

    fn data<'a>(&'a self) -> &'a BitVec {
        &self.data
    }

    fn search(&self, target: &Vec<u8>) -> Result<bool> {
        if target.len() < 3 {
            return Ok(false);
        }
        let mut trigram = ((target[0] as u32) << 8) | target[1] as u32;

        for byte in target[2..].iter() {
            trigram = ((trigram & 0xFFFF) << 8) | *byte as u32;

            let mut hasher = DefaultHasher::new();
            trigram.hash(&mut hasher);
    
            let index = hasher.finish() as usize % self.data.len();
            if !self.data.get(index).unwrap() {
                return Ok(false)
            }
        }

        return Ok(true)
    }
}

impl SimpleFilter {
    pub fn build<IN: std::io::Read>(settings: u32, input: IN) -> Result<Self> {
        // init buffer
        let mut data = BitVec::new();
        data.resize(settings as usize, false);
        let mut bytes = input.bytes();
        
        // build initial state
        let first = match bytes.next() {
            Some(first) => first?,
            None => return Ok(SimpleFilter { data }),
        };
        let second = match bytes.next() {
            Some(first) => first?,
            None => return Ok(SimpleFilter { data }),
        };
    
        let mut trigram: u32 = ((first as u32) << 8) | second as u32;

        // injest data
        for byte in bytes.into_iter() {
            trigram = ((trigram & 0xFFFF) << 8) | byte? as u32;

            let mut hasher = DefaultHasher::new();
            trigram.hash(&mut hasher);
    
            let index = hasher.finish() % settings as u64;
            data.set(index as usize, true);
        }
    
        // 
        Ok(Self { data })
    }

    pub fn label_as(size: u32) -> String {
        format!("simple:{}", size)
    }

    fn load(label: &str, data: BitVec) -> Result<Self> {
        let mut parts = label.split(":");
        let kind_name = parts.next().ok_or(anyhow!("Invalid label"))?;
        if kind_name != "simple" {
            return Err(anyhow!("Invalid label"));
        }
        let expected_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
        if data.len() != expected_size as usize {
            return Err(anyhow!("corrupt"));
        }
        return Ok(SimpleFilter { data })
    }

}


pub struct OpenBucketFilter {
    data: BitVec,
    bucket_size: u32,
}

impl Filter for OpenBucketFilter {
    fn label(&self) -> String {
        format!("open:{}:{}", self.data.len(), self.bucket_size)
    }

    fn data<'a>(&'a self) -> &'a BitVec {
        &self.data
    }

    fn search(&self, target: &Vec<u8>) -> Result<bool> {
        if target.len() < 3 {
            return Ok(false);
        }
        let mut trigram = ((target[0] as u32) << 8) | target[1] as u32;

        let mut active_offsets: Vec<usize> = (0..self.bucket_size as usize).collect();

        for (sub_index, byte) in target.iter().enumerate().skip(2) {
            trigram = ((trigram & 0xFFFF) << 8) | *byte as u32;

            let mut new_offsets = vec![];

            for offset in active_offsets {
                let mut hasher = DefaultHasher::new();
                trigram.hash(&mut hasher);
                ((offset + sub_index) % self.bucket_size as usize).hash(&mut hasher);

                let index = hasher.finish() as usize % self.data.len();
                if *self.data.get(index).unwrap() {
                    new_offsets.push(offset);
                }
            }

            if new_offsets.is_empty() {
                 return Ok(false)
            }
            active_offsets = new_offsets;
        }

        return Ok(true)
    }

}


impl OpenBucketFilter {
    pub fn build<IN: std::io::Read>(buffer_size: u32, bucket_size: u32, input: IN) -> Result<Self> {
        // init buffer
        let mut data = BitVec::new();
        data.resize(buffer_size as usize, false);
        let mut bytes = input.bytes();
        
        // build initial state
        let first = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, bucket_size }),
        };
        let second = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, bucket_size }),
        };
    
        let mut trigram: u32 = ((first as u32) << 8) | second as u32;

        // injest data
        for (index, byte) in bytes.into_iter().enumerate() {
            trigram = ((trigram & 0xFFFF) << 8) | byte? as u32;

            let mut hasher = DefaultHasher::new();
            trigram.hash(&mut hasher);
            (index % bucket_size as usize).hash(&mut hasher);
    
            let index = hasher.finish() % buffer_size as u64;
            data.set(index as usize, true);
        }
    
        // 
        Ok(Self { data, bucket_size })
    }

    // fn load(label: String, data: BitVec) -> Result<Self> {
    //     let mut parts = label.split(":");
    //     let kind_name = parts.next().ok_or(anyhow!("Invalid label"))?;
    //     if kind_name != "open" {
    //         return Err(anyhow!("Invalid label"));
    //     }
    //     let expected_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
    //     if data.len() != expected_size as usize {
    //         return Err(anyhow!("corrupt"));
    //     }
    //     let bucket_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
    //     if data.len() != expected_size as usize {
    //         return Err(anyhow!("corrupt"));
    //     }
    //     return Ok(Self { data, bucket_size })
    // }

}


pub struct ClosedBucketFilter {
    data: BitVec,
    buckets: u64,
    bucket_size: u64,
}

impl Filter for ClosedBucketFilter {
    fn label(&self) -> String {
        format!("closed:{}:{}", self.data.len(), self.bucket_size)
    }

    fn data<'a>(&'a self) -> &'a BitVec {
        &self.data
    }

    fn search(&self, target: &Vec<u8>) -> Result<bool> {
        if target.len() < 3 {
            return Ok(false);
        }
        let mut trigram = ((target[0] as u32) << 8) | target[1] as u32;

        let mut active_offsets: Vec<u64> = (0..self.bucket_size).collect();

        for (sub_index, byte) in target.iter().enumerate().skip(2) {
            trigram = ((trigram & 0xFFFF) << 8) | *byte as u32;

            let mut new_offsets = vec![];

            for offset in active_offsets {
                let mut hasher = DefaultHasher::new();
                trigram.hash(&mut hasher);

                let index = (hasher.finish() % self.buckets) * self.bucket_size + (offset + sub_index as u64) % self.bucket_size;
                if *self.data.get(index as usize).unwrap() {
                    new_offsets.push(offset);
                }
            }

            if new_offsets.is_empty() {
                 return Ok(false)
            }
            active_offsets = new_offsets;
        }

        return Ok(true)
    }

}


impl ClosedBucketFilter {
    pub fn build<IN: std::io::Read>(buffer_size: u64, bucket_size: u64, input: IN) -> Result<Self> {
        // init buffer
        let mut data = BitVec::new();
        data.resize(buffer_size as usize, false);
        let mut bytes = input.bytes();
        let buckets = buffer_size / bucket_size;
        if buckets * bucket_size != buffer_size {
            return Err(anyhow!("bucket size must be factor of buffer size"));
        }
        
        // build initial state
        let first = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, buckets, bucket_size }),
        };
        let second = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, buckets, bucket_size }),
        };
    
        let mut trigram: u32 = ((first as u32) << 8) | second as u32;

        // injest data
        for (index, byte) in bytes.into_iter().enumerate() {
            trigram = ((trigram & 0xFFFF) << 8) | byte? as u32;

            let mut hasher = DefaultHasher::new();
            trigram.hash(&mut hasher);
    
            let index = (hasher.finish() % buckets) * bucket_size + (index as u64) % bucket_size;
            data.set(index as usize, true);
        }
    
        // 
        Ok(Self { data, buckets, bucket_size })
    }

    // fn load(label: String, data: BitVec) -> Result<Self> {
    //     let mut parts = label.split(":");
    //     let kind_name = parts.next().ok_or(anyhow!("Invalid label"))?;
    //     if kind_name != "open" {
    //         return Err(anyhow!("Invalid label"));
    //     }
    //     let expected_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
    //     if data.len() != expected_size as usize {
    //         return Err(anyhow!("corrupt"));
    //     }
    //     let bucket_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
    //     if data.len() != expected_size as usize {
    //         return Err(anyhow!("corrupt"));
    //     }
    //     return Ok(Self { data, bucket_size })
    // }

}


pub struct KinFilter {
    data: BitVec,
    min_find: u32,
    max_set: u32,
}

impl Filter for KinFilter {
    fn label(&self) -> String {
        Self::label_as(self.data.len(), self.min_find, self.max_set)
    }

    fn data<'a>(&'a self) -> &'a BitVec {
        &self.data
    }

    fn search(&self, target: &Vec<u8>) -> Result<bool> {
        if target.len() < 3 {
            return Ok(false);
        }
        let mut trigram = ((target[0] as u32) << 8) | target[1] as u32;

        for byte in target[2..].iter() {
            trigram = ((trigram & 0xFFFF) << 8) | *byte as u32;

            let mut hits = 0;
            for index in Self::hash_trigram(trigram, self.max_set, self.data.len()) {
                if *self.data.get(index).unwrap() {
                    hits += 1;
                    if hits >= self.min_find {
                        break;
                    }
                }
            }

            if hits < self.min_find {
                return Ok(false)
            }
        }

        return Ok(true)
    }
}

impl KinFilter {
    pub fn build<IN: std::io::Read>(size: usize, min_find: u32, max_set: u32, input: IN) -> Result<Self> {
        // init buffer
        let mut data = BitVec::new();
        data.resize(size, false);

        let mut heatmap: Vec<Rc<RefCell<HashSet<u32>>>> = Default::default();
        heatmap.resize_with(size, || Rc::new(RefCell::new(Default::default())));

        let mut bytes = input.bytes();
        
        // build initial state
        let first = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, min_find, max_set }),
        };
        let second = match bytes.next() {
            Some(first) => first?,
            None => return Ok(Self { data, min_find, max_set }),
        };
    
        let mut trigram: u32 = ((first as u32) << 8) | second as u32;

        // injest data
        for byte in bytes.into_iter() {
            trigram = ((trigram & 0xFFFF) << 8) | byte? as u32;

            for index in Self::hash_trigram(trigram, max_set, size) {
                heatmap[index].borrow_mut().insert(trigram);
            }
        }
    
        let mut missing: Vec<(usize, Rc<RefCell<HashSet<u32>>>)> = heatmap.iter().cloned().enumerate().collect();
        let mut assignments = vec![0; 1 << 24];

        loop {
            let mut ii = 0;
            while ii < missing.len() {
                if missing[ii].1.borrow().is_empty() {
                    missing.swap_remove(ii);
                } else {
                    ii += 1;
                }
            }
            missing.sort_by_key(|a| a.1.borrow().len());

            let (index, trigrams) = match missing.pop() {
                Some(row) => row,
                None => break,
            };

            data.set(index, true);

            let trigrams: HashSet<u32> = trigrams.borrow_mut().clone();
            for trigram in trigrams {
                assignments[trigram as usize] += 1;
                if assignments[trigram as usize] >= min_find {
                    for index in Self::hash_trigram(trigram, max_set, size) {
                        heatmap[index].borrow_mut().remove(&trigram);
                    }
        
                    // for row in missing.iter_mut() {
                    //     row.1.remove(&trigram);
                    // }
                }
            }
        }
        
        return Ok(Self { data, min_find, max_set })
    }

    fn hash_trigram(trigram: u32, max_set: u32, bins: usize) -> Vec<usize> {
        let mut out: HashSet<usize> = HashSet::default();

        let mut hasher = DefaultHasher::new();
        trigram.hash(&mut hasher);

        let mut ii = 0;
        while out.len() < max_set as usize {
            out.insert(hasher.finish() as usize % bins);
            ii += 1;
            ii.hash(&mut hasher);
        }

        return out.into_iter().collect();
    }

    pub fn label_as(size: usize, min_find: u32, max_set: u32) -> String  {
        format!("kin:{}:{}:{}", size, min_find, max_set)
    }

    fn load(label: &str, data: BitVec) -> Result<Self> {
        let mut parts = label.split(":");
        let kind_name = parts.next().ok_or(anyhow!("Invalid label"))?;
        if kind_name != "kin" {
            return Err(anyhow!("Invalid label"));
        }
        let expected_size = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
        if data.len() != expected_size as usize {
            return Err(anyhow!("corrupt"));
        }
        let min_find = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
        let max_set = u32::from_str_radix(parts.next().ok_or(anyhow!("Invalid label"))?, 10)?;
        if min_find > max_set {
            return Err(anyhow!("couldn't figure out kin label: {label}"));
        }
        return Ok(KinFilter { data, min_find, max_set })
    }

}

struct Database {
    db: SqlitePool,
    // config: Config,
    // work_notification: tokio::sync::Notify,
    // _temp_dir: Option<tempfile::TempDir>,
}

impl Database {
    pub async fn new(url: &str) -> Result<Self> {

        let url = if url == "memory" {
            format!("sqlite::memory:")
        } else {
            let path = Path::new(url);
            if path.is_absolute() {
                format!("sqlite://{}?mode=rwc", url)
            } else {
                format!("sqlite:{}?mode=rwc", url)
            }
        };

        let pool = PoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(&url).await?;

        Self::initialize(&pool).await?;

        Ok(Self {
            db: pool,
            // config,
            // work_notification: Default::default(),
            // _temp_dir: None,
        })
    }

    async fn initialize(pool: &SqlitePool) -> Result<()> {
        let mut con = pool.acquire().await?;

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists filter_table (
            id INT PRIMARY KEY,
            label TEXT NOT NULL,
            filter BLOB NOT NULL,
            hash BLOB NOT NULL
        )")).execute(&mut con).await.context("error creating table filter_table")?;
        sqlx::query(&format!("CREATE INDEX if not exists filter_table_hash_label ON filter_table(hash, label)")).execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists paths (
            path TEXT PRIMARY KEY,
            hash BLOB NOT NULL
        )")).execute(&mut con).await.context("error creating table paths")?;

        return Ok(())
    }


    pub async fn get_cached_hash(&self, path: &Path) -> Result<Option<Vec<u8>>> {
        let row: Option<(Vec<u8>, )> = sqlx::query_as("SELECT hash FROM paths WHERE path = ?")
            .bind(path.as_os_str().to_str().unwrap())
            .fetch_optional(&self.db).await?;

        let row = match row {
            Some(row) => row.0,
            None => return Ok(None),
        };

        return Ok(Some(row))
    }

    pub async fn cache_hash(&self, path: &Path, hash: &Vec<u8>) -> Result<()> {
        sqlx::query("INSERT INTO paths(path, hash) VALUES(?, ?)")
            .bind(path.as_os_str().to_str().unwrap())
            .bind(hash)
            .execute(&self.db).await?;
        return Ok(())
    }

    pub async fn get(&self, hash: &Vec<u8>, label: &str) -> Result<Option<Box<dyn Filter>>> {
        let row: Option<(Vec<u8>, )> = sqlx::query_as("SELECT filter FROM filter_table WHERE hash = ? AND label = ?")
            .bind(hash)
            .bind(label)
            .fetch_optional(&self.db).await?;

        let row = match row {
            Some(row) => row.0,
            None => return Ok(None),
        };

        let mut data = BitVec::new();
        data.append(&mut BitVec::<_, bitvec::order::Lsb0>::from_vec(row));

        let filter = load(label, data)?;

        return Ok(Some(filter))
    }

    pub async fn insert(&self, hash: &Vec<u8>, label: &str, filter: &Vec<u8>) -> Result<()> {
        sqlx::query("INSERT INTO filter_table(filter, hash, label) VALUES(?, ?, ?)")
            .bind(filter)
            .bind(hash)
            .bind(label)
            .execute(&self.db).await?;
        return Ok(())
    }
}

// async fn fetch_filters(db: &mut Database, path: &Path) -> Result<(Vec<Box<dyn Filter>>, i32)> {
//     todo!()
// }

async fn fetch_filters(db: &mut Database, path: &Path) -> Result<(Vec<Box<dyn Filter>>, i32)> {
    let mut out = vec![];
    let mut new_built = 0;

    let hash = if let Some(value) = db.get_cached_hash(path).await? {
        value
    } else {
        let file = std::fs::File::open(path)?;
        let mut file = BufReader::new(file);
        let mut hasher = sha2::Sha256::new();

        let mut buff: Vec<u8> = vec![0; 1 << 20];
        loop {
            let read_bytes = file.read(&mut buff)?;
            if read_bytes == 0 {
                break
            }
            hasher.update(&buff);
        }

        let hash = hasher.finalize().to_vec();
        db.cache_hash(path, &hash).await?;
        hash
    };

    for size in 6..=18 {
        let label = SimpleFilter::label_as(1 << size);
        let simple = if let Some(filter) = db.get(&hash, &label).await? {
            filter
        } else {
            let file = std::fs::File::open(path)?;
            let file = BufReader::new(file);
            let filter = tokio::task::spawn_blocking(move ||{
                anyhow::Ok(SimpleFilter::build(1 << size, file)?)
            }).await??;
            let data: core::result::Result<Vec<u8>, _> = filter.data().clone().bytes().collect();
            db.insert(&hash, &label, &data?).await?;
            new_built += 1;
            Box::new(filter)
        };

        let label = KinFilter::label_as(1 << size, 2, 3);
        let kin = if let Some(filter) = db.get(&hash, &label).await? {
            filter
        } else {
            let file = std::fs::File::open(path)?;
            let file = BufReader::new(file);
            let filter = tokio::task::spawn_blocking(move ||{
                anyhow::Ok(KinFilter::build(1 << size, 2, 3, file)?)
            }).await??;
            let data: core::result::Result<Vec<u8>, _> = filter.data().clone().bytes().collect();
            db.insert(&hash, &label, &data?).await?;
            new_built += 1;
            Box::new(filter)
        };

        if simple.data().count_ones() <= kin.data().count_ones() {
            out.push(simple);
        } else {
            out.push(kin);
        }
    }

    return Ok((out, new_built))
}

#[tokio::main]
async fn main() -> Result<()> {

    let mut db = Database::new("./hashes.sqlite").await?;

    // let res = sqlx::query("DELETE FROM filter_table where label LIKE 'kin%' ").execute(&db.db).await?;
    // println!("removed {}", res.rows_affected());
    // return Ok(());

    let mut visited: HashSet<PathBuf> = Default::default();
    let mut dirs = vec![PathBuf::from("/bin/")];
    let mut added = 0;

    let mut filled_lines = vec![];

    loop {
        let current_dir = match dirs.pop() {
            Some(current) => current,
            None => break,
        };

        for item in std::fs::read_dir(current_dir)? {
            let item = item?;

            if added >= 10 {
                break
            }
        
            if visited.contains(&item.path()) {
                continue;
            }
            visited.insert(item.path());

            let file_type = item.file_type()?;
            if file_type.is_dir() {
                dirs.push(item.path());
            }

            if file_type.is_file() {
                println!("{:?}", item);

                let (filters, new_built) = fetch_filters(&mut db, &item.path()).await?;
                added += new_built;

                let mut line: Vec<(i64, f64, bool)> = vec![];
                for filter in filters {
                    let bits = filter.data();
                    let len = bits.len();
                    line.push((len as i64, bits.count_ones() as f64 / len as f64, filter.label().starts_with("simple")));
                }
                filled_lines.push(line);
            }
        }
    }

    let bins = filled_lines
        .iter()
        .map(|line| line.iter().map(|(point, _, _)| *point).collect())
        .reduce(|a: HashSet<i64>, b: HashSet<i64>|a.intersection(&b).cloned().collect()).unwrap();
    let mut bins: Vec<i64> = bins.into_iter().collect();
    bins.sort();

    // let bins: Vec<i64> = filled_lines.first().unwrap().iter().cloned().map(|(point, _)| point).collect();
    let mut percent_bins = vec![0; 51];
    for line in filled_lines.iter() {
        percent_bins[(line.last().unwrap().1 * 50.0) as usize] += 1;
    }
    percent_bins[49] += percent_bins[50];
    percent_bins.pop();
    
    let root = SVGBackend::new("line.svg", (1024, 768)).into_drawing_area();

    root.fill(&plotters::style::WHITE)?;

    let (main, axis) = root.split_horizontally(900);

    let mut chart = ChartBuilder::on(&main)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .set_label_area_size(LabelAreaPosition::Right, 30)
        .caption("Fliter density", ("sans-serif", 40))
        .build_cartesian_2d(0..(bins.len() as i64 - 1), 0.0f64..1.0)?;

    chart
        .configure_mesh()
        // .disable_x_mesh()
        // .disable_y_mesh()
        .x_label_formatter(&|x| {
            match bins.get(*x as usize) {
                Some(x) => format!("{x}"),
                None => format!("")
            }
        })
        .x_labels(bins.len())
        .draw()?;

    for line in filled_lines {
        let points: HashMap<i64, f64> = line.into_iter().map(|(a, b, c)|(a, b)).collect();
        let mut line = vec![];
        for (index, bin) in bins.iter().enumerate() {
            line.push((index as i64, *points.get(bin).unwrap()))
        }
        chart.draw_series(
            LineSeries::new(
                line,
                &plotters::style::RED.mix(0.2),
            )
            
            // .border_style(&plotters::style::RED),
        )?;

    }

    let bin_max = match percent_bins.iter().reduce(|a, b|a.max(b)) {
        Some(x) => *x,
        None => 1
    };
    println!("{bin_max} {percent_bins:?}");

    let mut y_hist_ctx = ChartBuilder::on(&axis)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .set_label_area_size(LabelAreaPosition::Top, 40)
        .build_cartesian_2d(0..bin_max, 0..percent_bins.len())?;

    // let y_hist = Histogram::horizontal(&y_hist_ctx)
    //     .margin(0)
    //     .data(percent_bins.iter().map(|count| (1i32, *count)));

    // y_hist_ctx.draw_series(y_hist)?;
    y_hist_ctx.draw_series(
        percent_bins
        .iter()
        .enumerate()
        .map(|(index, value)|{
            // let v = *value as f64 / bin_max as f64;
            Rectangle::new(
                [(0, index), (*value, index + 1)],
                plotters::style::RED //.mix(0.2)
                // HSLColor(
                //     240.0 / 360.0 - 240.0 / 360.0 * v,
                //     0.7,
                //     0.1 + 0.4 * v,
                // )
                // .filled(),
            )
        })
    )?;

    root.present()?;

    return Ok(())
}
