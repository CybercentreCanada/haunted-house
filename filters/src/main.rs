mod filters;
mod database;
mod filter_tree;

use std::collections::{HashSet, HashMap};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use bitvec::vec::BitVec;
use anyhow::{Result, Context};
use clap::{Parser, Subcommand};
use database::Database;
use filters::{Filter, load, SimpleFilter, KinFilter};
use plotters::prelude::{IntoDrawingArea, ChartBuilder, LabelAreaPosition, Rectangle, Circle, BitMapBackend};
use plotters::series::{LineSeries};
use plotters::style::{Color};
use sha2::Digest;


async fn fetch_simple_filter(db: Database, path: PathBuf, hash: Vec<u8>, size: usize) -> Result<(Box<dyn Filter>, i32)> {
    let mut new_built = 0;
    let label = SimpleFilter::label_as(1 << size);
    let simple = if let Some(filter) = db.get(&hash, &label).await? {
        filter
    } else {
        let file = std::fs::File::open(&path)?;
        let file = BufReader::new(file);
        let filter = tokio::task::spawn_blocking(move ||{
            anyhow::Ok(SimpleFilter::build(1 << size, file)?)
        }).await??;
        let data: core::result::Result<Vec<u8>, _> = filter.data().clone().bytes().collect();
        db.insert(&hash, &label, &data?).await?;
        new_built += 1;
        Box::new(filter)
    };
    return Ok((simple, new_built))
}

async fn fetch_kin2_filter(db: Database, path: PathBuf, hash: Vec<u8>, size: usize) -> Result<(Box<dyn Filter>, i32)> {
    let mut new_built = 0;
    let label = KinFilter::label_as(1 << size, 1,2);
    let kin = if let Some(filter) = db.get(&hash, &label).await? {
        filter
    } else {
        let file = std::fs::File::open(&path)?;
        let file = BufReader::new(file);
        let filter = tokio::task::spawn_blocking(move ||{
            anyhow::Ok(KinFilter::build(1 << size, 1, 2, file)?)
        }).await??;
        let data: core::result::Result<Vec<u8>, _> = filter.data().clone().bytes().collect();
        db.insert(&hash, &label, &data?).await?;
        new_built += 1;
        Box::new(filter)
    };
    return Ok((kin, new_built))
}

async fn fetch_kin3_filter(db: Database, path: PathBuf, hash: Vec<u8>, size: usize) -> Result<(Box<dyn Filter>, i32)> {
    let mut new_built = 0;
    let label = KinFilter::label_as(1 << size, 2, 3);
    let kin = if let Some(filter) = db.get(&hash, &label).await? {
        filter
    } else {
        let file = std::fs::File::open(&path)?;
        let file = BufReader::new(file);
        let filter = tokio::task::spawn_blocking(move ||{
            anyhow::Ok(KinFilter::build(1 << size, 2, 3, file)?)
        }).await??;
        let data: core::result::Result<Vec<u8>, _> = filter.data().clone().bytes().collect();
        db.insert(&hash, &label, &data?).await?;
        new_built += 1;
        Box::new(filter)
    };
    return Ok((kin, new_built))
}

async fn fetch_filters_size(db: Database, path: PathBuf, hash: Vec<u8>, size: usize) -> Result<(Box<dyn Filter>, i32)> {

    let (simple, kin3, kin2) = tokio::join!(
        fetch_simple_filter(db.clone(), path.clone(), hash.clone(), size),
        fetch_kin3_filter(db.clone(), path.clone(), hash.clone(), size),
        fetch_kin2_filter(db, path, hash, size)
    );

    let (simple, _a) = simple?;
    let (kin2, _c) = kin2?;
    let (kin3, _b) = kin3?;
    let built = _a + _b + _c;

    return Ok((simple, built));

    // if simple.data().count_ones() <= kin3.data().count_ones() {
    //     if simple.data().count_ones() <= kin2.data().count_ones() {
    //         Ok((simple, built))
    //     } else {
    //         Ok((kin2, built))
    //     }
    // } else {
    //     if kin3.data().count_ones() <= kin2.data().count_ones() {
    //         Ok((kin3, built))
    //     } else {
    //         Ok((kin2, built))
    //     }
    // }
}

async fn hash_file(db: &Database, path: &Path) -> Result<Vec<u8>> {
    let hash = if let Some(value) = db.get_cached_hash(path).await? {
        value
    } else {
        let hash = tokio::task::spawn_blocking({
            let path = path.to_owned();
            move ||{
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
                return anyhow::Ok(hash);
            }
        }).await??;

        db.cache_hash(path, &hash).await?;
        hash
    };
    return Ok(hash)
}

async fn fetch_filters(db: &mut Database, path: &Path) -> Result<(Vec<Box<dyn Filter>>, i32)> {
    let mut out = vec![];
    let mut new_built = 0;

    let hash = hash_file(db, path).await?;

    let mut futures = vec![];
    for size in 6..=20 {
        futures.push(tokio::spawn(fetch_simple_filter(db.clone(), path.to_owned(), hash.clone(), size)));
    }

    for result in futures {
        let (filter, built) = result.await??;
        out.push(filter);
        new_built += built;
    }

    return Ok((out, new_built));
}

#[derive(Subcommand, Debug, Clone)]
enum SubCommands {
    PlotSize {
        #[arg(short, long, default_value_t = 0.5)]
        target_density: f64,
        #[arg(short, long, default_value_t = u32::MAX)]
        file_limit: u32,
    }
}

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    cmd: SubCommands
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    match args.cmd {
        SubCommands::PlotSize { target_density, file_limit } => {
            plot_simple_size(target_density, file_limit).await.unwrap()
        }
    }
}

fn label_kind(label: &str) -> u8 {
    if label.starts_with("simple") {
        0
    } else {
        if label.ends_with(":1:2") {
            2
        } else {
            1
        }
    }
}

async fn run() -> Result<()> {

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

            // if added >= 1 {
            //     break
            // }

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

                let mut line: Vec<(i64, f64, u8)> = vec![];
                for filter in filters {
                    let bits = filter.data();
                    let len = bits.len();
                    line.push((len as i64, bits.count_ones() as f64 / len as f64, label_kind(&filter.label())));
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

    // let root = SVGBackend::new("line.svg", (1024, 768)).into_drawing_area();
    let width = 1 << 10;
    let root = BitMapBackend::new("line.png", (width, 1 << 10)).into_drawing_area();

    root.fill(&plotters::style::WHITE)?;

    let (main, axis) = root.split_horizontally(width * 9 / 10);

    let mut chart = ChartBuilder::on(&main)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .set_label_area_size(LabelAreaPosition::Right, 30)
        .caption("Filter density", ("sans-serif", 40))
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
        let mut non_simple: HashMap<i64, u8> = Default::default();
        for (size, _density, simple) in line.iter() {
            non_simple.insert(*size, *simple);
        }

        let points: HashMap<i64, f64> = line.into_iter().map(|(a, b, _)|(a, b)).collect();
        let mut line = vec![];
        let mut marked_points1 = vec![];
        let mut marked_points2 = vec![];
        for (index, bin) in bins.iter().enumerate() {
            line.push((index as i64, *points.get(bin).unwrap()));
            if non_simple.get(bin) == Some(&1) {
                marked_points1.push((index as i64, *points.get(bin).unwrap()));
            }
            if non_simple.get(bin) == Some(&2) {
                marked_points2.push((index as i64, *points.get(bin).unwrap()));
            }
        }
        chart.draw_series(LineSeries::new(line, &plotters::style::RED.mix(0.2)))?;
        // chart.draw_series(PointSeries::new(marked_points, 1, &plotters::style::BLUE.mix(0.2)))?;
        chart.draw_series(
            marked_points1
                .iter()
                .map(|(x, y)| Circle::new((*x, *y), 1, &plotters::style::BLUE.mix(0.2))),
        )?;
        chart.draw_series(
            marked_points2
                .iter()
                .map(|(x, y)| Circle::new((*x, *y), 1, &plotters::style::MAGENTA.mix(0.3))),
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

async fn process_compressed_simple_size(db: Database, target_density: f64, path: PathBuf) -> Result<(i64, f64)> {

    let temp = tokio::task::spawn_blocking(move || {
        let temp = tempfile::NamedTempFile::new()?;
        {
            let mut reader = std::fs::File::open(path)?;
            let mut writer = snap::write::FrameEncoder::new(temp.as_file());
            std::io::copy(&mut reader, &mut writer)?;
        }
        anyhow::Ok(temp)
    }).await??;

    return process_simple_size(db, target_density, temp.path().to_owned(), temp.as_file().metadata()?).await;
}

async fn process_simple_size(db: Database, target_density: f64, path: PathBuf, metadata: std::fs::Metadata) -> Result<(i64, f64)> {
    let hash = hash_file(&db, &path).await?;
    let mut found = None;

    for size in 6..=25 {
        let (filter, built) = fetch_simple_filter(db.clone(), path.clone(), hash.clone(), size).await?;

        let density = filter.data().count_ones() as f64/((1 << size) as f64);
        found = Some((filter, size));
        if density <= target_density {
            break;
        }
    }

    let (filter, size) = found.unwrap();
    let filter_bits = filter.data().len();
    let filter_bytes = filter_bits/8;
    let compression = filter_bytes as f64/metadata.len() as f64;
    println!("{}", compression);
    return Ok((size as i64, compression));
}


async fn plot_simple_size(target_density: f64, file_limit: u32) -> Result<()> {

    let db = Database::new("./hashes.sqlite").await?;

    let mut visited: HashSet<PathBuf> = Default::default();
    let mut dirs = vec![PathBuf::from("/bin/")];

    let mut active_jobs = vec![];
    let mut files_added = 0;

    'out: loop {
        let current_dir = match dirs.pop() {
            Some(current) => current,
            None => break,
        };

        for item in std::fs::read_dir(current_dir)? {
            let item = item?;

            if files_added >= file_limit {
                break 'out;
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
                active_jobs.push(tokio::spawn(process_simple_size(db.clone(), target_density, item.path(), item.metadata()?)));
                active_jobs.push(tokio::spawn(process_compressed_simple_size(db.clone(), target_density, item.path())));
                files_added += 1;
            }
        }
    }

    // Collect the information about the filters
    let mut points = vec![];
    let mut compression_roof = 0f64;
    let mut compression_total = 0f64;

    for job in active_jobs {
        let row = job.await??;
        points.push(row);
        compression_roof = compression_roof.max(row.1);
        compression_total += row.1;
    }
    println!("Files processed {} {}", points.len(), compression_total/points.len() as f64);

    // Round up the top of the graph to the nearest 10%
    compression_roof = ((compression_roof * 10f64).floor() + 1f64)/10f64;



    // // let root = SVGBackend::new("line.svg", (1024, 768)).into_drawing_area();
    let width = 1 << 10;
    let root = BitMapBackend::new("compression.png", (width, 1 << 10)).into_drawing_area();

    root.fill(&plotters::style::WHITE)?;

    // let (main, axis) = root.split_horizontally(width * 9 / 10);

    let mut chart = ChartBuilder::on(&root)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .set_label_area_size(LabelAreaPosition::Right, 30)
        .caption("Filter compression", ("sans-serif", 40))
        .build_cartesian_2d(5i64..24, 0.0f64..compression_roof)?;

    chart
        .configure_mesh()
        // .disable_x_mesh()
        // .disable_y_mesh()
        .x_label_formatter(&|x| {
            format!("{}", 1 << x)
        })
        .x_labels(15)
        .draw()?;

    chart.draw_series(
        points
        .iter()
        .map(|(x, y)| Circle::new((*x as i64, *y), 1, &plotters::style::BLUE.mix(0.5))),
    )?;


    root.present()?;

    return Ok(())
}
