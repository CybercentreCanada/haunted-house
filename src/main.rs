mod api;
mod config;
mod cache;
mod jobs;
mod storage;
mod error;
mod filter;
mod access;
mod ursadb;
mod varint;

use anyhow::Result;
use sqlx::{Pool, Sqlite};
use sqlx::pool::PoolOptions;

#[tokio::main]
async fn main() -> Result<()> {

    let tempdir = tempfile::tempdir()?;
    let path = tempdir.path().join("house.db");

    let path = if path.is_absolute() {
        format!("sqlite://{}?mode=rwc", path.to_str().unwrap())
    } else {
        format!("sqlite:{}?mode=rwc", path.to_str().unwrap())
    };

    let pool: Pool<Sqlite> = PoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect_lazy(&path)?;
    println!("Pool open");

    let mut con = pool.acquire().await?;
    println!("Pool connected");

    // let rows: Vec<(String, )> = sqlx::query_as("SELECT name FROM sqlite_master WHERE type='table' AND name=?;").bind("table_name").fetch_all(&mut con).await?;

    // for (row, ) in rows {
    //     println!("{:?}", row);
    // }

    return Ok(())
}
