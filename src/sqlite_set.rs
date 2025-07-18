//! A tool for creating temporary sets of unique data backed by sqlite

use std::marker::PhantomData;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::SqlitePool;
use anyhow::{Result, Context};
use sqlx::pool::PoolOptions;

/// A set of unique objects, backed by sqlite
pub struct SqliteSet<Item: Serialize + DeserializeOwned> {
    /// Sqlite connection
    db: SqlitePool,
    /// A scope guard making sure a temporary directory holding this set gets cleaned up on drop
    _temp_dir: Option<tempfile::TempDir>,
    /// A symbol tying the Item type to this struct forcing all items in a set to be that type
    _item_type: PhantomData<Item>
}


impl<Item: Serialize + DeserializeOwned> SqliteSet<Item> {
    /// Construct a file backed set at the given path
    pub async fn new(url: &str) -> Result<Self> {
        let url = if url == "memory" {
            String::from("sqlite::memory:")
        } else {
            let path = Path::new(url);

            if let Some(parent) = path.parent() {
                if parent != Path::new("") && parent != Path::new("/") && !parent.exists() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }

            if path.is_absolute() {
                format!("sqlite://{url}?mode=rwc")
            } else {
                format!("sqlite:{url}?mode=rwc")
            }
        };

        let pool = PoolOptions::new()
            .max_connections(200)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(&url).await?;

        Self::initialize(&pool).await?;

        Ok(Self {
            db: pool,
            _temp_dir: None,
            _item_type: Default::default()
        })
    }

    /// Create a file backed set in a temporary directory
    pub async fn new_temp() -> Result<Self> {
        let tempdir = tempfile::tempdir()?;
        let path = tempdir.path().join("set.db");

        let mut obj = Self::new(path.to_str().unwrap()).await?;
        obj._temp_dir = Some(tempdir);

        Ok(obj)
    }

    /// Setup the database
    async fn initialize(pool: &SqlitePool) -> Result<()> {
        let mut con = pool.acquire().await?;

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut *con).await?;

        sqlx::query("create table if not exists dataset (
            data BLOB NOT NULL PRIMARY KEY
        )").execute(&mut *con).await.context("error creating table for set")?;

        return Ok(())
    }

    /// Insert a batch objects
    pub async fn insert_batch(&self, items: &[Item]) -> Result<()> {
        let mut trans = self.db.begin().await?;

        for item in items {
            // Insert file entry
            let result = sqlx::query("INSERT INTO dataset(data) VALUES(?)")
            .bind(postcard::to_allocvec(&item)?)
            .execute(&mut *trans).await;
            match result {
                Ok(_) => continue,
                Err(err) => {
                    let constraint_failed = err.to_string().contains("UNIQUE constraint failed")
                        || err.to_string().contains("1062 (23000)");

                    if constraint_failed {
                        continue
                    }
                    return Err(err.into())
                },
            };
        }

        trans.commit().await?;
        return Ok(())
    }

    /// Pop up to limit items from the set
    pub async fn pop_batch(&self, limit: u32) -> Result<Vec<Item>> {
        let query_str = format!("DELETE FROM dataset WHERE data IN (SELECT data FROM dataset LIMIT {limit}) RETURNING data");
        let data: Vec<(Vec<u8>, )> = sqlx::query_as(&query_str).fetch_all(&self.db).await?;
        let mut output = vec![];
        for (data, ) in data {
            output.push(postcard::from_bytes(&data)?);
        }
        return Ok(output);
    }

    /// Get how many items are in the set
    pub async fn len(&self) -> Result<u64> {
        let query_str = "SELECT count(1) FROM dataset";
        let (hits, ): (i64, ) = sqlx::query_as(query_str).fetch_one(&self.db).await?;
        return Ok(hits as u64)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::SqliteSet;
    use anyhow::Result;

    #[tokio::test]
    async fn deduplicate() -> Result<()> {
        let collection = SqliteSet::<i64>::new_temp().await?;

        collection.insert_batch(&[10, 11, 10]).await?;
        collection.insert_batch(&[11]).await?;
        collection.insert_batch(&[10]).await?;
        collection.insert_batch(&[10, 11]).await?;
        collection.insert_batch(&[11, 11]).await?;


        let mut aa = collection.pop_batch(10).await?;
        let mut bb = collection.pop_batch(10).await?;
        aa.sort();
        bb.sort();

        assert_eq!(aa, vec![10, 11]);
        assert!(bb.is_empty());

        collection.insert_batch(&[10]).await?;
        collection.insert_batch(&[12, 11]).await?;

        let mut values = collection.pop_batch(2).await?;
        assert_eq!(values.len(), 2);
        values.extend(collection.pop_batch(2).await?);
        assert_eq!(HashSet::<i64>::from_iter(values), HashSet::from_iter([10, 11, 12]));
        assert!(collection.pop_batch(10).await?.is_empty());

        return Ok(())
    }
}