use std::marker::PhantomData;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::{Serialize};
use sqlx::SqlitePool;
use anyhow::{Result, Context};
use sqlx::pool::PoolOptions;


pub struct SqliteSet<Item: Serialize + DeserializeOwned> {
    db: SqlitePool,
    _temp_dir: Option<tempfile::TempDir>,
    _item_type: PhantomData<Item>
}


impl<Item: Serialize + DeserializeOwned> SqliteSet<Item> {
    pub async fn new(url: &str) -> Result<Self> {
        let url = if url == "memory" {
            format!("sqlite::memory:")
        } else {
            let path = Path::new(url);

            if let Some(parent) = path.parent() {
                if parent != Path::new("") && parent != Path::new("/") && !parent.exists() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }

            if path.is_absolute() {
                format!("sqlite://{}?mode=rwc", url)
            } else {
                format!("sqlite:{}?mode=rwc", url)
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

    pub async fn new_temp() -> Result<Self> {
        let tempdir = tempfile::tempdir()?;
        let path = tempdir.path().join("set.db");

        let mut obj = Self::new(path.to_str().unwrap()).await?;
        obj._temp_dir = Some(tempdir);

        Ok(obj)
    }

    async fn initialize(pool: &SqlitePool) -> Result<()> {
        let mut con = pool.acquire().await?;

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut con).await?;

        sqlx::query(&format!("create table if not exists dataset (
            data BLOB NOT NULL PRIMARY KEY,
        )")).execute(&mut con).await.context("error creating table for set")?;

        return Ok(())
    }

    pub async fn insert(&self, item: &Item) -> Result<bool> {
        // Insert file entry
        let result = sqlx::query(&format!("INSERT INTO dataset(data) VALUES(?)"))
        .bind(postcard::to_allocvec(&item)?)
        .execute(&self.db).await;
        match result {
            Ok(_) => return Ok(true),
            Err(err) => {
                let constraint_failed = err.to_string().contains("UNIQUE constraint failed")
                    || err.to_string().contains("1062 (23000)");

                if constraint_failed {
                    return Ok(false)
                }
                return Err(err.into())
            },
        };
    }

    pub async fn insert_batch(&self, items: &Vec<Item>) -> Result<()> {
        let mut trans = self.db.begin().await?;

        for item in items {
            // Insert file entry
            let result = sqlx::query(&format!("INSERT INTO dataset(data) VALUES(?)"))
            .bind(postcard::to_allocvec(&item)?)
            .execute(&mut trans).await;
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

    pub async fn pop_batch(&self, limit: u32) -> Result<Vec<Item>> {
        let data: Vec<(Vec<u8>, )> = sqlx::query_as(&format!("DELETE FROM dataset LIMIT {limit} RETURNING data"))
            .fetch_all(&self.db).await?;
        let mut output = vec![];
        for (data, ) in data {
            output.push(postcard::from_bytes(&data)?);
        }
        return Ok(output);
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

        collection.insert(&10).await?;
        collection.insert(&11).await?;
        collection.insert(&10).await?;
        collection.insert(&10).await?;
        collection.insert(&11).await?;

        assert_eq!(HashSet::<i64>::from_iter(collection.pop_batch(10).await?.into_iter()), HashSet::from_iter([10, 11]));
        assert!(collection.pop_batch(10).await?.is_empty());

        collection.insert(&10);
        collection.insert(&11);
        collection.insert(&12);

        let mut values = collection.pop_batch(2).await?;
        assert_eq!(values.len(), 2);
        values.extend(collection.pop_batch(2).await?);
        assert_eq!(HashSet::<i64>::from_iter(values), HashSet::from_iter([10, 11, 12]));
        assert!(collection.pop_batch(10).await?.is_empty());

        return Ok(())
    }
}