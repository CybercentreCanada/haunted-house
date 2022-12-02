// pub use crate::sqlite_kvd::{SKV, Collection};

use std::marker::PhantomData;

use log::info;
use serde::de::DeserializeOwned;
use serde::{Serialize, Deserialize};
use sqlx::{Sqlite, SqlitePool};
use sqlx::pool::{PoolConnection, PoolOptions};

use anyhow::Result;

pub struct SKV {
    connection: SqlitePool,
}

pub struct Collection<Type: Serialize + DeserializeOwned> {
    con: PoolConnection<Sqlite>,
    name: String,
    _kind: PhantomData<Type>,
}

impl SKV {

    pub async fn open(path: &str) -> Result<SKV> {
        let url = format!("sqlite:{path}");
        info!("Loading SKV at {url}");

        let pool = PoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect_lazy(&url)?;
        info!("SQLite connected.");
        pool.acquire().await?;
        info!("SQLite ready.");

        // let pool = SqlitePool::connect_with(options).await?;
        Ok(SKV {
            connection: pool
        })
    }

    pub async fn create_collection<Type: Serialize + DeserializeOwned>(&self, name: &str) -> Result<Collection<Type>> {
        let mut con = self.connection.acquire().await?;
        sqlx::query(&format!("create table if not exists {name} (
            key TEXT PRIMARY KEY,
            value BLOB NOT NULL
        )")).execute(&mut con).await?;
        Ok(Collection { con, name: name.to_owned(), _kind: Default::default() })
    }

    pub async fn open_collection<Type: Serialize + DeserializeOwned>(&self, name: &str) -> Result<Option<Collection<Type>>> {
        let mut con = self.connection.acquire().await?;
        let rows = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name=?;").bind(name).fetch_all(&mut con).await?;
        if rows.len() == 0 {
            Ok(None)
        } else {
            Ok(Some(Collection{ con, name: name.to_owned(), _kind: Default::default() }))
        }
    }

    pub async fn remove_collection(&self, name: &str) -> Result<()> {
        let mut con = self.connection.acquire().await?;
        sqlx::query(&format!("drop table if exists {name}")).execute(&mut con).await?;
        Ok(())
    }
}

impl<Type: Serialize + DeserializeOwned> Collection<Type> {

    fn key(key: &[u8]) -> String {
        hex::encode(key)
    }

    fn decode(input: Vec<(String, Vec<u8>)>) -> Result<Vec<(Vec<u8>, Type)>> {
        let mut out = vec![];
        for (key, value) in input {
            out.push((hex::decode(key)?, postcard::from_bytes(&value)?))
        }
        return Ok(out)
    }

    pub async fn list_all(&mut self) -> Result<Vec<(Vec<u8>, Type)>> {
        let result: Vec<(String, Vec<u8>)> = sqlx::query_as(&format!("SELECT key, value FROM {}", self.name))
            .fetch_all(&mut self.con).await?;
        return Ok(Self::decode(result)?)
    }

    pub async fn list_inclusive_prefix_range(&mut self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Type)>> {
        let result: Vec<(String, Vec<u8>)> = sqlx::query_as(&format!("SELECT key, value FROM {} where key >= ? AND key <= ?", self.name))
        .bind(Self::key(start))
        .bind(Self::key(end) + "\x7F")
        .fetch_all(&mut self.con).await?;
        return Ok(Self::decode(result)?)
    }

    pub async fn list_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Type)>> {
        let result: Vec<(String, Vec<u8>)> = sqlx::query_as(&format!("SELECT key, value FROM {} where key GLOB ?", self.name))
        .bind(Self::key(prefix) + "*")
        .fetch_all(&mut self.con).await?;
        return Ok(Self::decode(result)?)
    }

    pub async fn set(&mut self, key: &[u8], value: &Type) -> Result<()> {
        sqlx::query(&format!("INSERT INTO {}(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=?;", self.name))
            .bind(Self::key(key))
            .bind(&postcard::to_allocvec(value)?)
            .bind(&postcard::to_allocvec(value)?)
            .execute(&mut self.con).await?;
        return Ok(())
    }

    pub async fn set_odd<OddType: Serialize>(&mut self, key: &[u8], value: &OddType) -> Result<()> {
        sqlx::query(&format!("INSERT INTO {}(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=?;", self.name))
            .bind(Self::key(key))
            .bind(&postcard::to_allocvec(value)?)
            .bind(&postcard::to_allocvec(value)?)
            .execute(&mut self.con).await?;
        return Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Type>> {
        let mut result: Vec<(Vec<u8>,)> = sqlx::query_as(&format!("SELECT value FROM {} WHERE key = ? LIMIT 1", self.name))
            .bind(Self::key(key))
            .fetch_all(&mut self.con).await?;
        if let Some(row) = result.pop() {
            Ok(Some(postcard::from_bytes(&row.0)?))
        } else {
            Ok(None)
        }
    }

    pub async fn get_odd<OddType: DeserializeOwned>(&mut self, key: &[u8]) -> Result<Option<OddType>> {
        let mut result: Vec<(Vec<u8>,)> = sqlx::query_as(&format!("SELECT value FROM {} WHERE key = ? LIMIT 1", self.name))
            .bind(Self::key(key))
            .fetch_all(&mut self.con).await?;
        if let Some(row) = result.pop() {
            Ok(Some(postcard::from_bytes(&row.0)?))
        } else {
            Ok(None)
        }
    }

    pub async fn remove(&mut self, key: &[u8]) -> Result<bool> {
        let res = sqlx::query(&format!("DELETE FROM {} WHERE key = ?", self.name))
            .bind(Self::key(key))
            .execute(&mut self.con).await?;
        return Ok(res.rows_affected() > 0)
    }

    pub async fn cas(&mut self, key: &[u8], old: &Option<Type>, value: &Type) -> Result<bool> {
        match old {
            Some(old) => {
                let result = sqlx::query(&format!("UPDATE {} SET value = ? WHERE key = ? AND value = ?", self.name))
                    .bind(postcard::to_allocvec(value)?)
                    .bind(Self::key(key))
                    .bind(postcard::to_allocvec(old)?)
                    .execute(&mut self.con).await?;
                return Ok(result.rows_affected() > 0)
            },
            None => {
                let result = sqlx::query(&format!("INSERT INTO {}(key,value) VALUES(?,?)", self.name))
                    .bind(Self::key(key))
                    .bind(postcard::to_allocvec(value)?)
                    .execute(&mut self.con).await?;
                return Ok(result.rows_affected() > 0)
            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::SKV;
    use anyhow::{Result, Context};
    use assertor::{assert_that, VecAssertion};

    #[tokio::test]
    async fn set_get() -> Result<()> {
        let store = SKV::open(":memory:").await?;

        let mut a = store.create_collection::<String>("a").await.context("create_collection")?;
        let mut b = store.create_collection::<String>("b").await?;

        let v123 = "123".to_owned();
        let v456 = "456".to_owned();
        let v789 = "789".to_owned();

        a.set(b"abc", &v123).await?;
        a.set(b"abc1", &v456).await?;
        a.set(b"abc2", &v789).await?;

        assert_eq!(a.get(b"abc").await?.unwrap(), v123);
        assert_eq!(a.get(b"abc1").await?.unwrap(), v456);
        assert_eq!(a.get(b"abc2").await?.unwrap(), v789);
        assert_eq!(a.get(b"abc3").await?, None);

        assert!(b.list_all().await?.is_empty());
        assert_that!(a.list_all().await?).contains_exactly(vec![
            (b"abc".to_vec(), v123),
            (b"abc1".to_vec(), v456),
            (b"abc2".to_vec(), v789)
        ]);

        let v000 = "000".to_owned();
        let vdash = "---".to_owned();

        assert!(a.cas(b"abc", &Some("123".to_string()), &v000).await?);
        assert!(!a.cas(b"abc", &Some("0000".to_string()), &vdash).await?);
        assert_that!(a.list_all().await?).contains_exactly(vec![
            (b"abc".to_vec(), v000),
            (b"abc1".to_vec(), v456),
            (b"abc2".to_vec(), v789)
        ]);

        assert_that!(a.list_inclusive_prefix_range(b"abc", b"abc").await?).contains_exactly(vec![(b"abc".to_vec(), v000)]);
        assert_that!(a.list_inclusive_prefix_range(b"abc1", b"abc1").await?).contains_exactly(vec![(b"abc1".to_vec(), v456)]);
        assert_that!(a.list_inclusive_prefix_range(b"abc2", b"abc2").await?).contains_exactly(vec![(b"abc2".to_vec(), v789)]);
        assert_that!(a.list_inclusive_prefix_range(b"abc3", b"abc9").await?).contains_exactly(vec![]);

        assert_that!(a.list_inclusive_prefix_range(b"abc1", b"abc2").await?).contains_exactly(vec![
            (b"abc1".to_vec(), v456),
            (b"abc2".to_vec(), v789)
        ]);

        a.set(b"abb-10", &vdash).await?;
        a.set(b"abd-10", &vdash).await?;

        assert_that!(a.list_inclusive_prefix_range(b"abb", b"abd").await?).contains_exactly(vec![
            (b"abb-10".to_vec(), vdash),
            (b"abd-10".to_vec(), vdash)
        ]);

        return Ok(())
    }
}