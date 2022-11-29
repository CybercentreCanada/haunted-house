use sqlx::{Sqlite, SqlitePool};
use sqlx::pool::PoolConnection;

use anyhow::Result;

pub struct SKV {
    connection: SqlitePool,
}

pub struct Collection {
    con: PoolConnection<Sqlite>,
    name: String,
}

impl SKV {

    pub async fn open(path: &str) -> Result<SKV> {
        let pool = SqlitePool::connect(&format!("sqlite:{path}")).await?;
        Ok(SKV {
            connection: pool
        })
    }

    pub async fn create_collection(&self, name: &str) -> Result<Collection> {
        let mut con = self.connection.acquire().await?;
        sqlx::query(&format!("create table if not exists {name} (
            key BLOB PRIMARY KEY,
            value BLOB NOT NULL
        )")).execute(&mut con).await?;
        Ok(Collection { con, name: name.to_owned() })
    }

    pub async fn open_collection(&self, name: &str) -> Result<Option<Collection>> {
        let mut con = self.connection.acquire().await?;
        let rows = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name=?;").bind(name).fetch_all(&mut con).await?;
        if rows.len() == 0 {
            Ok(None)
        } else {
            Ok(Some(Collection { con, name: name.to_owned() }))
        }
    }

    pub async fn remove_collection(&self, name: &str) -> Result<()> {
        let mut con = self.connection.acquire().await?;
        sqlx::query(&format!("drop table if exists {name}")).execute(&mut con).await?;
        Ok(())
    }
}

impl Collection {

    pub async fn list_all(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let result: Vec<(Vec<u8>, Vec<u8>)> = sqlx::query_as(&format!("SELECT key, value FROM {}", self.name))
            .fetch_all(&mut self.con).await?;
        return Ok(result)
    }

    pub async fn list_inclusive_range(&mut self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let result: Vec<(Vec<u8>, Vec<u8>)> = sqlx::query_as(&format!("SELECT key, value FROM {} where key >= ? AND key <= ?", self.name))
            .bind(start)
            .bind(end)
            .fetch_all(&mut self.con).await?;
        return Ok(result)
    }

    pub async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        sqlx::query(&format!("INSERT INTO {}(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=?;", self.name))
            .bind(key)
            .bind(value)
            .bind(value)
            .execute(&mut self.con).await?;
        return Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut result: Vec<(Vec<u8>,)> = sqlx::query_as(&format!("SELECT value FROM {} WHERE key = ? LIMIT 1", self.name))
            .bind(key)
            .fetch_all(&mut self.con).await?;
        if let Some(row) = result.pop() {
            Ok(Some(row.0))
        } else {
            Ok(None)
        }
    }

    pub async fn remove(&mut self, key: &[u8]) -> Result<()> {
        sqlx::query(&format!("DELETE FROM {} WHERE key = ?", self.name))
            .bind(key)
            .execute(&mut self.con).await?;
        return Ok(())
    }

    pub async fn cas(&mut self, key: &[u8], old: Option<&[u8]>, value: &[u8]) -> Result<bool> {
        match old {
            Some(old) => {
                let result = sqlx::query(&format!("UPDATE {} SET value = ? WHERE key = ? AND value = ?", self.name))
                    .bind(value)
                    .bind(key)
                    .bind(old)
                    .execute(&mut self.con).await?;
                return Ok(result.rows_affected() > 0)
            },
            None => {
                let result = sqlx::query(&format!("INSERT INTO {}(key,value) VALUES(?,?)", self.name))
                    .bind(key)
                    .bind(value)
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

        let mut a = store.create_collection("a").await.context("create_collection")?;
        let mut b = store.create_collection("b").await?;

        a.set(b"abc", b"123").await?;
        a.set(b"abc1", b"456").await?;
        a.set(b"abc2", b"789").await?;

        assert_eq!(a.get(b"abc").await?.unwrap(), b"123");
        assert_eq!(a.get(b"abc1").await?.unwrap(), b"456");
        assert_eq!(a.get(b"abc2").await?.unwrap(), b"789");
        assert_eq!(a.get(b"abc3").await?, None);

        assert!(b.list_all().await?.is_empty());
        assert_that!(a.list_all().await?).contains_exactly(vec![
            (b"abc".to_vec(), b"123".to_vec()),
            (b"abc1".to_vec(), b"456".to_vec()),
            (b"abc2".to_vec(), b"789".to_vec())
        ]);

        assert!(a.cas(b"abc", Some(b"123"), b"000").await?);
        assert!(!a.cas(b"abc", Some(b"0000"), b"---").await?);
        assert_that!(a.list_all().await?).contains_exactly(vec![
            (b"abc".to_vec(), b"000".to_vec()),
            (b"abc1".to_vec(), b"456".to_vec()),
            (b"abc2".to_vec(), b"789".to_vec())
        ]);

        assert_that!(a.list_inclusive_range(b"abc", b"abc").await?).contains_exactly(vec![(b"abc".to_vec(), b"000".to_vec())]);
        assert_that!(a.list_inclusive_range(b"abc1", b"abc1").await?).contains_exactly(vec![(b"abc1".to_vec(), b"456".to_vec())]);
        assert_that!(a.list_inclusive_range(b"abc2", b"abc2").await?).contains_exactly(vec![(b"abc2".to_vec(), b"789".to_vec())]);
        assert_that!(a.list_inclusive_range(b"abc3", b"abc9").await?).contains_exactly(vec![]);

        assert_that!(a.list_inclusive_range(b"abc1", b"abc2").await?).contains_exactly(vec![
            (b"abc1".to_vec(), b"456".to_vec()),
            (b"abc2".to_vec(), b"789".to_vec())
        ]);

        return Ok(())
    }
}