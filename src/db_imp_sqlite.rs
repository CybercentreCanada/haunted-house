use std::path::Path;

use anyhow::{Result, Context};
use async_trait::async_trait;
use itertools::Itertools;
use sqlx::Connection;
use crate::database::IndexGroup;
use crate::database_generic_sql::{ImplDetails, PoolCon, file_table_name, filter_table_name};

#[derive(Default)]
pub struct SqliteImp {}


#[async_trait]
impl ImplDetails for SqliteImp {
    async fn initialize(&self, con: &mut PoolCon) -> Result<()> {

        sqlx::query("PRAGMA journal_mode=WAL").execute(&mut *con).await?;
        sqlx::query("PRAGMA foreign_keys=ON").execute(&mut *con).await?;
        sqlx::query("PRAGMA busy_timeout=120000").execute(&mut *con).await?;

        sqlx::query(&format!("create table if not exists searches (
            code TEXT PRIMARY KEY,
            stage TEXT NOT NULL,
            data BLOB NOT NULL,
            search_group TEXT,
            start_time TEXT
        )")).execute(&mut *con).await.context("error creating table searches")?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS searches_group_start ON searches(search_group, start_time)")).execute(&mut *con).await?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS searches_stage ON searches(stage)")).execute(&mut *con).await?;

        sqlx::query(&format!("create table if not exists yara_tasks (
            id INTEGER PRIMARY KEY,
            search TEXT NOT NULL,
            hashes BLOB NOT NULL,
            hash_count INTEGER NOT NULL,
            assigned_worker TEXT,
            assigned_time TEXT,
            FOREIGN KEY(search) REFERENCES searches(code)
        )
        ")).execute(&mut *con).await.context("Error creating table yara_tasks")?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS yara_assigned_work_index ON yara_tasks(assigned_worker)")).execute(&mut *con).await?;
        sqlx::query(&format!("CREATE INDEX IF NOT EXISTS yara_search_index ON yara_tasks(search)")).execute(&mut *con).await?;

        // sqlx::query(&format!("create table if not exists garbage (
        //     blob_id TEXT PRIMARY KEY,
        //     time TEXT NOT NULL
        // )")).execute(&mut *con).await.context("error creating table garbage")?;
        // sqlx::query(&format!("CREATE INDEX IF NOT EXISTS garbage_blob_id ON garbage(time)")).execute(&mut *con).await?;

        return Ok(())
    }

    async fn list_tables(&self, conn: &mut PoolCon) -> Result<Vec<String>> {
        let tables: Vec<(String, )> = sqlx::query_as("SELECT name FROM sqlite_schema WHERE type='table'")
            .fetch_all(&mut *conn).await?;
        let tables = tables.into_iter().map(|row|row.0).collect_vec();
        return Ok(tables)
    }

    async fn initialize_filter(&self, conn: &mut PoolCon, index: &IndexGroup) -> Result<()> {
        let mut trans = conn.begin().await?;

        let file_table = file_table_name(index);
        let filter_table = filter_table_name(index);

        sqlx::query(&format!("create table if not exists {filter_table} (
            id INTEGER PRIMARY KEY,
            leaves BOOLEAN,
            block INTEGER,
            filter BLOB NOT NULL,
            kind TEXT NOT NULL,
            FOREIGN KEY(block) REFERENCES {filter_table}(id)
        )")).execute(&mut trans).await.context("Create filter table")?;

        sqlx::query(&format!("create table if not exists {file_table} (
            hash BLOB PRIMARY KEY,
            access BLOB NOT NULL,
            block INTEGER NOT NULL,
            filter BLOB NOT NULL,
            kind TEXT NOT NULL,
            FOREIGN KEY(block) REFERENCES {filter_table}(id)
        )")).execute(&mut trans).await.context("Create file table")?;

        trans.commit().await?;
        return Ok(())
    }

}