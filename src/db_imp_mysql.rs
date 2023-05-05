use anyhow::{Result, Context};
use async_trait::async_trait;
use itertools::Itertools;
use sqlx::Connection;
use crate::database::IndexGroup;
use crate::database_generic_sql::{ImplDetails, PoolCon, file_table_name, filter_table_name};

#[derive(Default)]
pub struct MySQLImp {}


#[async_trait]
impl ImplDetails for MySQLImp {
    async fn initialize(&self, con: &mut PoolCon) -> Result<()> {

        // sqlx::query(&format!("drop table yara_tasks")).execute(&mut *con).await?;
        // sqlx::query(&format!("drop table searches")).execute(&mut *con).await?;
        // sqlx::query("SET binlog_expire_logs_seconds=86400;").execute(&mut *con).await?;

        sqlx::query(&format!("create table if not exists searches (
            code VARCHAR(64) NOT NULL PRIMARY KEY,
            stage VARCHAR(16) NOT NULL,
            data LONGBLOB NOT NULL,
            search_group VARCHAR(128),
            start_time VARCHAR(64),
            INDEX(search_group, start_time),
            INDEX(stage)
        )")).execute(&mut *con).await.context("error creating table searches")?;

        sqlx::query(&format!("create table if not exists yara_tasks (
            id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
            search VARCHAR(64) NOT NULL,
            hashes LONGBLOB NOT NULL,
            hash_count INTEGER NOT NULL,
            assigned_worker VARCHAR(128),
            assigned_time VARCHAR(64),
            FOREIGN KEY(search) REFERENCES searches(code),
            INDEX(assigned_worker),
            INDEX(search)
        )
        ")).execute(&mut *con).await.context("Error creating table yara_tasks")?;

        // sqlx::query(&format!("create table if not exists garbage (
        //     blob_id TEXT PRIMARY KEY,
        //     time TEXT NOT NULL
        // )")).execute(&mut *con).await.context("error creating table garbage")?;
        // sqlx::query(&format!("CREATE INDEX IF NOT EXISTS garbage_blob_id ON garbage(time)")).execute(&mut *con).await?;

        return Ok(())
    }

    async fn list_tables(&self, conn: &mut PoolCon) -> Result<Vec<String>> {
        let tables: Vec<(String, )> = sqlx::query_as("SHOW TABLES")
            .fetch_all(&mut *conn).await?;
        let tables = tables.into_iter().map(|row|row.0).collect_vec();
        return Ok(tables)
    }

    async fn initialize_filter(&self, conn: &mut PoolCon, index: &IndexGroup) -> Result<()> {
        let mut trans = conn.begin().await?;

        let file_table = file_table_name(index);
        let filter_table = filter_table_name(index);

        sqlx::query(&format!("create table if not exists {filter_table} (
            id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
            leaves BOOLEAN,
            block INTEGER,
            filter LONGBLOB NOT NULL,
            kind VARCHAR(32) NOT NULL,
            FOREIGN KEY(block) REFERENCES {filter_table}(id),
            INDEX(kind)
        )")).execute(&mut trans).await.context("Create filter table")?;

        sqlx::query(&format!("create table if not exists {file_table} (
            hash BINARY(32) NOT NULL PRIMARY KEY,
            access MEDIUMTEXT NOT NULL,
            block INTEGER NOT NULL,
            filter LONGBLOB NOT NULL,
            kind VARCHAR(32) NOT NULL,
            FOREIGN KEY(block) REFERENCES {filter_table}(id),
            INDEX(kind)
        )")).execute(&mut trans).await.context("Create file table")?;

        trans.commit().await?;
        return Ok(())
    }

}