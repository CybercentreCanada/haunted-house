use std::sync::Arc;

use log::error;
use anyhow::Result;

use crate::config::{IngestSource, ElasticsearchIngestSource};

use super::HouseCore;



async fn run_ingest(core: Arc<HouseCore>, config: IngestSource) {
    match &config {
        IngestSource::Elasticsearch(config) => {
            while let Err(err) = run_elasticsearch_ingest(core.clone(), config).await {
                error!("Ingester error: {err}");
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await
            }
        }
    }
}


async fn run_elasticsearch_ingest(core: Arc<HouseCore>, config: &ElasticsearchIngestSource) -> Result<()> {
    todo!()
}