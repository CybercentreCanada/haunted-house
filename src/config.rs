use std::collections::HashMap;

use serde::Deserialize;


#[derive(Deserialize)]
enum BlobStorageConfig {
    Directory(String),
}

#[derive(Deserialize)]
enum ScrapeConfig {
    Assemblyline {
        url: String
    }
}


#[derive(Deserialize)]
struct Config {
    file_storage: BlobStorageConfig,
    file_scraper: ScrapeConfig,
    index_storage: BlobStorageConfig,
    data_path: String,
    access_control_patterns: Option<HashMap<String, Vec<String>>>
}