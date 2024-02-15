use std::{collections::HashMap, marker::PhantomData, time::Duration};

use http::{Method, StatusCode};
use itertools::Itertools;
use log::{error, debug, warn};
use serde::{Deserialize, de::DeserializeOwned, Serialize};

// use anyhow::Result;
use serde_json::json;
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;
use chrono::{DateTime, Utc};

use crate::{error::Context, types::{FileInfo, JsonMap}};
use assemblyline_models::{datastore::{self as models, retrohunt::IndexCatagory, RetrohuntHit}, meta::default_settings, ElasticMeta, ExpandingClassification, ModelError};

use super::fetcher::FetchedFile;

const MAX_DELAY: Duration = Duration::from_secs(60);
const PIT_KEEP_ALIVE: &str = "5m";

#[derive(Clone)]
pub struct Datastore {
    connection: Elastic,    
    pub file: Collection<models::File>,
    pub retrohunt: Collection<models::Retrohunt>,
    pub retrohunt_hit: Collection<RetrohuntHit>,
}

impl Datastore {
    pub fn new(host: &str, ca_cert: Option<&str>, connect_unsafe: bool, archive_access: bool) -> Result<Self> {
        let connection = Elastic::new(host, ca_cert, connect_unsafe, archive_access)?;
        Ok(Self{
            file: Collection::new(Index::File, connection.clone()),
            retrohunt: Collection::new(Index::Retrohunt, connection.clone()),
            retrohunt_hit: Collection::new(Index::RetrohuntHit, connection.clone()),
            connection,
        })
    }

    pub async fn list_active_searches(&self) -> Result<Vec<models::Retrohunt>> {
        let mut result = self.retrohunt.search::<()>( "finished: false")
            .full_source(true)
            .scan().await?;

        let mut out = vec![];
        while let Some(search) = result.next().await? {
            out.push(search._source.ok_or(ElasticError::MalformedResponse)?);
        }
        Ok(out)
    }

    pub (crate) async fn fetch_files(&self, seek_point: chrono::DateTime<chrono::Utc>, batch_size: usize) -> Result<Vec<FetchedFile>> {
        #[derive(Debug, Deserialize, Default)]
        struct Fields {
            classification: Vec<String>,
            expiry_ts: Vec<DateTime<Utc>>,
            sha256: Vec<String>,
            #[serde(alias="seen.last")]
            seen: Vec<DateTime<Utc>>,
        }

        let result = self.file.search::<Fields>(&format!("seen.last: [{} TO *]", seek_point.to_rfc3339()))
            .size(batch_size)
            .full_source(false)
            .sort(json!({"seen.last": "asc"}))
            .fields(vec!["classification", "expiry_ts", "sha256", "seen.last"])
            .execute().await?;

        // read the body of our response
        let mut out = vec![];
        for mut row in result.hits.hits {
            out.push(FetchedFile { 
                seen: row.fields.seen.pop().ok_or(ElasticError::MalformedResponse)?, 
                sha256: row.fields.sha256.pop().ok_or(ElasticError::MalformedResponse)?, 
                classification: row.fields.classification.pop().ok_or(ElasticError::MalformedResponse)?, 
                expiry: row.fields.expiry_ts.pop(),
            })
        }
        return Ok(out)
    }

    pub async fn fatal_error(&self, data: &mut models::Retrohunt, error: String) -> Result<()> {
        data.errors.push(error);
        data.finished = true;
        data.completed_time = Some(chrono::Utc::now());
        self.retrohunt.save(&data.key, data, None).await?;
        Ok(())
    }

    pub async fn finalize_search(&self, data: &mut models::Retrohunt) -> Result<()> {
        data.finished = true;
        data.completed_time = Some(chrono::Utc::now());
        self.retrohunt.save(&data.key, data, None).await?;
        Ok(())
    }

    pub async fn count_retrohunt_hits(&self, search: &str, limit: u64) -> Result<u64> {
        let search = self.retrohunt_hit.search::<()>(&format!("search: {search}"))
            .size(0)
            .track_total_hits(limit)
            .execute().await?;
        Ok(search.hits.total.value)
    }
    
    pub async fn save_hits(&self, search: &str, hits: Vec<FileInfo>) -> Result<()> {
        let index = &self.retrohunt_hit.get_index_list(Some(IndexCatagory::Hot)).unwrap()[0];
        let ce = assemblyline_markings::get_default().unwrap();

        // build a bulk body
        let mut body = String::new();
        let mut fileinfo = HashMap::new();
        for info in hits {
            let key = format!("{search}_{}", info.hash);
            body += &serde_json::to_string(&json!({"create": {"_index": index, "_id": key, "require_alias": true}}))?;
            body += "\n";
            body += &serde_json::to_string(&RetrohuntHit{ 
                key: key.clone(), 
                classification: ExpandingClassification::new(info.access_string.clone())?,
                sha256: info.hash.to_string().parse()?, 
                expiry_ts: info.expiry.to_timestamp(), 
                search: search.to_owned() 
            })?;
            body += "\n";
            fileinfo.insert(key, info);
        }
        body += "\n";

        let mut bulk_url = self.connection.host.join("_bulk")?;
        bulk_url.query_pairs_mut().append_pair("refresh", "wait_for");
        'bulk_loop: loop {
            // execute the bulk body
            let response = self.connection.make_request_data(&mut 0, Method::POST, &bulk_url, body.as_bytes()).await?;
            let bulk_response: BulkResponse = response.json().await?;

            // pull out failed calls
            let mut missing_ids = vec![];
            for result in bulk_response.items {
                let result_data = result.into_data();
                if result_data.is_success() { continue }
                if let Some(error) = result_data.error {
                    if error.type_ == "index_not_found_exception" {
                        self.retrohunt_hit.ensure_collection().await?;
                        continue 'bulk_loop;
                    }
                }
                missing_ids.push(result_data._id);
            }

            // everything went well
            if missing_ids.is_empty() {
                return Ok(())
            }

            // batch fetch the existing document for those operations that failed
            let documents = self.retrohunt_hit.multiget(&missing_ids.iter().map(String::as_str).collect_vec()).await.context("multiget")?;

            // prepare a batch of update operations
            body.clear();
            for (_, document) in documents {
                if let Some(source) = document._source {
                    if let Some(info) = fileinfo.get(&document._id) {
                        body += &serde_json::to_string(&json!({"index": {
                            "_index": index, 
                            "_id": document._id, 
                            "require_alias": true, 
                            "if_seq_no": document._seq_no, 
                            "if_primary_term": document._primary_term
                        }}))?;
                        body += "\n";
                        body += &serde_json::to_string(&RetrohuntHit{ 
                            classification: ExpandingClassification::new(ce.min_classification(source.classification.as_str(), &info.access_string, false)?)?,
                            expiry_ts: match (source.expiry_ts, info.expiry.to_timestamp()) {
                                (Some(a), Some(b)) => Some(a.max(b)),
                                _ => None,
                            },
                            key: document._id,
                            sha256: source.sha256,
                            search: search.to_owned(), 
                        })?;
                        body += "\n";    
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct Elastic {
    client: reqwest::Client,
    host: reqwest::Url,
    archive_access: bool,
}

pub trait DSType: Serialize + DeserializeOwned + Described<ElasticMeta> {}
impl<T: Serialize + DeserializeOwned + Described<ElasticMeta>> DSType for T {}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, PartialEq, Eq, Debug, Clone, Copy, Hash)]
#[strum(serialize_all = "snake_case")]
pub enum Index {
    File,
    Retrohunt,
    RetrohuntHit,
}

impl Index {
    // /// is this an index that this module knows how to create
    // fn can_create(&self) -> bool {
    //     match self {
    //         Index::File => false,
    //         Index::Retrohunt => true,
    //         Index::RetrohuntHit => true,
    //     }
    // }

    fn archived(&self) -> bool {
        match self {
            Index::File => true,
            Index::Retrohunt => false,
            Index::RetrohuntHit => false,
        }
    }

    fn archive_name(&self) -> Option<String> {
        if self.archived() {
            Some(format!("{self}-ma"))
        } else {
            None
        }
    }

    fn is_archive_index(&self, index: &str) -> bool {
        match self.archive_name() {
            Some(archive_index) => index.starts_with(&archive_index),
            None => false,
        }
    }

    fn replicas(&self, archive: bool) -> Option<u32> {
        let name = self.to_string().to_uppercase();
        let replicas: u32 = match std::env::var(format!("ELASTIC_{name}_REPLICAS")) {
            Ok(var) => var.parse().ok()?,
            Err(_) => match std::env::var("ELASTIC_DEFAULT_REPLICAS") {
                Ok(var) => var.parse().ok()?,
                Err(_) => 0
            },
        };

        if archive {
            match std::env::var(format!("ELASTIC_{name}_ARCHIVE_REPLICAS")) {
                Ok(var) => var.parse().ok(),
                Err(_) => Some(replicas)
            }
        } else {
            Some(replicas)
        }
    }

    fn shards(&self, archive: bool) -> Option<u32> {
        let name = self.to_string().to_uppercase();
        let shards: u32 = match std::env::var(format!("ELASTIC_{name}_SHARDS")) {
            Ok(var) => var.parse().ok()?,
            Err(_) => match std::env::var("ELASTIC_DEFAULT_SHARDS") {
                Ok(var) => var.parse().ok()?,
                Err(_) => 1
            },
        };

        if archive {
            match std::env::var(format!("ELASTIC_{name}_ARCHIVE_SHARDS")) {
                Ok(var) => var.parse().ok(),
                Err(_) => Some(shards)
            }
        } else {
            Some(shards)
        }
    }
}


impl Elastic {

    pub fn new(host: &str, ca_cert: Option<&str>, connect_unsafe: bool, archive_access: bool) -> Result<Self> {
        let url: url::Url = host.parse()?;
        let mut builder = reqwest::Client::builder();

        if let Some(ca_cert) = ca_cert {
            let cert = reqwest::Certificate::from_pem(ca_cert.as_bytes())?;
            builder = builder.add_root_certificate(cert);
        }

        if connect_unsafe {
            builder = builder.danger_accept_invalid_certs(true);
        }

        Ok(Self {
            client: builder.build()?,
            host: url,
            archive_access,
        })
    }

    // fn get_joined_index(&self, index: Index, catagory: Option<IndexCatagory>) -> Result<String> {
    //     Ok(self.get_index_list(index, catagory)?.join(","))
    // }

    fn get_index_list(&self, index: Index, catagory: Option<IndexCatagory>) -> Result<Vec<String>> {
        match catagory {
            // Default value
            None => {
                // If has an archive: hot + archive
                if let Some(archive_name) = index.archive_name() {
                    if self.archive_access {
                        return Ok(vec![index.to_string(), archive_name])
                    }
                }
                // Otherwise just hot
                return Ok(vec![index.to_string()])
            }

            // If specified index is HOT
            Some(IndexCatagory::Hot) => {
                return Ok(vec![index.to_string()])
            }

            // If only archive asked
            Some(IndexCatagory::Archive) => {
                if let Some(archive_name) = index.archive_name() {
                    // Crash if no archive access
                    if self.archive_access {
                        // Return only archive index
                        return Ok(vec![archive_name])
                    } else {
                        return Err(ElasticError::ArchiveDisabled("Trying to get access to the archive on a datastore where archive_access is disabled"))
                    }
                } else {
                    // Crash if index has no archive
                    return Err(ElasticError::IndexHasNoArchive(index))
                }
            }

            Some(IndexCatagory::HotAndArchive) => {
                if let Some(archive_name) = index.archive_name() {
                    // Crash if no archive access
                    if !self.archive_access {
                        return Err(ElasticError::ArchiveDisabled("Trying to get access to the archive on a datastore where archive_access is disabled"))
                    } else {
                        // Otherwise return hot and archive indices
                        return Ok(vec![index.to_string(), archive_name])
                    }

                } else {
                    // Return HOT if asked for both but only has HOT
                    return Ok(vec![index.to_string()])
                }
            }
        }
    }


    pub async fn does_index_exist(&self, name: &str) -> Result<bool> {
        // self.with_retries(self.datastore.client.indices.exists, index=alias)
        let url = self.host.join(name)?;
        match self.make_request(&mut 0, reqwest::Method::HEAD, &url).await {
            Ok(result) => {
                Ok(result.status() == reqwest::StatusCode::OK)
            },
            Err(ElasticError::HTTPError{code: StatusCode::NOT_FOUND, ..}) => {
                Ok(false)
            },
            Err(err) => {
                Err(err)
            }
        }
    }

    pub async fn does_alias_exist(&self, name: &str) -> Result<bool> {
        // self.with_retries(self.datastore.client.indices.exists_alias, name=alias)
        let url = self.host.join("_alias/")?.join(name)?;
        let result = self.make_request(&mut 0, reqwest::Method::HEAD, &url).await?;
        Ok(result.status() == reqwest::StatusCode::OK)
    }

    pub async fn put_alias(&self, index: &str, name: &str) -> Result<()> {
        // self.with_retries(self.datastore.client.indices.put_alias, index=index, name=alias)
        let url = self.host.join(&format!("{index}/_alias/{name}"))?;
        self.make_request(&mut 0, reqwest::Method::PUT, &url).await?;
        Ok(())
    }

    fn get_index_settings(&self, index: Index, archive: bool) -> serde_json::Value {
        default_settings(json!({
            "number_of_shards": index.shards(archive), // self.shards if not archive else self.archive_shards,
            "number_of_replicas": index.replicas(archive), // self.replicas if not archive else self.archive_replicas    
        }))
    }

    async fn wait_for_status(&self, index: &str, min_status: Option<&str>) -> Result<()> {
        let min_status = min_status.unwrap_or("yellow");
        let mut url = self.host.join("_cluster/health/")?.join(index)?;
        url.query_pairs_mut().append_pair("timeout", "5s").append_pair("wait_for_status", min_status);

        loop {
            match self.client.request(Method::GET, url.clone()).send().await {
                Ok(response) => {
                    if response.status() == reqwest::StatusCode::REQUEST_TIMEOUT {
                        continue
                    } else if response.status() != reqwest::StatusCode::OK {
                        return Err(ElasticError::MalformedResponse)
                    }
                    let response: ElasticStatus = response.json().await?;
                    if !response.timed_out {
                        return Ok(())
                    }
                }
                Err(err) => {
                    if err.is_connect() || err.is_timeout() {
                        continue
                    }
                    return Err(err.into())
                }
            }
        }
    }

    async fn safe_index_copy(&self, copy_method: CopyMethod, src: &str, target: &str, settings: Option<serde_json::Value>, min_status: Option<&str>) -> Result<()> {
        let min_status = min_status.unwrap_or("yellow");
        let mut url = self.host.join(&format!("{src}/{copy_method}/{target}"))?;
        url.query_pairs_mut().append_pair("timeout", "60s");
        let body = settings.map(|value| json!({"settings": value}));
        let response = match body {
            Some(body) => self.make_request_json(&mut 0, Method::POST, &url, &body).await?,
            None => self.make_request(&mut 0, Method::POST, &url).await?
        };

        let ret: ElasticCommandResponse = response.json().await?;

        if !ret.acknowledged {
            return Err(ElasticError::FailedToCreateIndex(src.to_owned(), target.to_owned()))
        }

        self.wait_for_status(target, Some(min_status)).await
    }

    async fn handle_result(attempt: &mut u64, result: reqwest::Result<reqwest::Response>) -> Result<Option<reqwest::Response>> {
        // Handle connection errors with a retry, let other non http errors bubble up
        let response = match result {
            Ok(response) => response,
            Err(err) => {
                // always retry for connect and timeout errors
                if err.is_connect() || err.is_timeout() || err.is_request() {
                    error!("Error connecting to datastore: {err}");
                    let delay = MAX_DELAY.min(Duration::from_secs_f64((*attempt as f64).powf(2.0)/5.0));
                    tokio::time::sleep(delay).await;
                    return Ok(None)
                }

                return Err(err.into())
            },
        };

        // Handle server side http status errors with retry, let other error codes bubble up, decode successful bodies
        let status = response.status();
        
        return if status.is_server_error() {
            let body = response.text().await.unwrap_or(status.to_string());
            error!("Server error in datastore: {body}");
            let delay = MAX_DELAY.min(Duration::from_secs_f64((*attempt as f64).powf(2.0)/5.0));
            tokio::time::sleep(delay).await;
            return Ok(None)                        
        } else if status.is_client_error() {
            let path = response.url().path().to_owned();
            let body = response.text().await.unwrap_or(status.to_string());
            Err(ElasticError::HTTPError{path: Some(path), code: status, message: body})
        } else {
            Ok(Some(response))
        }
    }

    async fn make_request(&self, attempt: &mut u64, method: reqwest::Method, url: &reqwest::Url) -> Result<reqwest::Response> {
        loop {
            *attempt += 1;

            // Build and dispatch the request
            let result = self.client.request(method.clone(), url.clone())
                .send().await;
            
            // Handle connection errors with a retry, let other non http errors bubble up
            match Self::handle_result(attempt, result).await? {
                Some(response) => return Ok(response),
                None => continue,
            }
        }     
    }

    async fn make_request_json<R: Serialize>(&self, attempt: &mut u64, method: reqwest::Method, url: &reqwest::Url, body: &R) -> Result<reqwest::Response> {
        loop {
            *attempt += 1;

            // Build and dispatch the request
            let result = self.client.request(method.clone(), url.clone())
                .json(body)
                .send().await;
            
            // Handle connection errors with a retry, let other non http errors bubble up
            match Self::handle_result(attempt, result).await? {
                Some(response) => return Ok(response),
                None => continue,
            }
        }     
    }

    async fn make_request_data(&self, attempt: &mut u64, method: reqwest::Method, url: &reqwest::Url, body: &[u8]) -> Result<reqwest::Response> {
        // TODO: body can probably be a boxed stream of some sort which will be faster to clone
        loop {
            *attempt += 1;

            // Build and dispatch the request
            let result = self.client.request(method.clone(), url.clone())
                .header("Content-Type", "application/x-ndjson")
                .body(body.to_owned())
                .send().await;
            
            // Handle connection errors with a retry, let other non http errors bubble up
            match Self::handle_result(attempt, result).await? {
                Some(response) => return Ok(response),
                None => continue,
            }
        }     
    }

    /// This function should completely delete the collection
    /// THIS IS FOR TESTING
    #[cfg(test)]
    async fn wipe(&self, index: Index, catagory: IndexCatagory) -> Result<()> {
        for name in self.get_index_list(index, Some(catagory))? {
            let index = format!("{name}_hot");
            self.wipe_index(&index).await?;
        }
        Ok(())
        // if recreate:
        //     self._ensure_collection()
    }
    #[cfg(test)]
    async fn wipe_index(&self, index: &str) -> Result<()> {
        debug!("Wipe operation started for collection: {}", index.to_ascii_uppercase());
        if self.does_index_exist(&index).await.context("does_index_exist")? {
            let url = self.host.join(&index)?;
            if let Err(err) = self.make_request(&mut 0, Method::DELETE, &url).await {
                if let ElasticError::HTTPError{code: StatusCode::NOT_FOUND, ..} =  &err {
                    return Ok(())
                }
                return Err(err)
            }
        }
        Ok(())
    }

}

pub struct Collection<Schema: DSType> {
    index: Index,
    connection: Elastic,
    _type: PhantomData<Schema>,
}

impl<Schema: DSType> Clone for Collection<Schema> {
    fn clone(&self) -> Self {
        Self { index: self.index.clone(), connection: self.connection.clone(), _type: self._type.clone() }
    }
}

#[allow(unused)]
pub enum SaveOperation {
    Create,
    Version(Option<(i64, i64)>)
}

impl From<(i64, i64)> for SaveOperation {
    fn from(value: (i64, i64)) -> Self { Self::Version(Some(value)) }
}

impl From<Option<(i64, i64)>> for SaveOperation {
    fn from(value: Option<(i64, i64)>) -> Self { Self::Version(value) }
}

impl<Schema: DSType> Collection<Schema> {
    fn new(index: Index, connection: Elastic) -> Self {
        Collection { index, connection, _type: Default::default() }
    }

    pub fn search<'a, Fields: DeserializeOwned + Default>(&self, query: &'a str) -> SearchBuilder<'a, Fields, Schema> {
        SearchBuilder::<Fields, Schema>::new(self.clone(), query)
    }
    
    // def save(self, key, data, version=None, index_type=Index.HOT):
    // """
    // Save a to document to the datastore using the key as its document id.
    //
    // The document data will be normalized before being saved in the datastore.
    //
    // :param index_type: Type of indices to target
    // :param key: ID of the document to save
    // :param data: raw data or instance of the model class to save as the document
    // :param version: version of the document to save over, if the version check fails this will raise an exception
    // :return: True if the document was saved properly
    // """
    pub async fn save(&self, key: &str, data: &Schema, version: impl Into<SaveOperation>) -> Result<bool> {
        if key.contains(" ") {
            return Err(ElasticError::BadKey(key.to_owned()))
        }

        // saved_data['id'] = key
        let mut operation = "index";
        let mut parsed_version = None;

        match version.into() {
            SaveOperation::Create => {
                operation = "create";
            },
            SaveOperation::Version(version) => {
                parsed_version = version;
            }
            // } else {
            //     let mut parts = version.split("---");
            //     let seq_no: i64 = parts.next().ok_or(ElasticError::BadDocumentVersion)?.parse().map_err(|_| ElasticError::BadDocumentVersion)?;
            //     let primary_term: i64 = parts.next().ok_or(ElasticError::BadDocumentVersion)?.parse().map_err(|_| ElasticError::BadDocumentVersion)?;
            //     parsed_version = Some((seq_no, primary_term))
            // }
        }

        // serialize the body
        // let body = serde_json::to_string(data)?;

        // Hot indices should always return a single index
        let index_list = self.connection.get_index_list(self.index, Some(IndexCatagory::Hot))?;
        let index = &index_list[0];

        // build the url for the operation type
        let mut url = if operation == "index" {
            self.connection.host.join(&format!("{}/_doc/{key}", index))?
        } else {
            self.connection.host.join(&format!("{}/_create/{key}", index))?
        };

        url.query_pairs_mut()
            .append_pair("op_type", operation)
            .append_pair("require_alias", "true");

        if let Some((seq_no, primary_term)) = parsed_version {
            url.query_pairs_mut()
                .append_pair("if_seq_no", &seq_no.to_string())
                .append_pair("if_primary_term", &primary_term.to_string());
        }

        return match self.make_request_json(Method::PUT, &url, &data).await {
            Ok(_) => Ok(true),
            Err(ElasticError::HTTPError { code: StatusCode::CONFLICT, .. }) => Ok(false),
            Err(err) => Err(err)
        }
    }

    fn is_index_not_found_error(message: &str) -> bool {
        let mut message: JsonMap = match serde_json::from_str(&message) {
            Ok(message) => message,
            _ => return false,
        };

        let error = match message.remove("error") {
            Some(serde_json::Value::Object(error)) => error,
            _ => return false,
        };

        match error.get("type") {
            Some(serde_json::Value::String(value)) => value == "index_not_found_exception",
            _ => false
        }
    }

    async fn refresh(&self) -> Result<()> {
        for index in self.get_index_list(None)? {
            self.make_request(Method::POST, &self.connection.host.join(&format!("{index}/_refresh"))?).await?;
        }
        Ok(())
    }

    async fn make_request(&self, method: reqwest::Method, url: &reqwest::Url) -> Result<reqwest::Response> {
        let mut attempt = 0;
        loop {
            match self.connection.make_request(&mut attempt, method.clone(), url).await {
                Ok(response) => break Ok(response),
                Err(ElasticError::HTTPError { code: StatusCode::NOT_FOUND, message, path }) => {
                    if Self::is_index_not_found_error(&message) {
                        self.ensure_collection().await?;
                        continue    
                    }
                    break Err(ElasticError::HTTPError { path, code: StatusCode::NOT_FOUND, message })
                },
                Err(err) => break Err(err)    
            }
        }     
    }

    async fn make_request_json<R: Serialize>(&self, method: reqwest::Method, url: &reqwest::Url, body: &R) -> Result<reqwest::Response> {
        let mut attempt = 0;
        loop {
            match self.connection.make_request_json(&mut attempt, method.clone(), url, body).await {
                Ok(response) => break Ok(response),
                Err(ElasticError::HTTPError { code: StatusCode::NOT_FOUND, message, path }) => {
                    if Self::is_index_not_found_error(&message) {
                        self.ensure_collection().await?;
                        continue    
                    }
                    break Err(ElasticError::HTTPError { path, code: StatusCode::NOT_FOUND, message })
                },
                Err(err) => break Err(err)    
            }
        }     
    }

    fn get_joined_index(&self, catagory: Option<IndexCatagory>) -> Result<String> {
        Ok(self.get_index_list(catagory)?.join(","))
    }

    pub fn get_index_list(&self, catagory: Option<IndexCatagory>) -> Result<Vec<String>> {
        self.connection.get_index_list(self.index, catagory)
    }

    pub async fn multiget(&self, ids: &[&str]) -> Result<HashMap<String, GetResponse<Schema, ()>>> {
        if ids.is_empty() { return Ok(Default::default()) }

        // where to collect output
        let mut output = HashMap::new();

        // track which documents are outstanding
        let mut outstanding = vec![];

        //
        for index in self.get_index_list(None)? {
            // prepare the url
            let mut url = self.connection.host.join(&format!("{index}/_mget"))?;
            url.query_pairs_mut().append_pair("_source", "true");

            // prepare the request body
            let body = if outstanding.is_empty() {
                json!({ "ids": ids })
            } else {
                json!({ "ids": outstanding })
            };

            // fetch all the documents
            let response = self.make_request_json(Method::GET, &url, &body).await.context("mget request")?;
            let response: MGetResponse<Schema, ()> = response.json().await?;

            // track which ones we have found
            outstanding.clear();
            for resp in response.docs {
                let _id = resp._id.clone();
                if resp._source.is_some() {
                    output.insert(_id, resp);
                } else {
                    outstanding.push(_id)
                }
            }

            // finish if we have found everything we want
            if outstanding.is_empty() {
                break
            }
        }

        Ok(output)
    }

    pub async fn get(&self, id: &str) -> Result<Option<(Schema, (i64, i64))>> {
        //
        for index in self.get_index_list(None)? {
            // prepare the url
            let url = self.connection.host.join(&format!("{index}/_doc/{id}"))?;

            // fetch all the documents
            let response: GetResponse<Schema, ()> = match self.make_request(Method::GET, &url).await {
                Ok(response) => response.json().await?,
                Err(ElasticError::HTTPError { code: StatusCode::NOT_FOUND, .. }) => continue,
                Err(err) => return Err(err)
            };

            // return the document if found
            if let Some(doc) = response._source {
                return Ok(Some((doc, (response._seq_no, response._primary_term))))
            }
        }

        return Ok(None)
    }

    /// This function should test if the collection that you are trying to access does indeed exist
    /// and should create it if it does not.
    pub async fn ensure_collection(&self) -> Result<()> {
        for alias in self.get_index_list(None)? {
            let index = format!("{alias}_hot");

            // Create HOT index
            if !self.connection.does_index_exist(&alias).await.context("does_index_exist")? {
                debug!("Index {} does not exists. Creating it now...", alias.to_uppercase());

                let mut mapping = assemblyline_models::meta::build_mapping::<Schema>()?;
                mapping.apply_defaults();

                let body = json!({
                    "mappings": mapping,
                    "settings": self.connection.get_index_settings(self.index, self.index.is_archive_index(&index))
                });

                if let Err(err) = self.connection.make_request_json(&mut 0, Method::PUT, &self.connection.host.join(&index)?, &body).await {
                    match &err {
                        ElasticError::HTTPError{code: StatusCode::BAD_REQUEST, message, ..} => {
                            if message.contains("resource_already_exists_exception") {
                                warn!("Tried to create an index template that already exists: {}", alias.to_uppercase());    
                            } else {
                                return Err(err.into()).context("put index bad request")
                            }
                        },
                        _ => return Err(err).context("put index other error")
                    };
                };

                self.connection.put_alias(&index, &alias).await.context("put_alias")?;
            } else if !self.connection.does_index_exist(&index).await? && !self.connection.does_alias_exist(&alias).await.context("does_alias_exist")? {
                // Hold a write block for the rest of this section
                // self.with_retries(self.datastore.client.indices.put_settings, index=alias, settings=write_block_settings)
                let settings_url = self.connection.host.join(&format!("{index}/_settings"))?;
                self.connection.make_request_json(&mut 0, Method::PUT, &settings_url, &json!({"index.blocks.write": true})).await.context("create write block")?;
        
                // Create a copy on the result index
                self.connection.safe_index_copy(CopyMethod::Clone, &alias, &index, None, None).await?;

                // Make the hot index the new clone
                // self.with_retries(self.datastore.client.indices.update_aliases, actions=actions)
                self.connection.make_request_json(&mut 0, reqwest::Method::POST, &self.connection.host.join("_aliases")?, &json!({
                    "actions": [
                        {"add":  {"index": index, "alias": alias}}, 
                        {"remove_index": {"index": alias}}
                    ]
                })).await?;

                // self.with_retries(self.datastore.client.indices.put_settings, index=alias, settings=write_unblock_settings)
                self.connection.make_request_json(&mut 0, Method::PUT, &settings_url, &json!({"index.blocks.write": null})).await?;
            }
        }

        // todo!("self._check_fields()")
        Ok(())
    }
}

#[derive(Deserialize)]
struct MGetResponse<Source, Fields> {
    docs: Vec<GetResponse<Source, Fields>>,
}

#[derive(Deserialize)]
#[allow(unused)]
pub struct GetResponse<Source, Fields> {
    /// The name of the index the document belongs to. 
    pub _index: String,
    /// The unique identifier for the document. 
    pub _id: String,
    /// The document version. Incremented each time the document is updated. 
    pub _version: i64,
    /// The sequence number assigned to the document for the indexing operation. Sequence numbers are used to ensure an older version of a document doesn’t overwrite a newer version. See Optimistic concurrency control. 
    pub _seq_no: i64,
    /// The primary term assigned to the document for the indexing operation. See Optimistic concurrency control. 
    pub _primary_term: i64,
    /// Indicates whether the document exists: true or false. 
    pub found: bool,
    // The explicit routing, if set. 
    // _routing
    /// If found is true, contains the document data formatted in JSON. Excluded if the _source parameter is set to false or the stored_fields parameter is set to true. 
    #[serde(default="default_none")]
    pub _source: Option<Source>,
    /// If the stored_fields parameter is set to true and found is true, contains the document fields stored in the index. 
    #[serde(default="default_none")]
    pub _fields: Option<Fields>,
}

fn default_none<Source>() -> Option<Source> { None }

enum CopyMethod {
    Clone,
}

impl std::fmt::Display for CopyMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyMethod::Clone => f.write_str("_clone")
        }
    }
}

#[derive(Deserialize)]
struct ElasticCommandResponse {
    acknowledged: bool,
}

#[allow(unused)]
enum SourceParam<'a> {
    Include(bool),
    Fields(&'a str),
}

pub struct SearchBuilder<'a, FieldType, SourceType: DSType> {
    collection: Collection<SourceType>,
    catagories: Option<IndexCatagory>,
    query: &'a str,
    size: usize,
    sort: Vec<serde_json::Value>,
    fields: Vec<&'a str>,
    source: SourceParam<'a>,
    track_total_hits: Option<u64>,
    _field_data_type: PhantomData<FieldType>,
    _source_data_type: PhantomData<SourceType>
}

#[allow(unused)]
impl<'a, FieldType: DeserializeOwned + Default, SourceType: DSType> SearchBuilder<'a, FieldType, SourceType> {

    pub fn new(collection: Collection<SourceType>, query: &'a str) -> Self {
        Self {
            collection,
            catagories: None,
            query,
            size: 1000,
            sort: vec!["_doc".into()],
            fields: vec![],
            track_total_hits: None,
            source: SourceParam::Include(false),
            _field_data_type: Default::default(),
            _source_data_type: Default::default(),
        }
    }

    pub fn index_catagories(mut self, catagories: IndexCatagory) -> Self {
        self.catagories = Some(catagories); self
    }

    pub fn size(mut self, size: usize) -> Self {
        self.size = size; self
    }
    
    pub fn track_total_hits(mut self, hits: u64) -> Self {
        self.track_total_hits = Some(hits); self
    }

    pub fn sort(mut self, sort: serde_json::Value) -> Self {
        self.sort = vec![sort]; self
    }

    pub fn fields(mut self, fields: Vec<&'a str>) -> Self {
        self.fields = fields; self
    }

    pub fn full_source(mut self, include: bool) -> Self {
        self.source = SourceParam::Include(include); self
    }

    fn prepare_body(&self) -> JsonMap {
        let source = match self.source {
            SourceParam::Include(source) => json!(source),
            SourceParam::Fields(fields) => json!(fields),
        };

        let body = json!({
            "query": {
                "bool": {
                    "must": {
                        "query_string": {
                            "query": self.query
                        }
                    },
                    // 'filter': filter_queries
                }
            },
            "size": self.size,
            "sort": self.sort,
            "fields": self.fields,
            "_source": source,
        });

        let serde_json::Value::Object(body) = body else { panic!() };
        body
    }

    pub async fn execute(self) -> Result<SearchResult<FieldType, SourceType>> {
        let indices = self.collection.get_joined_index(self.catagories)?;
        let path = format!("{indices}/_search");
        let mut url = self.collection.connection.host.join(&path)?;
        let body = self.prepare_body();

        if let Some(total_hits) = self.track_total_hits {
            url.query_pairs_mut().append_pair("track_total_hits", &total_hits.to_string());
        }

        loop {
            // Build and dispatch the request
            let response = self.collection.make_request_json(Method::GET, &url, &body).await?;

            // Handle server side http status errors with retry, let other error codes bubble up, decode successful bodies
            let body: SearchResult<FieldType, SourceType> = response.json().await?;

            // retry on timeout
            if body.timed_out {
                continue
            }
    
            return Ok(body)
        }        
    }

    pub async fn scan(mut self) -> Result<ScanCursor<FieldType, SourceType>> {
        // create PIT
        let pit = {
            let mut url = self.collection.connection.host.join(&format!("{}/_pit", self.collection.index))?;
            url.query_pairs_mut().append_pair("keep_alive", PIT_KEEP_ALIVE);
            let response = self.collection.connection.make_request(&mut 0, reqwest::Method::POST, &url).await?;
            let response: PITResponse = response.json().await?;
            response.id
        };
        
        // Add tie_breaker sort using _shard_doc ID
        self.sort.push(json!({"_shard_doc": "desc"}));

        // Prepare details of the query we can do in advance
        let url = self.collection.connection.host.join("_search")?;
        let query_body = self.prepare_body();

        // create cursor
        let mut cursor = ScanCursor {
            client: self.collection.connection,
            url,
            query_body,
            pit,
            search_after: None,
            current_batch: vec![],
            finished: false,
            size: self.size,
        };

        // initial query
        cursor.next_batch().await?;
        Ok(cursor)
    }
}

#[derive(Deserialize)]
struct PITResponse {
    id: String,
}

#[derive(Deserialize)]
#[allow(unused)]
struct ElasticStatus {
    /// The name of the cluster. 
    pub cluster_name: String,
    /// Health status of the cluster, based on the state of its primary and replica shards. Statuses are:
    ///
    /// green: All shards are assigned.
    /// yellow: All primary shards are assigned, but one or more replica shards are unassigned. If a node in the cluster fails, some data could be unavailable until that node is repaired.
    /// red: One or more primary shards are unassigned, so some data is unavailable. This can occur briefly during cluster startup as primary shards are assigned.
    pub status: String,
    /// (Boolean) If false the response returned within the period of time that is specified by the timeout parameter (30s by default). 
    pub timed_out: bool,
    /// (integer) The number of nodes within the cluster. 
    pub number_of_nodes: i64,
    /// (integer) The number of nodes that are dedicated data nodes. 
    pub number_of_data_nodes: i64,
    /// (integer) The number of active primary shards. 
    pub active_primary_shards: i64,
    /// (integer) The total number of active primary and replica shards. 
    pub active_shards: i64,
    /// (integer) The number of shards that are under relocation. 
    pub relocating_shards: i64,
    /// (integer) The number of shards that are under initialization. 
    pub initializing_shards: i64,
    /// (integer) The number of shards that are not allocated. 
    pub unassigned_shards: i64,
    /// (integer) The number of shards whose allocation has been delayed by the timeout settings. 
    pub delayed_unassigned_shards: i64,
    /// (integer) The number of cluster-level changes that have not yet been executed. 
    pub number_of_pending_tasks: i64,
    /// (integer) The number of unfinished fetches. 
    pub number_of_in_flight_fetch: i64,
    /// (integer) The time expressed in milliseconds since the earliest initiated task is waiting for being performed. 
    pub task_max_waiting_in_queue_millis: i64,
    /// (float) The ratio of active shards in the cluster expressed as a percentage. 
    pub active_shards_percent_as_number: f64,
}

#[derive(Deserialize)]
#[allow(unused)]
struct BulkResponse {
    /// How long, in milliseconds, it took to process the bulk request. 
    pub took: i64,
    /// If true, one or more of the operations in the bulk request did not complete successfully. 
    pub errors: bool,
    /// Contains the result of each operation in the bulk request, in the order they were submitted.
    pub items: Vec<BulkResponseItem>,
}

/// The parameter name is an action associated with the operation. Possible values are create, delete, index, and update.
#[derive(Deserialize)]
#[serde(rename_all="lowercase")]
enum BulkResponseItem {
    Create(BulkResponseItemData),
    Delete(BulkResponseItemData),
    Index(BulkResponseItemData),
    Update(BulkResponseItemData)
}

impl BulkResponseItem {
    fn into_data(self) -> BulkResponseItemData {
        match self {
            BulkResponseItem::Create(data) => data,
            BulkResponseItem::Delete(data) => data,
            BulkResponseItem::Index(data) => data,
            BulkResponseItem::Update(data) => data,
        }
    }
}

#[derive(Deserialize)]
#[allow(unused)]
struct BulkResponseItemData {
    /// Name of the index associated with the operation. If the operation targeted a data stream, this is the backing index into which the document was written. 
    pub _index: String,
    /// The document ID associated with the operation. 
    pub _id: String,
    /// The document version associated with the operation. The document version is incremented each time the document is updated.
    /// This parameter is only returned for successful actions.
    #[serde(default)]
    pub _version: Option<i64>,
    /// Result of the operation. Successful values are created, deleted, and updated.
    /// This parameter is only returned for successful operations.        
    #[serde(default)]
    pub result: Option<String>,
    /// Contains shard information for the operation.
    /// This parameter is only returned for successful operations.
    #[serde(default)]
    pub _shards: Option<BulkResponseItemShards>,
    /// The sequence number assigned to the document for the operation. Sequence numbers are used to ensure an older version of a document doesn’t overwrite a newer version. See Optimistic concurrency control.
    /// This parameter is only returned for successful operations.
    #[serde(default)]
    pub _seq_no: Option<i64>,
    /// The primary term assigned to the document for the operation. See Optimistic concurrency control.
    /// This parameter is only returned for successful operations.
    #[serde(default)]
    pub _primary_term: Option<i64>,
    /// HTTP status code returned for the operation. 
    pub status: u32,
    /// Contains additional information about the failed operation.
    /// The parameter is only returned for failed operations.
    pub error: Option<BulkResponseItemError>,
}

impl BulkResponseItemData {
    fn is_success(&self) -> bool {
        self.result.is_some()
    }
}

#[derive(Deserialize)]
#[allow(unused)]
struct BulkResponseItemShards {
    /// Number of shards the operation attempted to execute on. 
    pub total: i64,
    /// Number of shards the operation succeeded on. 
    pub successful: i64,
    /// Number of shards the operation attempted to execute on but failed. 
    pub failed: i64,
}

#[derive(Deserialize, Debug)]
#[allow(unused)]
struct BulkResponseItemError {
    /// Error type for the operation. 
    #[serde(rename="type")]
    pub type_: String,
    /// Reason for the failed operation. 
    pub reason: String,
    /// The universally unique identifier (UUID) of the index associated with the failed operation. 
    pub index_uuid: String,
    /// ID of the shard associated with the failed operation. 
    pub shard: Option<String>,
    /// Name of the index associated with the failed operation. If the operation targeted a data stream, this is the backing index into which the document was attempted to be written. 
    pub index: String,
}


pub struct ScanCursor<FieldType, SourceType> {
    client: Elastic,
    url: reqwest::Url,
    query_body: JsonMap,
    pit: String,
    size: usize,
    search_after: Option<serde_json::Value>,
    finished: bool,
    current_batch: Vec<SearchResultHitItem<FieldType, SourceType>>,
}

impl<FieldType: DeserializeOwned + Default, SourceType: DeserializeOwned> ScanCursor<FieldType, SourceType> {

    pub async fn next(&mut self) -> Result<Option<SearchResultHitItem<FieldType, SourceType>>> {
        match self.current_batch.pop() {
            Some(item) => Ok(Some(item)),
            None => {
                if !self.finished {
                    self.next_batch().await?;
                }
                Ok(self.current_batch.pop())
            },
        }
    }

    async fn next_batch(&mut self) -> Result<()> {
        // Add pit and search_after
        self.query_body.insert("pit".to_owned(), json!({
            "id": self.pit,
            "keep_alive": PIT_KEEP_ALIVE
        }));
        if let Some(after) = &self.search_after {
            self.query_body.insert("search_after".to_owned(), after.clone());
        };
        
        let mut attempt = 0;
        let mut body = loop {
            // Build and dispatch the request
            let response = self.client.make_request_json(&mut attempt, reqwest::Method::GET, &self.url, &self.query_body).await?;

            // Handle server side http status errors with retry, let other error codes bubble up, decode successful bodies
            let body: SearchResult<FieldType, SourceType> = response.json().await?;

            // retry on timeout
            if body.timed_out {
                continue
            }

            break body
        };

        self.finished = body.hits.hits.len() < self.size;
        self.search_after = body.hits.hits.last().map(|row|row.sort.clone());

        self.current_batch.append(&mut body.hits.hits);
        self.current_batch.reverse();
        Ok(())
    }
}

impl<FieldType, SourceType> Drop for ScanCursor<FieldType, SourceType> {
    fn drop(&mut self) {
        // clear the PIT
        let client = self.client.clone();
        let pit = self.pit.clone();
        tokio::spawn(async move {
            let url = client.host.join("_pit").unwrap();
            _ = client.make_request_json(&mut 0, reqwest::Method::DELETE, &url, &json!({
                "id": pit
            })).await;
        });
    }
}

#[derive(Deserialize)]
pub struct SearchResult<FieldType: Default, SourceType> {
    pub took: u64,
    pub timed_out: bool,
    pub hits: SearchResultHits<FieldType, SourceType>
}

#[derive(Debug, Deserialize)]
pub struct SearchResultHits<FieldType: Default, SourceType> {
    pub total: SearchResultHitTotals,
    pub max_score: Option<f64>,
    pub hits: Vec<SearchResultHitItem<FieldType, SourceType>>,
}

#[derive(Debug, Deserialize)]
pub struct SearchResultHitTotals {
    pub value: u64,
    pub relation: String,
}

#[derive(Debug, Deserialize)]
pub struct SearchResultHitItem<FieldType, SourceType> {
    pub _index: String,
    pub _id: String,
    pub _score: Option<f64>,
    #[serde(default="default_source")]
    pub _source: Option<SourceType>,
    pub sort: serde_json::Value,
    #[serde(default)]
    pub fields: FieldType,
}

fn default_source<T>() -> Option<T> { None }

#[derive(Debug)]
pub enum ElasticError {
    FailedToCreateIndex(String, String),
    HTTPError{path: Option<String>, code: StatusCode, message: String},
    OtherHttpClient(reqwest::Error),
    URL(url::ParseError),
    JSON(serde_json::Error),
    MalformedResponse,
    BadKey(String),
    // BadDocumentVersion,
    SerializeError(ModelError),
    ArchiveDisabled(&'static str),
    IndexHasNoArchive(Index),
    MappingError(assemblyline_models::meta::MappingError),
    ChainedError(String, Box<ElasticError>),
    ClassificationError(assemblyline_markings::errors::Errors)
}

impl std::fmt::Display for ElasticError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ElasticError::FailedToCreateIndex(src, target) => f.write_fmt(format_args!("Failed to create index {target} from {src}.")),
            ElasticError::HTTPError{path, code, message} => f.write_fmt(format_args!("http error [{path:?}]: {code} {message}")),
            ElasticError::OtherHttpClient(err) => f.write_fmt(format_args!("Error from http client: {err}")),
            ElasticError::URL(error) => f.write_fmt(format_args!("URL parse error: {}", error)),
            ElasticError::JSON(error) => f.write_fmt(format_args!("Issue with serialize or deserialize: {}", error)),
            ElasticError::MalformedResponse => f.write_str("A server response was not formatted as expected"),
            ElasticError::BadKey(key) => f.write_fmt(format_args!("tried to save document with invalid key: {key}")),
            // ElasticError::BadDocumentVersion => f.write_str("An invalid document version string was encountered"),
            ElasticError::ArchiveDisabled(message) => f.write_str(message),
            ElasticError::IndexHasNoArchive(index) => f.write_fmt(format_args!("The index [{index}] has no archive, but one was requested.")),
            ElasticError::MappingError(err) => f.write_fmt(format_args!("Mapping error: {err}")),
            ElasticError::SerializeError(err) => f.write_fmt(format_args!("Error serializing data: {err}")),
            ElasticError::ClassificationError(err) => f.write_fmt(format_args!("Error with classification: {err}")),
            ElasticError::ChainedError(message, err) => f.write_fmt(format_args!("{err}\n{message}")),
        }
    }
}

impl From<reqwest::Error> for ElasticError {
    fn from(value: reqwest::Error) -> Self {
        match value.status() {
            Some(code) => {
                let path = value.url().map(|url|url.path().to_owned());
                let message = code.to_string();
                ElasticError::HTTPError { path, code, message }
            },
            None => ElasticError::OtherHttpClient(value),
        }
    }
}

impl From<url::ParseError> for ElasticError {
    fn from(value: url::ParseError) -> Self { Self::URL(value) }
}

impl From<serde_json::Error> for ElasticError {
    fn from(value: serde_json::Error) -> Self { Self::JSON(value) }
}

impl From<assemblyline_models::ModelError> for ElasticError {
    fn from(value: assemblyline_models::ModelError) -> Self { Self::SerializeError(value) }
}

impl From<assemblyline_models::meta::MappingError> for ElasticError {
    fn from(value: assemblyline_models::meta::MappingError) -> Self { Self::MappingError(value) }
}

impl From<assemblyline_markings::errors::Errors> for ElasticError {
    fn from(value: assemblyline_markings::errors::Errors) -> Self { Self::ClassificationError(value) }
}

pub (crate) type Result<T> = std::result::Result<T, ElasticError>;

impl std::error::Error for ElasticError {}

impl<T> crate::error::Context for Result<T> {
    fn context(self, message: &str) -> Self {
        self.map_err(|err| ElasticError::ChainedError(message.to_owned(), Box::new(err)))
    }
}

#[cfg(test)]
mod test {
    use crate::types::{ExpiryGroup, FileInfo};

    use super::{Datastore, Index};
    use assemblyline_markings::classification::ClassificationParser;
    use assemblyline_models::{datastore::{file::Seen, retrohunt::IndexCatagory}, ExpandingClassification, Sha256};
    use rand::{distributions::{Alphanumeric, DistString}, thread_rng, Rng};
    use serde::Deserialize;

    fn setup_classification() {
        assemblyline_markings::set_default(std::sync::Arc::new(ClassificationParser::new(serde_json::from_str(r#"{"enforce":true,"dynamic_groups":false,"dynamic_groups_type":"all","levels":[{"aliases":["OPEN"],"css":{"color":"default"},"description":"N/A","lvl":1,"name":"LEVEL 0","short_name":"L0"},{"aliases":[],"css":{"color":"default"},"description":"N/A","lvl":5,"name":"LEVEL 1","short_name":"L1"},{"aliases":[],"css":{"color":"default"},"description":"N/A","lvl":15,"name":"LEVEL 2","short_name":"L2"}],"required":[{"aliases":["LEGAL"],"description":"N/A","name":"LEGAL DEPARTMENT","short_name":"LE","require_lvl":null,"is_required_group":false},{"aliases":["ACC"],"description":"N/A","name":"ACCOUNTING","short_name":"AC","require_lvl":null,"is_required_group":false},{"aliases":[],"description":"N/A","name":"ORIGINATOR CONTROLLED","short_name":"ORCON","require_lvl":null,"is_required_group":true},{"aliases":[],"description":"N/A","name":"NO CONTRACTOR ACCESS","short_name":"NOCON","require_lvl":null,"is_required_group":true}],"groups":[{"aliases":[],"auto_select":false,"description":"N/A","name":"GROUP A","short_name":"A","solitary_display_name":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"GROUP B","short_name":"B","solitary_display_name":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"GROUP X","short_name":"X","solitary_display_name":"XX"}],"subgroups":[{"aliases":["R0"],"auto_select":false,"description":"N/A","name":"RESERVE ONE","short_name":"R1","require_group":null,"limited_to_group":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"RESERVE TWO","short_name":"R2","require_group":"X","limited_to_group":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"RESERVE THREE","short_name":"R3","require_group":null,"limited_to_group":"X"}],"restricted":"L2","unrestricted":"L0"}"#).unwrap()).unwrap()))
    }
    
    #[tokio::test]
    async fn list_files() {
        setup_classification();

        // connect
        let ds = Datastore::new("http://elastic:password@localhost:9200", None, false, true).unwrap();
        ds.connection.wipe(Index::File, IndexCatagory::Hot).await.unwrap();

        let mut prng = thread_rng();
        for _ in 0..1000 {
            let file = assemblyline_models::datastore::File { 
                ascii: Alphanumeric.sample_string(&mut prng, 10), 
                classification: ExpandingClassification::new("L2//R1".to_owned()).unwrap(), 
                entropy: 0.0, 
                expiry_ts: Some(chrono::Utc::now() + chrono::Duration::days(2)), 
                is_section_image: true, 
                is_supplementary: false,
                comments: Default::default(),
                label_categories: Default::default(),
                labels: Default::default(),
                hex: Alphanumeric.sample_string(&mut prng, 10), 
                md5: prng.gen(), 
                magic: Alphanumeric.sample_string(&mut prng, 10), 
                mime: None, 
                seen: Seen { 
                    count: prng.gen::<u16>() as u64, 
                    first: chrono::Utc::now(), 
                    last: chrono::Utc::now() 
                }, 
                sha1: prng.gen(), 
                sha256: prng.gen(), 
                size: prng.gen::<u16>() as u64, 
                ssdeep: prng.gen(), 
                file_type: Alphanumeric.sample_string(&mut prng, 10), 
                tlsh: None, 
                from_archive: false, 
                uri_info: None, 
            };
            ds.file.save(&file.sha256, &file, None).await.unwrap();
        }
        ds.file.refresh().await.unwrap();

        let current = chrono::Utc::now() - chrono::Duration::days(2);
        let files = ds.fetch_files(current, 500).await.unwrap();
        assert_eq!(files.len(), 500);

        let files = ds.fetch_files(current, 2500).await.unwrap();
        assert_eq!(files.len(), 1000);
    }

    #[tokio::test]
    async fn basic_elastic_operations_on_hits() {
        setup_classification();

        // connect
        let ds = Datastore::new("http://elastic:password@localhost:9200", None, false, true).unwrap();

        // delete old index, we'll recreate with a direct operation in the index
        ds.connection.wipe(Index::RetrohuntHit, IndexCatagory::Hot).await.unwrap();
        assert!(ds.retrohunt_hit.get("osntehuo.cenuhdon.chu").await.unwrap().is_none());

        // delete old index, we'll recreate wih a bulk operation        
        ds.connection.wipe(Index::RetrohuntHit, IndexCatagory::Hot).await.unwrap();

        // Load a few random objects into the database
        let mut hits = vec![];
        let mut prng = thread_rng();
        for _ in 0..1000 {
            hits.push(FileInfo{ 
                hash: prng.gen(), 
                access: "and(\"L2\", \"R1\")".parse().unwrap(),
                access_string: "L2//R1".parse().unwrap(),
                expiry: ExpiryGroup::today() 
            });
        }
        let mut subset: Vec<FileInfo> = hits[0..50].iter().cloned().collect();
        ds.save_hits("search", hits).await.unwrap();

        // Update some of those files
        for hit in &mut subset {
            hit.access = "\"L0\"".parse().unwrap();
            hit.access_string = "L0".to_owned();      
        }
        ds.save_hits("search", subset.clone()).await.unwrap();
        
        // count items in elastic
        let search = ds.retrohunt_hit.search::<()>("*:*")
            .size(0)
            .track_total_hits(500)
            .execute().await.unwrap();
        assert_eq!(search.hits.hits.len(), 0);
        assert_eq!(search.hits.total.value, 500);
        assert_eq!(search.hits.total.relation, "gte");

        let search = ds.retrohunt_hit.search::<()>("*")
            .size(0)
            .track_total_hits(50000)
            .execute().await.unwrap();
        assert_eq!(search.hits.total.relation, "eq");
        assert_eq!(search.hits.total.value, 1000);
        assert_eq!(search.hits.hits.len(), 0);

        // Do a search that returns a fraction of results
        #[derive(Deserialize, Default)]
        struct Fields {
            sha256: Vec<Sha256>,
            key: Vec<String>,
        }
        let search = ds.retrohunt_hit.search::<Fields>("classification: L0")
            .fields(vec!["sha256", "key"])
            .full_source(true)
            .size(500)
            .execute().await.unwrap();
        assert_eq!(search.timed_out, false);
        assert_eq!(search.hits.total.value, 50);
        assert_eq!(search.hits.hits.len(), 50);
        for item in search.hits.hits {
            let source = item._source.unwrap();
            let info = subset.iter().find(|x|x.hash.to_string() == source.sha256.to_string()).unwrap();
            assert_eq!(item.fields.sha256, vec![info.hash.to_string().parse().unwrap()]);
            assert_eq!(item.fields.key, vec!["search_".to_string() + &info.hash.to_string()]);
            assert_eq!(source.classification.as_str(), "L0");
        }

        // Do a search that returns all the results;
        let search = ds.retrohunt_hit.search::<()>("NOT classification: L0")
            .full_source(true)
            .size(2000)
            .execute().await.unwrap();
        assert_eq!(search.timed_out, false);
        assert_eq!(search.hits.total.value, 950);
        assert_eq!(search.hits.hits.len(), 950);
        
        // Do a scan with PIT that requires a single call
        let mut search = ds.retrohunt_hit.search::<()>("NOT classification: L0")
            .full_source(true)
            .size(2000)
            .scan().await.unwrap();
        let mut total = 0;
        while let Some(_) = search.next().await.unwrap() {
            total += 1;
        }
        assert_eq!(total, 950);

        // Do a scan with PIT that requires multiple calls
        let mut search = ds.retrohunt_hit.search::<()>("*")
            .full_source(true)
            .size(20)
            .scan().await.unwrap();
        let mut total = 0;
        while let Some(_) = search.next().await.unwrap() {
            total += 1;
        }
        assert_eq!(total, 1000);

        // test get and save
        let search = ds.retrohunt_hit.search::<()>("*:*")
            .full_source(true)
            .size(10)
            .execute().await.unwrap();
        assert_eq!(search.hits.hits.len(), 10);
        for result in search.hits.hits {
            let (mut doc, version) = ds.retrohunt_hit.get(&result._id).await.unwrap().unwrap();
            assert_eq!(doc, result._source.unwrap());
            doc.expiry_ts = Some(chrono::Utc::now());
            let bad_version = (version.0 - 1, version.1);
            assert!(!ds.retrohunt_hit.save(&result._id, &doc, bad_version).await.unwrap());
            assert!(ds.retrohunt_hit.save(&result._id, &doc, version).await.unwrap());
            let (doc2, version2) = ds.retrohunt_hit.get(&result._id).await.unwrap().unwrap();
            assert_eq!(doc, doc2);
            assert_ne!(version, version2);
        }
        assert!(ds.retrohunt_hit.get("osntehuo.cenuhdon.chu").await.unwrap().is_none());

        // test that we recreate an index on search
        // delete index
        ds.connection.wipe(Index::RetrohuntHit, IndexCatagory::Hot).await.unwrap();

        // count items in elastic
        let search = ds.retrohunt_hit.search::<()>("*:*")
            .size(0)
            .track_total_hits(500)
            .execute().await.unwrap();
        assert_eq!(search.hits.hits.len(), 0);
        assert_eq!(search.hits.total.value, 0);
        assert_eq!(search.hits.total.relation, "eq");
    }

}