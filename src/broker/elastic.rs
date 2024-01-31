use std::{borrow::Cow, collections::{HashMap, HashSet}, marker::PhantomData, time::Duration};

use chrono::{DateTime, Utc};
use http::{Method, StatusCode, request};
use itertools::Itertools;
use log::{error, debug, warn};
use serde::{Deserialize, de::DeserializeOwned, Serialize};

// use anyhow::Result;
use serde_json::json;
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;

use crate::{error::Context, types::{FileInfo, JsonMap}};
use assemblyline_models::{datastore::{retrohunt::{self as models, IndexCatagory}, RetrohuntHit}, ElasticMeta, meta::default_settings, ExpandingClassification, ModelError};

use super::fetcher::FetchedFile;

const MAX_DELAY: Duration = Duration::from_secs(60);
const PIT_KEEP_ALIVE: &str = "5m";
const CREATE_TOKEN: &str = "create";


#[derive(Clone)]
pub struct Elastic {
    client: reqwest::Client,
    host: reqwest::Url,
    archive_access: bool,
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, PartialEq, Eq, Debug, Clone, Copy, Hash)]
#[strum(serialize_all = "snake_case")]
enum Index {
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
        // strip the authentication information
        let mut url: url::Url = host.parse()?;
        // let username = url.username();
        // let password = url.password();
        // url.set_username("").unwrap();
        // url.set_password(None).unwrap();
        // println!("auth {}", url.authority());
        // println!("username {}", url.username());
        // println!("password {:?}", url.password());

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

    pub (crate) async fn fetch_files(&self, seek_point: chrono::DateTime<chrono::Utc>, batch_size: usize) -> Result<Vec<FetchedFile>> {
        let result = SearchBuilder::<JsonMap, JsonMap>::new(self.clone(), "file-ma,file", &format!("seen.last: [{} TO *]", seek_point.to_rfc3339()))
            .size(batch_size)
            .sort("seen.last:asc")
            .fields(vec!["classification", "expiry_ts", "sha256", "seen.last"])
            .execute().await?;

        // read the body of our response
        let mut out = vec![];
        for row in result.hits.hits {
            out.push(FetchedFile::extract(&row.fields).ok_or(ElasticError::MalformedResponse)?)
        }
        return Ok(out)
    }


    pub async fn list_active_searches(&self) -> Result<Vec<models::Retrohunt>> {
        let result = SearchBuilder::<JsonMap, models::Retrohunt>::new(self.clone(), "retrohunt", "finished: false")
            .fields(vec![])
            .full_source(true)
            .scan().await?;

        todo!()
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

    /// This function should test if the collection that you are trying to access does indeed exist
    /// and should create it if it does not.
    pub async fn ensure_collection<Model: Described<ElasticMeta>>(&self, name: Index) -> Result<()> {
        for alias in self.get_index_list(name, None)? {
            let index = format!("{alias}_hot");

            // Create HOT index
            if !self.does_index_exist(&alias).await.context("does_index_exist")? {
                debug!("Index {} does not exists. Creating it now...", alias.to_uppercase());

                let mut mapping = assemblyline_models::meta::build_mapping::<Model>()?;
                mapping.apply_defaults();

                let body = json!({
                    "mappings": mapping,
                    "settings": self.get_index_settings(name, name.is_archive_index(&index))
                });

                if let Err(err) = self.make_request_json(&mut 0, Method::PUT, &self.host.join(&index)?, &body).await {
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

                self.put_alias(&index, &alias).await.context("put_alias")?;
            } else if !self.does_index_exist(&index).await? && !self.does_alias_exist(&alias).await.context("does_alias_exist")? {
                // Hold a write block for the rest of this section
                // self.with_retries(self.datastore.client.indices.put_settings, index=alias, settings=write_block_settings)
                let settings_url = self.host.join(&format!("{index}/_settings"))?;
                self.make_request_json(&mut 0, Method::PUT, &settings_url, &json!({"index.blocks.write": true})).await.context("create write block")?;
        
                // Create a copy on the result index
                self.safe_index_copy(CopyMethod::Clone, &alias, &index, None, None).await?;

                // Make the hot index the new clone
                // self.with_retries(self.datastore.client.indices.update_aliases, actions=actions)
                self.make_request_json(&mut 0, reqwest::Method::POST, &self.host.join("_aliases")?, &json!({
                    "actions": [
                        {"add":  {"index": index, "alias": alias}}, 
                        {"remove_index": {"index": alias}}
                    ]
                })).await?;

                // self.with_retries(self.datastore.client.indices.put_settings, index=alias, settings=write_unblock_settings)
                self.make_request_json(&mut 0, Method::PUT, &settings_url, &json!({"index.blocks.write": null})).await?;
            }
        }

        // todo!("self._check_fields()")
        Ok(())
    }

    fn get_index_settings(&self, index: Index, archive: bool) -> serde_json::Value {
        default_settings(json!({
            "number_of_shards": index.shards(archive), // self.shards if not archive else self.archive_shards,
            "number_of_replicas": index.replicas(archive), // self.replicas if not archive else self.archive_replicas    
        }))
    }

    // fn get_index_mappings(&self) -> dict:
    //     mappings: dict = deepcopy(default_mapping)
    //     mappings['properties'], mappings['dynamic_templates'] = build_mapping(self.model_class.fields().values())
    //     mappings['dynamic_templates'].insert(0, default_dynamic_strings)

    //     if not mappings['dynamic_templates']:
    //         # Setting dynamic to strict prevents any documents with fields not in the properties to be added
    //         mappings['dynamic'] = "strict"

    //     mappings['properties']['id'] = {
    //         "store": True,
    //         "doc_values": True,
    //         "type": 'keyword'
    //     }

    //     mappings['properties']['__text__'] = {
    //         "store": False,
    //         "type": 'text',
    //     }

    //     return mappings

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
    pub async fn save<T: Serialize>(&self, index_list: &[&str], key: &str, data: &T, version: Option<&str>) -> Result<()> {
        if key.contains(" ") {
            return Err(ElasticError::BadKey(key.to_owned()))
        }

        // saved_data['id'] = key
        let mut operation = "index";
        let mut parsed_version = None;

        if let Some(version) = version {
            if version == CREATE_TOKEN {
                operation = "create";
            } else {
                let mut parts = version.split("---");
                let seq_no: i64 = parts.next().ok_or(ElasticError::BadDocumentVersion)?.parse().map_err(|_| ElasticError::BadDocumentVersion)?;
                let primary_term: i64 = parts.next().ok_or(ElasticError::BadDocumentVersion)?.parse().map_err(|_| ElasticError::BadDocumentVersion)?;
                parsed_version = Some((seq_no, primary_term))
            }
        }

        let body = serde_json::to_string(data)?;

        // index_list = self.get_index_list(index_type)
        for index in index_list {
            let mut url = if operation == "index" {
                self.host.join(&format!("{}/_doc/{key}", index))?
            } else {
                self.host.join(&format!("{}/_create/{key}", index))?
            };

            url.query_pairs_mut()
                .append_pair("raise_conflicts", "true")
                .append_pair("op_type", operation);

            if let Some((seq_no, primary_term)) = parsed_version {
                url.query_pairs_mut()
                    .append_pair("if_seq_no", &seq_no.to_string())
                    .append_pair("if_primary_term", &primary_term.to_string());
            }

            self.make_request_json(&mut 0, reqwest::Method::PUT, &url, &body).await?;
        }

        return Ok(())
    }

    pub async fn save_search(&self, data: &models::Retrohunt) -> Result<()> {
        self.save(&["retrohunt"], &data.key, data, None).await
    }

    pub async fn fatal_error(&self, data: &mut models::Retrohunt, error: String) -> Result<()> {
        data.errors.push(error);
        data.finished = true;
        data.completed_time = Some(chrono::Utc::now());
        self.save_search(data).await
    }

    pub async fn finalize_search(&self, data: &mut models::Retrohunt) -> Result<()> {
        data.finished = true;
        data.completed_time = Some(chrono::Utc::now());
        self.save_search(data).await
    }

    async fn handle_result(attempt: &mut u64, result: reqwest::Result<reqwest::Response>) -> Result<Option<reqwest::Response>> {
        // Handle connection errors with a retry, let other non http errors bubble up
        let response = match result {
            Ok(response) => response,
            Err(err) => {
                // always retry for connect and timeout errors
                if err.is_connect() || err.is_timeout() {
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

    pub async fn save_hits(&self, search: &str, hits: Vec<FileInfo>) -> Result<()> {
        let index = &self.get_index_list(Index::RetrohuntHit, Some(IndexCatagory::Hot)).unwrap()[0];
        println!("{index}");
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

        let mut bulk_url = self.host.join("_bulk")?;
        bulk_url.query_pairs_mut().append_pair("refresh", "wait_for");
        loop {
            // execute the bulk body
            let response = self.make_request_data(&mut 0, Method::POST, &bulk_url, body.as_bytes()).await?;
            let bulk_response: BulkResponse = response.json().await?;

            // pull out failed calls
            let mut missing_ids = vec![];
            for result in bulk_response.items {
                let result_data = result.into_data();
                if result_data.is_success() { continue }
                missing_ids.push(result_data._id);
            }

            // everything went well
            if missing_ids.is_empty() {
                return Ok(())
            }

            // batch fetch the existing document for those operations that failed
            let documents: Vec<GetResponse<RetrohuntHit, ()>> = self.multiget_source(&index, &missing_ids.iter().map(String::as_str).collect_vec()).await.context("multiget")?;

            // prepare a batch of update operations
            body.clear();
            for document in documents {
                if let Some(source) = document._source {
                    if let Some(info) = fileinfo.get(&document._id) {
                        // println!("{}", serde_json::to_string_pretty(&RetrohuntHit{ 
                        //     classification: ExpandingClassification::new(ce.min_classification(source.classification.as_str(), &info.access_string, false)?)?,
                        //     expiry_ts: match (source.expiry_ts, info.expiry.to_timestamp()) {
                        //         (Some(a), Some(b)) => Some(a.max(b)),
                        //         _ => None,
                        //     },
                        //     key: document._id.clone(),
                        //     sha256: source.sha256.clone(),
                        //     search: search.to_owned(), 
                        // }).unwrap());

                        body += &serde_json::to_string(&json!({"index": {"_index": index, "_id": document._id, "require_alias": true, "if_seq_no": document._seq_no, "if_primary_term": document._primary_term}}))?;
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

    pub async fn multiget_source<Source: DeserializeOwned>(&self, index: &str, ids: &[&str]) -> Result<Vec<GetResponse<Source, ()>>> {
        let mut url = self.host.join(&format!("{index}/_mget"))?;
        url.query_pairs_mut().append_pair("_source", "true");
        let response = self.make_request_json(&mut 0, Method::GET, &url, &json!({
            "ids": ids
        })).await.context("mget request")?;

        // let data = response.text().await?;
        // println!("{}", &data[420..]);
        // // let temp: serde_json::Value = serde_json::from_str(&data)?;
        // // println!("{}", serde_json::to_string_pretty(&temp)?);
        // todo!();
        let response: MGetResponse<Source, ()> = response.json().await?;
        Ok(response.docs)
    }

    /// This function should completely delete the collection
    /// THIS IS FOR TESTING
    #[cfg(test)]
    async fn wipe(&self, index: Index, catagory: IndexCatagory) -> Result<()> {
        for name in self.get_index_list(index, Some(catagory))? {
            let index = format!("{name}_hot");
            // log.debug("Wipe operation started for collection: %s" % name.upper())
            // if self.with_retries(self.datastore.client.indices.exists, index=index):
            if self.does_index_exist(&index).await? {
                let url = self.host.join(&index)?;
                if let Err(err) = self.make_request(&mut 0, Method::DELETE, &url).await {
                    if let ElasticError::HTTPError{code: StatusCode::NOT_FOUND, ..} =  &err {
                        continue
                    }
                    return Err(err)
                }
                // self.with_retries(self.datastore.client.indices.delete, index=index)
            }
        }
        Ok(())
        // if recreate:
        //     self._ensure_collection()
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


enum SourceParam<'a> {
    Include(bool),
    Fields(&'a str),
}

struct SearchBuilder<'a, FieldType, SourceType> {
    client: Elastic,
    indices: &'a str,
    query: &'a str,
    size: usize,
    sort: Vec<serde_json::Value>,
    fields: Vec<&'a str>,
    source: SourceParam<'a>,
    track_total_hits: Option<u64>,
    _field_data_type: PhantomData<FieldType>,
    _source_data_type: PhantomData<SourceType>
}

impl<'a, FieldType: DeserializeOwned + Default, SourceType: DeserializeOwned> SearchBuilder<'a, FieldType, SourceType> {

    pub fn new(client: Elastic, indices: &'a str, query: &'a str) -> Self {
        Self {
            client,
            indices,
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

    pub fn size(mut self, size: usize) -> Self {
        self.size = size; self
    }
    
    pub fn track_total_hits(mut self, hits: u64) -> Self {
        self.track_total_hits = Some(hits); self
    }

    pub fn sort(mut self, sort: &'a str) -> Self {
        self.sort = vec![sort.into()]; self
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

        let mut body = json!({
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
        let path = format!("{}/_search", self.indices);
        let mut url = self.client.host.join(&path)?;
        let mut attempt: u64 = 0;
        let body = self.prepare_body();

        if let Some(total_hits) = self.track_total_hits {
            url.query_pairs_mut().append_pair("track_total_hits", &total_hits.to_string());
        }

        loop {
            // Build and dispatch the request
            let response = self.client.make_request_json(&mut attempt, reqwest::Method::GET, &url, &body).await?;

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
            let mut url = self.client.host.join(&format!("{}/_pit", self.indices))?;
            url.query_pairs_mut().append_pair("keep_alive", PIT_KEEP_ALIVE);
            let response = self.client.make_request(&mut 0, reqwest::Method::POST, &url).await?;
            let response: PITResponse = response.json().await?;
            response.id
        };
        
        // Add tie_breaker sort using _shard_doc ID
        self.sort.push(json!({"_shard_doc": "desc"}));

        // Prepare details of the query we can do in advance
        let url = self.client.host.join("_search")?;
        let query_body = self.prepare_body();

        // create cursor
        let mut cursor = ScanCursor {
            client: self.client,
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
    fn data(&self) -> &BulkResponseItemData {
        match self {
            BulkResponseItem::Create(data) => data,
            BulkResponseItem::Delete(data) => data,
            BulkResponseItem::Index(data) => data,
            BulkResponseItem::Update(data) => data,
        }
    }

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
struct BulkResponseItemData {
    /// Name of the index associated with the operation. If the operation targeted a data stream, this is the backing index into which the document was written. 
    pub _index: String,
    /// The document ID associated with the operation. 
    pub _id: String,
    /// The document version associated with the operation. The document version is incremented each time the document is updated.
    /// This parameter is only returned for successful actions.
    pub _version: Option<i64>,
    /// Result of the operation. Successful values are created, deleted, and updated.
    /// This parameter is only returned for successful operations.        
    pub result: Option<String>,
    /// Contains shard information for the operation.
    /// This parameter is only returned for successful operations.
    pub _shards: Option<BulkResponseItemShards>,
    /// The sequence number assigned to the document for the operation. Sequence numbers are used to ensure an older version of a document doesn’t overwrite a newer version. See Optimistic concurrency control.
    /// This parameter is only returned for successful operations.
    pub _seq_no: Option<i64>,
    /// The primary term assigned to the document for the operation. See Optimistic concurrency control.
    /// This parameter is only returned for successful operations.
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
struct BulkResponseItemShards {
    /// Number of shards the operation attempted to execute on. 
    pub total: i64,
    /// Number of shards the operation succeeded on. 
    pub successful: i64,
    /// Number of shards the operation attempted to execute on but failed. 
    pub failed: i64,
}

#[derive(Deserialize)]
struct BulkResponseItemError {
    /// Error type for the operation. 
    #[serde(rename="type")]
    pub type_: String,
    /// Reason for the failed operation. 
    pub reason: String,
    /// The universally unique identifier (UUID) of the index associated with the failed operation. 
    pub index_uuid: String,
    /// ID of the shard associated with the failed operation. 
    pub shard: String,
    /// Name of the index associated with the failed operation. If the operation targeted a data stream, this is the backing index into which the document was attempted to be written. 
    pub index: String,
}


struct ScanCursor<FieldType, SourceType> {
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

#[derive(Deserialize)]
pub struct SearchResultHits<FieldType: Default, SourceType> {
    pub total: SearchResultHitTotals,
    pub max_score: Option<f64>,
    pub hits: Vec<SearchResultHitItem<FieldType, SourceType>>,
}

#[derive(Deserialize)]
pub struct SearchResultHitTotals {
    pub value: i64,
    pub relation: String,
}

#[derive(Deserialize)]
pub struct SearchResultHitItem<FieldType, SourceType> {
    pub _index: String,
    pub _id: String,
    pub _score: Option<f64>,
    pub _source: SourceType,
    pub sort: serde_json::Value,
    #[serde(default)]
    pub fields: FieldType,
}

#[derive(Debug)]
pub enum ElasticError {
    FailedToCreateIndex(String, String),
    HTTPError{path: Option<String>, code: StatusCode, message: String},
    OtherHttpClient(reqwest::Error),
    URL(url::ParseError),
    JSON(serde_json::Error),
    MalformedResponse,
    BadKey(String),
    BadDocumentVersion,
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
            ElasticError::BadDocumentVersion => f.write_str("An invalid document version string was encountered"),
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

type Result<T> = std::result::Result<T, ElasticError>;

impl std::error::Error for ElasticError {}

impl<T> crate::error::Context for Result<T> {
    fn context(self, message: &str) -> Self {
        self.map_err(|err| ElasticError::ChainedError(message.to_owned(), Box::new(err)))
    }
}

#[cfg(test)]
mod test {
    use crate::{broker::elastic::SearchBuilder, types::{ExpiryGroup, FileInfo}};

    use super::{Elastic, ElasticError, Index};
    use assemblyline_markings::classification::ClassificationParser;
    use assemblyline_models::{datastore::{retrohunt::IndexCatagory, RetrohuntHit}, Sha256};
    use rand::{thread_rng, Rng};
    use serde::Deserialize;

    fn setup_classification() {
        assemblyline_markings::set_default(std::sync::Arc::new(ClassificationParser::new(serde_json::from_str(r#"{"enforce":true,"dynamic_groups":false,"dynamic_groups_type":"all","levels":[{"aliases":["OPEN"],"css":{"color":"default"},"description":"N/A","lvl":1,"name":"LEVEL 0","short_name":"L0"},{"aliases":[],"css":{"color":"default"},"description":"N/A","lvl":5,"name":"LEVEL 1","short_name":"L1"},{"aliases":[],"css":{"color":"default"},"description":"N/A","lvl":15,"name":"LEVEL 2","short_name":"L2"}],"required":[{"aliases":["LEGAL"],"description":"N/A","name":"LEGAL DEPARTMENT","short_name":"LE","require_lvl":null,"is_required_group":false},{"aliases":["ACC"],"description":"N/A","name":"ACCOUNTING","short_name":"AC","require_lvl":null,"is_required_group":false},{"aliases":[],"description":"N/A","name":"ORIGINATOR CONTROLLED","short_name":"ORCON","require_lvl":null,"is_required_group":true},{"aliases":[],"description":"N/A","name":"NO CONTRACTOR ACCESS","short_name":"NOCON","require_lvl":null,"is_required_group":true}],"groups":[{"aliases":[],"auto_select":false,"description":"N/A","name":"GROUP A","short_name":"A","solitary_display_name":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"GROUP B","short_name":"B","solitary_display_name":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"GROUP X","short_name":"X","solitary_display_name":"XX"}],"subgroups":[{"aliases":["R0"],"auto_select":false,"description":"N/A","name":"RESERVE ONE","short_name":"R1","require_group":null,"limited_to_group":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"RESERVE TWO","short_name":"R2","require_group":"X","limited_to_group":null},{"aliases":[],"auto_select":false,"description":"N/A","name":"RESERVE THREE","short_name":"R3","require_group":null,"limited_to_group":"X"}],"restricted":"L2","unrestricted":"L0"}"#).unwrap()).unwrap()))
    }

    #[tokio::test]
    async fn basic_elastic_operations() {
        setup_classification();

        // connect
        let elastic = Elastic::new("http://elastic:password@localhost:9200", None, false, true).unwrap();

        // delete old index
        elastic.wipe(Index::RetrohuntHit, IndexCatagory::Hot).await.unwrap();

        // Create new index for testing
        elastic.ensure_collection::<RetrohuntHit>(Index::RetrohuntHit).await.unwrap();

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
        elastic.save_hits("search", hits).await.unwrap();

        // Update some of those files
        for hit in &mut subset {
            hit.access = "\"L0\"".parse().unwrap();
            hit.access_string = "L0".to_owned();      
        }
        elastic.save_hits("search", subset.clone()).await.unwrap();
        
        // count items in elastic
        let search = SearchBuilder::<Fields, RetrohuntHit>::new(elastic.clone(), "retrohunt_hit", "*:*")
            .size(0)
            .track_total_hits(500)
            .execute().await.unwrap();
        assert_eq!(search.hits.hits.len(), 0);
        assert_eq!(search.hits.total.value, 500);
        assert_eq!(search.hits.total.relation, "gte");

        let search = SearchBuilder::<Fields, RetrohuntHit>::new(elastic.clone(), "retrohunt_hit", "*")
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
        let search = SearchBuilder::<Fields, RetrohuntHit>::new(elastic.clone(), "retrohunt_hit", "classification: L0")
            .fields(vec!["sha256", "key"])
            .full_source(true)
            .size(500)
            .execute().await.unwrap();
        assert_eq!(search.timed_out, false);
        assert_eq!(search.hits.total.value, 50);
        assert_eq!(search.hits.hits.len(), 50);
        for item in search.hits.hits {
            let info = subset.iter().find(|x|x.hash.to_string() == item._source.sha256.to_string()).unwrap();
            assert_eq!(item.fields.sha256, vec![info.hash.to_string().parse().unwrap()]);
            assert_eq!(item.fields.key, vec!["search_".to_string() + &info.hash.to_string()]);
            assert_eq!(item._source.classification.as_str(), "L0");
        }

        // Do a search that returns all the results;
        let search = SearchBuilder::<(), RetrohuntHit>::new(elastic.clone(), "retrohunt_hit", "NOT classification: L0")
            .full_source(true)
            .size(2000)
            .execute().await.unwrap();
        assert_eq!(search.timed_out, false);
        assert_eq!(search.hits.total.value, 950);
        assert_eq!(search.hits.hits.len(), 950);
        
        // Do a scan with PIT that requires a single call
        let mut search = SearchBuilder::<(), RetrohuntHit>::new(elastic.clone(), "retrohunt_hit", "NOT classification: L0")
            .full_source(true)
            .size(2000)
            .scan().await.unwrap();
        let mut total = 0;
        while let Some(_) = search.next().await.unwrap() {
            total += 1;
        }
        assert_eq!(total, 950);

        // Do a scan with PIT that requires multiple calls
        let mut search = SearchBuilder::<(), RetrohuntHit>::new(elastic.clone(), "retrohunt_hit", "*")
            .full_source(true)
            .size(20)
            .scan().await.unwrap();
        let mut total = 0;
        while let Some(_) = search.next().await.unwrap() {
            total += 1;
        }
        assert_eq!(total, 1000);
    }

}