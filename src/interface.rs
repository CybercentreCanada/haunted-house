

use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use std::time::{Instant, Duration};
use anyhow::{Result, Context};

use chrono::serde::ts_seconds_option;
use futures::{StreamExt, SinkExt};
use log::{error, debug, info};
use poem::listener::{Listener, OpensslTlsConfig};
use poem::web::websocket::Message;
use poem::{post, EndpointExt, Endpoint, Middleware, Request, FromRequest, IntoResponse, Response};
use poem::web::{TypedHeader, Data, Json};
use poem::http::{StatusCode};
use poem::web::headers::Authorization;
use poem::web::headers::authorization::Bearer;
use poem::{get, handler, listener::TcpListener, web::Path, Route, Server};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use crate::access::AccessControl;
use crate::auth::Role;
use crate::config::TLSConfig;
use crate::core::{HouseCore, IngestStatus};
use crate::database::{BlobID, IndexID, IndexGroup};
use crate::query::Query;

type BearerToken = TypedHeader<Authorization<Bearer>>;

struct TokenMiddleware {
    core: Arc<HouseCore>,
}

impl TokenMiddleware {
    fn new(core: Arc<HouseCore>) -> Self {
        Self{core}
    }
}

impl<E: Endpoint> Middleware<E> for TokenMiddleware {
    type Output = TokenMiddlewareImpl<E>;

    fn transform(&self, ep: E) -> Self::Output {
        TokenMiddlewareImpl { ep, core: self.core.clone() }
    }
}

struct TokenMiddlewareImpl<E> {
    ep: E,
    core: Arc<HouseCore>,
}

#[poem::async_trait]
impl<E: Endpoint> Endpoint for TokenMiddlewareImpl<E> {
    type Output = E::Output;

    async fn call(&self, mut req: Request) -> poem::Result<Self::Output> {
        let roles = match BearerToken::from_request_without_body(&req).await {
            Ok(token) => self.core.authenticator.get_roles(token.token())?,
            Err(_) => Default::default()
        };

        if roles.contains(&Role::Worker) {
            req.extensions_mut().insert(WorkerInterface::new(self.core.clone()));
        }

        if roles.contains(&Role::Search) {
            req.extensions_mut().insert(SearcherInterface::new(self.core.clone()));
        }

        if roles.contains(&Role::Ingest) {
            req.extensions_mut().insert(IngestInterface::new(self.core.clone()));
        }

        if !roles.is_empty() {
            req.extensions_mut().insert(roles);
            req.extensions_mut().insert(StatusInterface::new(self.core.clone()));
        }

        // call the next endpoint.
        self.ep.call(req).await
    }
}


pub struct LoggerMiddleware;

impl<E: Endpoint> Middleware<E> for LoggerMiddleware {
    type Output = LoggerMiddlewareImpl<E>;

    fn transform(&self, ep: E) -> Self::Output {
        LoggerMiddlewareImpl { ep }
    }
}

pub struct LoggerMiddlewareImpl<E> {
    ep: E,
}


#[poem::async_trait]
impl<E: Endpoint> Endpoint for LoggerMiddlewareImpl<E> {
    type Output = E::Output;

    async fn call(&self, req: Request) -> poem::Result<Self::Output> {
        let start = Instant::now();
        let uri = req.uri().clone();
        match self.ep.call(req).await {
            Ok(resp) => {
                debug!("request for {uri} handled ({} ms)", start.elapsed().as_millis());
                Ok(resp)
            },
            Err(err) => {
                error!("error handling {uri} ({} ms) {err}", start.elapsed().as_millis());
                Err(err)
            },
        }
    }
}

struct StatusInterface {
    core: Arc<HouseCore>
}

impl StatusInterface {
    pub fn new(core: Arc<HouseCore>) -> Self {
        Self{core}
    }

    pub async fn get_status(&self) -> Result<StatusReport> {
        let ingest = self.core.ingest_status().await?;
        let mut indices = self.core.database.list_indices().await?;
        indices.sort();

        let mut file_count: HashMap<IndexGroup, u64> = Default::default();

        for (group, index) in indices.iter() {
            let mut count = *file_count.get(group).unwrap_or(&0);
            count += self.core.database.count_files(index).await?;
            file_count.insert(group.clone(), count);
        }

        let mut file_counts: Vec<_> = file_count.into_iter().collect();
        file_counts.sort();

        Ok(StatusReport {
            ingest,
            active_filters: indices,
            file_counts
        })
    }
}


struct WorkerInterface {
    core: Arc<HouseCore>
}

impl WorkerInterface {
    pub fn new(core: Arc<HouseCore>) -> Self {
        Self{core}
    }

    pub async fn get_work(&self, req: WorkRequest) -> Result<WorkPackage> {
        let start = std::time::Instant::now();
        loop {
            let work = self.core.get_work(&req).await?;
            if !work.is_empty() {
                return Ok(work);
            }

            let wait_max = match Duration::from_secs(30).checked_sub(start.elapsed()) {
                Some(wait) => wait,
                None => return Ok(Default::default()),
            };

            if let Err(_) = tokio::time::timeout(wait_max, self.core.get_work_notification()).await {
                return Ok(Default::default())
            }
        }
    }

    pub fn finish_work(&self, req: WorkResult) -> Result<()> {
        self.core.finish_work(req)
    }

    pub async fn work_error(&self, req: WorkError) -> Result<()> {
        self.core.work_error(req).await
    }
}

struct SearcherInterface {
    core: Arc<HouseCore>
}

impl SearcherInterface {
    pub fn new(core: Arc<HouseCore>) -> Self {
        Self{core}
    }

    pub async fn initialize_search(&self, req: SearchRequest) -> Result<InternalSearchStatus> {
        self.core.initialize_search(req).await
    }

    pub async fn search_status(&self, code: String) -> Result<Option<InternalSearchStatus>> {
        self.core.search_status(code).await
    }

    // pub async fn tag_prompt(&self, req: PromptQuery) -> Result<PromptResult> {
    //     self.core.database.tag_prompt(req).await
    // }
}

#[derive(Clone)]
struct IngestInterface {
    core: Arc<HouseCore>
}

impl IngestInterface {
    pub fn new(core: Arc<HouseCore>) -> Self {
        Self{core}
    }

    pub async fn ingest(&self, request: IngestRequest) -> Result<()> {
        let (send, recv) = oneshot::channel();
        let hash = hex::decode(request.hash)?;
        if hash.len() != 32 {
            return Err(anyhow::anyhow!("Invalid hash: wrong number of bytes"));
        }
        self.core.ingest_queue.send(crate::core::IngestMessage::IngestMessage(hash, request.access, request.expiry, send))?;
        return recv.await?;
    }
}


#[serde_as]
#[derive(Deserialize)]
pub struct SearchRequest {
    pub view: AccessControl,
    pub access: HashSet<String>,
    #[serde_as(as = "DisplayFromStr")]
    pub query: Query,
    pub group: String,
    pub yara_signature: String,
    pub start_date: Option<chrono::DateTime<chrono::Utc>>,
    pub end_date: Option<chrono::DateTime<chrono::Utc>>,
}

/// Explicitly not seralizable
pub struct InternalSearchStatus {
    pub view: AccessControl,
    pub resp: SearchRequestResponse
}

#[derive(Serialize)]
pub struct SearchRequestResponse {
    pub code: String,
    pub group: String,
    pub finished: bool,
    pub errors: Vec<String>,
    pub total_indices: u64,
    pub pending_indices: u64,
    pub pending_candidates: u64,
    pub hits: Vec<String>,
    pub truncated: bool,
}

#[handler]
async fn add_search(Data(interface): Data<&SearcherInterface>, Json(request): Json<SearchRequest>) -> Result<Json<SearchRequestResponse>> {
    if !request.view.can_access(&request.access) {
        return Err(anyhow::anyhow!("Search access too high."))
    }

    Ok(Json(interface.initialize_search(request).await?.resp))
}

#[derive(Serialize, Deserialize, Default)]
pub struct Access {
    pub access: HashSet<String>,
}


#[handler]
async fn search_status(Data(interface): Data<&SearcherInterface>, Path(code): Path<String>, query: Option<poem::web::Query<Access>>, body: Option<Json<Access>>) -> (StatusCode, Response) {
    let status = match interface.search_status(code).await {
        Ok(status) => status,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err}").into_response())
    };

    let status = match status {
        Some(status) => status,
        None => return (StatusCode::NOT_FOUND, "Search code not found.".into_response())
    };

    let access = match body {
        Some(body) => body.0,
        None => match query {
            Some(query) => query.0,
            None => Default::default()
        }
    };

    if !status.view.can_access(&access.access) {
        return (StatusCode::NOT_FOUND, "Search code not found.".into_response())
    }

    return (StatusCode::OK, Json(status.resp).into_response())
}

// pub struct ListRequest {

// }

// pub struct ListResponse {

// }

// #[handler]
// async fn list_searches(Data(interface): Data<&SearcherInterface>, query: Option<poem::web::Query<ListRequest>>, body: Option<Json<ListRequest>>) -> (StatusCode, Response) {
//     todo!()
// }


#[derive(Serialize, Deserialize)]
pub struct FilterTask {
    pub id: i64,
    pub search: String,
    pub filter_id: IndexID,
    pub filter_blob: BlobID,
    pub query: Query,
}

#[derive(Serialize, Deserialize)]
pub struct YaraTask {
    pub id: i64,
    pub search: String,
    pub yara_rule: String,
    pub hashes: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct WorkPackage {
    pub filter: Vec<FilterTask>,
    pub yara: Vec<YaraTask>
}

impl WorkPackage {
    pub fn is_empty(&self) -> bool {
        self.filter.is_empty() && self.yara.is_empty()
    }
}

#[derive(Serialize, Deserialize)]
pub struct WorkRequest {
    pub worker: String,
    pub cached_filters: HashSet<BlobID>
}

#[handler]
async fn get_work(Data(interface): Data<&WorkerInterface>, Json(request): Json<WorkRequest>) -> Result<Json<WorkPackage>> {
    Ok(Json(interface.get_work(request).await?))
}

#[derive(Serialize, Deserialize)]
pub enum WorkResultValue {
    Filter(IndexID, BlobID, Vec<u64>),
    Yara(Vec<Vec<u8>>),
}

#[derive(Serialize, Deserialize)]
pub struct WorkResult {
    pub id: i64,
    pub search: String,
    pub value: WorkResultValue
}

#[derive(Serialize, Deserialize)]
pub enum WorkError {
    Yara(i64, String),
    Filter(i64, String),
}

#[handler]
fn finish_work(Data(interface): Data<&WorkerInterface>, Json(request): Json<WorkResult>) -> Result<()> {
    interface.finish_work(request)
}

#[handler]
async fn work_error(Data(interface): Data<&WorkerInterface>, Json(error): Json<WorkError>) -> Result<()> {
    interface.work_error(error).await
}

#[derive(Serialize, Deserialize)]
pub struct IngestRequest {
    #[serde(default)]
    token: Option<String>,
    hash: String,
    #[serde(default = "default_blocking")]
    block: bool,
    access: crate::access::AccessControl,
    #[serde(with = "ts_seconds_option")]
    expiry: Option<chrono::DateTime<chrono::Utc>>
}
fn default_blocking() -> bool { true }

#[derive(Serialize, Deserialize)]
pub struct IngestResponse {
    token: Option<String>,
    hash: String,
    success: bool,
    error: String,
}


#[handler]
async fn insert_sha(Data(interface): Data<&IngestInterface>, Json(request): Json<IngestRequest>) -> Result<()> {
    if request.block {
        interface.ingest(request).await
    } else {
        let interface = interface.clone();
        tokio::spawn(async move {
            _ = interface.ingest(request).await;
        });
        return Ok(())
    }
}

#[handler]
async fn ingest_stream(Data(interface): Data<&IngestInterface>, ws: poem::web::websocket::WebSocket) -> impl IntoResponse {
    info!("Starting new websocket ingester");
    let interface = interface.clone();
    ws.protocols(vec!["ingest-stream"])
    .on_upgrade(|mut socket| async move {
        tokio::spawn(async move {
            let mut active: JoinSet<(String, Option<String>, Result<()>)> = JoinSet::new();
            loop {
                tokio::select! {
                    message = socket.next() => {
                        let message = match message {
                            Some(Ok(message)) => message,
                            Some(Err(err)) => {
                                error!("Websocket error: {err}");
                                break
                            },
                            None => {
                                info!("Websocket closed");
                                break
                            }
                        };

                        let message = match message {
                            Message::Text(message) => serde_json::from_str::<IngestRequest>(&message),
                            Message::Binary(message) => serde_json::from_slice::<IngestRequest>(&message),
                            Message::Ping(_) => continue,
                            Message::Pong(_) => continue,
                            Message::Close(_) => {
                                info!("Websocket going to close");
                                break
                            },
                        };

                        let mut message: IngestRequest = match message {
                            Ok(request) => request,
                            Err(err) => {
                                let response = IngestResponse {
                                    token: None,
                                    hash: Default::default(),
                                    success: false,
                                    error: format!("Could not decode message: {err}")
                                };

                                if let Ok(response) = serde_json::to_string(&response) {
                                    _ = socket.send(Message::Text(response)).await;
                                }
                                continue
                            }
                        };

                        let interface = interface.clone();
                        active.spawn(async move {
                            let hash = message.hash.clone();
                            let token = message.token.take();
                            let resp = interface.ingest(message).await;
                            return (hash, token, resp)
                        });
                    },
                    item = active.join_next(), if !active.is_empty() => {
                        let item = match item {
                            Some(Ok(item)) => item,
                            Some(Err(err)) => {
                                error!("JoinSet error: {err}");
                                continue
                            },
                            None => continue
                        };

                        let (hash, token, result) = item;
                        let response = match result {
                            Ok(()) => IngestResponse{
                                token,
                                hash,
                                error: Default::default(),
                                success: true
                            },
                            Err(err) => IngestResponse {
                                token,
                                hash,
                                error: format!("{}", err),
                                success: false
                            },
                        };

                        let response = match serde_json::to_string(&response) {
                            Ok(buffer) => buffer,
                            Err(err) => {
                                error!("Serialization error: {err}");
                                continue
                            }
                        };
                        if let Err(err) = socket.send(Message::Text(response)).await {
                            error!("WebSocket send error: {err}");
                        }
                    }
                };
            }
            info!("Stopping websocket ingester");
        })
    })
}

#[handler]
async fn get_status() -> Result<()> {
    return Ok(())
}

#[derive(Serialize, Deserialize)]
struct StatusReport {
    ingest: IngestStatus,
    active_filters: Vec<(IndexGroup, IndexID)>,
    file_counts: Vec<(IndexGroup, u64)>,
}

#[handler]
async fn get_detailed_status(interface: Data<&StatusInterface>) -> Result<Json<StatusReport>> {
    Ok(Json(interface.get_status().await?))
}

// #[derive(Serialize, Deserialize, Default)]
// pub struct PromptQuery {
//     tag: String,
//     value: Option<String>,
// }

// #[derive(Serialize, Deserialize)]
// pub struct PromptResult {
//     tag_suggestions: Vec<String>,
//     value_suggestions: Vec<String>,
// }

// #[handler]
// async fn tag_prompt(interface: Data<&SearcherInterface>, body: Option<Json<PromptQuery>>, query: Option<poem::web::Query<PromptQuery>>) -> Result<Json<PromptResult>> {
//     let query = match query {
//         Some(query) => query.0,
//         None => match body {
//             Some(query) => query.0,
//             None => Default::default()
//         }
//     };

//     Ok(Json(interface.tag_prompt(query).await?))
// }

pub async fn serve(bind_address: String, tls: Option<TLSConfig>, core: Arc<HouseCore>) {
    if let Err(err) = _serve(bind_address, tls, core).await {
        error!("Error with http interface: {err} {}", err.root_cause());
    }
}

pub async fn _serve(bind_address: String, tls: Option<TLSConfig>, core: Arc<HouseCore>) -> Result<(), anyhow::Error> {
    let app = Route::new()
        .at("/search/", post(add_search))
        .at("/search/:code", get(search_status))
        // .at("/search/", get(list_searches))
        // .at("/tags/prompt", get(tag_prompt))
        .at("/work/", get(get_work))
        .at("/work/finished/", post(finish_work))
        .at("/work/error/", post(work_error))
        .at("/ingest/sha256/", post(insert_sha))
        .at("/ingest/stream/", get(ingest_stream))
        .at("/status/online", get(get_status))
        .at("/status/detailed", get(get_detailed_status))

        .with(TokenMiddleware::new(core.clone()))
        .with(LoggerMiddleware);

    let listener = TcpListener::bind(bind_address);
    let tls_config = match tls {
        Some(tls) => {
            OpensslTlsConfig::new()
                .cert_from_data(tls.certificate_pem)
                .key_from_data(tls.key_pem)
        },
        None => {
            use openssl::{rsa::Rsa, x509::X509, pkey::PKey, asn1::{Asn1Integer, Asn1Time}, bn::BigNum};

            // Generate our keypair
            let key = Rsa::generate(1 << 11)?;
            let pkey = PKey::from_rsa(key)?;

            // Use that keypair to sign a certificate
            let mut builder = X509::builder()?;

            // Set serial number to 1
            let one = BigNum::from_u32(1)?;
            let serial_number = Asn1Integer::from_bn(&one)?;
            builder.set_serial_number(&serial_number)?;

            // set subject/issuer name
            let mut name = openssl::x509::X509NameBuilder::new()?;
            name.append_entry_by_text("C", "CA")?;
            name.append_entry_by_text("ST", "ON")?;
            name.append_entry_by_text("O", "Inside the house")?;
            name.append_entry_by_text("CN", "localhost")?;
            let name = name.build();
            builder.set_issuer_name(&name)?;
            builder.set_subject_name(&name)?;

            // Set not before/after
            let not_before = Asn1Time::from_unix((chrono::Utc::now() - chrono::Duration::days(1)).timestamp())?;
            builder.set_not_before(&not_before)?;
            let not_after = Asn1Time::from_unix((chrono::Utc::now() + chrono::Duration::days(366)).timestamp())?;
            builder.set_not_after(&not_after)?;

            // set public key
            builder.set_pubkey(&pkey)?;

            // sign and build
            builder.sign(&pkey, openssl::hash::MessageDigest::sha256()).context("Could not sign certificate.")?;
            let cert = builder.build();

            OpensslTlsConfig::new()
                .cert_from_data(cert.to_pem().context("Could not extract self signed certificate")?)
                .key_from_data(pkey.rsa()?.private_key_to_pem()?)
        }
    };
    let listener = listener.openssl_tls(tls_config);

    Server::new(listener)
        .run(app)
        .await.context("Error in server runtime.")?;
    Ok(())
}