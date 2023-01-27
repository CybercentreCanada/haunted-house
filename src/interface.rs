

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Instant, Duration};
use anyhow::Result;

use chrono::serde::ts_seconds_option;
use log::{error, debug};
use poem::{post, EndpointExt, Endpoint, Middleware, Request, FromRequest};
use poem::web::{TypedHeader, Data, Json};
use poem::web::headers::Authorization;
use poem::web::headers::authorization::Bearer;
use poem::{get, handler, listener::TcpListener, web::Path, Route, Server};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::auth::Role;
use crate::core::HouseCore;
use crate::database::{BlobID, IndexID};
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

        // call the next endpoint.
        self.ep.call(req).await
    }
}


struct LoggerMiddleware;

impl<E: Endpoint> Middleware<E> for LoggerMiddleware {
    type Output = LoggerMiddlewareImpl<E>;

    fn transform(&self, ep: E) -> Self::Output {
        LoggerMiddlewareImpl { ep }
    }
}

struct LoggerMiddlewareImpl<E> {
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

    pub async fn initialize_search(&self, req: SearchRequest) -> Result<SearchRequestResponse> {
        self.core.initialize_search(req).await
    }

    pub async fn search_status(&self, code: String) -> Result<SearchRequestResponse> {
        self.core.search_status(code).await
    }
}

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



#[derive(Deserialize)]
pub struct SearchRequest {
    pub access: HashSet<String>,
    pub query: Query,
    pub yara_signature: String,
    pub start_date: Option<chrono::DateTime<chrono::Utc>>,
    pub end_date: Option<chrono::DateTime<chrono::Utc>>,
}


#[derive(Serialize)]
pub struct SearchRequestResponse {
    pub code: String,
    pub finished: bool,
    pub errors: Vec<String>,
    pub pending_indices: u64,
    pub pending_candidates: u64,
    pub hits: Vec<String>,
    pub truncated: bool,
}

#[handler]
async fn add_search(Data(interface): Data<&SearcherInterface>, Json(request): Json<SearchRequest>) -> Result<Json<SearchRequestResponse>> {
    Ok(Json(interface.initialize_search(request).await?))
}

#[handler]
async fn search_status(Data(interface): Data<&SearcherInterface>, Path(code): Path<String>) -> Result<Json<SearchRequestResponse>> {
    Ok(Json(interface.search_status(code).await?))
}

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
    hash: String,
    access: crate::access::AccessControl,
    #[serde(with = "ts_seconds_option")]
    expiry: Option<chrono::DateTime<chrono::Utc>>
}

#[handler]
async fn insert_sha(Data(interface): Data<&IngestInterface>, Json(request): Json<IngestRequest>) -> Result<()> {
    interface.ingest(request).await
}

pub async fn serve(bind_address: String, core: Arc<HouseCore>) {
    if let Err(err) = _serve(bind_address, core).await {
        error!("Error with http interface: {err}");
    }
}

pub async fn _serve(bind_address: String, core: Arc<HouseCore>) -> Result<(), std::io::Error> {
    let app = Route::new()
        .at("/search/", post(add_search))
        .at("/search/:code", get(search_status))
        .at("/work/", get(get_work))
        .at("/work/finished/", post(finish_work))
        .at("/work/error/", post(work_error))
        .at("/insert/sha256/", post(insert_sha))

        .with(TokenMiddleware::new(core))
        .with(LoggerMiddleware);

    Server::new(TcpListener::bind(bind_address))
        .run(app)
        .await
}