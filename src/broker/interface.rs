//!
//! Http interface presented by the hauntedhouse service to other services.
//!
use std::collections::{HashSet, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use anyhow::{Result, Context};

use chrono::serde::ts_seconds_option;
use futures::{StreamExt, SinkExt};
use log::{error, info};
use poem::listener::{Listener, OpensslTlsConfig};
use poem::web::websocket::Message;
use poem::{post, EndpointExt, Endpoint, Middleware, Request, FromRequest, IntoResponse, Response};
use poem::web::{TypedHeader, Data, Json};
use poem::http::StatusCode;
use poem::web::headers::Authorization;
use poem::web::headers::authorization::Bearer;
use poem::{get, handler, listener::TcpListener, web::Path, Route, Server};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use crate::access::AccessControl;
use crate::config::TLSConfig;
use crate::logging::LoggerMiddleware;
use crate::query::Query;
use crate::types::{FileInfo, Sha256, ExpiryGroup, WorkerID, FilterID};
use crate::worker::interface::StorageStatus;

use super::{HouseCore, IngestTask, IngestCheckStatus, IngestWatchStatus};
use super::auth::Role;

/// Extractor for HTTP Header holding a bearer token in the Authorization field
type BearerToken = TypedHeader<Authorization<Bearer>>;

/// A middleware that extracts authentication tokens from the request and
/// gives access to the corresponding internal apis.
struct TokenMiddleware {
    /// Pointer to the common data for the broker server
    core: Arc<HouseCore>,
}

impl TokenMiddleware {
    /// Create a new middleware instance for the given server
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

/// Implementation details for the TokenMiddleware
struct TokenMiddlewareImpl<E> {
    /// Endpoint wrapped by this middleware
    ep: E,
    /// Reference to the broker server data
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

        // if roles.contains(&Role::Worker) {
        //     req.extensions_mut().insert(WorkerInterface::new(self.core.clone()));
        // }

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

/// An internal interface that allows access to system level status information.
/// This interface is available as long as any role is authenticated.
struct StatusInterface {
    /// Pointer to server data
    core: Arc<HouseCore>
}

impl StatusInterface {
    /// Create a new status API wrapper around the broker core
    pub fn new(core: Arc<HouseCore>) -> Self {
        Self{core}
    }

    /// Request system status from the broker core
    pub async fn get_status(&self) -> Result<StatusReport> {
        self.core.status().await
    }
}

/// An internal interface that allows access to search operations.
/// This interface is tied to the 'Search' role.
struct SearcherInterface {
    /// Pointer to server data
    core: Arc<HouseCore>
}

impl SearcherInterface {
    /// Create a new search API wrapper around the broker core
    pub fn new(core: Arc<HouseCore>) -> Self {
        Self{core}
    }

    /// Start a new search
    pub async fn initialize_search(&self, req: SearchRequest) -> Result<InternalSearchStatus> {
        self.core.initialize_search(req).await
    }

    /// Get the status or results for a given search
    pub async fn search_status(&self, code: String) -> Result<Option<InternalSearchStatus>> {
        self.core.search_status(code).await
    }

    pub fn parse_user_classification(&self, classification: &str) -> Result<HashSet<String>> {
        self.core.prepare_access(classification)
    }
}

/// An internal interface that allows access to ingestion operations.
/// This interface is tied to the 'Ingest' role.
#[derive(Clone)]
struct IngestInterface {
    /// Pointer to server data
    core: Arc<HouseCore>
}

impl IngestInterface {
    /// Create a new ingest API wrapper around the broker core
    pub fn new(core: Arc<HouseCore>) -> Self {
        Self{core}
    }

    /// Add a file to the search database. Waits for ingestion to finish.
    pub async fn ingest(&self, request: IngestRequest) -> Result<()> {
        let (send, recv) = oneshot::channel();
        let hash = Sha256::from_str(&request.hash)?;
        self.core.ingest_queue.send(super::IngestMessage::IngestMessage(IngestTask{
            info: FileInfo{
                hash,
                access: request.access,
                expiry: ExpiryGroup::create(&request.expiry)
            },
            response: vec![send]
        }))?;
        return recv.await?;
    }
}

/// Flag for reporting search status
#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchProgress {
    /// The search is not finished, but no worker was available to report status
    Unknown,
    /// The search is currently being run through filters to establish a candidate file list
    Filtering,
    /// The candidate files are being run through yara
    Yara,
    /// The search has been completed
    Finished
}

/// Request format for starting searches as coming in from http
/// Don't use this internally, use SearchRequest
#[serde_as]
#[derive(Deserialize)]
pub struct RawSearchRequest {
    /// Control governing who can see this search
    pub view: AccessControl,
    /// Classification string controlling which files this search can see
    pub classification: Option<String>,
    /// Access flags controlling which files this search can see
    pub access: Option<HashSet<String>>,
    /// filter query derived from yara signature
    #[serde_as(as = "DisplayFromStr")]
    pub query: Query,
    /// Yara signature searching with
    pub yara_signature: String,
    /// Earliest expiry date to be included in this search
    pub start_date: Option<chrono::DateTime<chrono::Utc>>,
    /// Latest expiry date to be included in this search
    pub end_date: Option<chrono::DateTime<chrono::Utc>>,
}

/// Search request normalized for internal processing
pub struct SearchRequest {
    /// Control governing who can see this search
    pub view: AccessControl,
    /// Access flags controlling which files this search can see
    pub access: HashSet<String>,
    /// filter query derived from yara signature
    pub query: Query,
    /// Yara signature searching with
    pub yara_signature: String,
    /// Earliest expiry date to be included in this search
    pub start_date: Option<chrono::DateTime<chrono::Utc>>,
    /// Latest expiry date to be included in this search
    pub end_date: Option<chrono::DateTime<chrono::Utc>>,
}


/// Explicitly not seralizable, the internal view used handling search status
/// reports before deciding whether to send it to the user.
pub struct InternalSearchStatus {
    /// The control governing who can see this search status
    pub view: AccessControl,
    /// The status report for this search
    pub resp: SearchRequestResponse
}

/// A user facing report on search status
#[derive(Serialize)]
pub struct SearchRequestResponse {
    /// Code identifying the search
    pub code: String,
    /// if the search has been completed
    pub finished: bool,
    /// list of errors encountered while running search
    pub errors: Vec<String>,
    /// which phase of the search is currently active
    pub phase: SearchProgress,
    /// progress measurements for the current phase only
    pub progress: (u64, u64),
    /// list of files found by this search
    pub hits: Vec<String>,
    /// flag indicating if the hits list has been truncated
    pub truncated: bool,
}

/// API endpoint for starting a new search
#[handler]
async fn add_search(Data(interface): Data<&SearcherInterface>, Json(request): Json<RawSearchRequest>) -> poem::Result<Json<SearchRequestResponse>> {
    if request.classification.is_some() == request.access.is_some() {
        return Err(poem::Error::from_string("Only one of 'classification' and 'access' may be set.", StatusCode::BAD_REQUEST))
    }

    let access = if let Some(request) = request.classification {
        interface.parse_user_classification(&request)?
    } else if let Some(access) = request.access {
        access
    } else {
        return Err(poem::Error::from_string("Only one of 'classification' and 'access' may be set.", StatusCode::BAD_REQUEST))
    };

    if !request.view.can_access(&access) {
        return Err(anyhow::anyhow!("Search access too high.").into())
    }

    let request = SearchRequest {
        view: request.view,
        access,
        query: request.query,
        yara_signature: request.yara_signature,
        start_date: request.start_date,
        end_date: request.end_date,
    };

    Ok(Json(interface.initialize_search(request).await?.resp))
}

/// Request body when requesting search status
#[derive(Serialize, Deserialize, Default)]
pub struct StatusRequestBody {
    /// Level of access to use in retreiving search status
    pub access: HashSet<String>,
}

/// API endpoint for fetching search status
#[handler]
async fn search_status(Data(interface): Data<&SearcherInterface>, Path(code): Path<String>, query: Option<poem::web::Query<StatusRequestBody>>, body: Option<Json<StatusRequestBody>>) -> (StatusCode, Response) {
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

/// Request body to ingest a file
#[derive(Serialize, Deserialize)]
pub struct IngestRequest {
    /// An opaque token returned with the response to ingesting this file.
    /// This is to help the client track which files have been ingested when
    /// using the websocket to communicate.
    #[serde(default)]
    token: Option<String>,
    /// SHA256 of the file to be ingested
    hash: String,
    /// Whether the query should block and return success or failure of the ingestion
    /// or return the moment the file has been added to an ingest queue
    #[serde(default = "default_blocking")]
    block: bool,
    /// What access control to apply to this file
    access: crate::access::AccessControl,
    /// When to expire this file from hauntedhouse
    #[serde(with = "ts_seconds_option")]
    expiry: Option<chrono::DateTime<chrono::Utc>>
}

/// default value for IngestRequest::block
fn default_blocking() -> bool { true }

/// Response body when ingesting a file
#[derive(Serialize, Deserialize)]
pub struct IngestResponse {
    /// opaque token provided with the request to ingest this file
    token: Option<String>,
    /// sha256 of file that has been ingested
    hash: String,
    /// whether this file has been ingested properly
    success: bool,
    /// error message if an error has occurred
    error: String,
}

/// API endpoint to ingest a single file
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

/// API endpoint to open a websocket for high volume file ingestion
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

/// API endpoint for null status that is always available
#[handler]
async fn get_status() -> Result<()> {
    return Ok(())
}

/// Response body from the status endpoint
#[derive(Serialize, Deserialize)]
pub (crate) struct StatusReport {
    /// Status of the check stage of ingestion
    pub ingest_check: IngestCheckStatus,
    /// Status for the per-worker ingestion watcher
    pub ingest_watchers: HashMap<WorkerID, HashMap<FilterID, IngestWatchStatus>>,
    /// Number of active searches
    pub active_searches: u32,
    /// Number of pending ingest files that haven't been assigned to a worker yet
    pub pending_tasks: HashMap<String, u32>,
    /// Information about filters
    pub filters: Vec<(String, WorkerID, FilterID, u64)>,
    /// Storage information
    pub storage: HashMap<WorkerID, StorageStatus>,
}

/// API endpoint for detailed system status
#[handler]
async fn get_detailed_status(interface: Data<&StatusInterface>) -> Result<Json<StatusReport>> {
    Ok(Json(interface.get_status().await?))
}


/// Function that serves the http API
pub async fn serve(bind_address: String, tls: Option<TLSConfig>, core: Arc<HouseCore>) {
    if let Err(err) = _serve(bind_address, tls, core).await {
        error!("Error with http interface: {err} {}", err.root_cause());
    }
}

/// Implementation detail for 'serve' method
pub async fn _serve(bind_address: String, tls: Option<TLSConfig>, core: Arc<HouseCore>) -> Result<(), anyhow::Error> {
    let app = Route::new()
        .at("/search/", post(add_search))
        .at("/search/:code", get(search_status))
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