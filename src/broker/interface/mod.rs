//!
//! Http interface presented by the hauntedhouse service to other services.
//!
//! POST /search
//! WS GET /search/<code>
//! GET /status
//! GET /status/detailed

use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{Result, Context};

use chrono::{DateTime, Utc};
use futures::SinkExt;
use log::{error, info};
use poem::listener::{Listener, OpensslTlsConfig};
use poem::web::websocket::Message;
use poem::{post, EndpointExt, Endpoint, Middleware, Request, FromRequest, IntoResponse};
use poem::web::{TypedHeader, Data, Json};
use poem::web::headers::Authorization;
use poem::web::headers::authorization::Bearer;
use poem::{get, handler, listener::TcpListener, web::Path, Route, Server};
use serde::{Deserialize, Serialize};
use assemblyline_models::ClassificationString;
use assemblyline_models::datastore::retrohunt as models;
use tokio::sync::watch;

use crate::config::TLSConfig;
use crate::logging::LoggerMiddleware;
use crate::timing::ResourceReport;
use crate::types::{WorkerID, FilterID};
use crate::worker::interface::StorageStatus;

use super::{FetchStatus, HouseCore, IngestCheckStatus, IngestWatchStatus, RepeatOutcome};
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

        // always allow status
        // if !roles.is_empty() {
        req.extensions_mut().insert(StatusInterface::new(self.core.clone()));
        // }

        // call the next endpoint.
        self.ep.call(req).await
    }
}

/// An internal interface that allows access to system level status information.
/// This interface is available as long as any role is authenticated.
#[derive(Clone)]
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
#[derive(Clone)]
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
    pub async fn initialize_search(&self, req: SearchRequest) -> Result<String> {
        self.core.initialize_search(req).await
    }

    /// Get the status or results for a given search
    pub async fn search_status(&self, code: &str) -> Result<Option<watch::Receiver<SearchProgress>>> {
        let searches = self.core.running_searches.read().await;
        if let Some(socket) = searches.get(code).map(|entry|entry.1.clone()) {
            return Ok(Some(socket))
        }
        if let Some((search, _)) = self.core.database.retrohunt.get(code).await? {
            let (_, mut reciever) = watch::channel(SearchProgress::Finished { key: code.to_owned(), search });
            reciever.mark_changed();
            return Ok(Some(reciever))
        }
        Ok(None)
    }

    /// Rerun a search without creating a new record
    pub async fn repeat_search(&self, key: &str, classification: ClassificationString, expiry: Option<DateTime<Utc>>) -> Result<RepeatOutcome> {
        self.core.repeat_search(key, classification, expiry).await
    }
}


/// Parameters to request a retrohunt search
#[derive(Deserialize)]
pub struct SearchRequest {
    /// Classification for the retrohunt job
    pub classification: ClassificationString,
    /// Maximum classification of results in the search
    pub search_classification: ClassificationString,

    /// User who created this retrohunt job
    pub creator: String,
    /// Human readable description of this retrohunt job
    pub description: String,

    /// Expiry timestamp of this retrohunt job
    pub expiry_ts: Option<DateTime<Utc>>,

    /// What sorts of indices to run on
    pub indices: models::IndexCatagory,

    /// Text of original yara signature run
    pub yara_signature: String,
}

/// Response when a search has been successfully added
#[derive(Serialize)]
struct AddSearchResponse {
    /// Code identifying the search
    pub code: String,
}

/// API endpoint for starting a new search
#[handler]
async fn add_search(Data(interface): Data<&SearcherInterface>, Json(request): Json<SearchRequest>) -> poem::Result<Json<AddSearchResponse>> {
    info!("classification enabled: {}", assemblyline_markings::get_default().unwrap().original_definition.enforce);
    Ok(Json(AddSearchResponse{
        code: interface.initialize_search(request).await?
    }))
}


/// Parameters to request a retrohunt search
#[derive(Deserialize)]
pub struct RepeatSearchRequest {
    /// key of search to repeat
    pub key: String,
    /// Maximum classification of results in the search
    pub search_classification: ClassificationString,
    pub expiry: Option<DateTime<Utc>>,
}

/// API endpoint for starting a new search
#[handler]
async fn repeat_search(Data(interface): Data<&SearcherInterface>, Json(request): Json<RepeatSearchRequest>) -> poem::Result<http::StatusCode> {
    Ok(match interface.repeat_search(&request.key, request.search_classification, request.expiry).await? {
        RepeatOutcome::Started => http::StatusCode::OK,
        RepeatOutcome::NotFound => http::StatusCode::NOT_FOUND,
        RepeatOutcome::AlreadyRunning => http::StatusCode::CONFLICT,
    })
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize)]
#[serde(tag="type", rename="lowercase")]
pub (crate) enum SearchProgress {
    Starting {
        key: String,
    },
    Filtering {
        key: String,
        progress: f64, 
    },
    Yara {
        key: String,
        progress: f64, 
    },
    Finished {
        key: String,
        search: models::Retrohunt
    },
}

/// Endpoint that providesa stream of status messages
#[handler]
async fn search_status(Data(interface): Data<&SearcherInterface>, Path(code): Path<String>, ws: poem::web::websocket::WebSocket) -> poem::Result<impl IntoResponse> {
    let mut feed = match interface.search_status(&code).await? {
        Some(feed) => feed,
        None => return Err(poem::http::StatusCode::NOT_FOUND.into()),
    };

    Ok(ws.protocols(vec!["search-progress"])
    .on_upgrade(|mut socket| async move {
        loop {
            let (status, finished) = {
                let value = feed.borrow_and_update();
                let finished = matches!(&*value, SearchProgress::Finished {..});
                match serde_json::to_string(&*value) {
                    Ok(message) => (message, finished),
                    Err(_) => break,
                }
            };
            if socket.send(Message::Text(status)).await.is_err() {
                break
            }
            if finished {
                break
            }
            if feed.changed().await.is_err() {
                break;
            }
        }
        _ = socket.close().await;
    }))
}

/// API endpoint for null status that is always available
#[handler]
async fn get_status() -> Result<()> {
    return Ok(())
}

/// Response body from the status endpoint
#[derive(Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub (crate) struct FilterStatus {
    pub expiry: String,
    pub worker: WorkerID,
    pub id: FilterID,
    pub size: u64,
}

/// Response body from the status endpoint
#[derive(Serialize, Deserialize)]
pub (crate) struct StatusReport {
    /// Status of the check stage of ingestion
    pub ingest_check: IngestCheckStatus,
    pub fetcher: FetchStatus,
    /// Status for the per-worker ingestion watcher
    pub ingest_watchers: HashMap<WorkerID, HashMap<FilterID, IngestWatchStatus>>,
    /// Number of active searches
    pub active_searches: u32,
    /// Number of pending ingest files that haven't been assigned to a worker yet
    pub pending_tasks: HashMap<String, u32>,
    /// Information about filters
    pub filters: Vec<FilterStatus>,
    /// Storage information
    pub storage: HashMap<WorkerID, StorageStatus>,
    pub timeouts: Vec<String>,
    pub resources: HashMap<String, ResourceReport>,
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
        .at("/repeat/", post(repeat_search))
        .at("/status", get(get_status))
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