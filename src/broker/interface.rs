

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
use poem::http::{StatusCode};
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

use super::{HouseCore, IngestTask, IngestCheckStatus, IngestWatchStatus};
use super::auth::Role;

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



struct StatusInterface {
    core: Arc<HouseCore>
}

impl StatusInterface {
    pub fn new(core: Arc<HouseCore>) -> Self {
        Self{core}
    }

    pub async fn get_status(&self) -> Result<StatusReport> {
        Ok(self.core.status().await?)
    }
}


// struct WorkerInterface {
//     core: Arc<HouseCore>
// }

// impl WorkerInterface {
//     pub fn new(core: Arc<HouseCore>) -> Self {
//         Self{core}
//     }

//     pub async fn get_work(&self, req: WorkRequest) -> Result<WorkPackage> {
//         let start = std::time::Instant::now();
//         loop {
//             let work = self.core.get_work(&req).await?;
//             if !work.is_empty() {
//                 return Ok(work);
//             }

//             let wait_max = match Duration::from_secs(30).checked_sub(start.elapsed()) {
//                 Some(wait) => wait,
//                 None => return Ok(Default::default()),
//             };

//             if let Err(_) = tokio::time::timeout(wait_max, self.core.get_work_notification()).await {
//                 return Ok(Default::default())
//             }
//         }
//     }

//     pub fn finish_work(&self, req: WorkResult) -> Result<()> {
//         self.core.finish_work(req)
//     }

//     pub async fn work_error(&self, req: WorkError) -> Result<()> {
//         self.core.work_error(req).await
//     }
// }

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


#[serde_as]
#[derive(Deserialize)]
pub struct SearchRequest {
    pub view: AccessControl,
    pub access: HashSet<String>,
    #[serde_as(as = "DisplayFromStr")]
    pub query: Query,
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
    pub finished: bool,
    pub errors: Vec<String>,
    // pub total_indices: u64,
    // pub pending_indices: u64,
    // pub pending_candidates: u64,
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
pub struct StatusReport {
    pub ingest_check: IngestCheckStatus,
    pub ingest_watchers: HashMap<WorkerID, HashMap<FilterID, IngestWatchStatus>>,
    pub active_searches: u32,
    pub pending_tasks: HashMap<ExpiryGroup, u32>,
}

#[handler]
async fn get_detailed_status(interface: Data<&StatusInterface>) -> Result<Json<StatusReport>> {
    Ok(Json(interface.get_status().await?))
}


pub async fn serve(bind_address: String, tls: Option<TLSConfig>, core: Arc<HouseCore>) {
    if let Err(err) = _serve(bind_address, tls, core).await {
        error!("Error with http interface: {err} {}", err.root_cause());
    }
}

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