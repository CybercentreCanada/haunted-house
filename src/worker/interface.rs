use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context};
use futures::{SinkExt, StreamExt};
use log::{error};
use poem::web::websocket::{WebSocket, Message};
use poem::{handler, Route, get, EndpointExt, Server, post, IntoResponse, put};
use poem::listener::{TcpListener, OpensslTlsConfig, Listener};
use poem::web::{Data, Json};
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;

use crate::config::TLSConfig;
use crate::logging::LoggerMiddleware;
use crate::query::Query;
use crate::types::{Sha256, ExpiryGroup, FileInfo, FilterID};
use crate::worker::YaraTask;

use super::manager::{WorkerState};

// use crate::config::TLSConfig;
// use crate::interface::LoggerMiddleware;
// use crate::worker::StatusReport;
// use super::manager::WorkerMessage;

// type Connection = mpsc::UnboundedSender<WorkerMessage>;

#[derive(Serialize, Deserialize)]
pub struct CreateIndexRequest {
    pub filter_id: FilterID,
    pub expiry: ExpiryGroup
}

#[handler]
async fn create_index(state: Data<&Arc<WorkerState>>, request: Json<CreateIndexRequest>) -> poem::Result<()> {
    state.create_index(request.filter_id, request.expiry.clone()).await?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct FilterSearchRequest {
    pub expiry_group_range: (ExpiryGroup, ExpiryGroup),
    pub query: Query,
    pub access: HashSet<String>
}

#[derive(Serialize, Deserialize)]
pub enum FilterSearchResponse {
    Filters(Vec<FilterID>),
    Candidates(FilterID, Vec<Sha256>),
    Error(Option<FilterID>, String),
}

impl std::fmt::Debug for FilterSearchResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Filters(items) => f.debug_tuple("Filters").field(&items.len()).finish(),
            Self::Candidates(filter, items) => f.debug_tuple("Candidates").field(filter).field(&items.len()).finish(),
            Self::Error(filter, error) => f.debug_tuple("Error").field(filter).field(error).finish(),
        }
    }
}

#[handler]
async fn run_filter_search(ws: WebSocket, state: Data<&Arc<WorkerState>>) -> impl IntoResponse {
    let state = state.clone();
    ws.on_upgrade(|mut socket| async move {
        // wait for our filter command
        let request: FilterSearchRequest = match socket.next().await {
            Some(Ok(request)) => if let Message::Text(text) = request {
                if let Ok(request) = serde_json::from_str(&text) {
                    request
                } else {
                    return
                }
            } else {
                return
            },
            Some(Err(err)) => {
                error!("Bad filter command: {err}");
                return
            }
            None => return,
        };

        // Gather the filters related to this query
        let filter_ids = match state.get_filters(&request.expiry_group_range.0, &request.expiry_group_range.1).await {
            Ok(filter_ids) => filter_ids,
            Err(_err) => {
                return
            }
        };
        let message = serde_json::to_string(&FilterSearchResponse::Filters(filter_ids.clone())).unwrap();
        socket.send(Message::Text(message)).await.unwrap();

        // Dispatch a query to each of those filters
        let mut recv = {
            let (send, recv) = mpsc::channel(128);
            // let mut queries = tokio::task::JoinSet::new();
            for filter_id in &filter_ids {
                let state = state.clone();
                tokio::spawn(state.query_filter(*filter_id, request.query.clone(), request.access.clone(), send.clone()));
            }
            recv
        };

        // Collect the results
        while let Some(mut message) = recv.recv().await {
            _ = socket.send(Message::Text(match serde_json::to_string(&message) {
                Ok(message) => message,
                Err(_) => continue
            })).await;
        }
        _ = socket.close().await;
    })
}


#[derive(Serialize, Deserialize)]
pub struct YaraSearchResponse {
    pub hits: Vec<Sha256>,
    pub errors: Vec<String>,
}

#[handler]
async fn run_yara_search(state: Data<&Arc<WorkerState>>, request: Json<YaraTask>) -> Json<YaraSearchResponse> {
    let (hits, errors) = match state.run_yara(request.0).await {
        Ok((hits, errors)) => (hits, errors),
        Err(err) => (vec![], vec![format!("yara task error: {err}")])
    };

    Json(YaraSearchResponse { hits, errors })
}

#[derive(Serialize, Deserialize)]
pub struct UpdateFileInfoRequest {
    pub files: Vec<FileInfo>,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateFileInfoResponse {
    pub processed: Vec<Sha256>,
    pub pending: HashMap<Sha256, FilterID>,
    // pub assignments: HashMap<Sha256, Vec<FilterID>>,
    // pub storage_pressure: bool,
    // pub filter_pending: HashMap<FilterID, HashSet<Sha256>>,
    // pub filters: HashMap<FilterID, (ExpiryGroup, u32)>,
    // pub filters: HashMap<FilterID, ExpiryGroup>,
}

#[handler]
async fn update_file_info(state: Data<&Arc<WorkerState>>, request: Json<UpdateFileInfoRequest>) -> poem::Result<Json<UpdateFileInfoResponse>> {
    Ok(Json(state.update_files(request.0.files).await?))
}

#[derive(Serialize, Deserialize)]
pub struct IngestFilesRequest {
    pub files: Vec<(FilterID, FileInfo)>
}

#[derive(Serialize, Deserialize)]
pub struct IngestFilesResponse {
    pub completed: Vec<Sha256>,
    // pub unknown_filters: Vec<FilterID>,
    pub filter_pending: HashMap<FilterID, HashSet<Sha256>>,
    pub filter_size: HashMap<FilterID, u64>,
    pub expiry_groups: HashMap<ExpiryGroup, Vec<FilterID>>,
    pub storage_pressure: bool,
}

#[handler]
async fn ingest_files(state: Data<&Arc<WorkerState>>, request: Json<IngestFilesRequest>) -> poem::Result<Json<IngestFilesResponse>> {
    Ok(Json(state.ingest_file(request.0.files).await?))
}

#[handler]
async fn list_ingest_files(state: Data<&Arc<WorkerState>>) -> poem::Result<Json<HashMap<FilterID, Vec<FileInfo>>>> {
    let expiries = match state.database.get_expiry(&ExpiryGroup::min(), &ExpiryGroup::max()).await {
        Ok(result) => result,
        Err(_err) => return Err(poem::http::StatusCode::INTERNAL_SERVER_ERROR.into())
    };
    let expiries: HashMap<FilterID, ExpiryGroup> = expiries.into_iter().collect();

    let mut result = HashMap::new();
    for (filter, hashes) in state.database.filter_pending().await? {
        let mut files = vec![];
        if let Some(expiry) = expiries.get(&filter) {
            for hash in hashes {
                match state.database.get_file_access(filter, &hash).await {
                    Ok(Some(access)) => {files.push(FileInfo { hash, access, expiry: expiry.clone() });}
                    Ok(None) => {},
                    Err(err) => {
                        error!("{err}");
                    }
                }
            }
        }
        result.insert(filter, files);
    }
    Ok(Json(result))
}

#[handler]
fn get_online_status() -> () {
    return ()
}

#[handler]
async fn get_ready_status(state: Data<&Arc<WorkerState>>) -> poem::http::StatusCode {
    match state.is_ready().await {
        Ok(ready) => if ready {
            poem::http::StatusCode::OK
        } else {
            poem::http::StatusCode::SERVICE_UNAVAILABLE
        },
        Err(_) => poem::http::StatusCode::INTERNAL_SERVER_ERROR
    }
}

#[derive(Serialize, Deserialize)]
pub (crate) struct StorageStatus {
    pub capacity: u64,
    pub high_water: u64,
    pub used: u64,
}

#[derive(Serialize, Deserialize)]
pub (crate) struct DetailedStatus {
    pub filters: Vec<(ExpiryGroup, FilterID, u64)>,
    pub storage: StorageStatus
}

#[handler]
async fn get_detail_status(state: Data<&Arc<WorkerState>>) -> poem::Result<Json<DetailedStatus>> {
    // Collect filter info
    let filter_expiry = state.database.get_expiry(&ExpiryGroup::min(), &ExpiryGroup::max()).await?;
    let filter_size = state.database.filter_sizes().await?;
    let mut filters = vec![];
    for (filter, expiry) in filter_expiry {
        filters.push((expiry, filter, *filter_size.get(&filter).unwrap_or(&0)));
    }

    Ok(Json(DetailedStatus {
        filters,
        storage: state.storage_status().await?
    }))
}

pub async fn serve(bind_address: SocketAddr, tls: Option<TLSConfig>, state: Arc<WorkerState>, exit: Arc<tokio::sync::Notify>) -> anyhow::Result<()> {
    let app = Route::new()
        .at("/index/create", put(create_index))
        .at("/search/filter", get(run_filter_search))
        .at("/search/yara", get(run_yara_search))
        .at("/files/update", post(update_file_info))
        .at("/files/ingest", post(ingest_files))
        .at("/files/ingest-queues", get(list_ingest_files))
        .at("/status/online", get(get_online_status))
        .at("/status/ready", get(get_ready_status))
        .at("/status/detail", get(get_detail_status))
        .data(state)
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
        .run_with_graceful_shutdown(app, exit.notified(), None)
        .await.context("Error in http runtime.")?;
    Ok(())
}