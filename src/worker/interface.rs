use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context};
use poem::{handler, Route, get, EndpointExt, Server, delete};
use poem::listener::{TcpListener, OpensslTlsConfig, Listener};
use poem::web::{Data, Json};
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;

use crate::access::AccessControl;
use crate::config::TLSConfig;
use crate::logging::LoggerMiddleware;
use crate::query::Query;
use crate::types::{Sha256, ExpiryGroup, FileInfo};
use crate::worker::YaraTask;

use super::manager::WorkerState;

// use crate::config::TLSConfig;
// use crate::interface::LoggerMiddleware;
// use crate::worker::StatusReport;
// use super::manager::WorkerMessage;

// type Connection = mpsc::UnboundedSender<WorkerMessage>;

#[handler]
fn create_index() -> poem::Result<()> {
    todo!();
}

#[handler]
fn delete_index() -> poem::Result<()> {
    todo!();
}

#[derive(Serialize, Deserialize)]
pub struct FilterSearchRequest {
    expiry_group_range: (ExpiryGroup, ExpiryGroup),
    query: Query,
    access: AccessControl
}

#[derive(Serialize, Deserialize)]
pub struct FilterSearchResponse {
    candidates: Vec<Sha256>,
    errors: Vec<String>,
}

#[handler]
async fn run_filter_search(state: Data<&Arc<WorkerState>>, request: Json<FilterSearchRequest>) -> Json<FilterSearchResponse> {
    // Gather the filters related to this query
    let filter_ids = match state.get_filters(&request.expiry_group_range.0, &request.expiry_group_range.1).await {
        Ok(filter_ids) => filter_ids,
        Err(err) => {
            return Json(FilterSearchResponse { candidates: vec![], errors: vec![err.to_string()] })
        }
    };

    // Dispatch a query to each of those filters
    let mut queries = tokio::task::JoinSet::new();
    for filter_id in filter_ids {
        let state = state.clone();
        queries.spawn(state.query_filter(filter_id, request.query.clone(), request.access.clone()));
    }

    // Collect the results
    let mut candidates = BTreeSet::new();
    let mut errors = vec![];
    while let Some(result) = queries.join_next().await {
        match result {
            Ok(Ok(values)) => {
                candidates.extend(values.into_iter());
            },
            Ok(Err(err)) => {
                errors.push(format!("Search error: {err}"));
            },
            Err(err) => {
                errors.push(format!("Task join error: {err}"));
            }
        }
    }

    // Build the response
    return Json(FilterSearchResponse {
        candidates: candidates.into_iter().collect(),
        errors
    })
}


#[derive(Serialize, Deserialize)]
pub struct YaraSearchResponse {
    hits: Vec<Sha256>,
    errors: Vec<String>,
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
    files: Vec<FileInfo>,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateFileInfoResponse {
    proccessed: Vec<Sha256>,
    in_progress: Vec<Sha256>
}

#[handler]
fn update_file_info(state: Data<&Arc<WorkerState>>, request: Json<UpdateFileInfoRequest>) -> Json<UpdateFileInfoResponse> {
    todo!();
}

#[handler]
fn ingest_file() -> poem::Result<()> {
    todo!();
}

#[handler]
fn get_status() -> () {
    return ()
}

#[derive(Serialize, Deserialize)]
pub struct DetailedStatusResponse {

}

#[handler]
async fn get_detailed_status(state: Data<&Arc<WorkerState>>) -> poem::Result<Json<DetailedStatusResponse>> {
    todo!();
    // let (send, recv) = tokio::sync::oneshot::channel();
    // if let Err(_) = interface.send(WorkerMessage::Status(send)) {
    //     return Err(anyhow::anyhow!("Worker manager not reachable"))
    // }

    // Ok(Json(recv.await?))
}

pub async fn serve(bind_address: SocketAddr, tls: Option<TLSConfig>, state: Arc<WorkerState>, exit: Arc<tokio::sync::Notify>) -> anyhow::Result<()> {
    let app = Route::new()
        .at("/index/create", get(create_index))
        .at("/index/:id", delete(delete_index))
        .at("/search/filter", get(run_filter_search))
        .at("/search/yara", get(run_yara_search))
        .at("/file/update", post(update_file_info))
        .at("/file/ingest", get(ingest_file))
        .at("/status/online", get(get_status))
        .at("/status/detailed", get(get_detailed_status))
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