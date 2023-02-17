use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Result, Context};
use poem::{handler, Route, get, EndpointExt, Server};
use poem::listener::{TcpListener, OpensslTlsConfig, Listener};
use poem::web::{Data, Json};
use tokio::sync::mpsc;

use crate::config::TLSConfig;
use crate::interface::LoggerMiddleware;
use crate::worker::StatusReport;

use super::manager::WorkerMessage;

type Connection = mpsc::UnboundedSender<WorkerMessage>;

#[handler]
fn get_status() -> Result<()> {
    return Ok(())
}

#[handler]
async fn get_detailed_status(interface: Data<&Connection>) -> Result<Json<StatusReport>> {

    let (send, recv) = tokio::sync::oneshot::channel();
    if let Err(_) = interface.send(WorkerMessage::Status(send)) {
        return Err(anyhow::anyhow!("Worker manager not reachable"))
    }

    Ok(Json(recv.await?))
}

pub async fn serve(bind_address: SocketAddr, tls: Option<TLSConfig>, core: Connection, exit: Arc<tokio::sync::Notify>) -> Result<(), anyhow::Error> {
    let app = Route::new()
        .at("/status/online", get(get_status))
        .at("/status/detailed", get(get_detailed_status))
        .data(core)
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