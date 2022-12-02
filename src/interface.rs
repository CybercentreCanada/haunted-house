

use std::sync::Arc;
use anyhow::Result;

use poem::http::StatusCode;
use poem::middleware::AddData;
use poem::{post, EndpointExt, Response};
use poem::web::{TypedHeader, Data, Json};
use poem::web::headers::Authorization;
use poem::web::headers::authorization::Bearer;
use poem::{get, handler, listener::TcpListener, web::Path, IntoResponse, Route, Server};
use serde::{Deserialize, Serialize};

use crate::access::AccessControl;
use crate::auth::Role;
use crate::core::HouseCore;
use crate::query::Query;

type BearerToken = TypedHeader<Authorization<Bearer>>;

#[derive(Clone)]
struct TokenCheck {
    core: Arc<HouseCore>
}

impl TokenCheck {
    pub fn new(core: Arc<HouseCore>) -> Self {
        Self {
            core
        }
    }

    pub fn authenticate_worker(&self, token: &str) -> Option<WorkerInterface> {
        if self.core.authenticator.is_role_assigned(token, Role::Worker) {
            Some(WorkerInterface { core: self.core.clone() })
        } else {
            None
        }
    }

    pub fn authenticate_searcher(&self, token: &str) -> Option<SearcherInterface> {
        if self.core.authenticator.is_role_assigned(token, Role::Search) {
            Some(SearcherInterface { core: self.core.clone() })
        } else {
            None
        }
    }
}


struct WorkerInterface {
    core: Arc<HouseCore>
}

struct SearcherInterface {
    core: Arc<HouseCore>
}

impl SearcherInterface {
    pub async fn initialize_search(&self, req: SearchRequest) -> Result<SearchRequestResponse> {
        self.core.initialize_search(req).await
    }

    pub async fn search_status(&self, code: String) -> Result<SearchRequestResponse> {
        self.core.search_status(code).await
    }
}


#[derive(Deserialize)]
pub struct SearchRequest {
    pub access: AccessControl,
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
}

#[handler]
async fn add_search(TypedHeader(auth): BearerToken, back: Data<&TokenCheck>, Json(request): Json<SearchRequest>) -> Result<Response> {
    let interface = match back.authenticate_searcher(auth.token()) {
        Some(interface) => interface,
        None => return Ok(StatusCode::FORBIDDEN.into())
    };

    let status: SearchRequestResponse = interface.initialize_search(request).await?;
    Ok(Json(status).into_response())
}

#[handler]
async fn search_status(TypedHeader(auth): BearerToken, back: Data<&TokenCheck>, Path(code): Path<String>) -> Result<Response> {
    let interface = match back.authenticate_searcher(auth.token()) {
        Some(interface) => interface,
        None => return Ok(StatusCode::FORBIDDEN.into())
    };

    let status: SearchRequestResponse = interface.search_status(code).await?;
    Ok(Json(status).into_response())
}

#[derive(Deserialize)]
struct WorkRequest {

}

#[handler]
fn get_work(TypedHeader(auth): BearerToken, back: Data<&TokenCheck>, Json(request): Json<WorkRequest>) -> String {
    format!("hello: {}", auth.token())
}

#[derive(Deserialize)]
struct WorkResult {

}

#[handler]
fn finish_work(TypedHeader(auth): BearerToken, back: Data<&TokenCheck>, Json(request): Json<WorkResult>) -> String {
    format!("hello: {}", auth.token())
}

pub async fn serve(bind_address: String, core: Arc<HouseCore>) -> Result<(), std::io::Error> {
    let app = Route::new()
        .at("/search/", post(add_search))
        .at("/search/:code", get(search_status))
        .at("/work/", get(get_work))
        .at("/finished/", get(finish_work))
        .with(AddData::new(TokenCheck::new(core)));

    Server::new(TcpListener::bind(bind_address))
        .run(app)
        .await
}