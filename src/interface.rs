

use std::sync::Arc;

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


#[derive(Deserialize)]
struct SearchRequest {
    access: AccessControl,
    yara_signature: String,
    start_date: Option<chrono::DateTime<chrono::Utc>>,
    end_date: Option<chrono::DateTime<chrono::Utc>>,
}


#[derive(Serialize)]
struct SearchRequestResponse {
    code: String,
    finished: bool,
    errors: Vec<String>,
}

#[handler]
fn add_search(TypedHeader(auth): BearerToken, back: Data<&TokenCheck>, Json(request): Json<SearchRequest>) -> Response {
    let interface = match back.authenticate_searcher(auth.token()) {
        Some(interface) => interface,
        None => return StatusCode::FORBIDDEN.into()
    };

    todo!();
    // interface.initialize_search(request)
}

#[handler]
fn search_status(TypedHeader(auth): BearerToken, back: Data<&TokenCheck>, Path(code): Path<String>) -> (StatusCode, Json<SearchRequestResponse>) {
    todo!()
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