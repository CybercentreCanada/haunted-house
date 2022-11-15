

use poem::http::StatusCode;
use poem::middleware::AddData;
use poem::{post, EndpointExt};
use poem::web::{TypedHeader, Data, Json};
use poem::web::headers::Authorization;
use poem::web::headers::authorization::Bearer;
use poem::{get, handler, listener::TcpListener, web::Path, IntoResponse, Route, Server};
use serde::{Deserialize, Serialize};

use crate::access::AccessControl;

type BearerToken = TypedHeader<Authorization<Bearer>>;

#[derive(Clone)]
struct TokenCheck {

}

impl TokenCheck {
    pub fn new() -> Self {
        Self {

        }
    }

    pub fn authenticate_worker(&self, token: &str) -> Option<WorkerInterface> {
        todo!()
    }

    pub fn authenticate_searcher(&self, token: &str) -> Option<SearcherInterface> {
        todo!()
    }
}


struct WorkerInterface {

}

struct SearcherInterface {

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
fn add_search(TypedHeader(auth): BearerToken, back: Data<&TokenCheck>, Json(request): Json<SearchRequest>) -> (StatusCode, Json<SearchRequestResponse>) {
    let interface = back.authenticate_searcher(auth.token());

    todo!()
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

async fn serve(bind_address: String) -> Result<(), std::io::Error> {
    let app = Route::new()
        .at("/search/", post(add_search))
        .at("/search/:code", get(search_status))
        .at("/work/", get(get_work))
        .at("/finished/", get(finish_work))
        .with(AddData::new(TokenCheck::new()));

    Server::new(TcpListener::bind(&bind_address))
        .run(app)
        .await
}