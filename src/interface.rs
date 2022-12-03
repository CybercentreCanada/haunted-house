

use std::collections::HashSet;
use std::sync::Arc;
use anyhow::Result;

use log::error;
use poem::{post, EndpointExt, Endpoint, Middleware, Request, FromRequest};
use poem::web::{TypedHeader, Data, Json};
use poem::web::headers::Authorization;
use poem::web::headers::authorization::Bearer;
use poem::{get, handler, listener::TcpListener, web::Path, Route, Server};
use serde::{Deserialize, Serialize};

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

        // call the next endpoint.
        self.ep.call(req).await
    }
}


// #[derive(Clone)]
// struct TokenCheck {
//     core: Arc<HouseCore>
// }

// impl TokenCheck {
//     pub fn new(core: Arc<HouseCore>) -> Self {
//         Self {
//             core
//         }
//     }

//     pub fn authenticate_worker(&self, token: &str) -> Option<WorkerInterface> {
//         if self.core.authenticator.is_role_assigned(token, Role::Worker) {
//             Some(WorkerInterface { core: self.core.clone() })
//         } else {
//             None
//         }
//     }

//     pub fn authenticate_searcher(&self, token: &str) -> Option<SearcherInterface> {
//         if self.core.authenticator.is_role_assigned(token, Role::Search) {
//             Some(SearcherInterface { core: self.core.clone() })
//         } else {
//             None
//         }
//     }
// }


struct WorkerInterface {
    core: Arc<HouseCore>
}

impl WorkerInterface {
    pub fn new(core: Arc<HouseCore>) -> Self {
        Self{core}
    }

    pub async fn get_work(&self, req: WorkRequest) -> Result<WorkPackage> {
        self.core.get_work(req).await
    }

    pub fn finish_work(&self, req: WorkResult) -> Result<()> {
        self.core.finish_work(req)
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
}

#[handler]
async fn add_search(Data(interface): Data<&SearcherInterface>, Json(request): Json<SearchRequest>) -> Result<Json<SearchRequestResponse>> {
    Ok(Json(interface.initialize_search(request).await?))
}

#[handler]
async fn search_status(Data(interface): Data<&SearcherInterface>, Path(code): Path<String>) -> Result<Json<SearchRequestResponse>> {
    Ok(Json(interface.search_status(code).await?))
}

#[derive(Serialize)]
pub struct FilterTask {
    pub search: String,
    pub filter_id: IndexID,
    pub filter_blob: BlobID,
    pub query: Query,
}

#[derive(Serialize)]
pub struct YaraTask {
    pub search: String,
    pub yara_rule: String,
    pub hashes: Vec<Vec<u8>>, 
}

#[derive(Serialize)]
pub struct WorkPackage {
    pub filter: Vec<FilterTask>,
    pub yara: Vec<YaraTask>
}

#[derive(Deserialize)]
pub struct WorkRequest {
    pub worker: String,
    pub cached_filters: HashSet<BlobID>
}

#[handler]
async fn get_work(Data(interface): Data<&WorkerInterface>, Json(request): Json<WorkRequest>) -> Result<Json<WorkPackage>> {
    Ok(Json(interface.get_work(request).await?))
}

#[derive(Deserialize)]
pub struct WorkResult {
    pub search: String,
    pub filter: Vec<(IndexID, BlobID, Vec<u64>)>,
    pub yara: Vec<Vec<u8>>
}

#[handler]
fn finish_work(Data(interface): Data<&WorkerInterface>, Json(request): Json<WorkResult>) -> Result<()> {
    interface.finish_work(request)
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
        .at("/finished/", get(finish_work))
        .with(TokenMiddleware::new(core));

    Server::new(TcpListener::bind(bind_address))
        .run(app)
        .await
}