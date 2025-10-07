use std::sync::Arc;
use axum::{routing::post, Router, Json};
use tower_http::cors::{CorsLayer, Any};
use serde::{Deserialize, Serialize};
use axum::http::StatusCode;
use tracing::{info, warn};
use crate::interface::QueryInterface;
use crate::traqula::Engine;

#[derive(Deserialize)]
pub struct QueryRequest {
    pub script: String,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub id: u64,
    pub status: String,
    pub elapsed_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_types: Option<Vec<Vec<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limited: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")] 
    pub rows: Option<Vec<Vec<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")] 
    pub error: Option<String>,
}

pub fn router(interface: Arc<QueryInterface>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([axum::http::Method::POST])
        .allow_headers(Any);
    Router::new()
        .route("/v1/query", post(move |Json(req): Json<QueryRequest>| {
        let iface = Arc::clone(&interface);
        async move {
            // We run the query in a blocking thread since Engine is synchronous today.
            let started = std::time::Instant::now();
            let script = req.script.clone();
            let _stream = req.stream; // placeholder for future streaming
            let _timeout = req.timeout_ms.map(std::time::Duration::from_millis);
            let rows_result = tokio::task::spawn_blocking(move || {
                let engine = Engine::new(iface.database());
                engine.execute_collect(&script)
            }).await.map_err(|e| {
                warn!(error=%e, "Join error");
                (StatusCode::INTERNAL_SERVER_ERROR, "Join error")
            })?;
            let total_elapsed = started.elapsed();
            let elapsed_ms_f64 = total_elapsed.as_secs_f64() * 1000.0;
            match rows_result {
                Ok(result) => {
                    info!(ms=elapsed_ms_f64, rows=result.row_count, limited=result.limited, "query complete");
                    let body = QueryResponse { id: 0, status: "ok".into(), elapsed_ms: elapsed_ms_f64, columns: Some(result.columns), row_types: Some(result.row_types), row_count: Some(result.row_count), limited: Some(result.limited), rows: Some(result.rows), error: None };
                    Ok::<_, (StatusCode, &'static str)>((StatusCode::OK, Json(body)))
                }
                Err(e) => {
                    let is_parse = matches!(e, crate::error::BarecladError::Parse { .. });
                    let status = if is_parse { StatusCode::BAD_REQUEST } else { StatusCode::INTERNAL_SERVER_ERROR };
                    let msg = format!("{e}");
                    warn!(%msg, code=%status.as_u16(), "query error");
                    let body = QueryResponse { id: 0, status: "error".into(), elapsed_ms: elapsed_ms_f64, columns: None, row_types: None, row_count: None, limited: None, rows: None, error: Some(msg) };
                    let json = Json(body);
                    // Axum requires returning Err(status, msg). We'll serialize JSON manually on error by responding with Ok and mapping status via IntoResponse
                    // Simpler: build a tuple (StatusCode, Json<_>) which implements IntoResponse.
                    return Ok::<_, (StatusCode, &'static str)>((status, json));
                }
            }
        }
    }))
    .layer(cors)
}
