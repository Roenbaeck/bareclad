use std::sync::Arc;
use axum::{routing::post, Router, Json};
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
    pub elapsed_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
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
    Router::new().route("/v1/query", post(move |Json(req): Json<QueryRequest>| {
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
            match rows_result {
                Ok(result) => {
                    info!(ms=total_elapsed.as_millis(), rows=result.row_count, limited=result.limited, "query complete");
                    Ok::<_, (StatusCode, &'static str)>(Json(QueryResponse { id: 0, status: "ok".into(), elapsed_ms: total_elapsed.as_millis(), columns: Some(result.columns), row_count: Some(result.row_count), limited: Some(result.limited), rows: Some(result.rows), error: None }))
                }
                Err(e) => {
                    let is_parse = matches!(e, crate::error::BarecladError::Parse { .. });
                    let status = if is_parse { StatusCode::BAD_REQUEST } else { StatusCode::INTERNAL_SERVER_ERROR };
                    let msg = format!("{e}");
                    warn!(%msg, "query error");
                    return Err((status, "query failed"));
                }
            }
        }
    }))
}
