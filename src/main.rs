//! Binary entrypoint – see crate-level docs (`lib.rs`) for conceptual overview.

// =========== TESTING BELOW ===========

use config::*;
use std::collections::HashMap;
use std::fs::{read_to_string, remove_file};

use bareclad::construct::{Database, PersistenceMode};
use bareclad::interface::{QueryInterface, QueryOptions};
use bareclad::error::{BarecladError, Result};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    if let Err(e) = real_main().await {
        eprintln!("bareclad error: {e}");
        std::process::exit(1);
    }
}

async fn real_main() -> Result<()> {
    let settings = Config::builder()
        .add_source(File::with_name("bareclad.json"))
        .build()
        .map_err(|e| BarecladError::Config(format!("Cannot read config: {e}")))?;
    let temp: HashMap<String, config::Value> = settings
        .try_deserialize()
        .map_err(|e| BarecladError::Config(format!("Invalid config structure: {e}")))?;
    let settings_lookup: HashMap<String, String> =
        temp.into_iter().map(|(k, v)| (k, v.to_string())).collect();
    let database_file_and_path = settings_lookup
        .get("database_file_and_path")
        .ok_or_else(|| BarecladError::Config("Missing 'database_file_and_path'".into()))?;
    let enable_persistence = settings_lookup
        .get("enable_persistence")
        .map(|v| v == "true")
        .unwrap_or(true);
    let recreate_database_on_startup = settings_lookup
        .get("recreate_database_on_startup")
        .map(|v| v == "true")
        .unwrap_or(false);
    if recreate_database_on_startup {
        match remove_file(database_file_and_path) {
            Ok(_) => (),
            Err(e) => {
                println!(
                    "Could not remove the file '{}': {}",
                    database_file_and_path, e
                );
            }
        }
    }
    let mode = if enable_persistence {
        println!(
            "Using file-backed persistence at '{}'.",
            database_file_and_path
        );
        PersistenceMode::File(database_file_and_path.clone())
    } else {
        println!("Persistence disabled (ephemeral in-memory engine).");
        PersistenceMode::InMemory
    };
    let bareclad = Database::new(mode)?;
    let db = Arc::new(bareclad);
    let interface = Arc::new(QueryInterface::new(Arc::clone(&db)));
    let traqula_file_to_run_on_startup = settings_lookup
        .get("traqula_file_to_run_on_startup")
        .ok_or_else(|| BarecladError::Config("Missing 'traqula_file_to_run_on_startup'".into()))?;
    println!(
        "Traqula file to run on startup: {}",
        traqula_file_to_run_on_startup
    );
    let traqula_content = read_to_string(traqula_file_to_run_on_startup)
        .map_err(|e| BarecladError::Config(format!("Could not read traqula file: {e}")))?;
    // Use the interface submission method (currently executes synchronously under the hood)
    let handle = interface.start_query(
        traqula_content,
        QueryOptions {
            stream_results: false,
            timeout: None,
        },
    );
    // Wait for the startup script to finish before printing diagnostics
    handle.join();
    if cfg!(debug_assertions) {
        if let Ok(rk) = db.role_keeper().lock() {
            println!("Kept roles: {}", rk.len());
        }
        if let Ok(ak) = db.appearance_keeper().lock() {
            println!("Kept appearances: {}", ak.len());
        }
        if let Ok(ask) = db.appearance_set_keeper().lock() {
            println!("Kept appearance sets: {}", ask.len());
        }
        if let Ok(pk) = db.posit_keeper().lock() {
            println!("Kept posits: {}", pk.len());
        }
        if let Ok(parts) = db.role_name_to_data_type_lookup().lock() {
            println!("Role->data type partitions: {:?}", parts);
        }
        if let Ok(p) = db.persistor.lock() {
            if let Some((head, count)) = p.current_superhash() {
                println!("Integrity ledger head: {} ({} posits)", head, count);
            }
        }
    }
    // Start HTTP server (simple /v1/query endpoint)
    let app = bareclad::server::router(Arc::clone(&interface));
    let addr = std::net::SocketAddr::from(([127,0,0,1], 8080));
    tracing::info!(?addr, "HTTP server listening");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| BarecladError::Execution(format!("bind error: {e}")))?;
    axum::serve(listener, app.into_make_service())
        .await
        .map_err(|e| BarecladError::Execution(format!("server error: {e}")))?;
    Ok(())
}
