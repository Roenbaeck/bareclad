//! Binary entrypoint – see crate-level docs (`lib.rs`) for conceptual overview.

// =========== TESTING BELOW ===========

use config::*;
use std::collections::HashMap;
use std::fs::{read_to_string, remove_file};

use bareclad::construct::{Database, PersistenceMode};
use bareclad::interface::{QueryInterface, QueryOptions};
use std::sync::Arc;

fn main() {
    let settings = Config::builder()
        .add_source(File::with_name("bareclad.json"))
        .build()
        .unwrap();
    let temp: HashMap<String, config::Value> = settings.try_deserialize().unwrap();
    let settings_lookup: HashMap<String, String> =
        temp.into_iter().map(|(k, v)| (k, v.to_string())).collect();
    let database_file_and_path = settings_lookup.get("database_file_and_path").unwrap();
    let enable_persistence = settings_lookup
        .get("enable_persistence")
        .map(|v| v == "true")
        .unwrap_or(true);
    let recreate_database_on_startup =
        settings_lookup.get("recreate_database_on_startup").unwrap() == "true";
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
    let bareclad = Database::new(mode);
    // Wrap database in Arc so it can be shared with the threaded interface
    let db = Arc::new(bareclad);
    let interface = QueryInterface::new(Arc::clone(&db));
    let traqula_file_to_run_on_startup = settings_lookup
        .get("traqula_file_to_run_on_startup")
        .unwrap();
    println!(
        "Traqula file to run on startup: {}",
        traqula_file_to_run_on_startup
    );
    let traqula_content = read_to_string(traqula_file_to_run_on_startup).unwrap();
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
        println!("Kept roles: {}", db.role_keeper().lock().unwrap().len());
        println!(
            "Kept appearances: {}",
            db.appearance_keeper().lock().unwrap().len()
        );
        println!(
            "Kept appearance sets: {}",
            db.appearance_set_keeper().lock().unwrap().len()
        );
        println!("Kept posits: {}", db.posit_keeper().lock().unwrap().len());
        println!(
            "Role->data type partitions: {:?}",
            db.role_name_to_data_type_lookup().lock().unwrap()
        );
        if let Some((head, count)) = db.persistor.lock().unwrap().current_superhash() {
            println!("Integrity ledger head: {} ({} posits)", head, count);
        }
    }
}
