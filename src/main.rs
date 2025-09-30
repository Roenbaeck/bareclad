//! Binary entrypoint – see crate-level docs (`lib.rs`) for conceptual overview.

// =========== TESTING BELOW ===========

use config::*;
use std::collections::HashMap;
use std::fs::{read_to_string, remove_file};

use bareclad::construct::Database;
use bareclad::persist::Persistor;
use bareclad::traqula::Engine;
use rusqlite::Connection;

fn main() {
    let settings = Config::builder()
        .add_source(File::with_name("bareclad.json"))
        .build()
        .unwrap();
    let temp: HashMap<String, config::Value> = settings.try_deserialize().unwrap();
    let settings_lookup: HashMap<String, String> =
        temp.into_iter().map(|(k, v)| (k, v.to_string())).collect();
    let database_file_and_path = settings_lookup.get("database_file_and_path").unwrap();
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
    let sqlite = Connection::open(database_file_and_path).unwrap();
    println!(
        "The path to the database file is '{}'.",
        sqlite.path().unwrap()
    );
    let persistor = Persistor::new(&sqlite);
    let bareclad = Database::new(persistor);
    let engine = Engine::new(&bareclad);
    let traqula_file_to_run_on_startup = settings_lookup
        .get("traqula_file_to_run_on_startup")
        .unwrap();
    println!(
        "Traqula file to run on startup: {}",
        traqula_file_to_run_on_startup
    );
    let traqula_content = read_to_string(traqula_file_to_run_on_startup).unwrap();
    engine.execute(&traqula_content);
    println!(
        "Kept roles: {}",
        bareclad.role_keeper().lock().unwrap().len()
    );
    println!(
        "Kept appearances: {}",
        bareclad.appearance_keeper().lock().unwrap().len()
    );
    println!(
        "Kept appearance sets: {}",
        bareclad.appearance_set_keeper().lock().unwrap().len()
    );
    println!(
        "Kept posits: {}",
        bareclad.posit_keeper().lock().unwrap().len()
    );
    println!(
        "Role->data type partitions: {:?}",
        bareclad.role_name_to_data_type_lookup().lock().unwrap()
    );
}
