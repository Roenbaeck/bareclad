//!
//! Implements a database based on the "posit" concept from Transitional Modeling.
//!
//! [{(wife, 42), (husband, 43)}, "married", 2009-12-31]
//!
//! Popular version can be found in these blog posts:
//! http://www.anchormodeling.com/tag/transitional/
//!
//! Scientific version can be found in this publication:
//! https://www.researchgate.net/publication/329352497_Modeling_Conflicting_Unreliable_and_Varying_Information
//!
//! Contains its fundamental constructs:
//! - Things
//! - Roles
//! - Appearances = (Role, Thing)
//! - Appearance sets = {Appearance_1, ..., Appearance_N}
//! - Posits = (Appearance set, V, T) where V is an arbitraty "value type" and T is an arbitrary "time type"
//!
//! Along with these a "keeper" pattern is used, with the intention to own constructs and
//! guarantee their uniqueness. These can be seen as the database "storage".
//! The following keepers are needed:
//! - RoleKeeper
//! - AppearanceKeeper
//! - AppearanceSetKeeper
//! - PositKeeper
//!
//! Roles will have the additional ability of being reserved. This is necessary for some
//! strings that will be used to implement more "traditional" features found in other
//! databases. For example 'class' and 'constraint'.
//!  
//! In order to perform searches smart lookups between constructs are needed.
//! Role -> Appearance -> AppearanceSet -> Posit (at the very least for reserved roles)
//! Thing -> Appearance -> AppearanceSet -> Posit
//! V -> Posit
//! T -> Posit
//!
//! A datatype for Certainty is also available, since this is something that will be
//! used frequently and that needs to be treated with special care.
//!
//! TODO: Remove internal identities from the relational model and let everything be "Things"
//! TODO: Extend Role, Appearance and AppearanceSet with an additional field for the thing_identity.
//! TODO: Check what needs to keep pub scope.
//! TODO: Implement a log db (for high level log messages, controlled by verbosity in the config)


// =========== TESTING BELOW ===========

use config::*;
use std::fs::{remove_file, read_to_string};
use std::collections::{HashMap};

use bareclad::construct::Database;
use bareclad::traqula::Engine;
use bareclad::persist::Persistor;
use rusqlite::{Connection};

fn main() {
    let mut settings = Config::default();
    settings
        .merge(File::with_name("bareclad.json")).unwrap();
    let settings_lookup = settings.try_into::<HashMap<String, String>>().unwrap();
    let database_file_and_path = settings_lookup.get("database_file_and_path").unwrap();
    let recreate_database_on_startup = settings_lookup.get("recreate_database_on_startup").unwrap() == "true";
    if recreate_database_on_startup {
        match remove_file(database_file_and_path) {
            Ok(_) => (), 
            Err(e) => {
                println!("Could not remove the file '{}': {}", database_file_and_path, e);
            }
        }
    }
    let sqlite = Connection::open(database_file_and_path).unwrap();
    println!(
        "The path to the database file is '{}'.",
        sqlite.path().unwrap().display()
    );
    let persistor = Persistor::new(&sqlite);
    let bareclad = Database::new(persistor);
    let engine = Engine::new(&bareclad);
    let traqula_file_to_run_on_startup = settings_lookup.get("traqula_file_to_run_on_startup").unwrap();
    println!("Traqula file to run on startup: {}", traqula_file_to_run_on_startup);
    let traqula_content = read_to_string(traqula_file_to_run_on_startup).unwrap();
    engine.execute(&traqula_content);
    println!("Total number of kept roles: {}", bareclad.role_keeper().lock().unwrap().len());
    println!("Total number of kept appearances: {}", bareclad.appearance_keeper().lock().unwrap().len());
    println!("Total number of kept appearance sets: {}", bareclad.appearance_set_keeper().lock().unwrap().len());
    println!("Total number of kept posits: {}", bareclad.posit_keeper().lock().unwrap().len());
    println!("Posit partitioning:\n{:?}", bareclad.role_name_to_data_type_lookup().lock().unwrap());

}
