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

mod persist;
mod bareclad;
mod traqula;

// =========== TESTING BELOW ===========

use config::*;
use std::fs::{remove_file, read_to_string};
use std::collections::{HashMap};

use bareclad::Database;
use traqula::Engine;
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
    let bareclad = Database::new(&sqlite);
    let traqula = Engine::new(bareclad);
    let traqula_file_to_run_on_startup = settings_lookup.get("traqula_file_to_run_on_startup").unwrap();
    println!("Traqula file to run on startup: {}", traqula_file_to_run_on_startup);
    let traqula_content = read_to_string(traqula_file_to_run_on_startup).unwrap();
    traqula.execute(&traqula_content);

    /* 
    // does it really have to be this elaborate?
    let i1 = bareclad.create_thing();
    println!("Enter a role name: ");
    let mut role: String = read!("{}");
    role.truncate(role.trim_end().len());

    let r1 = bareclad.create_role(role.clone(), false);
    let rdup = bareclad.create_role(role.clone(), false);
    println!("{:?}", bareclad.role_keeper());
    // drop(r); // just to make sure it moved
    let a1 = bareclad.create_apperance(Arc::clone(&i1), Arc::clone(&r1));
    let a2 = bareclad.create_apperance(Arc::clone(&i1), Arc::clone(&r1));
    println!("{:?}", bareclad.appearance_keeper());
    let i2 = bareclad.create_thing();

    println!("Enter another role name: ");
    let mut another_role: String = read!("{}");
    another_role.truncate(another_role.trim_end().len());

    let r2 = bareclad.create_role(another_role.clone(), false);
    let a3 = bareclad.create_apperance(Arc::clone(&i2), Arc::clone(&r2));
    let as1 = bareclad.create_appearance_set([a1, a3].to_vec());
    println!("{:?}", bareclad.appearance_set_keeper());

    println!(
        "Enter a value that appears with '{}' and '{}': ",
        role, another_role
    );
    let mut v1: String = read!("{}");
    v1.truncate(v1.trim_end().len());

    let p1 = bareclad.create_posit(Arc::clone(&as1), v1.clone(), 42i64); // this 42 represents a point in time (for now)
    let p2 = bareclad.create_posit(Arc::clone(&as1), v1.clone(), 42i64);

    println!(
        "Enter a different value that appears with '{}' and '{}': ",
        role, another_role
    );
    let mut v2: String = read!("{}");
    v2.truncate(v2.trim_end().len());

    let p3 = bareclad.create_posit(Arc::clone(&as1), v2.clone(), 21i64);
    println!("{:?}", p1);
    println!("Posit id: {:?}", p1.posit());
    println!("Posit id: {:?} (should be the same)", p2.posit());
    println!("--- Contents of the Posit<String, i64> keeper:");
    println!(
        "{:?}",
        bareclad
            .posit_keeper()
            .lock()
            .unwrap()
            .kept
            .get::<Posit<String, i64>>()
    );
    let asserter = bareclad.create_thing();
    let c1: Certainty = Certainty::new(1.0);
    println!("Certainty 1: {:?}", c1);
    let t1: DateTime<Utc> = Utc::now();
    bareclad.assert(Arc::clone(&asserter), Arc::clone(&p3), c1, t1);
    let c2: Certainty = Certainty::new(0.5);
    println!("Certainty 2: {:?}", c2);
    let t2: DateTime<Utc> = Utc::now();
    bareclad.assert(Arc::clone(&asserter), Arc::clone(&p3), c2, t2);

    println!("--- Contents of the Posit<Certainty, DateTime<Utc>> keeper (after two assertions):");
    println!(
        "{:?}",
        bareclad
            .posit_keeper()
            .lock()
            .unwrap()
            .kept
            .get::<Posit<Certainty, DateTime<Utc>>>()
    );
    println!(
        "--- Contents of the Posit<String, i64> after the assertions that identify the posit:"
    );
    println!(
        "{:?}",
        bareclad
            .posit_keeper()
            .lock()
            .unwrap()
            .kept
            .get::<Posit<String, i64>>()
    );
    println!("--- Contents of the appearance to appearance set lookup:");
    println!(
        "{:?}",
        bareclad
            .appearance_to_appearance_set_lookup()
            .lock()
            .unwrap()
    );
    */

    // TODO: Fix this, broken at the moment
    /*
    println!("--- Posit things for thing {}: ", pid1);
    let ids: Vec<Arc<Thing>> = bareclad.posits_involving_thing(&pid1);
    println!(
        "{:?}",
        ids
    );
    println!("--- and the actual posits are: ");
    for px in ids.iter() {
        println!(
            "{:?}",
            bareclad
                .posit_keeper()
                .lock()
                .unwrap()
                .posit::<String, i64>(px.clone()) // you need to know the data type of what you want to find
        );
    }
    */
}
