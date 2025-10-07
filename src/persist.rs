//! Persistence layer: SQLite schema management and (re)construction logic.
//!
//! The `Persistor` encapsulates prepared statements for both writing and
//! restoring constructs. It owns no *logical* state besides a small cache of
//! seen data type identifiers (`seen_data_types`) to avoid redundant catalog
//! inserts.
//!
//! # Schema Overview
//! * `Thing(Thing_Identity)` – canonical identity table.
//! * `Role(Role_Identity, Role, Reserved)` – role metadata (identity FK to Thing).
//! * `DataType(DataType_Identity, DataType)` – catalog of logical value/time types.
//! * `Posit(Posit_Identity, AppearanceSet, AppearingValue, ValueType_Identity, AppearanceTime)` – stored propositions.
//!
//! Appearance sets are serialized as a pipe separated list of `thing,role` pairs
//! in natural order: `thing_id,role_id|thing_id,role_id|...`.
//!
//! # Lifecyle
//! * During startup `Database::new` calls restoration helpers which replay
//!   persisted rows into in-memory keepers.
//! * New constructs invoke `persist_*` methods which perform idempotent writes
//!   (checking for existing rows first) and return whether the row already existed.
//!
//! # Adding New Data Types
//! After implementing [`crate::datatype::DataType`] for a type, extend the
//! match section in `restore_posits` so the value can be reconstructed.
//!
//! # Error Handling
//! Current implementation panics on unexpected SQLite errors. A future revision
//! could propagate a domain error type instead.
// used for persistence
use blake3;
use rusqlite::{Connection, Error, params};

/// 64 zero hex string representing the genesis (no previous) hash in the integrity chain.
const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

// our own stuff
use crate::construct::{Appearance, AppearanceSet, Database, Posit, Role, Thing};
use crate::datatype::{Certainty, DataType, Decimal, JSON, Time};

// ------------- Persistence -------------
pub struct Persistor {
    /// File path of the SQLite database, if file-backed. If None, using in-memory (runtime writes/restores are no-ops).
    db_path: Option<String>,
    /// Cache of data type identifiers already inserted into `DataType`.
    seen_data_types: Vec<u8>,
}
impl Persistor {
    /// Create a file-backed persistor given a filesystem path; opens a connection to initialize schema and records the path for later calls.
    pub fn new_from_file(path: &str) -> Persistor {
        let conn = Connection::open(path).unwrap();
        // Enable WAL for better concurrency on file-backed DBs
        let _ = conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;");
        conn.execute_batch(
            "
            create table if not exists Thing (
                Thing_Identity integer not null, 
                constraint unique_and_referenceable_Thing_Identity primary key (
                    Thing_Identity
                )
            ) STRICT;
            create table if not exists Role (
                Role_Identity integer not null,
                Role text not null,
                Reserved integer not null,
                constraint Role_is_Thing foreign key (
                    Role_Identity
                ) references Thing(Thing_Identity),
                constraint referenceable_Role_Identity primary key (
                    Role_Identity
                ),
                constraint unique_Role unique (
                    Role
                )
            ) STRICT;
            create table if not exists DataType (
                DataType_Identity integer not null,
                DataType text not null,
                constraint referenceable_DataType_Identity primary key (
                    DataType_Identity
                ),
                constraint unique_DataType unique (
                    DataType
                )
            ) STRICT;
            create table if not exists Posit (
                Posit_Identity integer not null,
                AppearanceSet text not null,
                AppearingValue any null, 
                ValueType_Identity integer not null, 
                AppearanceTime any null,
                constraint Posit_is_Thing foreign key (
                    Posit_Identity
                ) references Thing(Thing_Identity),
                constraint ValueType_is_DataType foreign key (
                    ValueType_Identity
                ) references DataType(DataType_Identity),
                constraint referenceable_Posit_Identity primary key (
                    Posit_Identity
                ),
                constraint unique_Posit unique (
                    AppearanceSet,
                    AppearingValue,
                    AppearanceTime
                )
            ) STRICT;
            create table if not exists PositHash (
                Posit_Identity integer not null,
                PrevHash text not null,
                Hash text not null,
                constraint PositHash_is_Posit foreign key (
                    Posit_Identity
                ) references Posit(Posit_Identity),
                constraint referenceable_PositHash_Identity primary key (
                    Posit_Identity
                )
            ) STRICT;
            create table if not exists LedgerHead (
                Name text not null,
                HeadHash text not null,
                Count integer not null,
                constraint referenceable_LedgerHead_Name primary key (
                    Name
                )
            ) STRICT;
            ",
        )
        .unwrap();
        Persistor {
            db_path: Some(path.to_string()),
            seen_data_types: Vec::new(),
        }
    }

    /// Create a persistor that performs no persistence at runtime (no file I/O).
    pub fn new_no_persistence() -> Persistor {
        Persistor {
            db_path: None,
            seen_data_types: Vec::new(),
        }
    }

    /// Helper: run an operation with a Connection. For file-backed databases, opens a fresh
    /// connection per call to avoid sharing Connection across threads. For in-memory, falls back
    /// to the primary connection created by the caller.
    fn with_conn<T>(&self, mut op: impl FnMut(&Connection) -> T) -> Option<T> {
        if let Some(ref path) = self.db_path {
            let conn = Connection::open(path).unwrap();
            // Busy timeout helps under concurrent writes
            let _ = conn.busy_timeout(std::time::Duration::from_millis(5000));
            Some(op(&conn))
        } else {
            // In-memory mode: no shared path to reopen; treat persistence as a no-op at runtime
            None
        }
    }
    /// Persist a thing identity if not already present.
    /// Returns true if the record already existed.
    pub fn persist_thing(&mut self, thing: &Thing) -> bool {
        let mut existing = false;
        let _ = self.with_conn(|conn| {
            match conn
                .prepare("select Thing_Identity from Thing where Thing_Identity = ?")
                .unwrap()
                .query_row::<usize, _, _>(params![&thing], |r| r.get(0))
            {
                Ok(_) => {
                    existing = true;
                }
                Err(Error::QueryReturnedNoRows) => {
                    conn.prepare("insert into Thing (Thing_Identity) values (?)")
                        .unwrap()
                        .execute(params![&thing])
                        .unwrap();
                }
                Err(err) => {
                    panic!(
                        "Could not check if the thing '{}' is persisted: {}",
                        &thing, err
                    );
                }
            }
        });
        existing
    }
    /// Persist a role row by unique role name. Returns true if already present.
    pub fn persist_role(&mut self, role: &Role) -> bool {
        let mut existing = false;
        let _ = self.with_conn(|conn| {
            match conn
                .prepare("select Role_Identity from Role where Role = ?")
                .unwrap()
                .query_row::<usize, _, _>(params![&role.name()], |r| r.get(0))
            {
                Ok(_) => {
                    existing = true;
                }
                Err(Error::QueryReturnedNoRows) => {
                    conn.prepare(
                        "insert into Role (Role_Identity, Role, Reserved) values (?, ?, ?)",
                    )
                    .unwrap()
                    .execute(params![&role.role(), &role.name(), &role.reserved()])
                    .unwrap();
                }
                Err(err) => {
                    panic!(
                        "Could not check if the role '{}' is persisted: {}",
                        &role.name(),
                        err
                    );
                }
            }
        });
        existing
    }
    /// Persist a posit (idempotent). If unseen, ensures associated value & time
    /// data types are catalogued. Returns true if the posit already existed.
    pub fn persist_posit<V: 'static + DataType>(&mut self, posit: &Posit<V>) -> bool {
        let mut appearances = Vec::new();
        let appearance_set = posit.appearance_set();
        for appearance in appearance_set.appearances().iter() {
            appearances
                .push(appearance.thing().to_string() + "," + &appearance.role().role().to_string());
        }
        let apperance_set_as_text = appearances.join("|");
        let mut existing = false;
        // Existence check
        let _ = self.with_conn(|conn| {
            match conn
            .prepare("select Posit_Identity from Posit where AppearanceSet = ? and AppearingValue = ? and AppearanceTime = ?")
            .unwrap()
            .query_row::<usize, _, _>(params![&apperance_set_as_text, &posit.value(), &posit.time()], |r| r.get(0))
            {
                Ok(_) => {
                    existing = true;
                }
                Err(Error::QueryReturnedNoRows) => { /* will insert below */ }
                Err(err) => {
                    panic!(
                        "Could not check if the posit {} is persisted: {}",
                        &posit.posit(),
                        err
                    );
                }
            }
        });
        if !existing {
            // Update cache outside of connection borrow
            let need_value_dt = !self.seen_data_types.contains(&posit.value().identifier());
            let need_time_dt = !self.seen_data_types.contains(&posit.time().identifier());
            if need_value_dt {
                self.seen_data_types.push(posit.value().identifier());
            }
            if need_time_dt {
                self.seen_data_types.push(posit.time().identifier());
            }
            // Perform inserts
            let _ = self.with_conn(|conn| {
                if need_value_dt {
                    conn.prepare("insert or ignore into DataType (DataType_Identity, DataType) values (?, ?)")
                        .unwrap()
                        .execute(params![&posit.value().identifier(), &posit.value().data_type()])
                        .unwrap();
                }
                if need_time_dt {
                    conn.prepare("insert or ignore into DataType (DataType_Identity, DataType) values (?, ?)")
                        .unwrap()
                        .execute(params![&posit.time().identifier(), &posit.time().data_type()])
                        .unwrap();
                }
                conn.prepare("insert into Posit (Posit_Identity, AppearanceSet, AppearingValue, ValueType_Identity, AppearanceTime) values (?, ?, ?, ?, ?)")
                    .unwrap()
                    .execute(params![&posit.posit(), &apperance_set_as_text, &posit.value(), &posit.value().identifier(), &posit.time()])
                    .unwrap();

                // Integrity ledger (canonical hash):
                // Fetch previous hash first, then re-select the just inserted row using CAST for a stable textual form
                let prev_hash: String = {
                    let mut stmt = conn.prepare("select Hash from PositHash order by Posit_Identity desc limit 1").unwrap();
                    let mut rows = stmt.query([]).unwrap();
                    if let Some(row) = rows.next().unwrap() { row.get::<_, String>(0).unwrap() } else { GENESIS_HASH.to_string() }
                };
                let mut stmt = conn.prepare("select AppearanceSet, cast(AppearingValue as text), ValueType_Identity, AppearanceTime from Posit where Posit_Identity = ?").unwrap();
                let mut rows = stmt.query(params![&posit.posit()]).unwrap();
                if let Some(r) = rows.next().unwrap() {
                    let aset_text: String = r.get_unwrap(0);
                    let value_text: String = r.get_unwrap(1);
                    let vtid: i64 = r.get_unwrap(2);
                    let atime_text: String = r.get_unwrap(3);
                    let input = format!("{}|{}|{}|{}|{}|prev={}", &posit.posit(), aset_text, vtid, value_text, atime_text, prev_hash);
                    let hash_hex = blake3::hash(input.as_bytes()).to_hex().to_string();
                    conn.prepare("insert into PositHash (Posit_Identity, PrevHash, Hash) values (?, ?, ?)")
                        .unwrap()
                        .execute(params![&posit.posit(), &prev_hash, &hash_hex])
                        .unwrap();
                    // Update ledger head
                    let count: i64 = conn
                        .prepare("select count(1) from PositHash")
                        .unwrap()
                        .query_row([], |r| r.get(0))
                        .unwrap();
                    conn.prepare("insert into LedgerHead (Name, HeadHash, Count) values ('PositLedger', ?, ?) on conflict(Name) do update set HeadHash=excluded.HeadHash, Count=excluded.Count")
                        .unwrap()
                        .execute(params![&hash_hex, &count])
                        .unwrap();
                }
            });
        }
        existing
    }
    /// Rehydrate all thing identities into the in-memory generator.
    pub fn restore_things(&mut self, db: &Database) {
        if let Some(ref path) = self.db_path {
            let conn = Connection::open(path).unwrap();
            let mut stmt = conn.prepare("select Thing_Identity from Thing").unwrap();
            let rows = stmt
                .query_map([], |row| Ok(row.get::<_, Thing>(0).unwrap()))
                .unwrap();
            for thing in rows {
                db.thing_generator().lock().unwrap().retain(thing.unwrap());
            }
        }
    }
    /// Rehydrate all roles into the in-memory keeper.
    pub fn restore_roles(&mut self, db: &Database) {
        if let Some(ref path) = self.db_path {
            let conn = Connection::open(path).unwrap();
            let mut stmt = conn
                .prepare("select Role_Identity, Role, Reserved from Role")
                .unwrap();
            let rows = stmt
                .query_map([], |row| {
                    Ok(Role::new(
                        row.get(0).unwrap(),
                        row.get(1).unwrap(),
                        row.get(2).unwrap(),
                    ))
                })
                .unwrap();
            for role in rows {
                db.keep_role(role.unwrap());
            }
        }
    }
    /// Rehydrate all posits (including nested appearance sets) into memory.
    ///
    /// Appearance sets are parsed from their serialized pipe-separated form.
    pub fn restore_posits(&mut self, db: &Database) {
        if self.db_path.is_none() {
            return;
        }
        let conn = Connection::open(self.db_path.as_ref().unwrap()).unwrap();
        let mut stmt = conn
            .prepare(
                "select p.Posit_Identity, p.AppearanceSet, p.AppearingValue, v.DataType as ValueType, p.AppearanceTime from Posit p join DataType v on v.DataType_Identity = p.ValueType_Identity",
            )
            .unwrap();
        let mut rows = stmt.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            let value_type: String = row.get_unwrap(3);
            let thing: Thing = row.get_unwrap(0);
            let appearances: String = row.get_unwrap(1);
            let mut appearance_set = Vec::new();
            for appearance_text in appearances.split('|') {
                let (thing, role) = appearance_text.split_once(',').unwrap();
                let appearance = Appearance::new(
                    thing.parse().unwrap(),
                    db.role_keeper()
                        .lock()
                        .unwrap()
                        .lookup(&role.parse::<Thing>().unwrap()),
                );
                let (kept_appearance, _) = db.keep_appearance(appearance);
                appearance_set.push(kept_appearance);
            }
            let (kept_appearance_set, _) =
                db.keep_appearance_set(AppearanceSet::new(appearance_set).unwrap());

            // MAINTENANCE: The section below needs to be extended when new data types are added
            match value_type.as_str() {
                String::DATA_TYPE => {
                    db.keep_posit(Posit::new(
                        thing,
                        kept_appearance_set,
                        <String as DataType>::convert(&row.get_ref_unwrap(2)),
                        Time::convert(&row.get_ref_unwrap(4)),
                    ));
                }
                i64::DATA_TYPE => {
                    db.keep_posit(Posit::new(
                        thing,
                        kept_appearance_set,
                        <i64 as DataType>::convert(&row.get_ref_unwrap(2)),
                        Time::convert(&row.get_ref_unwrap(4)),
                    ));
                }
                Decimal::DATA_TYPE => {
                    db.keep_posit(Posit::new(
                        thing,
                        kept_appearance_set,
                        <Decimal as DataType>::convert(&row.get_ref_unwrap(2)),
                        Time::convert(&row.get_ref_unwrap(4)),
                    ));
                }
                Time::DATA_TYPE => {
                    db.keep_posit(Posit::new(
                        thing,
                        kept_appearance_set,
                        <Time as DataType>::convert(&row.get_ref_unwrap(2)),
                        Time::convert(&row.get_ref_unwrap(4)),
                    ));
                }
                JSON::DATA_TYPE => {
                    db.keep_posit(Posit::new(
                        thing,
                        kept_appearance_set,
                        <JSON as DataType>::convert(&row.get_ref_unwrap(2)),
                        Time::convert(&row.get_ref_unwrap(4)),
                    ));
                }
                Certainty::DATA_TYPE => {
                    db.keep_posit(Posit::new(
                        thing,
                        kept_appearance_set,
                        <Certainty as DataType>::convert(&row.get_ref_unwrap(2)),
                        Time::convert(&row.get_ref_unwrap(4)),
                    ));
                }
                _ => (),
            }
        }
    }

    /// Verify the integrity chain of posits (no auto backfill / rebuild).
    /// Emits warnings if ledger missing or hashes mismatch.
    pub fn verify_integrity(&mut self) {
        if self.db_path.is_none() {
            return;
        }
        let conn = Connection::open(self.db_path.as_ref().unwrap()).unwrap();
        // Quick counts
        let posit_count: i64 = conn
            .prepare("select count(1) from Posit")
            .unwrap()
            .query_row([], |r| r.get(0))
            .unwrap();
        if posit_count == 0 {
            return;
        }
        let hash_count: i64 = conn
            .prepare("select count(1) from PositHash")
            .unwrap()
            .query_row([], |r| r.get(0))
            .unwrap();
        if hash_count == 0 {
            eprintln!(
                "[bareclad] INTEGRITY WARNING: ledger missing ({} posits).",
                posit_count
            );
            return;
        }
        // Verify existing chain (no repair)
        let mut stmt = conn
            .prepare("select p.Posit_Identity, p.AppearanceSet, cast(p.AppearingValue as text), p.ValueType_Identity, p.AppearanceTime, h.Hash from Posit p join PositHash h on h.Posit_Identity = p.Posit_Identity order by p.Posit_Identity asc")
            .unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut prev = GENESIS_HASH.to_string();
        let mut mismatches = 0usize;
        let mut first_bad: Option<i64> = None;
        let mut last_hash = prev.clone();
        while let Some(row) = rows.next().unwrap() {
            let thing: i64 = row.get_unwrap(0);
            let aset: String = row.get_unwrap(1);
            let aval: String = row.get_unwrap(2);
            let vtid: i64 = row.get_unwrap(3);
            let atime: String = row.get_unwrap(4);
            let stored_hash: String = row.get_unwrap(5);
            let input = format!(
                "{}|{}|{}|{}|{}|prev={}",
                thing, aset, vtid, aval, atime, prev
            );
            let calc = blake3::hash(input.as_bytes()).to_hex().to_string();
            if calc != stored_hash {
                mismatches += 1;
                if first_bad.is_none() {
                    first_bad = Some(thing);
                }
            }
            prev = stored_hash.clone();
            last_hash = stored_hash;
        }
        // Update LedgerHead to reflect current chain state
        conn.prepare("insert into LedgerHead (Name, HeadHash, Count) values ('PositLedger', ?, ?) on conflict(Name) do update set HeadHash=excluded.HeadHash, Count=excluded.Count")
            .unwrap()
            .execute(params![&last_hash, &posit_count])
            .unwrap();

        if mismatches > 0 {
            eprintln!(
                "[bareclad] INTEGRITY VIOLATION: {} mismatched hashes (first at Posit_Identity={}). (No auto repair)",
                mismatches,
                first_bad.unwrap_or(-1)
            );
        }
    }

    /// Returns the current integrity ledger head hash and count, when persistence is enabled and the ledger exists.
    pub fn current_superhash(&self) -> Option<(String, i64)> {
        if self.db_path.is_none() {
            return None;
        }
        let conn = Connection::open(self.db_path.as_ref().unwrap()).unwrap();
        let mut stmt = conn
            .prepare("select HeadHash, Count from LedgerHead where Name = 'PositLedger'")
            .unwrap();
        let mut rows = stmt.query([]).unwrap();
        if let Some(row) = rows.next().unwrap() {
            let head: String = row.get_unwrap(0);
            let count: i64 = row.get_unwrap(1);
            Some((head, count))
        } else {
            None
        }
    }
}
