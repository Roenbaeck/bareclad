use std::sync::{Arc, Mutex};

// used in the keeper of posits, since they are generically typed: Posit<V,T> and 
// therefore require a HashSet per type combo
use typemap::{Key, TypeMap};

// used to keep the one-to-one mapping between posits and their assigned identities
use bimap::BiMap;

// other keepers use HashSet or HashMap
use core::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::collections::hash_map::{Entry, RandomState};
use std::collections::{HashMap, HashSet};
use std::collections::hash_set::Iter;
use std::hash::Hash;

// custom made ordering for appearances
use std::cmp::Ordering;

use std::fmt::{self, Result};
use std::ops;

// used for persistence
use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use rusqlite::{Connection};

// used for timestamps in the database
use chrono::{DateTime, Utc, NaiveDate};
// used when parsing a string to a DateTime<Utc>
use std::str::FromStr;
// used to print constructs
use std::fmt::Display;

use crate::persist::Persistor;

pub trait DataType: Display + Eq + Hash + Send + Sync + ToSql + FromSql {
    // static stuff which needs to be implemented downstream
    type TargetType;
    const UID: u8;
    const DATA_TYPE: &'static str;
    fn convert(value: &ValueRef) -> Self::TargetType;
    // instance callable with pre-made implementation
    fn data_type(&self) -> &'static str {
        Self::DATA_TYPE
    }
    fn identifier(&self) -> u8 {
        Self::UID
    }
}

// ------------- Thing -------------
pub type Thing = u64; 

#[derive(Debug, Clone, Copy, Default)]
pub struct ThingHash(Thing);

impl Hasher for ThingHash {
    fn finish(&self) -> u64 {
        self.0 as u64
    }

    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!("ThingHasher only supports u64 keys")
    }

    fn write_u64(&mut self, i: Thing) {
        self.0 = i;
    }
}

type ThingHasher = BuildHasherDefault<ThingHash>;

pub const GENESIS: Thing = 0;

#[derive(Debug)]
pub struct ThingGenerator {
    lower_bound: Thing,
    retained: HashSet<Thing, ThingHasher>,
    released: Vec<Thing>,
}

impl ThingGenerator {
    pub fn new() -> Self {
        Self {
            lower_bound: GENESIS,
            retained: HashSet::<Thing, ThingHasher>::default(),
            released: Vec::new(),
        }
    }
    // Things may be explicitly referenced, but only implicitly created.
    // The following will throw an error if 42 does not already exist.
    // add posit [{(+idw, wife), (42, husband)}, "married", '2004-06-19'];
    // The retain function is necessary though, when restoring an existing
    // persisted database. 
    pub fn retain(&mut self, t: Thing) {
        self.retained.insert(t);
        if t > self.lower_bound {
            self.lower_bound = t;
        }
    }
    pub fn check(&self, t: Thing) -> Option<Thing> {
        self.retained.get(&t).cloned()
    }
    pub fn release(&mut self, t: Thing) {
        if self.retained.remove(&t) {
            self.released.push(t);
        }
    }
    pub fn generate(&mut self) -> Thing {
        self.released.pop().unwrap_or_else(|| {
            self.lower_bound += 1;
            self.retained.insert(self.lower_bound);
            self.lower_bound
        })
    }
    pub fn iter(&self) -> Iter<Thing> {
        self.retained.iter()
    }
}

// ------------- Role -------------
#[derive(Eq, Debug)]
pub struct Role {
    role: Thing, // let it be a thing so we can "talk" about roles using posits
    name: String,
    reserved: bool,
}

impl Role {
    pub fn new(role: Thing, name: String, reserved: bool) -> Self {
        Self {
            role,
            name,
            reserved,
        }
    }
    // It's intentional to encapsulate the name in the struct
    // and only expose it using a "getter", because this yields
    // true immutability for objects after creation.
    pub fn role(&self) -> Thing {
        self.role
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn reserved(&self) -> bool {
        self.reserved
    }
}
impl Ord for Role {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}
impl PartialOrd for Role {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Role {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Hash for Role {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.reserved.hash(state);
    }
}

#[derive(Debug)]
pub struct RoleKeeper {
    kept: HashMap<String, Arc<Role>>,
    lookup: HashMap<Thing, Arc<Role>>, // double indexing, but roles should be few so it's not a big deal
}
impl RoleKeeper {
    pub fn new() -> Self {
        Self {
            kept: HashMap::new(),
            lookup: HashMap::new(),
        }
    }
    pub fn keep(&mut self, role: Role) -> (Arc<Role>, bool) {
        let thing = role.role();
        let keepsake = role.name().to_owned();
        let mut previously_kept = true;
        match self.kept.entry(keepsake.clone()) {
            Entry::Vacant(e) => {
                e.insert(Arc::new(role));
                previously_kept = false;
            }
            Entry::Occupied(_e) => (),
        };
        let kept_role = self.kept.get(&keepsake).unwrap();
        if !previously_kept {
            self.lookup.insert(thing, Arc::clone(kept_role));
        }
        (Arc::clone(kept_role), previously_kept)
    }
    pub fn get(&self, name: &str) -> Arc<Role> {
        Arc::clone(self.kept.get(name).unwrap())
    }
    pub fn lookup(&self, role: &Thing) -> Arc<Role> {
        Arc::clone(self.lookup.get(role).unwrap())
    }
    pub fn len(&self) -> usize {
        self.kept.len()
    }
}

// ------------- Appearance -------------
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Appearance {
    thing: Thing,
    role: Arc<Role>,
}
impl Appearance {
    pub fn new(thing: Thing, role: Arc<Role>) -> Self {
        Self { thing, role }
    }
    pub fn thing(&self) -> Thing {
        self.thing
    }
    pub fn role(&self) -> Arc<Role> {
        Arc::clone(&self.role)
    }
}
impl Ord for Appearance {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.role, &self.thing).cmp(&(&other.role, &other.thing))
    }
}
impl PartialOrd for Appearance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl fmt::Display for Appearance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {})", self.thing, self.role.name())
    }
}

#[derive(Debug)]
pub struct AppearanceKeeper {
    kept: HashSet<Arc<Appearance>>,
}
impl AppearanceKeeper {
    pub fn new() -> Self {
        Self {
            kept: HashSet::new(),
        }
    }
    pub fn keep(&mut self, appearance: Appearance) -> (Arc<Appearance>, bool) {
        let keepsake = Arc::new(appearance);
        let previously_kept = !self.kept.insert(Arc::clone(&keepsake));
        (
            Arc::clone(self.kept.get(&keepsake).unwrap()),
            previously_kept,
        )
    }
    pub fn len(&self) -> usize {
        self.kept.len()
    }
}

// ------------- AppearanceSet -------------
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct AppearanceSet {
    appearances: Arc<Vec<Arc<Appearance>>>,
}
impl AppearanceSet {
    pub fn new(mut set: Vec<Arc<Appearance>>) -> Option<Self> {
        set.sort_unstable();
        if set.windows(2).any(|x| x[0].role == x[1].role) {
            return None;
        }
        Some(Self {
            appearances: Arc::new(set),
        })
    }
    pub fn appearances(&self) -> &Vec<Arc<Appearance>> {
        &self.appearances
    }
}
impl fmt::Display for AppearanceSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();
        for a in self.appearances() {
            s += &(a.to_string() + ",");
        }
        s.pop();
        write!(f, "{{{}}}", s)
    }
}


#[derive(Debug)]
pub struct AppearanceSetKeeper {
    kept: HashSet<Arc<AppearanceSet>>,
}
impl AppearanceSetKeeper {
    pub fn new() -> Self {
        Self {
            kept: HashSet::new(),
        }
    }
    pub fn keep(&mut self, appearance_set: AppearanceSet) -> (Arc<AppearanceSet>, bool) {
        let keepsake = Arc::new(appearance_set);
        let previously_kept = !self.kept.insert(Arc::clone(&keepsake));
        (
            Arc::clone(self.kept.get(&keepsake).unwrap()),
            previously_kept,
        )
    }
    pub fn len(&self) -> usize {
        self.kept.len()
    }
}

// --------------- Posit ----------------
#[derive(Eq, PartialOrd, Ord, Debug)]
pub struct Posit<V: DataType, T: DataType + Ord> {
    posit: Thing, // a posit is also a thing we can "talk" about
    appearance_set: Arc<AppearanceSet>,
    value: V, // imprecise value
    time: T,  // imprecise time (note that this must be a data type with a natural ordering)
}
impl<V: DataType, T: DataType + Ord> Posit<V, T> {
    pub fn new(
        posit: Thing,
        appearance_set: Arc<AppearanceSet>,
        value: V,
        time: T,
    ) -> Posit<V, T> {
        Self {
            posit,
            value,
            time,
            appearance_set,
        }
    }
    pub fn posit(&self) -> Thing {
        self.posit
    }
    pub fn appearance_set(&self) -> Arc<AppearanceSet> {
        Arc::clone(&self.appearance_set)
    }
    pub fn value(&self) -> &V {
        &self.value
    }
    pub fn time(&self) -> &T {
        &self.time
    }
}
impl<V: DataType, T: DataType + Ord> PartialEq for Posit<V, T> {
    fn eq(&self, other: &Self) -> bool {
        self.appearance_set == other.appearance_set
            && self.value == other.value
            && self.time == other.time
    }
}
impl<V: DataType, T: DataType + Ord> Hash for Posit<V, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.appearance_set.hash(state);
        self.value.hash(state);
        self.time.hash(state);
    }
}
impl<V: DataType, T: DataType + Ord> fmt::Display for Posit<V, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} [{}, {}, {}]", 
            self.posit,
            self.appearance_set, 
            self.value.to_string() + "::<" + self.value.data_type() + ">", 
            self.time.to_string() + "::<" + self.time.data_type() + ">"
        )
    }
}

// This key needs to be defined in order to store posits in a TypeMap.
impl<V: 'static + DataType, T: 'static + DataType + Ord> Key for Posit<V, T> {
    type Value = BiMap<Arc<Posit<V, T>>, Thing>;
}

pub struct PositKeeper {
    pub kept: TypeMap,
    pub length: usize
}
impl PositKeeper {
    pub fn new() -> Self {
        Self {
            kept: TypeMap::new(),
            length: 0
        }
    }
    pub fn keep<V: 'static + DataType, T: 'static + DataType + Ord>(
        &mut self,
        posit: Posit<V, T>,
    ) -> (Arc<Posit<V, T>>, bool) {
        // ensure the map can work with this particular type combo
        let map = self
            .kept
            .entry::<Posit<V, T>>()
            .or_insert(BiMap::<Arc<Posit<V, T>>, Thing>::new());
        let keepsake_thing = posit.posit();
        let keepsake = Arc::new(posit);
        let mut previously_kept = false;
        let thing = match map.get_by_left(&keepsake) {
            Some(kept_thing) => {
                previously_kept = true;
                kept_thing
            }
            None => {
                map.insert(Arc::clone(&keepsake), keepsake.posit());
                self.length += 1;
                &keepsake_thing
            }
        };
        (
            Arc::clone(map.get_by_right(thing).unwrap()),
            previously_kept,
        )
    }
    pub fn thing<V: 'static + DataType, T: 'static + DataType + Ord>(
        &mut self,
        posit: Arc<Posit<V, T>>,
    ) -> Thing {
        let map = self
            .kept
            .entry::<Posit<V, T>>()
            .or_insert(BiMap::<Arc<Posit<V, T>>, Thing>::new());
        *map.get_by_left(&posit).unwrap()
    }
    pub fn posit<V: 'static + DataType, T: 'static + DataType + Ord>(
        &mut self,
        thing: Thing,
    ) -> Arc<Posit<V, T>> {
        let map = self
            .kept
            .entry::<Posit<V, T>>()
            .or_insert(BiMap::<Arc<Posit<V, T>>, Thing>::new());
        Arc::clone(map.get_by_right(&thing).unwrap())
    }
    pub fn len(&self) -> usize {
        self.length
    }
}

/*
Certainty is a subjective measure that can be held against a posit.
This ranges from being certain of a posit to certain of its opposite,
exemplified by the following statements:

The master will certainly win.
The master will probably win.
The master may win.
The master is unlikely to win.
The master has a small chance of winning.
I have no idea whether the master could win or lose (not win).
The master has a small risk of losing.
The master is unlikely to lose.
The master may lose.
The master will probably lose.
The master will certainly lose.

*/

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub struct Certainty {
    alpha: i8,
}

impl Certainty {
    pub fn new<T: Into<f64>>(a: T) -> Self {
        let a = a.into();
        let a = if a < -1. {
            -1.
        } else if a > 1. {
            1.
        } else {
            a
        };
        Self {
            alpha: (100f64 * a) as i8,
        }
    }
    pub fn consistent(rs: &[Certainty]) -> bool {
        let r_total = rs
            .iter()
            .map(|r: &Certainty| r.alpha as i32)
            .filter(|i| *i != 0)
            .fold(0, |sum, i| sum + 100 * (1 - i.signum()))
            / 2
            + rs.iter()
                .map(|r: &Certainty| r.alpha as i32)
                .filter(|i| *i != 0)
                .sum::<i32>();

        r_total <= 100
    }
}
impl ops::Add for Certainty {
    type Output = f64;
    fn add(self, other: Certainty) -> f64 {
        (self.alpha as f64 + other.alpha as f64) / 100f64
    }
}
impl ops::Mul for Certainty {
    type Output = f64;
    fn mul(self, other: Certainty) -> f64 {
        (self.alpha as f64 / 100f64) * (other.alpha as f64 / 100f64)
    }
}
impl fmt::Display for Certainty {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.alpha {
            -100 => write!(f, "-1"),
            -99..=-1 => write!(f, "-0.{}", -self.alpha),
            0 => write!(f, "0"),
            1..=99 => write!(f, "0.{}", self.alpha),
            100 => write!(f, "1"),
            _ => write!(f, "?"),
        }
    }
}
impl From<Certainty> for f64 {
    fn from(r: Certainty) -> f64 {
        r.alpha as f64 / 100f64
    }
}
impl<'a> From<&'a Certainty> for f64 {
    fn from(r: &Certainty) -> f64 {
        r.alpha as f64 / 100f64
    }
}
impl ToSql for Certainty {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.alpha))
    }
}
impl FromSql for Certainty {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        rusqlite::Result::Ok(Certainty {
            alpha: i8::try_from(value.as_i64().unwrap()).ok().unwrap(),
        })
    }
}

// ------------- Data Types --------------
impl DataType for Certainty {
    type TargetType = Certainty;
    const UID: u8 = 1; // needs to be unique
    const DATA_TYPE: &'static str = "Certainty";
    fn convert(value: &ValueRef) -> Self::TargetType {
        Certainty {
            alpha: i8::try_from(value.as_i64().unwrap()).unwrap(),
        }
    }
}
impl DataType for String {
    type TargetType = String;
    const UID: u8 = 2;
    const DATA_TYPE: &'static str = "String";
    fn convert(value: &ValueRef) -> Self::TargetType {
        String::from(value.as_str().unwrap())
    }
}
impl DataType for DateTime<Utc> {
    type TargetType = DateTime<Utc>;
    const UID: u8 = 3;
    const DATA_TYPE: &'static str = "DateTime::<Utc>";
    fn convert(value: &ValueRef) -> Self::TargetType {
        DateTime::<Utc>::from_str(value.as_str().unwrap()).unwrap()
    }
}
impl DataType for NaiveDate {
    type TargetType = NaiveDate;
    const UID: u8 = 4;
    const DATA_TYPE: &'static str = "NaiveDate";
    fn convert(value: &ValueRef) -> Self::TargetType {
        NaiveDate::from_str(value.as_str().unwrap()).unwrap()
    }
}
impl DataType for i64 {
    type TargetType = i64;
    const UID: u8 = 5;
    const DATA_TYPE: &'static str = "i64";
    fn convert(value: &ValueRef) -> Self::TargetType {
        value.as_i64().unwrap()
    }
}

// ------------- Lookups -------------
#[derive(Debug)]
pub struct Lookup<K, V, H = RandomState> {
    index: HashMap<K, HashSet<V>, H>,
}
impl<K: Eq + Hash, V: Eq + Hash, H: BuildHasher + Default> Lookup<K, V, H> {
    pub fn new() -> Self {
        Self {
            index: HashMap::<K, HashSet<V>, H>::default(),
        }
    }
    pub fn insert(&mut self, key: K, value: V) {
        let map = self.index.entry(key).or_insert(HashSet::<V>::new());
        map.insert(value);
    }
    pub fn lookup(&self, key: &K) -> &HashSet<V> {
        self.index.get(key).unwrap()
    }
}

// ------------- Database -------------
// This sets up the database with the necessary structures
pub struct Database<'db> {
    // owns a thing generator
    pub thing_generator: Arc<Mutex<ThingGenerator>>,
    // owns keepers for the available constructs
    pub role_keeper: Arc<Mutex<RoleKeeper>>,
    pub appearance_keeper: Arc<Mutex<AppearanceKeeper>>,
    pub appearance_set_keeper: Arc<Mutex<AppearanceSetKeeper>>,
    pub posit_keeper: Arc<Mutex<PositKeeper>>,
    // owns lookups between constructs (similar to database indexes)
    pub thing_to_appearance_lookup: Arc<Mutex<Lookup<Thing, Arc<Appearance>, ThingHasher>>>,
    pub role_to_appearance_lookup: Arc<Mutex<Lookup<Arc<Role>, Arc<Appearance>>>>,
    pub appearance_to_appearance_set_lookup: Arc<Mutex<Lookup<Arc<Appearance>, Arc<AppearanceSet>>>>,
    pub appearance_set_to_posit_thing_lookup: Arc<Mutex<Lookup<Arc<AppearanceSet>, Thing>>>,
    // responsible for the the persistence layer
    pub persistor: Arc<Mutex<Persistor<'db>>>,
}

impl<'db> Database<'db> {
    pub fn new<'connection>(connection: &'connection Connection) -> Database<'connection> {
        // Create all the stuff that goes into a database engine
        let thing_generator = ThingGenerator::new();
        let role_keeper = RoleKeeper::new();
        let appearance_keeper = AppearanceKeeper::new();
        let appearance_set_keeper = AppearanceSetKeeper::new();
        let posit_keeper = PositKeeper::new();
        let thing_to_appearance_lookup = Lookup::<Thing, Arc<Appearance>, ThingHasher>::new();
        let role_to_appearance_lookup = Lookup::<Arc<Role>, Arc<Appearance>>::new();
        let appearance_to_appearance_set_lookup = Lookup::<Arc<Appearance>, Arc<AppearanceSet>>::new();
        let appearance_set_to_posit_thing_lookup = Lookup::<Arc<AppearanceSet>, Thing>::new();
        let persistor = Persistor::new(connection);

        // Create the database so that we can prime it before returning it
        let database = Database {
            thing_generator: Arc::new(Mutex::new(thing_generator)),
            role_keeper: Arc::new(Mutex::new(role_keeper)),
            appearance_keeper: Arc::new(Mutex::new(appearance_keeper)),
            appearance_set_keeper: Arc::new(Mutex::new(appearance_set_keeper)),
            posit_keeper: Arc::new(Mutex::new(posit_keeper)),
            thing_to_appearance_lookup: Arc::new(Mutex::new(thing_to_appearance_lookup)),
            role_to_appearance_lookup: Arc::new(Mutex::new(role_to_appearance_lookup)),
            appearance_to_appearance_set_lookup: Arc::new(Mutex::new(
                appearance_to_appearance_set_lookup,
            )),
            appearance_set_to_posit_thing_lookup: Arc::new(Mutex::new(
                appearance_set_to_posit_thing_lookup,
            )),
            persistor: Arc::new(Mutex::new(persistor)),
        };

        // Restore the existing database
        database.persistor.lock().unwrap().restore_things(&database);
        database.persistor.lock().unwrap().restore_roles(&database);
        database.persistor.lock().unwrap().restore_posits(&database);

        // Reserve some roles that will be necessary for implementing features
        // commonly found in many other (including non-tradtional) databases.
        database.create_role(String::from("posit"), false);
        database.create_role(String::from("ascertains"), true);
        database.create_role(String::from("thing"), false);
        database.create_role(String::from("classification"), true);

        database
    }
    // functions to access the owned generator and keepers
    pub fn thing_generator(&self) -> Arc<Mutex<ThingGenerator>> {
        Arc::clone(&self.thing_generator)
    }
    pub fn role_keeper(&self) -> Arc<Mutex<RoleKeeper>> {
        Arc::clone(&self.role_keeper)
    }
    pub fn appearance_keeper(&self) -> Arc<Mutex<AppearanceKeeper>> {
        Arc::clone(&self.appearance_keeper)
    }
    pub fn appearance_set_keeper(&self) -> Arc<Mutex<AppearanceSetKeeper>> {
        Arc::clone(&self.appearance_set_keeper)
    }
    pub fn posit_keeper(&self) -> Arc<Mutex<PositKeeper>> {
        Arc::clone(&self.posit_keeper)
    }
    pub fn thing_to_appearance_lookup(
        &self,
    ) -> Arc<Mutex<Lookup<Thing, Arc<Appearance>, ThingHasher>>> {
        Arc::clone(&self.thing_to_appearance_lookup)
    }
    pub fn role_to_appearance_lookup(&self) -> Arc<Mutex<Lookup<Arc<Role>, Arc<Appearance>>>> {
        Arc::clone(&self.role_to_appearance_lookup)
    }
    pub fn appearance_to_appearance_set_lookup(
        &self,
    ) -> Arc<Mutex<Lookup<Arc<Appearance>, Arc<AppearanceSet>>>> {
        Arc::clone(&self.appearance_to_appearance_set_lookup)
    }
    pub fn appearance_set_to_posit_thing_lookup(
        &self,
    ) -> Arc<Mutex<Lookup<Arc<AppearanceSet>, Thing>>> {
        Arc::clone(&self.appearance_set_to_posit_thing_lookup)
    }
    pub fn create_thing(&self) -> Arc<Thing> {
        let thing = self.thing_generator.lock().unwrap().generate();
        self.persistor.lock().unwrap().persist_thing(&thing);
        Arc::new(thing)
    }
    // functions to create constructs for the keepers to keep that also populate the lookups
    pub fn keep_role(&self, role: Role) -> (Arc<Role>, bool) {
        let (kept_role, previously_kept) = self.role_keeper.lock().unwrap().keep(role);
        (kept_role, previously_kept)
    }
    pub fn create_role(&self, role_name: String, reserved: bool) -> (Arc<Role>, bool) {
        let role_thing = self.thing_generator.lock().unwrap().generate();
        let (kept_role, previously_kept) =
            self.keep_role(Role::new(role_thing, role_name, reserved));
        if !previously_kept {
            self.persistor
                .lock()
                .unwrap()
                .persist_thing(&kept_role.role());
            self.persistor.lock().unwrap().persist_role(&kept_role);
        } else {
            self.thing_generator.lock().unwrap().release(role_thing);
        }
        (kept_role, previously_kept)
    }
    pub fn keep_appearance(&self, appearance: Appearance) -> (Arc<Appearance>, bool) {
        let (kept_appearance, previously_kept) =
            self.appearance_keeper.lock().unwrap().keep(appearance);
        if !previously_kept {
            self.thing_to_appearance_lookup
                .lock()
                .unwrap()
                .insert(kept_appearance.thing(), Arc::clone(&kept_appearance));
            if kept_appearance.role().reserved {
                self.role_to_appearance_lookup
                    .lock()
                    .unwrap()
                    .insert(kept_appearance.role(), Arc::clone(&kept_appearance));
            }
        }
        (kept_appearance, previously_kept)
    }
    pub fn create_apperance(&self, thing: Thing, role: Arc<Role>) -> (Arc<Appearance>, bool) {
        self.keep_appearance(Appearance::new(thing, role))
    }
    pub fn keep_appearance_set(
        &self,
        appearance_set: AppearanceSet,
    ) -> (Arc<AppearanceSet>, bool) {
        let (kept_appearance_set, previously_kept) = self
            .appearance_set_keeper
            .lock()
            .unwrap()
            .keep(appearance_set);
        if !previously_kept {
            for appearance in kept_appearance_set.appearances().iter() {
                self.appearance_to_appearance_set_lookup
                    .lock()
                    .unwrap()
                    .insert(Arc::clone(appearance), Arc::clone(&kept_appearance_set));
            }
        }
        (kept_appearance_set, previously_kept)
    }
    pub fn create_appearance_set(
        &self,
        appearance_set: Vec<Arc<Appearance>>,
    ) -> (Arc<AppearanceSet>, bool) {
        self.keep_appearance_set(AppearanceSet::new(appearance_set).unwrap())
    }
    pub fn keep_posit<V: 'static + DataType, T: 'static + DataType + Ord>(
        &self,
        posit: Posit<V, T>,
    ) -> (Arc<Posit<V, T>>, bool) {
        let (kept_posit, previously_kept) = self.posit_keeper.lock().unwrap().keep(posit);
        if !previously_kept {
            self.appearance_set_to_posit_thing_lookup
                .lock()
                .unwrap()
                .insert(kept_posit.appearance_set(), kept_posit.posit());
        }
        (kept_posit, previously_kept)
    }
    pub fn create_posit<V: 'static + DataType, T: 'static + DataType + Ord>(
        &self,
        appearance_set: Arc<AppearanceSet>,
        value: V,
        time: T,
    ) -> Arc<Posit<V, T>> {
        let posit_thing = self.thing_generator.lock().unwrap().generate();
        let (kept_posit, previously_kept) =
            self.keep_posit(Posit::new(posit_thing, appearance_set, value, time));
        if !previously_kept {
            self.persistor
                .lock()
                .unwrap()
                .persist_thing(&kept_posit.posit());
            self.persistor.lock().unwrap().persist_posit(&kept_posit);
        } else {
            self.thing_generator.lock().unwrap().release(posit_thing);
        }
        kept_posit
    }
    // finally, now that the database exists we can start to make assertions
    pub fn assert<V: 'static + DataType, T: 'static + DataType + Ord>(
        &self,
        asserter: Thing,
        posit: Arc<Posit<V, T>>,
        certainty: Certainty,
        assertion_time: DateTime<Utc>,
    ) -> Arc<Posit<Certainty, DateTime<Utc>>> {
        let posit_thing: Thing =
            self.posit_keeper.lock().unwrap().thing(posit);
        let asserter_role = self
            .role_keeper
            .lock()
            .unwrap()
            .get(&"ascertains".to_owned());
        let posit_role = self.role_keeper.lock().unwrap().get(&"posit".to_owned());
        let (asserter_appearance, _) = self.create_apperance(asserter, asserter_role);
        let (posit_appearance, _) = self.create_apperance(posit_thing, posit_role);
        let (appearance_set, _) =
            self.create_appearance_set([asserter_appearance, posit_appearance].to_vec());
        self.create_posit(appearance_set, certainty, assertion_time)
    }
}

