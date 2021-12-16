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
mod bareclad {
    use std::sync::{Arc, Mutex};

    // used in the keeper of posits, since they are generically typed: Posit<V,T> and therefore require a HashSet per type combo
    use typemap::{Key, TypeMap};

    // used to keep the one-to-one mapping between posits and their assigned identities
    use bimap::BiMap;

    // other keepers use HashSet or HashMap
    use core::hash::{BuildHasher, BuildHasherDefault, Hasher};
    use std::collections::hash_map::{Entry, RandomState};
    use std::collections::{HashMap, HashSet};
    use std::hash::Hash;

    use std::fmt::{self};
    use std::ops;

    // used for persistence
    use rusqlite::{params, Connection, Statement, Error};
    use rusqlite::types::{ToSql, ToSqlOutput, FromSql, FromSqlResult, ValueRef};

    // used for timestamps in the database
    use chrono::{DateTime, Utc};
    // used when parsing a string to a DateTime<Utc>
    use std::str::FromStr;

    pub trait DataType : ToString + Eq + Hash + Send + Sync + ToSql + FromSql {
        type TargetType;
        fn convert(value: &ValueRef) -> Self::TargetType;
        fn data_type(&self) -> &'static str;
    }
    pub trait TimeType : ToString + Ord + Eq + Hash + Send + Sync + ToSql + FromSql {
        type TargetType;
        fn convert(value: &ValueRef) -> Self::TargetType;
        fn time_type(&self) -> &'static str;
    }

    // ------------- Thing -------------
    // TODO: Investigate using AtomicUsize instead.
    // https://rust-lang.github.io/rust-clippy/master/index.html#mutex_integer
    pub type Thing = usize;
    const GENESIS: Thing = 0;

    #[derive(Debug)]
    pub struct ThingGenerator {
        pub lower_bound: Thing,
        released: Vec<Thing>,
    }

    impl ThingGenerator {
        pub fn new() -> Self {
            Self {
                lower_bound: GENESIS,
                released: Vec::new(),
            }
        }
        pub fn release(&mut self, g: Thing) {
            self.released.push(g);
        }
        pub fn generate(&mut self) -> Thing {
            self.released.pop().unwrap_or_else(|| {
                self.lower_bound += 1;
                self.lower_bound
            })
        }
    }

    #[derive(Debug, Clone, Copy, Default)]
    pub struct ThingHash(Thing);

    impl Hasher for ThingHash {
        fn finish(&self) -> u64 {
            self.0 as u64
        }

        fn write(&mut self, _bytes: &[u8]) {
            unimplemented!("ThingHasher only supports usize keys")
        }

        fn write_usize(&mut self, i: Thing) {
            self.0 = i;
        }
    }

    type ThingHasher = BuildHasherDefault<ThingHash>;

    // ------------- Role -------------
    #[derive(Eq, PartialOrd, Ord, Debug)]
    pub struct Role {
        role: Arc<Thing>, // let it be a thing so we can "talk" about roles using posits
        name: String,
        reserved: bool,
    }

    impl Role {
        pub fn new(role: Thing, name: String, reserved: bool) -> Self {
            Self {
                role: Arc::new(role), 
                name,
                reserved,
            }
        }
        // It's intentional to encapsulate the name in the struct
        // and only expose it using a "getter", because this yields
        // true immutability for objects after creation.
        pub fn role(&self) -> Arc<Thing> {
            self.role.clone()
        }
        pub fn name(&self) -> &str {
            &self.name
        }
        pub fn reserved(&self) -> bool {
            self.reserved
        }
    }
    impl PartialEq for Role {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name && self.reserved == other.reserved
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
    }
    impl RoleKeeper {
        pub fn new() -> Self {
            Self {
                kept: HashMap::new(),
            }
        }
        pub fn keep(&mut self, role: Role) -> (Arc<Role>, bool) {
            let keepsake = role.name().to_owned();
            let mut previously_kept = true;
            match self.kept.entry(keepsake.clone()) {
                Entry::Vacant(e) => {
                    e.insert(Arc::new(role));
                    previously_kept = false;
                }
                Entry::Occupied(_e) => (),
            };
            (Arc::clone(self.kept.get(&keepsake).unwrap()), previously_kept)
        }
        pub fn get(&self, name: &str) -> Arc<Role> {
            Arc::clone(self.kept.get(name).unwrap())
        }
    }

    // ------------- Appearance -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Appearance {
        thing: Arc<Thing>,
        role: Arc<Role>,
    }
    impl Appearance {
        pub fn new(thing: Arc<Thing>, role: Arc<Role>) -> Self {
            Self { 
                thing,
                role 
            }
        }
        pub fn thing(&self) -> Arc<Thing> {
            self.thing.clone()
        }
        pub fn role(&self) -> Arc<Role> {
            self.role.clone()
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
            (Arc::clone(self.kept.get(&keepsake).unwrap()), previously_kept)
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
            (Arc::clone(self.kept.get(&keepsake).unwrap()), previously_kept)
        }
    }

    // --------------- Posit ----------------
    #[derive(Eq, PartialOrd, Ord, Debug)]
    pub struct Posit<V: DataType, T: TimeType> {
        posit: Arc<Thing>, // a posit is also a thing we can "talk" about
        appearance_set: Arc<AppearanceSet>,
        value: V, // imprecise value
        time: T,  // imprecise time
    }
    impl<V: DataType, T: TimeType> Posit<V, T> {
        pub fn new(posit: Thing, appearance_set: Arc<AppearanceSet>, value: V, time: T) -> Posit<V, T> {
            Self {
                posit: Arc::new(posit),
                value,
                time,
                appearance_set,
            }
        }
        pub fn posit(&self) -> Arc<Thing> {
            self.posit.clone()
        }
        pub fn appearance_set(&self) -> Arc<AppearanceSet> {
            self.appearance_set.clone()
        }
        pub fn value(&self) -> &V {
            &self.value
        }
        pub fn time(&self) -> &T {
            &self.time
        }
    }
    impl<V: DataType, T: TimeType> PartialEq for Posit<V, T> {
        fn eq(&self, other: &Self) -> bool {
            self.appearance_set == other.appearance_set &&
            self.value == other.value &&
            self.time == other.time
        }
    }
    impl<V: DataType, T: TimeType> Hash for Posit<V, T> {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.appearance_set.hash(state);
            self.value.hash(state);
            self.time.hash(state);
        }
    }

    // This key needs to be defined in order to store posits in a TypeMap.
    impl<V: 'static + DataType, T: 'static + TimeType> Key for Posit<V, T> {
        type Value = BiMap<Arc<Posit<V, T>>, Arc<Thing>>;
    }

    pub struct PositKeeper {
        pub kept: TypeMap,
    }
    impl PositKeeper {
        pub fn new() -> Self {
            Self {
                kept: TypeMap::new(),
            }
        }
        pub fn keep<V: 'static + DataType, T: 'static + TimeType>(
            &mut self,
            posit: Posit<V, T>,
        ) -> (Arc<Posit<V, T>>, bool) {
            // ensure the map can work with this particular type combo
            let map = self
                .kept
                .entry::<Posit<V, T>>()
                .or_insert(BiMap::<Arc<Posit<V, T>>, Arc<Thing>>::new());
            let keepsake_thing = Arc::clone(&posit.posit());
            let keepsake = Arc::new(posit);     
            let mut previously_kept = false;
            let thing = match map.get_by_left(&keepsake) {
                Some(kept_thing) => {
                    previously_kept = true;
                    kept_thing
                },
                None => {
                    map.insert(Arc::clone(&keepsake), Arc::clone(&keepsake.posit()));
                    &keepsake_thing
                }
            };
            (
                Arc::clone(map.get_by_right(thing).unwrap()),
                previously_kept
            )
        }
        pub fn thing<V: 'static + DataType, T: 'static + TimeType>(
            &mut self,
            posit: Arc<Posit<V, T>>,
        ) -> Arc<Thing> {
            let map = self
                .kept
                .entry::<Posit<V, T>>()
                .or_insert(BiMap::<Arc<Posit<V, T>>, Arc<Thing>>::new());
            Arc::clone(map.get_by_left(&posit).unwrap())
        }
        pub fn posit<V: 'static + DataType, T: 'static + TimeType>(
            &mut self,
            thing: Arc<Thing>,
        ) -> Arc<Posit<V, T>> {
            let map = self
                .kept
                .entry::<Posit<V, T>>()
                .or_insert(BiMap::<Arc<Posit<V, T>>, Arc<Thing>>::new());
            Arc::clone(map.get_by_right(&thing).unwrap())
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
            rusqlite::Result::Ok(Certainty { alpha: i8::try_from(value.as_i64().unwrap()).ok().unwrap() })
        }
    }

    // ------------- Lookups -------------
    #[derive(Debug)]
    pub struct Lookup<K, V, H = RandomState> {
        index: HashMap<Arc<K>, HashSet<Arc<V>>, H>,
    }
    impl<K: Eq + Hash, V: Eq + Hash, H: BuildHasher + Default> Lookup<K, V, H> {
        pub fn new() -> Self {
            Self {
                index: HashMap::<Arc<K>, HashSet<Arc<V>>, H>::default(),
            }
        }
        pub fn insert(&mut self, key: Arc<K>, value: Arc<V>) {
            let map = self.index.entry(key).or_insert(HashSet::<Arc<V>>::new());
            map.insert(value);
        }
        pub fn lookup(&self, key: &K) -> &HashSet<Arc<V>> {
            self.index.get(key).unwrap()
        }
    }

    // ------------- Data Types --------------
    impl DataType for Certainty { 
        type TargetType = Certainty;
        fn data_type(&self) -> &'static str {
            "Certainty"
        }
        fn convert(value: &ValueRef) -> Self::TargetType {
            Certainty {
                alpha: i8::try_from(value.as_i64().unwrap()).unwrap()
            }
        }
    }
    impl DataType for String { 
        type TargetType = String;
        fn data_type(&self) -> &'static str {
            "String"
        }
        fn convert(value: &ValueRef) -> Self::TargetType {
            String::from(value.as_str().unwrap())
        }
    }
    impl TimeType for DateTime<Utc> { 
        type TargetType = DateTime<Utc>;
        fn time_type(&self) -> &'static str {
            "DateTime<Utc>"
        }
        fn convert(value: &ValueRef) -> Self::TargetType {
            DateTime::<Utc>::from_str(value.as_str().unwrap()).unwrap()
        }
    }
    impl TimeType for i64 {
        type TargetType = i64;
        fn time_type(&self) -> &'static str {
            "i64"
        }
        fn convert(value: &ValueRef) -> Self::TargetType {
            value.as_i64().unwrap()
        }
    }
    // ------------- Persistence -------------
    pub struct Persistor<'db> {
        pub db: &'db Connection,
        // Adders
        pub add_thing: Statement<'db>,
        pub add_role: Statement<'db>,
        pub add_posit: Statement<'db>, 
        // Get the identity of one thing
        pub get_thing: Statement<'db>,
        pub get_role: Statement<'db>,
        pub get_posit: Statement<'db>,
        // Get everything for all things
        pub all_things: Statement<'db>,
        pub all_roles: Statement<'db>,
        pub all_posits: Statement<'db>,
    }
    impl<'db> Persistor<'db> {
        pub fn new<'connection>(connection: &'connection Connection) -> Persistor<'connection> {
            // The "STRICT" keyword introduced in 3.37.0 breaks JDBC connections, which makes 
            // debugging using an external tool like DBeaver impossible
            connection.execute_batch(
                "
                create table if not exists Thing (
                    Thing_Identity integer not null, 
                    constraint unique_and_referenceable_Thing_Identity primary key (
                        Thing_Identity
                    )
                );-- STRICT;
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
                );-- STRICT;
                create table if not exists Posit (
                    Posit_Identity integer not null,
                    AppearanceSet text not null,
                    AppearingValue any null, 
                    ValueType text not null, -- normalize later
                    AppearanceTime any null,
                    TimeType text not null, -- normalize later
                    constraint Posit_is_Thing foreign key (
                        Posit_Identity
                    ) references Thing(Thing_Identity),
                    constraint referenceable_Posit_Identity primary key (
                        Posit_Identity
                    ),
                    constraint unique_Posit unique (
                        AppearanceSet,
                        AppearingValue,
                        AppearanceTime
                    )
                );-- STRICT;
                "
            ).unwrap();
            Persistor {
                db: connection,
                add_thing: connection.prepare(
                    "insert into Thing (Thing_Identity) values (?)"
                ).unwrap(),
                add_role: connection.prepare(
                    "insert into Role (Role_Identity, Role, Reserved) values (?, ?, ?)"
                ).unwrap(),
                add_posit: connection.prepare(
                    "insert into Posit (Posit_Identity, AppearanceSet, AppearingValue, ValueType, AppearanceTime, TimeType) values (?, ?, ?, ?, ?, ?)"
                ).unwrap(),
                get_thing: connection.prepare(
                    "select Thing_Identity from Thing where Thing_Identity = ?"
                ).unwrap(),
                get_role: connection.prepare(
                    "select Role_Identity from Role where Role = ?"
                ).unwrap(),
                get_posit: connection.prepare(
                    "select Posit_Identity from Posit where AppearanceSet = ? and AppearingValue = ? and AppearanceTime = ?"
                ).unwrap(), 
                all_things: connection.prepare(
                    "select coalesce(max(Thing_Identity), 0) from Thing"
                ).unwrap(),
                all_roles: connection.prepare(
                    "select Role_Identity, Role, Reserved from Role"
                ).unwrap(),
                all_posits: connection.prepare(
                    "select Posit_Identity, AppearanceSet, AppearingValue, ValueType, AppearanceTime, TimeType from Posit"
                ).unwrap(),
            }
        }
        pub fn persist_thing(&mut self, thing: &Thing) -> bool {
            let mut existing = false;
            match self.get_thing.query_row::<usize, _, _>(params![&thing], |r| r.get(0)) {
                Ok(_) => {
                    existing = true;
                },
                Err(Error::QueryReturnedNoRows) => {
                    self.add_thing.execute(params![&thing]).unwrap();
                },
                Err(err) => {
                    panic!("Could not check if the thing '{}' is persisted: {}", &thing, err);
                }
            }
            existing
        }
        pub fn persist_role(&mut self, role: &Role) -> bool {
            let mut existing = false;
            match self.get_role.query_row::<usize, _, _>(params![&role.name()], |r| r.get(0)) {
                Ok(_) => {
                    existing = true;
                },
                Err(Error::QueryReturnedNoRows) => {
                    self.add_role.execute(params![&role.role(), &role.name(), &role.reserved()]).unwrap();
                },
                Err(err) => {
                    panic!("Could not check if the role '{}' is persisted: {}", &role.name(), err);
                }
            }
            existing
        }
        pub fn persist_posit<
            V: 'static + DataType,
            T: 'static + TimeType,
        >(&mut self, posit: &Posit<V, T>) -> bool {
            let mut appearances = Vec::new();
            let appearance_set = posit.appearance_set();
            for appearance in appearance_set.appearances().iter() {
                appearances.push(appearance.thing().to_string() + "," + appearance.role().name());
            }
            let apperance_set_as_text = appearances.join("|");
            let mut existing = false;
            match self.get_posit.query_row::<usize, _, _>(
                params![
                    &apperance_set_as_text, 
                    &posit.value(), 
                    &posit.time()
                ], |r| r.get(0)
            ) {
                Ok(_) => {
                    existing = true;
                },
                Err(Error::QueryReturnedNoRows) => {
                    self.add_posit.execute(
                        params![
                            &posit.posit(), 
                            &apperance_set_as_text, 
                            &posit.value(), 
                            &posit.value().data_type(), 
                            &posit.time(),
                            &posit.time().time_type()
                        ]
                    ).unwrap();
                },
                Err(err) => {
                    panic!("Could not check if the posit {} is persisted: {}", &posit.posit(), err);
                }
            }
            existing
        }
        pub fn restore_things(&mut self, db: &Database) {
            match self.all_things.query_row::<usize, _, _>([], |r| r.get(0)) {
                Ok(max_thing) => {
                    db.thing_generator().lock().unwrap().lower_bound = max_thing;
                }
                Err(err) => {
                    panic!("Could not restore things: {}", err);
                }
            }
        }
        pub fn restore_roles(&mut self, db: &Database) {
            let role_iter = self.all_roles.query_map([], |row| {
                Ok(Role {
                    role: Arc::new(row.get(0).unwrap()),
                    name: row.get(1).unwrap(),
                    reserved: row.get(2).unwrap()
                })
            }).unwrap();
            for role in role_iter {
                db.keep_role(role.unwrap());
            }
        }
        pub fn restore_posits(&mut self, db: &Database) {
            let mut rows = self.all_posits.query([]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                let data_type: String = row.get_unwrap(3);
                let time_type: String = row.get_unwrap(5);
                let thing: usize = row.get_unwrap(0);
                let appearances: String = row.get_unwrap(1);
                let mut appearance_set = Vec::new();
                for appearance_text in appearances.split('|') {
                    let (thing, role_name) = appearance_text.split_once(',').unwrap();
                    let appearance = Appearance {
                        thing: Arc::new(thing.parse().unwrap()),
                        role: db.role_keeper().lock().unwrap().get(role_name)
                    };
                    let (kept_appearance, _) = db.keep_appearance(appearance);
                    appearance_set.push(kept_appearance);
                }
                let (kept_appearance_set, _) = db.keep_appearance_set(AppearanceSet { appearances: Arc::new(appearance_set) });
                match (data_type.as_str(), time_type.as_str()) {
                    // massive boilerplate here
                    ("String", "i64") => {
                        db.keep_posit(
                            Posit { 
                                posit: Arc::new(thing), 
                                appearance_set: kept_appearance_set, 
                                value: String::convert(&row.get_ref_unwrap(2)), 
                                time: i64::convert(&row.get_ref_unwrap(4))
                            }
                        );
                    },
                    ("String", "Datetime<Utc>") => {
                        db.keep_posit(
                            Posit { 
                                posit: Arc::new(thing), 
                                appearance_set: kept_appearance_set, 
                                value: String::convert(&row.get_ref_unwrap(2)), 
                                time: DateTime::<Utc>::convert(&row.get_ref_unwrap(4))
                            }
                        );
                    },
                    ("Certainty", "i64") => {
                        db.keep_posit(
                            Posit { 
                                posit: Arc::new(thing), 
                                appearance_set: kept_appearance_set, 
                                value: Certainty::convert(&row.get_ref_unwrap(2)), 
                                time: i64::convert(&row.get_ref_unwrap(4))
                            }
                        );
                    },
                    ("Certainty", "Datetime<Utc>") => {
                        db.keep_posit(
                            Posit { 
                                posit: Arc::new(thing), 
                                appearance_set: kept_appearance_set, 
                                value: Certainty::convert(&row.get_ref_unwrap(2)), 
                                time: DateTime::<Utc>::convert(&row.get_ref_unwrap(4))
                            }
                        );
                    }, 
                    _ => ()
                }
            }
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
        pub thing_to_appearance_lookup: Arc<Mutex<Lookup<Thing, Appearance, ThingHasher>>>,
        pub role_to_appearance_lookup: Arc<Mutex<Lookup<Role, Appearance>>>,
        pub appearance_to_appearance_set_lookup: Arc<Mutex<Lookup<Appearance, AppearanceSet>>>,
        pub appearance_set_to_posit_thing_lookup: Arc<Mutex<Lookup<AppearanceSet, Thing>>>,
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
            let thing_to_appearance_lookup =
                Lookup::<Thing, Appearance, ThingHasher>::new();
            let role_to_appearance_lookup = Lookup::<Role, Appearance>::new();
            let appearance_to_appearance_set_lookup = Lookup::<Appearance, AppearanceSet>::new();
            let appearance_set_to_posit_thing_lookup = Lookup::<AppearanceSet, Thing>::new();   
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
        ) -> Arc<Mutex<Lookup<Thing, Appearance, ThingHasher>>> {
            Arc::clone(&self.thing_to_appearance_lookup)
        }
        pub fn role_to_appearance_lookup(&self) -> Arc<Mutex<Lookup<Role, Appearance>>> {
            Arc::clone(&self.role_to_appearance_lookup)
        }
        pub fn appearance_to_appearance_set_lookup(
            &self,
        ) -> Arc<Mutex<Lookup<Appearance, AppearanceSet>>> {
            Arc::clone(&self.appearance_to_appearance_set_lookup)
        }
        pub fn appearance_set_to_posit_thing_lookup(
            &self,
        ) -> Arc<Mutex<Lookup<AppearanceSet, Thing>>> {
            Arc::clone(&self.appearance_set_to_posit_thing_lookup)
        }
        pub fn create_thing(&self) -> Arc<Thing> {
            let thing = self.thing_generator.lock().unwrap().generate();
            self.persistor.lock().unwrap().persist_thing(&thing);
            Arc::new(thing)
        }
        // functions to create constructs for the keepers to keep that also populate the lookups
        pub fn keep_role(&self, role: Role) -> (Arc<Role>, bool) {
            let (kept_role, previously_kept) = self.role_keeper
            .lock()
            .unwrap()
            .keep(role);
            (kept_role, previously_kept)
        }
        pub fn create_role(&self, role_name: String, reserved: bool) -> Arc<Role> {
            let role_thing = self.thing_generator.lock().unwrap().generate();
            let (kept_role, previously_kept) = self.keep_role(Role::new(role_thing, role_name, reserved));
            if !previously_kept {
                self.persistor.lock().unwrap().persist_thing(&kept_role.role());
                self.persistor.lock().unwrap().persist_role(&kept_role);
            }
            else {
                self.thing_generator.lock().unwrap().release(role_thing);
            }
            kept_role     
        }
        pub fn keep_appearance(&self, appearance: Appearance) -> (Arc<Appearance>, bool) {
            let (kept_appearance, previously_kept) = self
                .appearance_keeper
                .lock()
                .unwrap()
                .keep(appearance);
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
        pub fn create_apperance(&self, thing: Arc<Thing>, role: Arc<Role>) -> Arc<Appearance> {
            let (kept_appearance, _) = self.keep_appearance(Appearance::new(thing, role));
            kept_appearance
        }
        pub fn keep_appearance_set(&self, appearance_set: AppearanceSet) -> (Arc<AppearanceSet>, bool) {
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
        pub fn create_appearance_set(&self, appearance_set: Vec<Arc<Appearance>>) -> Arc<AppearanceSet> {
            let (kept_appearance_set, _) = self.keep_appearance_set(AppearanceSet::new(appearance_set).unwrap());
            kept_appearance_set
        }
        pub fn keep_posit<
            V: 'static + DataType,
            T: 'static + TimeType,
        >(&self, posit: Posit<V, T>) -> (Arc<Posit<V, T>>, bool) {
            let (kept_posit, previously_kept) = self.posit_keeper.lock().unwrap().keep(posit);
            if !previously_kept {
                self.appearance_set_to_posit_thing_lookup
                    .lock()
                    .unwrap()
                    .insert(kept_posit.appearance_set(), kept_posit.posit());
            }
            (kept_posit, previously_kept)
        }
        pub fn create_posit<
            V: 'static + DataType,
            T: 'static + TimeType,
        >(
            &self,
            appearance_set: Arc<AppearanceSet>,
            value: V,
            time: T,
        ) -> Arc<Posit<V, T>> {
            let posit_thing = self.thing_generator.lock().unwrap().generate();
            let (kept_posit, previously_kept) = self.keep_posit(
                Posit::new(posit_thing, appearance_set, value, time)
            );
            if !previously_kept {
                self.persistor.lock().unwrap().persist_thing(&kept_posit.posit());
                self.persistor.lock().unwrap().persist_posit(&kept_posit);
            }
            else {
                self.thing_generator.lock().unwrap().release(posit_thing);
            }
            kept_posit
        }
        // finally, now that the database exists we can start to make assertions
        pub fn assert<V: 'static + DataType, T: 'static + TimeType>(
            &self,
            asserter: Arc<Thing>,
            posit: Arc<Posit<V, T>>,
            certainty: Certainty,
            assertion_time: DateTime<Utc>,
        ) -> Arc<Posit<Certainty, DateTime<Utc>>> {
            let posit_thing: Arc<Thing> = self
                .posit_keeper
                .lock()
                .unwrap()
                .thing(Arc::clone(&posit));
            let asserter_role = self.role_keeper.lock().unwrap().get(&"ascertains".to_owned());
            let posit_role = self.role_keeper.lock().unwrap().get(&"posit".to_owned());
            let asserter_appearance = self.create_apperance(asserter, asserter_role);
            let posit_appearance = self.create_apperance(posit_thing, posit_role);
            let appearance_set =
                self.create_appearance_set([asserter_appearance, posit_appearance].to_vec());
            self.create_posit(appearance_set, certainty, assertion_time)
        }
        // search functions in order to find posits matching certain circumstances
        pub fn posits_involving_thing(&self, thing: &Thing) -> Vec<Arc<Thing>> {
            let mut posits: Vec<Arc<Thing>> = Vec::new();
            for appearance in self
                .thing_to_appearance_lookup
                .lock()
                .unwrap()
                .lookup(thing)
            {
                for appearance_set in self
                    .appearance_to_appearance_set_lookup
                    .lock()
                    .unwrap()
                    .lookup(appearance)
                {
                    for posit_thing in self
                        .appearance_set_to_posit_thing_lookup
                        .lock()
                        .unwrap()
                        .lookup(appearance_set)
                    {
                        posits.push(Arc::clone(posit_thing));
                    }
                }
            }
            posits
        }
    }
}

 

// =========== TESTING BELOW ===========

use chrono::{DateTime, Utc};
use std::sync::Arc;
use text_io::read;

use rusqlite::{Connection};
use bareclad::{Appearance, AppearanceSet, Certainty, Database, Thing, Posit, Role, Persistor};

fn main() {
    let sqlite = Connection::open("bareclad.db").unwrap();
    println!("The path to the database file is '{}'.", sqlite.path().unwrap().display());      
    // TODO: if the file exists, populate the in-memory database
    let bareclad = Database::new(&sqlite);

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

    println!("Enter a value that appears with '{}' and '{}': ", role, another_role);
    let mut v1: String = read!("{}");
    v1.truncate(v1.trim_end().len());

    let p1 = bareclad.create_posit(Arc::clone(&as1), v1.clone(), 42i64); // this 42 represents a point in time (for now)
    let p2 = bareclad.create_posit(Arc::clone(&as1), v1.clone(), 42i64);

    println!("Enter a different value that appears with '{}' and '{}': ", role, another_role);
    let mut v2: String = read!("{}");
    v2.truncate(v2.trim_end().len());

    let p3 =
        bareclad.create_posit(Arc::clone(&as1), v2.clone(), 21i64);
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
