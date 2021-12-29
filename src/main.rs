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
    use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
    use rusqlite::{params, Connection, Error, Statement};

    // used for timestamps in the database
    use chrono::{DateTime, Utc, NaiveDate};
    // used when parsing a string to a DateTime<Utc>
    use std::str::FromStr;

    pub trait DataType: ToString + Eq + Hash + Send + Sync + ToSql + FromSql {
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
        pub fn retain(&mut self, t: Thing) {
            if t > self.lower_bound {
                self.lower_bound = t;
            }
        }
        pub fn release(&mut self, t: Thing) {
            self.released.push(t);
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
        lookup: HashMap<Thing, Arc<Role>>,
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
                self.lookup.insert(*thing, Arc::clone(kept_role));
            }
            (Arc::clone(kept_role), previously_kept)
        }
        pub fn get(&self, name: &str) -> Arc<Role> {
            Arc::clone(self.kept.get(name).unwrap())
        }
        pub fn lookup(&self, role: &Thing) -> Arc<Role> {
            Arc::clone(self.lookup.get(role).unwrap())
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
            Self { thing, role }
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
            (
                Arc::clone(self.kept.get(&keepsake).unwrap()),
                previously_kept,
            )
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
            (
                Arc::clone(self.kept.get(&keepsake).unwrap()),
                previously_kept,
            )
        }
    }

    // --------------- Posit ----------------
    #[derive(Eq, PartialOrd, Ord, Debug)]
    pub struct Posit<V: DataType, T: DataType + Ord> {
        posit: Arc<Thing>, // a posit is also a thing we can "talk" about
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

    // This key needs to be defined in order to store posits in a TypeMap.
    impl<V: 'static + DataType, T: 'static + DataType + Ord> Key for Posit<V, T> {
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
        pub fn keep<V: 'static + DataType, T: 'static + DataType + Ord>(
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
                }
                None => {
                    map.insert(Arc::clone(&keepsake), Arc::clone(&keepsake.posit()));
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
        ) -> Arc<Thing> {
            let map = self
                .kept
                .entry::<Posit<V, T>>()
                .or_insert(BiMap::<Arc<Posit<V, T>>, Arc<Thing>>::new());
            Arc::clone(map.get_by_left(&posit).unwrap())
        }
        pub fn posit<V: 'static + DataType, T: 'static + DataType + Ord>(
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

    // Macro courtsey of Chayim Friedman
    // https://stackoverflow.com/q/70390836/1407530
    macro_rules! generate_match {
        // First, we generate a table of permutations.
        // Suppose we have the tuple (String, usize, ()).
        // The table we generate will be the following:
        // [
        //     [ String, usize,  ()     ]
        //     [ usize,  (),     String ]
        //     [ (),     String, usize  ]
        // ]

        // Empty case
        { @generate_permutations_table
            $row:ident
            $thing:ident
            $appearance_set:ident
            $keeper:ident
            match ($e:expr)
            table = [ $($table:tt)* ]
            rest = [ ]
            transformed = [ $($transformed:ty,)* ]
        } => {
            generate_match! { @permutate_entry
                $row
                $thing
                $appearance_set
                $keeper
                match ($e) { }
                table = [ $($table)* ]
            }
        };
        { @generate_permutations_table
            $row:ident
            $thing:ident
            $appearance_set:ident
            $keeper:ident
            match ($e:expr)
            table = [ $($table:tt)* ]
            rest = [ $current:ty, $($rest:ty,)* ]
            transformed = [ $($transformed:ty,)* ]
        } => {
            generate_match! { @generate_permutations_table
                $row
                $thing
                $appearance_set
                $keeper
                match ($e)
                table = [
                    $($table)*
                    [ $current, $($rest,)* $($transformed,)* ]
                ]
                rest = [ $($rest,)* ]
                transformed = [ $($transformed,)* $current, ]
            }
        };

        // For each entry in the table, we generate all combinations of the first type with the others.
        // For example, for the entry [ String, usize, () ] we'll generate the following permutations:
        // [
        //     (String, usize)
        //     (String, ())
        // ]

        // Empty case
        { @permutate_entry
            $row:ident
            $thing:ident
            $appearance_set:ident
            $keeper:ident
            match ($e:expr) { $($match_tt:tt)* }
            table = [ ]
        } => {
            match $e {
                $($match_tt)*
                _ => {}
            }
        };
        { @permutate_entry
            $row:ident
            $thing:ident
            $appearance_set:ident
            $keeper:ident
            match ($e:expr) { $($match_tt:tt)* }
            table = [
                [ $current:ty, $($others:ty,)* ]
                $($table:tt)*
            ]
        } => {
            generate_match! { @generate_arm
                $row
                $thing
                $appearance_set
                $keeper
                match ($e) { $($match_tt)* }
                table = [ $($table)* ]
                current = [ $current ]
                others = [ $($others,)* ]
            }
        };

        // Finally, We generate `match` arms from each pair.
        // For example, for the pair (String, usize):
        //     ("String", "usize") => {
        //         let value = GenericStruct {
        //             value: <String as DataType>::convert(&row.get_ref_unwrap(0)),
        //             time: <usize as DataType>::convert(&row.get_ref_unwrap(2)),
        //         };
        //         // Process `value...`
        //     }

        // Empty case: permutate the next table entry.
        { @generate_arm
            $row:ident
            $thing:ident
            $appearance_set:ident
            $keeper:ident
            match ($e:expr) { $($match_tt:tt)* }
            table = [ $($table:tt)* ]
            current = [ $current:ty ]
            others = [ ]
        } => {
            generate_match! { @permutate_entry
                $row
                $thing
                $appearance_set
                $keeper
                match ($e) { $($match_tt)* }
                table = [ $($table)* ]
            }
        };
        { @generate_arm
            $row:ident
            $thing:ident
            $appearance_set:ident
            $keeper:ident
            match ($e:expr) { $($match_tt:tt)* }
            table = [ $($table:tt)* ]
            current = [ $current:ty ]
            others = [ $first_other:ty, $($others:ty,)* ]
        } => {
            generate_match! { @generate_arm
                $row
                $thing
                $appearance_set
                $keeper
                match ($e) {
                    $($match_tt)*
                    (stringify!($current), stringify!($first_other)) => {
                        $keeper.keep_posit(
                            Posit {
                                posit: Arc::new($thing),
                                appearance_set: $appearance_set,
                                value: <$current as DataType>::convert(&$row.get_ref_unwrap(2)),
                                time: <$first_other as DataType>::convert(&$row.get_ref_unwrap(4))
                            }
                        );
                    }
                }
                table = [ $($table)* ]
                current = [ $current ]
                others = [ $($others,)* ]
            }
        };

        // Entry
        (
            match ($e:expr) from ($($ty:ty),+) in $row:ident with $thing:ident, $appearance_set:ident into $keeper:ident
        ) => {
            generate_match! { @generate_permutations_table
                $row
                $thing
                $appearance_set
                $keeper
                match ($e)
                table = [ ]
                rest = [ $($ty,)+ ]
                transformed = [ ]
            }
        };
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
        // DataType particulars
        pub add_data_type: Statement<'db>,
        pub seen_data_types: Vec<u8>,
    }
    impl<'db> Persistor<'db> {
        pub fn new<'connection>(connection: &'connection Connection) -> Persistor<'connection> {
            // The "STRICT" keyword introduced in 3.37.0 breaks JDBC connections, which makes
            // debugging using an external tool like DBeaver impossible
            connection
                .execute_batch(
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
                create table if not exists DataType (
                    DataType_Identity integer not null,
                    DataType text not null,
                    constraint referenceable_DataType_Identity primary key (
                        DataType_Identity
                    ),
                    constraint unique_DataType unique (
                        DataType
                    )
                );-- STRICT;
                create table if not exists Posit (
                    Posit_Identity integer not null,
                    AppearanceSet text not null,
                    AppearingValue any null, 
                    ValueType_Identity integer not null, 
                    AppearanceTime any null,
                    TimeType_Identity integer not null, 
                    constraint Posit_is_Thing foreign key (
                        Posit_Identity
                    ) references Thing(Thing_Identity),
                    constraint ValueType_is_DataType foreign key (
                        ValueType_Identity
                    ) references DataType(DataType_Identity),
                    constraint TimeType_is_DataType foreign key (
                        TimeType_Identity
                    ) references DataType(DataType_Identity),
                    constraint referenceable_Posit_Identity primary key (
                        Posit_Identity
                    ),
                    constraint unique_Posit unique (
                        AppearanceSet,
                        AppearingValue,
                        AppearanceTime
                    )
                );-- STRICT;
                ",
                )
                .unwrap();
            Persistor {
                db: connection,
                add_thing: connection
                    .prepare(
                        "
                    insert into Thing (
                        Thing_Identity
                    ) values (?)
                ",
                    )
                    .unwrap(),
                add_role: connection
                    .prepare(
                        "
                    insert into Role (
                        Role_Identity, 
                        Role, 
                        Reserved
                    ) values (?, ?, ?)
                ",
                    )
                    .unwrap(),
                add_posit: connection
                    .prepare(
                        "
                    insert into Posit (
                        Posit_Identity, 
                        AppearanceSet, 
                        AppearingValue, 
                        ValueType_Identity, 
                        AppearanceTime, 
                        TimeType_Identity
                    ) values (?, ?, ?, ?, ?, ?)
                ",
                    )
                    .unwrap(),
                get_thing: connection
                    .prepare(
                        "
                    select Thing_Identity 
                      from Thing 
                     where Thing_Identity = ?
                ",
                    )
                    .unwrap(),
                get_role: connection
                    .prepare(
                        "
                    select Role_Identity 
                      from Role 
                     where Role = ?
                ",
                    )
                    .unwrap(),
                get_posit: connection
                    .prepare(
                        "
                    select Posit_Identity 
                      from Posit 
                     where AppearanceSet = ? 
                       and AppearingValue = ? 
                       and AppearanceTime = ?
                ",
                    )
                    .unwrap(),
                all_things: connection
                    .prepare(
                        "
                    select coalesce(max(Thing_Identity), 0) as Max_Thing_Identity
                      from Thing
                ",
                    )
                    .unwrap(),
                all_roles: connection
                    .prepare(
                        "
                    select Role_Identity, Role, Reserved 
                      from Role
                ",
                    )
                    .unwrap(),
                all_posits: connection
                    .prepare(
                        "
                    select p.Posit_Identity, 
                           p.AppearanceSet, 
                           p.AppearingValue, 
                           v.DataType as ValueType, 
                           p.AppearanceTime, 
                           t.DataType as TimeType 
                      from Posit p
                      join DataType v
                        on v.DataType_Identity = p.ValueType_Identity
                      join DataType t
                        on t.DataType_Identity = p.TimeType_Identity
                ",
                    )
                    .unwrap(),
                add_data_type: connection
                    .prepare(
                        "
                    insert or ignore into DataType (
                        DataType_Identity, 
                        DataType
                    ) values (?, ?)
                ",
                    )
                    .unwrap(),
                seen_data_types: Vec::new(),
            }
        }
        pub fn persist_thing(&mut self, thing: &Thing) -> bool {
            let mut existing = false;
            match self
                .get_thing
                .query_row::<usize, _, _>(params![&thing], |r| r.get(0))
            {
                Ok(_) => {
                    existing = true;
                }
                Err(Error::QueryReturnedNoRows) => {
                    self.add_thing.execute(params![&thing]).unwrap();
                }
                Err(err) => {
                    panic!(
                        "Could not check if the thing '{}' is persisted: {}",
                        &thing, err
                    );
                }
            }
            existing
        }
        pub fn persist_role(&mut self, role: &Role) -> bool {
            let mut existing = false;
            match self
                .get_role
                .query_row::<usize, _, _>(params![&role.name()], |r| r.get(0))
            {
                Ok(_) => {
                    existing = true;
                }
                Err(Error::QueryReturnedNoRows) => {
                    self.add_role
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
            existing
        }
        pub fn persist_posit<V: 'static + DataType, T: 'static + DataType + Ord>(
            &mut self,
            posit: &Posit<V, T>,
        ) -> bool {
            let mut appearances = Vec::new();
            let appearance_set = posit.appearance_set();
            for appearance in appearance_set.appearances().iter() {
                appearances.push(
                    appearance.thing().to_string() + "," + &appearance.role().role().to_string(),
                );
            }
            let apperance_set_as_text = appearances.join("|");
            let mut existing = false;
            match self.get_posit.query_row::<usize, _, _>(
                params![&apperance_set_as_text, &posit.value(), &posit.time()],
                |r| r.get(0),
            ) {
                Ok(_) => {
                    existing = true;
                }
                Err(Error::QueryReturnedNoRows) => {
                    if !self.seen_data_types.contains(&posit.value().identifier()) {
                        self.add_data_type
                            .execute(params![
                                &posit.value().identifier(),
                                &posit.value().data_type()
                            ])
                            .unwrap();
                        self.seen_data_types.push(posit.value().identifier());
                    }
                    if !self.seen_data_types.contains(&posit.time().identifier()) {
                        self.add_data_type
                            .execute(params![
                                &posit.time().identifier(),
                                &posit.time().data_type()
                            ])
                            .unwrap();
                        self.seen_data_types.push(posit.time().identifier());
                    }
                    self.add_posit
                        .execute(params![
                            &posit.posit(),
                            &apperance_set_as_text,
                            &posit.value(),
                            &posit.value().identifier(),
                            &posit.time(),
                            &posit.time().identifier()
                        ])
                        .unwrap();
                }
                Err(err) => {
                    panic!(
                        "Could not check if the posit {} is persisted: {}",
                        &posit.posit(),
                        err
                    );
                }
            }
            existing
        }
        pub fn restore_things(&mut self, db: &Database) {
            match self.all_things.query_row::<usize, _, _>([], |r| r.get(0)) {
                Ok(max_thing) => {
                    db.thing_generator().lock().unwrap().retain(max_thing);
                }
                Err(err) => {
                    panic!("Could not restore things: {}", err);
                }
            }
        }
        pub fn restore_roles(&mut self, db: &Database) {
            let role_iter = self
                .all_roles
                .query_map([], |row| {
                    Ok(Role {
                        role: Arc::new(row.get(0).unwrap()),
                        name: row.get(1).unwrap(),
                        reserved: row.get(2).unwrap(),
                    })
                })
                .unwrap();
            for role in role_iter {
                db.keep_role(role.unwrap());
            }
        }
        pub fn restore_posits(&mut self, db: &Database) {
            let mut rows = self.all_posits.query([]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                let value_type: String = row.get_unwrap(3);
                let time_type: String = row.get_unwrap(5);
                let thing: usize = row.get_unwrap(0);
                let appearances: String = row.get_unwrap(1);
                let mut appearance_set = Vec::new();
                for appearance_text in appearances.split('|') {
                    let (thing, role) = appearance_text.split_once(',').unwrap();
                    let appearance = Appearance {
                        thing: Arc::new(thing.parse().unwrap()),
                        role: db
                            .role_keeper()
                            .lock()
                            .unwrap()
                            .lookup(&role.parse::<usize>().unwrap()),
                    };
                    let (kept_appearance, _) = db.keep_appearance(appearance);
                    appearance_set.push(kept_appearance);
                }
                let (kept_appearance_set, _) = db.keep_appearance_set(AppearanceSet {
                    appearances: Arc::new(appearance_set),
                });
                // the magical macro that generates all the boilerplate stuff
                generate_match!(
                    match ((value_type.as_str(), time_type.as_str()))
                        from (String, i64, DateTime::<Utc>, Certainty)
                        in row
                        with thing, kept_appearance_set
                        into db
                );
            }
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
            let thing_to_appearance_lookup = Lookup::<Thing, Appearance, ThingHasher>::new();
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
        pub fn create_apperance(&self, thing: Arc<Thing>, role: Arc<Role>) -> (Arc<Appearance>, bool) {
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
            asserter: Arc<Thing>,
            posit: Arc<Posit<V, T>>,
            certainty: Certainty,
            assertion_time: DateTime<Utc>,
        ) -> Arc<Posit<Certainty, DateTime<Utc>>> {
            let posit_thing: Arc<Thing> =
                self.posit_keeper.lock().unwrap().thing(Arc::clone(&posit));
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

mod traqula {
    use regex::Regex;
    use std::sync::Arc;
    use crate::bareclad::{Database, Role, Posit, Appearance, AppearanceSet, Thing};
    use logos::{Logos, Lexer};
    use std::collections::HashMap;
    use chrono::NaiveDate;

    type Variables = HashMap<String, Arc<Thing>>;

    #[derive(Logos, Debug, PartialEq)]
    enum Command {
        #[error]
        #[regex(r"[\t\n\r\f]+", logos::skip)] 
        Error,

        #[regex(r"add role ([a-z A-Z]+[,]?)+")]
        AddRole,

        #[regex(r"add posit (\[[^\]]*\][,]?)+")]
        AddPosit,

        #[regex(r"search [^;]+")]
        Search,

        #[token(";")]
        CommandTerminator,
    } 
    fn parse_command(mut command: Lexer<Command>, database: &Database, variables: &mut Variables) {
        while let Some(token) = command.next() {
            match token {
                Command::AddRole => {
                    println!("Adding roles...");
                    let trimmed_command = command.slice().trim().replacen("add role ", "", 1);
                    for add_role_result in parse_add_role(AddRole::lexer(&trimmed_command), database, variables) {
                        println!("{: >15} -> known: {}", add_role_result.role.name(), add_role_result.known);
                    }
                }, 
                Command::AddPosit => {
                    println!("Adding posits...");
                    let trimmed_command = command.slice().trim().replacen("add posit ", "", 1);
                    parse_add_posit(AddPosit::lexer(&trimmed_command), database, variables);
                }, 
                Command::Search => {
                    println!("Search: {}", command.slice());
                }, 
                Command::CommandTerminator => (), 
                _ => {
                    println!("Unrecognized command: {}", command.slice());
                }
            }
        }
    }
    
    #[derive(Logos, Debug, PartialEq)]
    enum AddRole {
        #[error]
        #[regex(r"[\t\n\r\f]+", logos::skip, priority = 2)] 
        Error,

        #[regex(r"[^,]+")]
        Role,

        #[token(",")]
        ItemSeparator,
    }
    struct AddRoleResult {
        role: Arc<Role>,
        known: bool
    }
    fn parse_add_role(mut add_role: Lexer<AddRole>, database: &Database, variables: &mut Variables) -> Vec<AddRoleResult> {
        let mut roles: Vec<AddRoleResult> = Vec::new();
        while let Some(token) = add_role.next() {
            match token {
                AddRole::Role => {
                    let role_name = String::from(add_role.slice().trim());
                    let (role, previously_known) = database.create_role(role_name, false);
                    roles.push(AddRoleResult { role: role, known: previously_known });
                },
                AddRole::ItemSeparator => (), 
                _ => {
                    println!("Unrecognized role: {}", add_role.slice());
                }
            } 
        }
        roles
    }

    #[derive(Logos, Debug, PartialEq)]
    enum AddPosit {
        #[error]
        #[regex(r"[\t\n\r\f]+", logos::skip)] 
        Error,

        #[regex(r"\[[^\]]+\]")]
        Posit,

        #[token(",")]
        ItemSeparator,
    }

    fn parse_add_posit(mut add_posit: Lexer<AddPosit>, database: &Database, variables: &mut Variables) {
        while let Some(token) = add_posit.next() {
            match token {
                AddPosit::Posit => {
                    let posit_enclosure = Regex::new(r"\[|\]").unwrap();
                    let posit = posit_enclosure.replace_all(add_posit.slice().trim(), "");
                    parse_posit(&posit, database, variables);
                },
                AddPosit::ItemSeparator => (), 
                _ => {
                    println!("Unrecognized posit: {}", add_posit.slice());
                }
            }
        }
    }
    
    fn parse_posit(mut posit: &str, database: &Database, variables: &mut Variables) {
        // println!("\t[{}]", posit);
        let component_regex = Regex::new(r#"\{([^\}]+)\},(.*),'(.*)'"#).unwrap();
        let captures = component_regex.captures(posit).unwrap();
        let appearance_set = captures.get(1).unwrap().as_str();
        let appearance_set_result = parse_appearance_set(LexicalAppearanceSet::lexer(&appearance_set), database, variables);
        let value = captures.get(2).unwrap().as_str();
        let time = captures.get(3).unwrap().as_str();
        let naive_date = NaiveDate::parse_from_str(time, "%Y-%m-%d").unwrap();
        // determine type of value
        if value.chars().nth(0).unwrap() == '"' {
            let string_value = value.replace("\"", "").replace(Engine::substitute, "\"");
            database.create_posit(appearance_set_result.appearance_set, string_value, naive_date);
        }
    }

    #[derive(Logos, Debug, PartialEq)]
    enum LexicalAppearanceSet {
        #[error]
        #[regex(r"[\t\n\r\f]+", logos::skip)] 
        Error,

        #[regex(r"\([^\)]+\)")]
        Appearance,

        #[token(",")]
        ItemSeparator,
    }
    struct AppearanceSetResult {
        appearance_results: Vec<AppearanceResult>,
        appearance_set: Arc<AppearanceSet>,
        known: bool
    }
    fn parse_appearance_set(mut appearance_set: Lexer<LexicalAppearanceSet>, database: &Database, variables: &mut Variables) -> AppearanceSetResult {
        let mut appearances = Vec::new();
        let mut appearance_results = Vec::new();
        while let Some(token) = appearance_set.next() {
            match token {
                LexicalAppearanceSet::Appearance => {
                    let appearance_enclosure = Regex::new(r"\(|\)").unwrap();
                    let appearance = appearance_enclosure.replace_all(appearance_set.slice().trim(), "");
                    // println!("\tParsing appearance: {}", appearance);
                    let appearance_result = parse_appearance(&appearance, database, variables);
                    appearances.push(appearance_result.appearance.clone());
                    appearance_results.push(appearance_result);
                },
                LexicalAppearanceSet::ItemSeparator => (),
                _ => {
                    println!("Unrecognized appearance: {}", appearance_set.slice());
                }
            } 
        }
        let (kept_appearance_set, previously_known) = database.create_appearance_set(appearances);
        AppearanceSetResult {
            appearance_results: appearance_results,
            appearance_set: kept_appearance_set,
            known: previously_known
        }
    }

    struct AppearanceResult {
        appearance: Arc<Appearance>,
        known: bool
    }
    fn parse_appearance(appearance: &str, database: &Database, variables: &mut Variables) -> AppearanceResult {
        let component_regex = Regex::new(r#"([^,]+),(.+)"#).unwrap();
        let captures = component_regex.captures(appearance).unwrap();
        let qualified_thing = captures.get(1).unwrap().as_str();
        let role_name = captures.get(2).unwrap().as_str();
        let (qualifier, thing_or_variable) = if qualified_thing.parse::<usize>().is_ok() {
            ('#', qualified_thing)
        }
        else {
            let mut chars = qualified_thing.chars();
            (chars.next().unwrap(), chars.as_str())
        };
        let thing = match qualifier {
            '#' => { 
                // println!("\tNumeric value"); 
                let t = thing_or_variable.parse::<usize>().unwrap();
                database.thing_generator().lock().unwrap().retain(t);
                Some(Arc::new(t))
            },
            '+' => { 
                // println!("\tGenerate identity"); 
                let t = Arc::new(database.thing_generator().lock().unwrap().generate());
                variables.insert(thing_or_variable.to_string(), t.clone());
                Some(t)
            },
            '$' => { 
                // println!("\tFetch identity"); 
                let t = variables.get(thing_or_variable).unwrap().clone();
                Some(t)
            },
            _ => None
        };
        let role = database.role_keeper().lock().unwrap().get(role_name);
        let (kept_appearance, previously_known) = database.create_apperance(thing.unwrap(), role);
        AppearanceResult {
            appearance: kept_appearance,
            known: previously_known
        } 
    }
    pub struct Engine<'db> {
        database: Database<'db>, 
    }
    impl<'db> Engine<'db> {
        const substitute: char = 26 as char;
        pub fn new(database: Database<'db>) -> Self {
            Self {
                database
            }
        }
        pub fn execute(&self, traqula: &str) {
            let mut in_string = false;
            let mut in_comment = false;
            let mut previous_c = Engine::substitute;
            let mut oneliner = String::new();
            for c in traqula.chars() {
                // first determine mode
                if c == '#' && !in_string {
                    in_comment = true;
                }
                else if c == '\n' && !in_string {
                    in_comment = false;
                }
                else if c == '"' && !in_string {
                    in_string = true;
                }
                else if c == '"' && previous_c != '"' && in_string {
                    in_string = false;
                }
                // mode dependent push
                if c == '"' && previous_c == '"' && in_string {
                    oneliner.pop();
                    oneliner.push(Engine::substitute);
                    previous_c = Engine::substitute;
                }
                else if (c == '\n' || c == '\r') && !in_string {
                    if !previous_c.is_whitespace() && previous_c != ',' && previous_c != ';' { 
                        oneliner.push(' '); 
                    }
                    previous_c = ' ';
                }
                else if c.is_whitespace() && (previous_c.is_whitespace() || previous_c == ',' || previous_c == ';') && !in_string {
                    previous_c = c;
                }
                else if !in_comment {
                    oneliner.push(c);
                    previous_c = c;
                }
            }
            let mut variables: Variables = Variables::new();
            println!("Traqula:\n{}", &oneliner.trim());
            parse_command(Command::lexer(&oneliner.trim()), &self.database, &mut variables);  
        }  
    }
}

// =========== TESTING BELOW ===========

use chrono::{DateTime, Utc};
use std::sync::Arc;
use config::*;
use std::fs::{remove_file, read_to_string};
use text_io::read;
use std::collections::{HashMap};

use bareclad::{Appearance, AppearanceSet, Certainty, Database, Persistor, Posit, Role, Thing};
use traqula::{Engine};
use rusqlite::{Connection, Error};

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
