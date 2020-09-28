//!
//! Implements a database based on the "posit" concept from Transitional Modeling.
//!
//! Popular version can be found in these blog posts:
//! http://www.anchormodeling.com/tag/transitional/
//!
//! Scientific version can be found in this publication:
//! https://www.researchgate.net/publication/329352497_Modeling_Conflicting_Unreliable_and_Varying_Information
//!
//! Contains its fundamental constructs:
//! - Identities
//! - Roles
//! - Appearances = (Role, Identity)
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
//! Identities are special in that they either are generated internally or given as input.
//! Internal generation is something that can be triggered if new data is added.
//! Identities given as input is something that may happen when a database is restored.
//! The current approach is using a dumb integer, which after a restore could be set
//! to a lower_bound equal to the largest integer found in the restore.
//! TODO: Rework identities into a better solution.
//!
//! Roles will have the additional ability of being reserved. This is necessary for some
//! strings that will be used to implement more "traditional" features found in other
//! databases. For example 'class' and 'constraint'.
//!  
//! In order to perform searches smart lookups between constructs are needed.
//! Role -> Appearance -> AppearanceSet -> Posit (at the very least for reserved roles)
//! Identity -> Appearance -> AppearanceSet -> Posit
//! V -> Posit
//! T -> Posit
//!
//! A datatype for Certainty is also available, since this is something that will be
//! used frequently and that needs to be treated with special care.
//!
//! TODO: Check what needs to keep pub scope.
mod bareclad {
    use std::sync::{Arc, Mutex};

    // used in the keeper of posits, since they are generically typed: Posit<V,T> and therefore require a HashSet per type combo
    use typemap::{Key, TypeMap};

    // other keepers use HashSet or HashMap
    use core::hash::{BuildHasher, BuildHasherDefault, Hasher};
    use std::collections::hash_map::{Entry, RandomState};
    use std::collections::{HashMap, HashSet};
    use std::hash::Hash;

    use std::fmt;
    use std::ops;

    // used for timestamps in the database
    use chrono::{DateTime, Utc};

    pub type Ref<T> = Arc<T>; // to allow for easy switching of referencing style

    // ------------- Identity -------------
    // TODO: Investigate using AtomicUsize instead.
    pub type Identity = usize;
    const GENESIS: Identity = 0;

    #[derive(Debug)]
    pub struct IdentityGenerator {
        pub lower_bound: Identity,
        released: Vec<Identity>,
    }

    impl IdentityGenerator {
        pub fn new() -> Self {
            Self {
                lower_bound: GENESIS,
                released: Vec::new(),
            }
        }
        pub fn release(&mut self, g: Identity) {
            self.released.push(g);
        }
        pub fn generate(&mut self) -> Identity {
            self.released.pop().unwrap_or_else(|| {
                self.lower_bound += 1;
                self.lower_bound
            })
        }
    }

    #[derive(Debug, Clone, Copy, Default)]
    pub struct IdentityHash(Identity);

    impl Hasher for IdentityHash {
        fn finish(&self) -> u64 {
            self.0 as u64
        }

        fn write(&mut self, _bytes: &[u8]) {
            unimplemented!("IdentityHasher only supports usize keys")
        }

        fn write_usize(&mut self, i: Identity) {
            self.0 = i;
        }
    }

    type IdentityHasher = BuildHasherDefault<IdentityHash>;

    // ------------- Role -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Role {
        name: String,
        reserved: bool,
    }

    impl Role {
        pub fn new(role: String, reserved: bool) -> Self {
            Self {
                name: role,
                reserved,
            }
        }
        // It's intentional to encapsulate the name in the struct
        // and only expose it using a "getter", because this yields
        // true immutability for objects after creation.
        pub fn name(&self) -> &str {
            &self.name
        }
    }

    #[derive(Debug)]
    pub struct RoleKeeper {
        kept: HashMap<String, Ref<Role>>,
    }
    impl RoleKeeper {
        pub fn new() -> Self {
            Self {
                kept: HashMap::new(),
            }
        }
        pub fn keep(&mut self, role: Role) -> Ref<Role> {
            let keepsake = role.name().to_owned();
            match self.kept.entry(keepsake.clone()) {
                Entry::Vacant(e) => {
                    e.insert(Ref::new(role));
                }
                Entry::Occupied(_e) => (),
            };
            Ref::clone(self.kept.get(&keepsake).unwrap())
        }
        pub fn get(&self, name: &String) -> Ref<Role> {
            Ref::clone(self.kept.get(name).unwrap())
        }
    }

    // ------------- Appearance -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Appearance {
        role: Ref<Role>,
        identity: Ref<Identity>,
    }
    impl Appearance {
        pub fn new(role: Ref<Role>, identity: Ref<Identity>) -> Self {
            Self { role, identity }
        }
        pub fn role(&self) -> &Role {
            &self.role
        }
        pub fn identity(&self) -> &Identity {
            &self.identity
        }
    }

    #[derive(Debug)]
    pub struct AppearanceKeeper {
        kept: HashSet<Ref<Appearance>>,
    }
    impl AppearanceKeeper {
        pub fn new() -> Self {
            Self {
                kept: HashSet::new(),
            }
        }
        pub fn keep(&mut self, appearance: Appearance) -> Ref<Appearance> {
            let keepsake = Ref::new(appearance);
            self.kept.insert(Ref::clone(&keepsake));
            Ref::clone(self.kept.get(&keepsake).unwrap())
        }
    }

    // ------------- AppearanceSet -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct AppearanceSet {
        members: Ref<Vec<Ref<Appearance>>>,
    }
    impl AppearanceSet {
        pub fn new(mut set: Vec<Ref<Appearance>>) -> Option<Self> {
            set.sort_unstable();
            if set.windows(2).any(|x| x[0].role == x[1].role) {
                return None;
            }
            Some(Self {
                members: Ref::new(set),
            })
        }
        pub fn members(&self) -> &Vec<Ref<Appearance>> {
            &self.members
        }
    }

    #[derive(Debug)]
    pub struct AppearanceSetKeeper {
        kept: HashSet<Ref<AppearanceSet>>,
    }
    impl AppearanceSetKeeper {
        pub fn new() -> Self {
            Self {
                kept: HashSet::new(),
            }
        }
        pub fn keep(&mut self, appearance_set: AppearanceSet) -> Ref<AppearanceSet> {
            let keepsake = Ref::new(appearance_set);
            self.kept.insert(Ref::clone(&keepsake));
            Ref::clone(self.kept.get(&keepsake).unwrap())
        }
    }

    // --------------- Posit ----------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Posit<V, T> {
        appearance_set: Ref<AppearanceSet>,
        value: V, // imprecise value
        time: T,  // imprecise time
    }
    impl<V, T> Posit<V, T> {
        pub fn new(appearance_set: Ref<AppearanceSet>, value: V, time: T) -> Posit<V, T> {
            Self {
                value,
                time,
                appearance_set,
            }
        }
        pub fn value(&self) -> &V {
            &self.value
        }
        pub fn time(&self) -> &T {
            &self.time
        }
        pub fn appearance_set(&self) -> &AppearanceSet {
            &self.appearance_set
        }
    }

    // This key needs to be defined in order to store posits in a TypeMap.
    impl<V: 'static, T: 'static> Key for Posit<V, T> {
        type Value = HashMap<Ref<Posit<V, T>>, Ref<Identity>>;
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
        pub fn keep<V: 'static + Eq + Hash, T: 'static + Eq + Hash>(
            &mut self,
            posit: Posit<V, T>,
        ) -> Ref<Posit<V, T>> {
            let map = self
                .kept
                .entry::<Posit<V, T>>()
                .or_insert(HashMap::<Ref<Posit<V, T>>, Ref<Identity>>::new());
            let keepsake = Ref::new(posit);
            map.insert(Ref::clone(&keepsake), Ref::new(GENESIS)); // will be set to an actual identity once asserted
            Ref::clone(map.get_key_value(&keepsake).unwrap().0)
        }
        pub fn identify<V: 'static + Eq + Hash, T: 'static + Eq + Hash>(
            &mut self,
            posit: Ref<Posit<V, T>>,
        ) -> Ref<Identity> {
            let map = self
                .kept
                .entry::<Posit<V, T>>()
                .or_insert(HashMap::<Ref<Posit<V, T>>, Ref<Identity>>::new());
            Ref::clone(map.get(&posit).unwrap())
        }
        pub fn assign<V: 'static + Eq + Hash, T: 'static + Eq + Hash>(
            &mut self,
            posit: Ref<Posit<V, T>>,
            identity: Ref<Identity>,
        ) {
            let map = self
                .kept
                .entry::<Posit<V, T>>()
                .or_insert(HashMap::<Ref<Posit<V, T>>, Ref<Identity>>::new());
            map.insert(posit, identity);
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

    // ------------- Lookups -------------
    pub struct Lookup<K, V, H = RandomState> {
        index: HashMap<Ref<K>, Ref<V>, H>,
    }
    impl<K: Eq + Hash, V, H: BuildHasher + Default> Lookup<K, V, H> {
        pub fn new() -> Self {
            Self {
                index: HashMap::<Ref<K>, Ref<V>, H>::default(),
            }
        }
        pub fn insert(&mut self, key: Ref<K>, value: Ref<V>) {
            self.index.insert(key, value);
        }
        pub fn lookup(&self, key: &K) -> Ref<V> {
            Ref::clone(self.index.get(key).unwrap())
        }
    }

    // ------------- Database -------------
    // This sets up the database with the necessary structures
    pub struct Database {
        // owns an identity generator
        pub identity_generator: Ref<Mutex<IdentityGenerator>>,
        // owns keepers for the available constructs
        pub role_keeper: Ref<Mutex<RoleKeeper>>,
        pub appearance_keeper: Ref<Mutex<AppearanceKeeper>>,
        pub appearance_set_keeper: Ref<Mutex<AppearanceSetKeeper>>,
        pub posit_keeper: Ref<Mutex<PositKeeper>>,
        // owns lookups between constructs (similar to database indexes)
        pub identity_to_appearance_lookup: Ref<Mutex<Lookup<Identity, Appearance, IdentityHasher>>>,
        pub appearance_to_appearance_set_lookup: Ref<Mutex<Lookup<Appearance, AppearanceSet>>>,
    }

    impl Database {
        pub fn new() -> Self {
            let identity_generator = IdentityGenerator::new();
            let mut role_keeper = RoleKeeper::new();
            let appearance_keeper = AppearanceKeeper::new();
            let appearance_set_keeper = AppearanceSetKeeper::new();
            let posit_keeper = PositKeeper::new();
            let identity_to_appearance_lookup =
                Lookup::<Identity, Appearance, IdentityHasher>::new();
            let appearance_to_appearance_set_lookup = Lookup::<Appearance, AppearanceSet>::new();

            // Reserve some roles that will be necessary for implementing features
            // commonly found in many other databases.
            role_keeper.keep(Role::new(String::from("asserter"), true));
            role_keeper.keep(Role::new(String::from("posit"), true));

            Self {
                identity_generator: Ref::new(Mutex::new(identity_generator)),
                role_keeper: Ref::new(Mutex::new(role_keeper)),
                appearance_keeper: Ref::new(Mutex::new(appearance_keeper)),
                appearance_set_keeper: Ref::new(Mutex::new(appearance_set_keeper)),
                posit_keeper: Ref::new(Mutex::new(posit_keeper)),
                identity_to_appearance_lookup: Ref::new(Mutex::new(identity_to_appearance_lookup)),
                appearance_to_appearance_set_lookup: Ref::new(Mutex::new(
                    appearance_to_appearance_set_lookup,
                )),
            }
        }
        // functions to access the owned generator and keepers
        pub fn identity_generator(&self) -> Ref<Mutex<IdentityGenerator>> {
            Ref::clone(&self.identity_generator)
        }
        pub fn role_keeper(&self) -> Ref<Mutex<RoleKeeper>> {
            Ref::clone(&self.role_keeper)
        }
        pub fn appearance_keeper(&self) -> Ref<Mutex<AppearanceKeeper>> {
            Ref::clone(&self.appearance_keeper)
        }
        pub fn appearance_set_keeper(&self) -> Ref<Mutex<AppearanceSetKeeper>> {
            Ref::clone(&self.appearance_set_keeper)
        }
        pub fn posit_keeper(&self) -> Ref<Mutex<PositKeeper>> {
            Ref::clone(&self.posit_keeper)
        }
        pub fn identity_to_appearance_lookup(
            &self,
        ) -> Ref<Mutex<Lookup<Identity, Appearance, IdentityHasher>>> {
            Ref::clone(&self.identity_to_appearance_lookup)
        }
        pub fn appearance_to_appearance_set_lookup(
            &self,
        ) -> Ref<Mutex<Lookup<Appearance, AppearanceSet>>> {
            Ref::clone(&self.appearance_to_appearance_set_lookup)
        }
        // function that generates an identity
        pub fn generate_identity(&self) -> Ref<Identity> {
            Ref::new(self.identity_generator.lock().unwrap().generate())
        }
        // functions to create constructs for the keepers to keep that also populate the lookups
        pub fn create_role(&self, role: String, reserved: bool) -> Ref<Role> {
            self.role_keeper
                .lock()
                .unwrap()
                .keep(Role::new(role, reserved))
        }
        pub fn create_apperance(
            &self,
            role: Ref<Role>,
            identity: Ref<Identity>,
        ) -> Ref<Appearance> {
            let lookup_identity = Ref::clone(&identity);
            let kept_appearance = self
                .appearance_keeper
                .lock()
                .unwrap()
                .keep(Appearance::new(role, identity));
            self.identity_to_appearance_lookup
                .lock()
                .unwrap()
                .insert(lookup_identity, Ref::clone(&kept_appearance));
            kept_appearance
        }
        pub fn create_appearance_set(
            &self,
            appearance_set: Vec<Ref<Appearance>>,
        ) -> Ref<AppearanceSet> {
            let lookup_appearance_set = appearance_set.clone();
            let appearance_set = self
                .appearance_set_keeper
                .lock()
                .unwrap()
                .keep(AppearanceSet::new(appearance_set).unwrap());
            for appearance in lookup_appearance_set.iter() {
                self.appearance_to_appearance_set_lookup
                    .lock()
                    .unwrap()
                    .insert(Ref::clone(&appearance), Ref::clone(&appearance_set));
            }
            appearance_set
        }
        pub fn create_posit<V: 'static + Eq + Hash, T: 'static + Eq + Hash>(
            &self,
            appearance_set: Ref<AppearanceSet>,
            value: V,
            time: T,
        ) -> Ref<Posit<V, T>> {
            self.posit_keeper
                .lock()
                .unwrap()
                .keep(Posit::new(appearance_set, value, time))
        }
        // finally, now that the database exists we can start to make assertions
        pub fn assert<V: 'static + Eq + Hash, T: 'static + Eq + Hash>(
            &self,
            asserter: Ref<Identity>,
            posit: Ref<Posit<V, T>>,
            certainty: Certainty,
            assertion_time: DateTime<Utc>,
        ) -> Ref<Posit<Certainty, DateTime<Utc>>> {
            let mut posit_identity: Ref<Identity> = self
                .posit_keeper
                .lock()
                .unwrap()
                .identify(Ref::clone(&posit));
            if *posit_identity == GENESIS {
                posit_identity = self.generate_identity();
                self.posit_keeper
                    .lock()
                    .unwrap()
                    .assign(posit, Ref::clone(&posit_identity));
            }
            let asserter_role = self.role_keeper.lock().unwrap().get(&"asserter".to_owned());
            let posit_role = self.role_keeper.lock().unwrap().get(&"posit".to_owned());
            let asserter_appearance = self.create_apperance(asserter_role, asserter);
            let posit_appearance = self.create_apperance(posit_role, posit_identity);
            let appearance_set =
                self.create_appearance_set([asserter_appearance, posit_appearance].to_vec());
            self.create_posit(appearance_set, certainty, assertion_time)
        }
    }
}

// =========== TESTING BELOW ===========

use chrono::{DateTime, Utc};
use std::sync::Arc;

use bareclad::{Appearance, AppearanceSet, Certainty, Database, Identity, Posit, Role};

pub type Ref<T> = Arc<T>;

fn main() {
    let bareclad = Database::new();
    // does it really have to be this elaborate?
    let i1 = bareclad.generate_identity();
    let r1 = bareclad.create_role(String::from("color"), false);
    let rdup = bareclad.create_role(String::from("color"), false);
    println!("{:?}", bareclad.role_keeper());
    // drop(r); // just to make sure it moved
    let a1 = bareclad.create_apperance(Ref::clone(&r1), Ref::clone(&i1));
    let a2 = bareclad.create_apperance(Ref::clone(&r1), Ref::clone(&i1));
    println!("{:?}", bareclad.appearance_keeper());
    let i2 = bareclad.generate_identity();
    let r2 = bareclad.create_role(String::from("intensity"), false);
    let a3 = bareclad.create_apperance(Ref::clone(&r2), Ref::clone(&i2));
    let as1 = bareclad.create_appearance_set([a1, a3].to_vec());
    println!("{:?}", bareclad.appearance_set_keeper());
    let p1 = bareclad.create_posit(Ref::clone(&as1), String::from("same value"), 42i64);
    let p2 = bareclad.create_posit(Ref::clone(&as1), String::from("same value"), 42i64);
    let p3 = bareclad.create_posit(Ref::clone(&as1), String::from("different value"), 21i64);
    println!("{:?}", p1);
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
    let asserter = bareclad.generate_identity();
    let c1: Certainty = Certainty::new(100);
    let t1: DateTime<Utc> = Utc::now();
    bareclad.assert(Ref::clone(&asserter), Ref::clone(&p3), c1, t1);
    let c2: Certainty = Certainty::new(99);
    let t2: DateTime<Utc> = Utc::now();
    bareclad.assert(Ref::clone(&asserter), Ref::clone(&p3), c2, t2);
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
}
