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

    // used to keep the one-to-one mapping between posits and their assigned identities
    use bimap::BiMap;

    // other keepers use HashSet or HashMap
    use core::hash::{BuildHasher, BuildHasherDefault, Hasher};
    use std::collections::hash_map::{Entry, RandomState};
    use std::collections::{HashMap, HashSet};
    use std::hash::Hash;

    use std::fmt;
    use std::ops;

    // used for timestamps in the database
    use chrono::{DateTime, Utc};

    pub trait DataType : ToString + Eq + Hash + Send + Sync {
    }
    pub trait TimeType : ToString + Eq + Hash + Send + Sync {
    }

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
        kept: HashMap<String, Arc<Role>>,
    }
    impl RoleKeeper {
        pub fn new() -> Self {
            Self {
                kept: HashMap::new(),
            }
        }
        pub fn keep(&mut self, role: Role) -> Arc<Role> {
            let keepsake = role.name().to_owned();
            match self.kept.entry(keepsake.clone()) {
                Entry::Vacant(e) => {
                    e.insert(Arc::new(role));
                }
                Entry::Occupied(_e) => (),
            };
            Arc::clone(self.kept.get(&keepsake).unwrap())
        }
        pub fn get(&self, name: &String) -> Arc<Role> {
            Arc::clone(self.kept.get(name).unwrap())
        }
    }

    // ------------- Appearance -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Appearance {
        role: Arc<Role>,
        identity: Arc<Identity>,
    }
    impl Appearance {
        pub fn new(role: Arc<Role>, identity: Arc<Identity>) -> Self {
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
        kept: HashSet<Arc<Appearance>>,
    }
    impl AppearanceKeeper {
        pub fn new() -> Self {
            Self {
                kept: HashSet::new(),
            }
        }
        pub fn keep(&mut self, appearance: Appearance) -> Arc<Appearance> {
            let keepsake = Arc::new(appearance);
            self.kept.insert(Arc::clone(&keepsake));
            Arc::clone(self.kept.get(&keepsake).unwrap())
        }
    }

    // ------------- AppearanceSet -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct AppearanceSet {
        members: Arc<Vec<Arc<Appearance>>>,
    }
    impl AppearanceSet {
        pub fn new(mut set: Vec<Arc<Appearance>>) -> Option<Self> {
            set.sort_unstable();
            if set.windows(2).any(|x| x[0].role == x[1].role) {
                return None;
            }
            Some(Self {
                members: Arc::new(set),
            })
        }
        pub fn members(&self) -> &Vec<Arc<Appearance>> {
            &self.members
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
        pub fn keep(&mut self, appearance_set: AppearanceSet) -> Arc<AppearanceSet> {
            let keepsake = Arc::new(appearance_set);
            self.kept.insert(Arc::clone(&keepsake));
            Arc::clone(self.kept.get(&keepsake).unwrap())
        }
    }

    // --------------- Posit ----------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Posit<V: DataType, T: TimeType> {
        appearance_set: Arc<AppearanceSet>,
        value: V, // imprecise value
        time: T,  // imprecise time
    }
    impl<V: DataType, T: TimeType> Posit<V, T> {
        pub fn new(appearance_set: Arc<AppearanceSet>, value: V, time: T) -> Posit<V, T> {
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
    impl<V: 'static + DataType, T: 'static + TimeType> Key for Posit<V, T> {
        type Value = BiMap<Arc<Posit<V, T>>, Arc<Identity>>;
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
            identity_generator: Arc<Mutex<IdentityGenerator>>,
        ) -> (Arc<Posit<V, T>>, Arc<Identity>) {
            let map = self
                .kept
                .entry::<Posit<V, T>>()
                .or_insert(BiMap::<Arc<Posit<V, T>>, Arc<Identity>>::new());
            let keepsake = Arc::new(posit);
            let kept_identity = match map.get_by_left(&keepsake) {
                Some(id) => Arc::clone(id),
                None => Arc::new(identity_generator.lock().unwrap().generate()),
            };
            map.insert(Arc::clone(&keepsake), Arc::clone(&kept_identity));
            (
                Arc::clone(map.get_by_right(&kept_identity).unwrap()),
                Arc::clone(&kept_identity),
            )
        }
        pub fn identity<V: 'static + DataType, T: 'static + TimeType>(
            &mut self,
            posit: Arc<Posit<V, T>>,
        ) -> Arc<Identity> {
            let map = self
                .kept
                .entry::<Posit<V, T>>()
                .or_insert(BiMap::<Arc<Posit<V, T>>, Arc<Identity>>::new());
            Arc::clone(map.get_by_left(&posit).unwrap())
        }
        pub fn posit<V: 'static + DataType, T: 'static + TimeType>(
            &mut self,
            identity: Arc<Identity>,
        ) -> Arc<Posit<V, T>> {
            let map = self
                .kept
                .entry::<Posit<V, T>>()
                .or_insert(BiMap::<Arc<Posit<V, T>>, Arc<Identity>>::new());
            Arc::clone(map.get_by_right(&identity).unwrap())
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
    impl DataType for Certainty { }
    impl DataType for String { }
    impl TimeType for DateTime<Utc> { }
    impl TimeType for i64 { }

    // ------------- Database -------------
    // This sets up the database with the necessary structures
    pub struct Database {
        // owns an identity generator
        pub identity_generator: Arc<Mutex<IdentityGenerator>>,
        // owns keepers for the available constructs
        pub role_keeper: Arc<Mutex<RoleKeeper>>,
        pub appearance_keeper: Arc<Mutex<AppearanceKeeper>>,
        pub appearance_set_keeper: Arc<Mutex<AppearanceSetKeeper>>,
        pub posit_keeper: Arc<Mutex<PositKeeper>>,
        // owns lookups between constructs (similar to database indexes)
        pub identity_to_appearance_lookup: Arc<Mutex<Lookup<Identity, Appearance, IdentityHasher>>>,
        pub role_to_appearance_lookup: Arc<Mutex<Lookup<Role, Appearance>>>,
        pub appearance_to_appearance_set_lookup: Arc<Mutex<Lookup<Appearance, AppearanceSet>>>,
        pub appearance_set_to_posit_identity_lookup: Arc<Mutex<Lookup<AppearanceSet, Identity>>>,
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
            let role_to_appearance_lookup = Lookup::<Role, Appearance>::new();
            let appearance_to_appearance_set_lookup = Lookup::<Appearance, AppearanceSet>::new();
            let appearance_set_to_posit_identity_lookup = Lookup::<AppearanceSet, Identity>::new();

            // Reserve some roles that will be necessary for implementing features
            // commonly found in many other databases.
            role_keeper.keep(Role::new(String::from("asserter"), true));
            role_keeper.keep(Role::new(String::from("posit"), true));

            Self {
                identity_generator: Arc::new(Mutex::new(identity_generator)),
                role_keeper: Arc::new(Mutex::new(role_keeper)),
                appearance_keeper: Arc::new(Mutex::new(appearance_keeper)),
                appearance_set_keeper: Arc::new(Mutex::new(appearance_set_keeper)),
                posit_keeper: Arc::new(Mutex::new(posit_keeper)),
                identity_to_appearance_lookup: Arc::new(Mutex::new(identity_to_appearance_lookup)),
                role_to_appearance_lookup: Arc::new(Mutex::new(role_to_appearance_lookup)),
                appearance_to_appearance_set_lookup: Arc::new(Mutex::new(
                    appearance_to_appearance_set_lookup,
                )),
                appearance_set_to_posit_identity_lookup: Arc::new(Mutex::new(
                    appearance_set_to_posit_identity_lookup,
                )),
            }
        }
        // functions to access the owned generator and keepers
        pub fn identity_generator(&self) -> Arc<Mutex<IdentityGenerator>> {
            Arc::clone(&self.identity_generator)
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
        pub fn identity_to_appearance_lookup(
            &self,
        ) -> Arc<Mutex<Lookup<Identity, Appearance, IdentityHasher>>> {
            Arc::clone(&self.identity_to_appearance_lookup)
        }
        pub fn role_to_appearance_lookup(&self) -> Arc<Mutex<Lookup<Role, Appearance>>> {
            Arc::clone(&self.role_to_appearance_lookup)
        }
        pub fn appearance_to_appearance_set_lookup(
            &self,
        ) -> Arc<Mutex<Lookup<Appearance, AppearanceSet>>> {
            Arc::clone(&self.appearance_to_appearance_set_lookup)
        }
        pub fn appearance_set_to_posit_identity_lookup(
            &self,
        ) -> Arc<Mutex<Lookup<AppearanceSet, Identity>>> {
            Arc::clone(&self.appearance_set_to_posit_identity_lookup)
        }
        // function that generates an identity
        pub fn generate_identity(&self) -> Arc<Identity> {
            Arc::new(self.identity_generator.lock().unwrap().generate())
        }
        // functions to create constructs for the keepers to keep that also populate the lookups
        pub fn create_role(&self, role: String, reserved: bool) -> Arc<Role> {
            self.role_keeper
                .lock()
                .unwrap()
                .keep(Role::new(role, reserved))
        }
        pub fn create_apperance(
            &self,
            role: Arc<Role>,
            identity: Arc<Identity>,
        ) -> Arc<Appearance> {
            let lookup_identity = Arc::clone(&identity);
            let lookup_role = Arc::clone(&role);
            let kept_appearance = self
                .appearance_keeper
                .lock()
                .unwrap()
                .keep(Appearance::new(role, identity));
            self.identity_to_appearance_lookup
                .lock()
                .unwrap()
                .insert(lookup_identity, Arc::clone(&kept_appearance));
            if lookup_role.reserved {
                self.role_to_appearance_lookup
                    .lock()
                    .unwrap()
                    .insert(lookup_role, Arc::clone(&kept_appearance));
            }
            kept_appearance
        }
        pub fn create_appearance_set(
            &self,
            appearance_set: Vec<Arc<Appearance>>,
        ) -> Arc<AppearanceSet> {
            let lookup_appearance_set = appearance_set.clone();
            let appearance_set = self
                .appearance_set_keeper
                .lock()
                .unwrap()
                .keep(AppearanceSet::new(appearance_set).unwrap());
            for lookup_appearance in lookup_appearance_set.iter() {
                self.appearance_to_appearance_set_lookup
                    .lock()
                    .unwrap()
                    .insert(Arc::clone(&lookup_appearance), Arc::clone(&appearance_set));
            }
            appearance_set
        }
        pub fn create_posit<
            V: 'static + DataType,
            T: 'static + TimeType,
        >(
            &self,
            appearance_set: Arc<AppearanceSet>,
            value: V,
            time: T,
        ) -> (Arc<Posit<V, T>>, Arc<Identity>) {
            let lookup_appearance_set = appearance_set.clone();
            let (posit, identity) = self.posit_keeper.lock().unwrap().keep(
                Posit::new(appearance_set, value, time),
                Arc::clone(&self.identity_generator),
            );
            self.appearance_set_to_posit_identity_lookup
                .lock()
                .unwrap()
                .insert(Arc::clone(&lookup_appearance_set), Arc::clone(&identity));
            (posit, identity)
        }
        // finally, now that the database exists we can start to make assertions
        pub fn assert<V: 'static + DataType, T: 'static + TimeType>(
            &self,
            asserter: Arc<Identity>,
            posit: Arc<Posit<V, T>>,
            certainty: Certainty,
            assertion_time: DateTime<Utc>,
        ) -> Arc<Posit<Certainty, DateTime<Utc>>> {
            let posit_identity: Arc<Identity> = self
                .posit_keeper
                .lock()
                .unwrap()
                .identity(Arc::clone(&posit));
            let asserter_role = self.role_keeper.lock().unwrap().get(&"asserter".to_owned());
            let posit_role = self.role_keeper.lock().unwrap().get(&"posit".to_owned());
            let asserter_appearance = self.create_apperance(asserter_role, asserter);
            let posit_appearance = self.create_apperance(posit_role, posit_identity);
            let appearance_set =
                self.create_appearance_set([asserter_appearance, posit_appearance].to_vec());
            self.create_posit(appearance_set, certainty, assertion_time)
                .0
        }
        // search functions in order to find posits matching certain circumstances
        pub fn posits_involving_identity(&self, identity: &Identity) -> Vec<Arc<Identity>> {
            let mut posits: Vec<Arc<Identity>> = Vec::new();
            for appearance in self
                .identity_to_appearance_lookup
                .lock()
                .unwrap()
                .lookup(identity)
            {
                for appearance_set in self
                    .appearance_to_appearance_set_lookup
                    .lock()
                    .unwrap()
                    .lookup(appearance)
                {
                    for posit_identity in self
                        .appearance_set_to_posit_identity_lookup
                        .lock()
                        .unwrap()
                        .lookup(appearance_set)
                    {
                        posits.push(Arc::clone(posit_identity));
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

use bareclad::{Appearance, AppearanceSet, Certainty, Database, Identity, Posit, Role};

fn main() {
    let bareclad = Database::new();
    // does it really have to be this elaborate?
    let i1 = bareclad.generate_identity();
    println!("Enter a role name: ");
    let role: String = read!("{}\n");

    let r1 = bareclad.create_role(role.clone(), false);
    let rdup = bareclad.create_role(role.clone(), false);
    println!("{:?}", bareclad.role_keeper());
    // drop(r); // just to make sure it moved
    let a1 = bareclad.create_apperance(Arc::clone(&r1), Arc::clone(&i1));
    let a2 = bareclad.create_apperance(Arc::clone(&r1), Arc::clone(&i1));
    println!("{:?}", bareclad.appearance_keeper());
    let i2 = bareclad.generate_identity();

    println!("Enter another role name: ");
    let another_role: String = read!("{}\n");

    let r2 = bareclad.create_role(another_role.clone(), false);
    let a3 = bareclad.create_apperance(Arc::clone(&r2), Arc::clone(&i2));
    let as1 = bareclad.create_appearance_set([a1, a3].to_vec());
    println!("{:?}", bareclad.appearance_set_keeper());

    println!("Enter a value that appears with '{}' and '{}': ", role, another_role);
    let v1: String = read!("{}\n");

    let (p1, pid1) = bareclad.create_posit(Arc::clone(&as1), v1.clone(), 42i64);
    let (p2, pid2) = bareclad.create_posit(Arc::clone(&as1), v1.clone(), 42i64);

    println!("Enter a different value that appears with '{}' and '{}': ", role, another_role);
    let v2: String = read!("{}\n");

    let (p3, pid3) =
        bareclad.create_posit(Arc::clone(&as1), v2.clone(), 21i64);
    println!("{:?}", p1);
    println!("{:?}", pid1);
    println!("{:?}", pid2);
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
    bareclad.assert(Arc::clone(&asserter), Arc::clone(&p3), c1, t1);
    let c2: Certainty = Certainty::new(99);
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
}
