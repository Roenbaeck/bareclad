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

    // used to store the 1-1 mapping between a string representing a role and its corresponding Role object
    use bimap::BiMap;
    // used in the keeper of posits, since they are generically typed: Posit<V,T> and therefore require a HashSet per type combo
    use typemap::{Key, TypeMap};

    use std::collections::hash_map::Entry::{Occupied, Vacant};
    use std::collections::{HashMap, HashSet};
    use std::fmt;
    use std::hash::Hash;
    use std::ops;

    // used for timestamps in the database
    use chrono::{DateTime, Utc};

    pub type Ref<T> = Arc<T>; // to allow for easy switching of referencing style

    // ------------- Identity -------------
    pub type Identity = usize;
    static GENESIS: Identity = 0;

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
            if self.released.len() > 0 {
                self.released.pop().unwrap()
            } else {
                self.lower_bound += 1;
                self.lower_bound
            }
        }
    }

    // ------------- Role -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Role {
        // this is the only way I found that works with also
        // using the string as a lookup in the bimap.
        name: &'static str,
        reserved: bool,
    }

    impl Role {
        pub fn new(role: &String, reserved: bool) -> Self {
            Self {
                name: Box::leak(role.clone().into_boxed_str()),
                reserved: reserved,
            }
        }
        // It's intentional to encapsulate the name in the struct
        // and only expose it using a "getter", because this yields
        // true immutability for objects after creation.
        pub fn name(&self) -> &'static str {
            &self.name
        }
    }

    #[derive(Debug)]
    pub struct RoleKeeper {
        kept: BiMap<&'static str, Ref<Role>>,
    }
    impl RoleKeeper {
        pub fn new() -> Self {
            Self { kept: BiMap::new() }
        }
        pub fn keep(&mut self, role: Role) -> Ref<Role> {
            let keepsake = role.name();
            self.kept.insert(keepsake, Ref::new(role));
            self.kept.get_by_left(&keepsake).unwrap().clone()
        }
        pub fn get(&self, name: &'static str) -> Ref<Role> {
            self.kept.get_by_left(&name).unwrap().clone()
        }
    }

    // ------------- Appearance -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Appearance {
        role: Ref<Role>,
        identity: Ref<Identity>,
    }
    impl Appearance {
        pub fn new(role: &Ref<Role>, identity: &Ref<Identity>) -> Self {
            Self {
                role: Ref::clone(role),
                identity: Ref::clone(identity),
            }
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
            self.kept.insert(keepsake.clone());
            self.kept.get(&keepsake).unwrap().clone()
        }
    }

    // ------------- AppearanceSet -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct AppearanceSet {
        members: Vec<Ref<Appearance>>,
    }
    impl AppearanceSet {
        pub fn new(mut s: Vec<Ref<Appearance>>) -> Option<Self> {
            s.sort_unstable();
            if s.windows(2).any(|x| x[0].role == x[1].role) {
                return None;
            }
            Some(Self { members: s })
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
            self.kept.insert(keepsake.clone());
            self.kept.get(&keepsake).unwrap().clone()
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
        pub fn new(appearance_set: &Ref<AppearanceSet>, value: V, time: T) -> Posit<V, T>
        where
            V: Clone,
        {
            Self {
                value: value.clone(),
                time: time,
                appearance_set: Ref::clone(appearance_set),
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

    impl<V: 'static, T: 'static> Key for Posit<V, T> {
        type Value = HashSet<Ref<Posit<V, T>>>;
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
        pub fn keep<V: 'static, T: 'static>(&mut self, posit: Posit<V, T>) -> Ref<Posit<V, T>>
        where
            T: Eq + Hash,
            V: Eq + Hash,
        {
            let set = self
                .kept
                .entry::<Posit<V, T>>()
                .or_insert(HashSet::<Ref<Posit<V, T>>>::new());
            let keepsake = Ref::new(posit);
            set.insert(keepsake.clone());
            set.get(&keepsake).unwrap().clone()
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
            let mut a_f64: f64 = a.into();
            a_f64 = if a_f64 < -1f64 {
                -1f64
            } else if a_f64 > 1f64 {
                1f64
            } else {
                a_f64
            };
            Self {
                alpha: (100f64 * a_f64) as i8,
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
                    .fold(0, |sum, i| sum + i);

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

    // ------------- Database -------------
    // This sets up the database with the necessary structures
    pub struct Database {
        // owns an identity generator
        pub identity_generator: Ref<Mutex<IdentityGenerator>>,
        // owns keepers for the available constructs
        pub role_keeper: Ref<Mutex<RoleKeeper>>,
        pub appearance_keeper: Ref<Mutex<AppearanceKeeper>>,
        pub appearance_set_keeper: Ref<Mutex<AppearanceSetKeeper>>,
        pub posit_keeper: Ref<Mutex<PositKeeper>>, // owns lookups between constructs (database indexes)
                                                   // TODO
    }

    impl Database {
        pub fn new() -> Self {
            let identity_generator = IdentityGenerator::new();
            let mut role_keeper = RoleKeeper::new();
            let appearance_keeper = AppearanceKeeper::new();
            let appearance_set_keeper = AppearanceSetKeeper::new();
            let posit_keeper = PositKeeper::new();

            // Reserve some roles that will be necessary for implementing features
            // commonly found in many other databases.
            role_keeper.keep(Role::new(&String::from("asserter"), true));
            role_keeper.keep(Role::new(&String::from("posit"), true));

            Self {
                identity_generator: Ref::new(Mutex::new(identity_generator)),
                role_keeper: Ref::new(Mutex::new(role_keeper)),
                appearance_keeper: Ref::new(Mutex::new(appearance_keeper)),
                appearance_set_keeper: Ref::new(Mutex::new(appearance_set_keeper)),
                posit_keeper: Ref::new(Mutex::new(posit_keeper)),
            }
        }
        // is getters/setters the "rusty" way?
        pub fn identity_generator(&self) -> Ref<Mutex<IdentityGenerator>> {
            self.identity_generator.clone()
        }
        pub fn role_keeper(&self) -> Ref<Mutex<RoleKeeper>> {
            self.role_keeper.clone()
        }
        pub fn appearance_keeper(&self) -> Ref<Mutex<AppearanceKeeper>> {
            self.appearance_keeper.clone()
        }
        pub fn appearance_set_keeper(&self) -> Ref<Mutex<AppearanceSetKeeper>> {
            self.appearance_set_keeper.clone()
        }
        pub fn posit_keeper(&self) -> Ref<Mutex<PositKeeper>> {
            self.posit_keeper.clone()
        }
        // now that the database exists we can start to think about assertions
        pub fn assert<V, T>(
            &self,
            asserter: Ref<Identity>,
            posit: Ref<Posit<V, T>>,
            certainty: Certainty,
            assertion_time: DateTime<Utc>,
        ) -> Ref<Posit<Certainty, DateTime<Utc>>> {
            // TODO: posits need their own identities
            let posit_identity: Ref<Identity> =
                Ref::new(self.identity_generator.lock().unwrap().generate());
            let asserter_role = self.role_keeper.lock().unwrap().get("asserter");
            let posit_role = self.role_keeper.lock().unwrap().get("posit");
            let asserter_appearance = Appearance::new(&asserter_role, &asserter);
            let kept_asserter_appearance = self
                .appearance_keeper
                .lock()
                .unwrap()
                .keep(asserter_appearance);
            let posit_appearance = Appearance::new(&posit_role, &posit_identity);
            let kept_posit_appearance = self
                .appearance_keeper
                .lock()
                .unwrap()
                .keep(posit_appearance);
            let appearance_set =
                AppearanceSet::new([kept_asserter_appearance, kept_posit_appearance].to_vec())
                    .unwrap();
            let kept_appearance_set = self
                .appearance_set_keeper
                .lock()
                .unwrap()
                .keep(appearance_set);
            let assertion: Posit<Certainty, DateTime<Utc>> =
                Posit::new(&kept_appearance_set, certainty, assertion_time);
            let kept_assertion = self.posit_keeper.lock().unwrap().keep(assertion);
            kept_assertion
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
    let i1: Ref<Identity> = Ref::new(bareclad.identity_generator().lock().unwrap().generate());
    let r1 = Role::new(&String::from("color"), false);
    let kept_r1 = bareclad.role_keeper().lock().unwrap().keep(r1);
    // drop(r); // just to make sure it moved
    let a1 = Appearance::new(&kept_r1, &i1);
    let kept_a1 = bareclad.appearance_keeper().lock().unwrap().keep(a1); // transfer ownership to the keeper
    let a2 = Appearance::new(&kept_r1, &i1);
    let kept_a2 = bareclad.appearance_keeper().lock().unwrap().keep(a2);
    println!("{} {}", kept_a1.role().name(), kept_a1.identity());
    println!("{} {}", kept_a2.role().name(), kept_a2.identity());
    println!("{:?}", bareclad.appearance_keeper());
    let i2: Ref<Identity> = Ref::new(bareclad.identity_generator().lock().unwrap().generate());
    let r2 = Role::new(&String::from("intensity"), false);
    let kept_r2 = bareclad.role_keeper().lock().unwrap().keep(r2);
    let a3 = Appearance::new(&kept_r2, &i2);
    let kept_a3 = bareclad.appearance_keeper().lock().unwrap().keep(a3);
    let as1 = AppearanceSet::new([kept_a1, kept_a3].to_vec()).unwrap();
    let kept_as1 = bareclad.appearance_set_keeper().lock().unwrap().keep(as1);
    println!("{:?}", bareclad.appearance_set_keeper());
    let p1: Posit<String, i64> = Posit::new(&kept_as1, String::from("same value"), 42);
    let kept_p1 = bareclad.posit_keeper().lock().unwrap().keep(p1);
    let p2: Posit<String, i64> = Posit::new(&kept_as1, String::from("same value"), 42);
    let kept_p2 = bareclad.posit_keeper().lock().unwrap().keep(p2);
    let p3: Posit<String, i64> = Posit::new(&kept_as1, String::from("different value"), 42);
    let kept_p3 = bareclad.posit_keeper().lock().unwrap().keep(p3);
    println!("{:?}", kept_p1);
    println!("{:?}", kept_p2);
    println!("{:?}", kept_p3);
    println!("Contents of the posit keeper:");
    println!(
        "{:?}",
        bareclad
            .posit_keeper()
            .lock()
            .unwrap()
            .kept
            .get::<Posit<String, i64>>()
    );
    let asserter: Ref<Identity> =
        Ref::new(bareclad.identity_generator().lock().unwrap().generate());
    let c1: Certainty = Certainty::new(100);
    let t1: DateTime<Utc> = Utc::now();
    bareclad.assert(asserter, kept_p3, c1, t1);
    println!("Contents of the posit keeper (after one assertion):");
    println!(
        "{:?}",
        bareclad
            .posit_keeper()
            .lock()
            .unwrap()
            .kept
            .get::<Posit<Certainty, DateTime<Utc>>>()
    );
}
