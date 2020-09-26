///
/// Implements a database based on the "posit" concept from Transitional Modeling.
/// 
/// Popular version can be found in these blog posts: 
/// http://www.anchormodeling.com/tag/transitional/
/// 
/// Scientific version can be found in this publication:
/// https://www.researchgate.net/publication/329352497_Modeling_Conflicting_Unreliable_and_Varying_Information
/// 
/// Contains its fundamental constructs:
/// - Identities
/// - Roles
/// - Appearances = (Role, Identity)
/// - Appearance sets = {Appearance_1, ..., Appearance_N}
/// - Posits = (Appearance set, V, T) where V is an arbitraty "value type" and T is an arbitrary "time type"
/// 
/// Along with these a "keeper" pattern is used, with the intention to own constructs and
/// guarantee their uniqueness. These can be seen as the database "storage".
/// The following keepers are needed:
/// - RoleKeeper
/// - AppearanceKeeper
/// - AppearanceSetKeeper
/// - PositKeeper
/// 
/// Identities are special in that they either are generated internally or given as input. 
/// Internal generation is something that can be triggered if new data is added. 
/// Identities given as input is something that may happen when a database is restored.
/// The current approach is using a dumb integer, which after a restore could be set 
/// to a lower_bound equal to the largest integer found in the restore. 
/// TODO: Rework identities into a better solution.
/// 
/// Roles will have the additional ability of being reserved. This is necessary for some
/// strings that will be used to implement more "traditional" features found in other
/// databases. Some examples are 'class', 'certainty', and 'constraint'. 
///  
/// In order to perform searches smart lookups between constructs are needed.
/// Role -> Appearance -> AppearanceSet -> Posit (at the very least for reserved roles)
/// Identity -> Appearance -> AppearanceSet -> Posit
/// V -> Posit
/// T -> Posit
/// 
/// A datatype for Certainty is also available, since this is something that will be 
/// used frequently and that needs to be treated with special care. 
/// 
/// TODO: Check what needs to keep pub scope.

// used for timestamps in the database
extern crate chrono;  
// used to store the 1-1 mapping between a string representing a role and its corresponding Role object  
extern crate bimap;     
// used in the keeper of posits, since they are generically typed: Posit<V,T> and therefore require a HashSet per type combo
extern crate typemap;   

mod bareclad {
    use std::sync::{Arc, Mutex};

    use bimap::BiMap;
    use typemap::{TypeMap, Key};

    use std::collections::hash_map::Entry::{Occupied, Vacant};
    use std::collections::{HashMap, HashSet};
    use std::hash::{Hash};
    use std::ops;
    use std::fmt;
    use chrono::{DateTime, Utc};


    pub type Ref<T> = Arc<T>; // to allow for easy switching of referencing style

    // ------------- Identity -------------
    pub type Identity = usize;
    static GENESIS: Identity = 0;

    #[derive(Debug)]
    pub struct IdentityGenerator {
        lower_bound: Identity,
        released: Vec<Identity>
    }

    impl IdentityGenerator {
        pub fn new() -> IdentityGenerator {
            IdentityGenerator {
                lower_bound: GENESIS,
                released: Vec::new()
            }
        }
        pub fn release(&mut self, g: Identity) {
            self.released.push(g);
        }
        pub fn generate(&mut self) -> Identity {
            let id: Identity;
            if self.released.len() > 0 {
                id = self.released.pop().unwrap()
            }
            else {
                self.lower_bound += 1;
                id = self.lower_bound;
            }
            id
        }
        pub fn get_lower_bound(&self) -> Identity {
            self.lower_bound
        }
        pub fn set_lower_bound(&mut self, lower_bound: Identity) {
            self.lower_bound = lower_bound;
        }
    }

    // ------------- Role -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Role {
        name: &'static str,
        reserved: bool
    }

    impl Role {
        pub fn new(role: &String, reserved: bool) -> Role {
            Role {
                name: Box::leak(role.clone().into_boxed_str()),
                reserved: reserved
            }
        }
        pub fn get_name(&self) -> &'static str {
            &self.name
        }
    }

    #[derive(Debug)]
    pub struct RoleKeeper {
        kept: BiMap<&'static str, Ref<Role>>
    }
    impl RoleKeeper {
        pub fn new() -> RoleKeeper {
            RoleKeeper {
                kept: BiMap::new()
            }
        }
        pub fn keep(&mut self, role: Role) -> Ref<Role> {
            let keepsake = role.get_name();
            self.kept.insert(keepsake, Ref::new(role));
            self.kept.get_by_left(&keepsake).unwrap().clone()
        }
        pub fn get_role(&self, name: &'static str) -> Ref<Role> {
            self.kept.get_by_left(&name).unwrap().clone()
        }
    }

    // ------------- Appearance -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Appearance {
        role:           Ref<Role>,
        identity:       Ref<Identity>
    }
    impl Appearance {
        pub fn new(role: &Ref<Role>, identity: &Ref<Identity>) -> Appearance {
            Appearance {
                role: Ref::clone(role),
                identity: Ref::clone(identity)
            }
        }
        pub fn get_role(&self) -> &Role {
            &self.role
        }
        pub fn get_identity(&self) -> &Identity {
            &self.identity
        }
    }

    #[derive(Debug)]
    pub struct AppearanceKeeper {
        kept: HashSet<Ref<Appearance>>
    }
    impl AppearanceKeeper {
        pub fn new() -> AppearanceKeeper {
            AppearanceKeeper {
                kept: HashSet::new()
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
        members: Vec<Ref<Appearance>>
    }
    impl AppearanceSet {
        pub fn new(mut s: Vec<Ref<Appearance>>) -> Option<AppearanceSet> {
            s.sort();
            if s.len() > 1 {
                for i in 1..s.len() {
                    if s[i].role == s[i-1].role { return None };
                }
            }
            Some(AppearanceSet{ members: s })
        }
        pub fn get_members(&self) -> &Vec<Ref<Appearance>> {
            &self.members
        }
    }

    #[derive(Debug)]
    pub struct AppearanceSetKeeper {
        kept: HashSet<Ref<AppearanceSet>>
    }
    impl AppearanceSetKeeper {
        pub fn new() -> AppearanceSetKeeper {
            AppearanceSetKeeper {
                kept: HashSet::new()
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
    pub struct Posit<V,T> where V:Clone {
        appearance_set: Ref<AppearanceSet>,
        value:          V,  // imprecise value
        time:           T   // imprecise time
    }
    impl<V,T> Posit<V,T> where V:Clone {
        pub fn new(appearance_set: &Ref<AppearanceSet>, value: V, time: T) -> Posit<V,T> {
            Posit {
                value: value.clone(),
                time: time,
                appearance_set: Ref::clone(appearance_set)
            }
        }
        pub fn get_value(&self) -> &V {
            &self.value
        }
        pub fn get_time(&self) -> &T {
            &self.time
        }
        pub fn get_appearance_set(&self) -> &AppearanceSet {
            &self.appearance_set
        }
    }    

    impl<V: 'static, T: 'static> Key for Posit<V,T> where V:Clone { 
        type Value = HashSet<Ref<Posit<V,T>>>; 
    }

    pub struct PositKeeper {
        pub kept: TypeMap
    }
    impl PositKeeper {
        pub fn new() -> PositKeeper {
            PositKeeper {
                kept: TypeMap::new()
            }
        }
        pub fn keep<V: 'static,T: 'static>(&mut self, posit: Posit<V,T>) -> Ref<Posit<V,T>> where T: Eq + Hash, V:Clone + Eq + Hash {
            let set = self.kept.entry::<Posit<V,T>>().or_insert(HashSet::<Ref<Posit<V,T>>>::new());            
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
        pub fn new<T: Into<f64>>(a: T) -> Certainty {
            let mut a_f64: f64 = a.into();
            a_f64 = if a_f64 < -1f64 {
                -1f64
            } else if a_f64 > 1f64 {
                1f64
            } else {
                a_f64
            };
            Certainty { alpha: (100f64 * a_f64) as i8 }
        }
        pub fn consistent(rs: &[Certainty]) -> bool {
            let r_total =
                rs.iter().map(|r: &Certainty| r.alpha as i32)
                    .filter(|i| *i != 0)
                    .fold(0, |sum, i|
                        sum + 100 * (1 - i.signum())
                    ) / 2 +
                rs.iter().map(|r: &Certainty| r.alpha as i32)
                    .filter(|i| *i != 0)
                    .fold(0, |sum, i|
                        sum + i
                    );

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
                -100     => write!(f, "-1"),
                -99..=-1 => write!(f, "-0.{}", -self.alpha),
                0        => write!(f, "0"),
                1..=99   => write!(f, "0.{}", self.alpha),
                100      => write!(f, "1"),
                _        => write!(f, "?"),
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
        pub identity_generator:             Ref<Mutex<IdentityGenerator>>,
        // owns keepers for the available constructs 
        pub role_keeper:                    Ref<Mutex<RoleKeeper>>,
        pub appearance_keeper:              Ref<Mutex<AppearanceKeeper>>,
        pub appearance_set_keeper:          Ref<Mutex<AppearanceSetKeeper>>,
        pub posit_keeper:                   Ref<Mutex<PositKeeper>>
        // owns lookups between constructs (database indexes)
        // TODO
    }    

    impl Database {
        pub fn new() -> Database {
            let identity_generator = IdentityGenerator::new();
            let mut role_keeper = RoleKeeper::new();
            let appearance_keeper = AppearanceKeeper::new();
            let appearance_set_keeper = AppearanceSetKeeper::new();
            let posit_keeper = PositKeeper::new();

            // Reserve some roles that will be necessary for implementing features 
            // commonly found in many other databases.
            role_keeper.keep(Role::new(&String::from("asserter"), true));
            role_keeper.keep(Role::new(&String::from("posit"), true));

            Database {
                identity_generator:         Ref::new(Mutex::new(identity_generator)),
                role_keeper:                Ref::new(Mutex::new(role_keeper)),
                appearance_keeper:          Ref::new(Mutex::new(appearance_keeper)),
                appearance_set_keeper:      Ref::new(Mutex::new(appearance_set_keeper)),
                posit_keeper:               Ref::new(Mutex::new(posit_keeper))
            }
        }
        // is getters/setters the "rusty" way?
        pub fn get_identity_generator(&self) -> Ref<Mutex<IdentityGenerator>> {
            self.identity_generator.clone()
        }
        pub fn get_role_keeper(&self) -> Ref<Mutex<RoleKeeper>> {
            self.role_keeper.clone()
        }
        pub fn get_appearance_keeper(&self) -> Ref<Mutex<AppearanceKeeper>> {
            self.appearance_keeper.clone()
        }
        pub fn get_appearance_set_keeper(&self) -> Ref<Mutex<AppearanceSetKeeper>> {
            self.appearance_set_keeper.clone()
        }
        pub fn get_posit_keeper(&self) -> Ref<Mutex<PositKeeper>> {
           self.posit_keeper.clone()
        }
        // now that the database exists we can start to think about assertions
        pub fn assert<V,T>(&self, asserter: Ref<Identity>, posit: Ref<Posit<V,T>>, certainty: Certainty, assertion_time: DateTime<Utc>) -> Ref<Posit<Certainty,DateTime<Utc>>> where V:Clone {
            // TODO: posits need their own identities
            let posit_identity: Ref<Identity> = Ref::new(self.identity_generator.lock().unwrap().generate());
            let asserter_role = self.role_keeper.lock().unwrap().get_role("asserter");
            let posit_role = self.role_keeper.lock().unwrap().get_role("posit");
            let asserter_appearance = Appearance::new(&asserter_role, &asserter);
            let kept_asserter_appearance = self.appearance_keeper.lock().unwrap().keep(asserter_appearance);
            let posit_appearance = Appearance::new(&posit_role, &posit_identity);
            let kept_posit_appearance = self.appearance_keeper.lock().unwrap().keep(posit_appearance);
            let appearance_set = AppearanceSet::new([kept_asserter_appearance, kept_posit_appearance].to_vec()).unwrap();
            let kept_appearance_set = self.appearance_set_keeper.lock().unwrap().keep(appearance_set);
            let assertion: Posit<Certainty, DateTime<Utc>> = Posit::new(&kept_appearance_set, certainty, assertion_time);
            let kept_assertion = self.posit_keeper.lock().unwrap().keep(assertion);
            kept_assertion
        }
    } // end of Database

} // end of mod

// =========== TESTING BELOW =========== 

use std::sync::Arc;
use chrono::{DateTime, Utc};

use bareclad::{
    Identity, 
    Role, 
    Appearance, 
    AppearanceSet,
    Posit,
    Certainty,
    Database
};

pub type Ref<T> = Arc<T>;

pub fn main() {
    let bareclad = Database::new();
    // does it really have to be this elaborate? 
    let i1: Ref<Identity> = Ref::new(bareclad.get_identity_generator().lock().unwrap().generate());
    let r1 = Role::new(&String::from("color"), false);
    let kept_r1 = bareclad.get_role_keeper().lock().unwrap().keep(r1);
    // drop(r); // just to make sure it moved
    let a1 = Appearance::new(&kept_r1, &i1);
    let kept_a1 = bareclad.get_appearance_keeper().lock().unwrap().keep(a1); // transfer ownership to the keeper
    let a2 = Appearance::new(&kept_r1, &i1);
    let kept_a2 = bareclad.get_appearance_keeper().lock().unwrap().keep(a2);
    println!("{} {}", kept_a1.get_role().get_name(), kept_a1.get_identity());
    println!("{} {}", kept_a2.get_role().get_name(), kept_a2.get_identity());
    println!("{:?}", bareclad.get_appearance_keeper());
    let i2: Ref<Identity> = Ref::new(bareclad.get_identity_generator().lock().unwrap().generate());
    let r2 = Role::new(&String::from("intensity"), false);
    let kept_r2 = bareclad.get_role_keeper().lock().unwrap().keep(r2);
    let a3 = Appearance::new(&kept_r2, &i2);
    let kept_a3 = bareclad.get_appearance_keeper().lock().unwrap().keep(a3);
    let as1 = AppearanceSet::new([kept_a1, kept_a3].to_vec()).unwrap();
    let kept_as1 = bareclad.get_appearance_set_keeper().lock().unwrap().keep(as1);
    println!("{:?}", bareclad.get_appearance_set_keeper());
    let p1: Posit<String, i64> = Posit::new(&kept_as1, String::from("same value"), 42);
    let kept_p1 = bareclad.get_posit_keeper().lock().unwrap().keep(p1);
    let p2: Posit<String, i64> = Posit::new(&kept_as1, String::from("same value"), 42);
    let kept_p2 = bareclad.get_posit_keeper().lock().unwrap().keep(p2);
    let p3: Posit<String, i64> = Posit::new(&kept_as1, String::from("different value"), 42);
    let kept_p3 = bareclad.get_posit_keeper().lock().unwrap().keep(p3);
    println!("{:?}", kept_p1);
    println!("{:?}", kept_p2);
    println!("{:?}", kept_p3);
    println!("Contents of the posit keeper:");
    println!("{:?}", bareclad.get_posit_keeper().lock().unwrap().kept.get::<Posit<String, i64>>());
    let asserter: Ref<Identity> = Ref::new(bareclad.get_identity_generator().lock().unwrap().generate());
    let c1: Certainty = Certainty::new(100);
    let t1: DateTime<Utc> = Utc::now();
    bareclad.assert(asserter, kept_p3, c1, t1);
    println!("Contents of the posit keeper (after one assertion):");
    println!("{:?}", bareclad.get_posit_keeper().lock().unwrap().kept.get::<Posit<Certainty, DateTime<Utc>>>());
}
