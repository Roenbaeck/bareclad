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
/// databases. Some examples are 'class', 'reliability', and 'constraint'. 
///  
/// In order to perform searches smart lookups between constructs are needed.
/// Role -> Appearance -> AppearanceSet -> Posit (at the very least for reserved roles)
/// Identity -> Appearance -> AppearanceSet -> Posit
/// V -> Posit
/// T -> Posit
/// 
/// A datatype for Reliability is also available, since this is something that will be 
/// used frequently and that needs to be treated with special care. 
/// 

// used for timestamps in the database
extern crate chrono;  
// used to store the 1-1 mapping between a string representing a role and its corresponding Role object  
extern crate bimap;     
// used in the keeper of posits, since they are generically typed: Posit<V,T> and therefore require a HashSet per type combo
extern crate typemap;   


use chrono::{DateTime, Utc};
use std::io;


// we will use keepers as a pattern to own some things

mod bareclad {
    use std::sync::Arc;

    use bimap::BiMap;
    use typemap::{TypeMap, Key};

    use std::collections::hash_map::Entry::{Occupied, Vacant};
    use std::collections::{HashMap, HashSet};
    use std::hash::{Hash};
    use std::cell::RefCell;
    use std::ops;
    use std::fmt;

    pub type Ref<T> = Arc<T>;
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

    // ------------- Appearance -------------
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
        pub fn new(value: V, time: T, appearance_set: &Ref<AppearanceSet>) -> Posit<V,T> {
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

} // end of mod

use std::sync::Arc;
use bareclad::{
    Identity, 
    IdentityGenerator, 
    Role, 
    RoleKeeper, 
    Appearance, 
    AppearanceKeeper, 
    AppearanceSet,
    AppearanceSetKeeper,
    Posit,
    PositKeeper
};

pub type Ref<T> = Arc<T>;

pub fn main() {
    let mut generator = IdentityGenerator::new();
    let mut role_keeper = RoleKeeper::new();
    let i1: Ref<Identity> = Ref::new(generator.generate());
    let r1 = Role::new(&String::from("color"), false);
    let kept_r1 = role_keeper.keep(r1);
    // drop(r); // just to make sure it moved
    let mut appearance_keeper = AppearanceKeeper::new();
    let a1 = Appearance::new(&kept_r1, &i1);
    let kept_a1 = appearance_keeper.keep(a1); // transfer ownership to the keeper
    let a2 = Appearance::new(&kept_r1, &i1);
    let kept_a2 = appearance_keeper.keep(a2);
    println!("{} {}", kept_a1.get_role().get_name(), kept_a1.get_identity());
    println!("{} {}", kept_a2.get_role().get_name(), kept_a2.get_identity());
    println!("{:?}", appearance_keeper);
    let i2: Ref<Identity> = Ref::new(generator.generate());
    let r2 = Role::new(&String::from("intensity"), false);
    let kept_r2 = role_keeper.keep(r2);
    let a3 = Appearance::new(&kept_r2, &i2);
    let kept_a3 = appearance_keeper.keep(a3);
    let as1 = AppearanceSet::new([kept_a1, kept_a3].to_vec()).unwrap();
    let mut appearance_set_keeper = AppearanceSetKeeper::new();
    let kept_as1 = appearance_set_keeper.keep(as1);
    println!("{:?}", appearance_set_keeper);
    let mut posit_keeper = PositKeeper::new();
    let p1: Posit<String, i64> = Posit::new(String::from("same value"), 42, &kept_as1);
    let kept_p1 = posit_keeper.keep(p1);
    let p2: Posit<String, i64> = Posit::new(String::from("same value"), 42, &kept_as1);
    let kept_p2 = posit_keeper.keep(p2);
    let p3: Posit<String, i64> = Posit::new(String::from("different value"), 42, &kept_as1);
    let kept_p3 = posit_keeper.keep(p3);
    println!("{:?}", kept_p1);
    println!("{:?}", kept_p2);
    println!("{:?}", kept_p3);
    println!("Contents of the posit keeper:");
    println!("{:?}", posit_keeper.kept.get::<Posit<String, i64>>());
}

    /*
    The master will certainly win.
    The master will probably win.
    The master will likely win.
    The master may win.
    The master is unlikely to win.
    The master has a small chance of winning.
    */

/*
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Reliability {
        alpha: i8,
    }

    impl Reliability {
        pub fn new<T: Into<f64>>(a: T) -> Reliability {
            let mut a_f64: f64 = a.into();
            a_f64 = if a_f64 < -1f64 {
                -1f64
            } else if a_f64 > 1f64 {
                1f64
            } else {
                a_f64
            };
            Reliability { alpha: (100f64 * a_f64) as i8 }
        }
        pub fn consistent(rs: &[Reliability]) -> bool {
            let r_total =
                rs.iter().map(|r: &Reliability| r.alpha as i32)
                    .filter(|i| *i != 0)
                    .fold(0, |sum, i|
                        sum + 100 * (1 - i.signum())
                    ) / 2 +
                rs.iter().map(|r: &Reliability| r.alpha as i32)
                    .filter(|i| *i != 0)
                    .fold(0, |sum, i|
                        sum + i
                    );

            r_total <= 100
        }
    }
    impl ops::Add for Reliability {
        type Output = f64;
        fn add(self, other: Reliability) -> f64 {
            (self.alpha as f64 + other.alpha as f64) / 100f64
        }
    }
    impl ops::Mul for Reliability {
        type Output = f64;
        fn mul(self, other: Reliability) -> f64 {
            (self.alpha as f64 / 100f64) * (other.alpha as f64 / 100f64)
        }
    }
    impl fmt::Display for Reliability {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match self.alpha {
                -100     => write!(f, "-1"),
                -99..=-1 => write!(f, "-0.{}", -self.alpha),
                0        => write!(f, "0"),
                0..=99   => write!(f, "0.{}", self.alpha),
                100      => write!(f, "1"),
                _        => write!(f, "?"),
            }
        }
    }
    impl From<Reliability> for f64 {
        fn from(r: Reliability) -> f64 {
            r.alpha as f64 / 100f64
        }
    }
    impl<'a> From<&'a Reliability> for f64 {
        fn from(r: &Reliability) -> f64 {
            r.alpha as f64 / 100f64
        }
    }



    // ------------- AppearanceSet ------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct AppearanceSet {
        set: Vec<Rc<Appearance>>
    }
    impl AppearanceSet {
        pub fn new(mut s: Vec<Rc<Appearance>>) -> Option<AppearanceSet> {
            s.sort();
            if s.len() > 1 {
                for i in 1..s.len() {
                    if s[i].role == s[i-1].role { return None };
                }
            }
            Some(AppearanceSet{ set: s })
        }
        pub fn get_set(&self) -> &Vec<Rc<Appearance>> {
            &self.set
        }
    }

    // --------------- Posit ----------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Posit<T> {
        value:          T,      // borrowed types correspond to knots
        time:           i64,    // unix time from DateTime<Utc>,
        AppearanceSet:    Rc<AppearanceSet>
    }
    impl<T> Posit<T> {
        pub fn new(value: T, time: i64, AppearanceSet: Rc<AppearanceSet>) -> Posit<T> {
            Posit {
                value: value,
                time: time,
                AppearanceSet: AppearanceSet
            }
        }
        pub fn get_value(&self) -> &T {
            &self.value
        }
        pub fn get_time(&self) -> i64 {
            self.time
        }
        pub fn get_AppearanceSet(&self) -> &AppearanceSet {
            &self.AppearanceSet
        }
    }

    // ------------- Assertion --------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Assertion<T> {
        positor:        usize,  // borrowed in theory
        reliability:    Reliability,
        time:           i64,    // unix time from DateTime<Utc>,
        posit:          Rc<Posit<T>>
    }
    impl<T> Assertion<T> {
        pub fn new(positor: usize, reliability: f64, time: i64, posit: Rc<Posit<T>>) -> Assertion<T> {
            Assertion {
                positor: positor,
                reliability: Reliability::new(reliability),
                time: time,
                posit: posit
            }
        }
        pub fn get_positor(&self) -> usize {
            self.positor
        }
        pub fn get_reliability(&self) -> &Reliability {
            &self.reliability
        }
        pub fn get_time(&self) -> i64 {
            self.time
        }
        pub fn get_posit(&self) -> &Posit<T> {
            &self.posit
        }
    }

    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    pub struct Anchor {
        positor:        usize,  // borrowed in theory
        reliability:    Reliability,
        time:           i64,    // unix time from DateTime<Utc>,
        identity:       usize,  // borrowed in theory
        tag:            usize   // borrowed in theory
    }

    // TODO: remove pub once all methods are in place
    // TODO: add anchor to identity index
    // --------------- Database ---------------
    pub struct Database {
        // owns an identity Generator
        id_generator:               Generator,
        // owns indexes
        pub role_index:                 KeptIndex<String>,
        pub tag_index:                  KeptIndex<String>,
        pub appearance_index:           KeptIndex<Appearance>,
        pub AppearanceSet_index:          KeptIndex<AppearanceSet>,
        pub posit_index:                AnyMap,
        pub assertion_index:            AnyMap,
        // lends their references to lookups
        pub role_to_identity:           Lookup<String,()>,
        pub identity_to_appearance:     Lookup<(), Appearance>,
        pub appearance_to_AppearanceSet:  Lookup<Appearance, AppearanceSet>,
        pub AppearanceSet_to_posit:       AnyMap,
        pub posit_to_assertion:         AnyMap
    }

    impl Database {
        pub fn new() -> Database {
            let id_generator = Generator::new();
            // indexes
            let role_index: KeptIndex<String> = Rc::new(RefCell::new(Index::new()));
            let tag_index: KeptIndex<String> = Rc::new(RefCell::new(Index::new()));
            let appearance_index: KeptIndex<Appearance> = Rc::new(RefCell::new(Index::new()));
            let AppearanceSet_index: KeptIndex<AppearanceSet> = Rc::new(RefCell::new(Index::new()));
            let posit_index = AnyMap::new();
            let assertion_index = AnyMap::new();
            // lookups
            let role_to_identity: Lookup<String, ()> = Lookup::new_with_source(role_index.clone());
            let identity_to_appearance: Lookup<(), Appearance> = Lookup::new_with_target(appearance_index.clone());
            let appearance_to_AppearanceSet: Lookup<Appearance, AppearanceSet> = Lookup::new(appearance_index.clone(), AppearanceSet_index.clone());
            let AppearanceSet_to_posit = AnyMap::new();
            let posit_to_assertion = AnyMap::new();

            Database {
                id_generator:               id_generator,
                role_index:                 role_index,
                tag_index:                  tag_index,
                appearance_index:           appearance_index,
                AppearanceSet_index:          AppearanceSet_index,
                posit_index:                posit_index,
                assertion_index:            assertion_index,
                role_to_identity:           role_to_identity,
                identity_to_appearance:     identity_to_appearance,
                appearance_to_AppearanceSet:  appearance_to_AppearanceSet,
                AppearanceSet_to_posit:       AppearanceSet_to_posit,
                posit_to_assertion:         posit_to_assertion
            }
        }
        // expose Generator methods
        pub fn release(&mut self, g: usize) {
            self.id_generator.release(g);
        }
        pub fn generate(&mut self) -> usize {
            self.id_generator.generate()
        }
        pub fn current(&self) -> usize {
            self.id_generator.current()
        }
        // expose adds for each Index (and hide the ugly explicit borrows)
        // also populate Lookups for relevant adds
        pub fn add_role(&self, role: String) -> usize {
            self.role_index.borrow_mut().keep(role)
        }
        pub fn add_tag(&self, tag: String) -> usize {
            self.tag_index.borrow_mut().keep(tag)
        }
        pub fn add_appearance(&mut self, appearance: Appearance) -> usize {
            let role = appearance.role;
            let identity = appearance.identity;
            self.role_to_identity.keep(role, identity);
            let appearance_kept = self.appearance_index.borrow_mut().keep(appearance);
            self.identity_to_appearance.keep(identity, appearance_kept);
            appearance_kept
        }
        pub fn add_AppearanceSet(&mut self, AppearanceSet: AppearanceSet) -> usize {
            let AppearanceSet_kept = self.AppearanceSet_index.borrow_mut().keep(AppearanceSet);
            for a in &self.AppearanceSet_index.borrow().find(AppearanceSet_kept).unwrap().set {
                self.appearance_to_AppearanceSet.keep(
                    self.appearance_index.borrow().index_of(a).unwrap(),
                    AppearanceSet_kept
                );
            }
            AppearanceSet_kept
        }
        pub fn get_posit<T>(&self, posit_kept: usize) -> Option<Rc<Posit<T>>> where T: 'static + Eq + Hash {
            let posit_index = match self.posit_index.get::<KeptIndex<Posit<T>>>() {
                Some(index) => index.clone(),
                None => Rc::new(RefCell::new(Index::new()))
            };
            let posit = posit_index.borrow().find(posit_kept);
            posit
        }
        pub fn add_posit<T>(&mut self, posit: Posit<T>) -> usize where T: 'static + Eq + Hash {
            // TODO: If the index did not exist and a new one is created it must be
            //       stored, so perhaps switch to entry() here.
            let posit_index = match self.posit_index.get::<KeptIndex<Posit<T>>>() {
                Some(index) => index.clone(),
                None => Rc::new(RefCell::new(Index::new()))
            };
            let posit_kept = posit_index.borrow_mut().keep(posit);
            let AppearanceSet: &AppearanceSet = &posit_index.borrow().find(posit_kept).unwrap().AppearanceSet;
            // TODO: Dangerous unwrap below (if the AppearanceSet has not been added)
            let AppearanceSet_kept = self.AppearanceSet_index.borrow().index_of(AppearanceSet).unwrap();
            match self.AppearanceSet_to_posit.entry::<Lookup<AppearanceSet, Posit<T>>>() {
                anymap::Entry::Occupied(mut entry) => {
                    entry.get_mut().keep(AppearanceSet_kept, posit_kept)
                },
                anymap::Entry::Vacant(entry) => {
                    entry.insert(Lookup::new(self.AppearanceSet_index.clone(), posit_index.clone())).keep(AppearanceSet_kept, posit_kept)
                }
            };
            posit_kept
        }
        pub fn add_assertion<T>(&mut self, assertion: Assertion<T>) -> usize where T: 'static + Eq + Hash {
            let assertion_index = match self.assertion_index.get::<KeptIndex<Assertion<T>>>() {
                Some(index) => index.clone(),
                None => Rc::new(RefCell::new(Index::new()))
            };
            let posit_index = match self.posit_index.get::<KeptIndex<Posit<T>>>() {
                Some(index) => index.clone(),
                None => Rc::new(RefCell::new(Index::new()))
            };
            let assertion_kept = assertion_index.borrow_mut().keep(assertion);
            let posit: &Posit<T> = &assertion_index.borrow().find(assertion_kept).unwrap().posit;
            // TODO: Dangerous unwrap below (if the posit has not been added)
            let posit_kept = posit_index.borrow().index_of(posit).unwrap();
            match self.posit_to_assertion.entry::<Lookup<Posit<T>, Assertion<T>>>() {
                anymap::Entry::Occupied(mut entry) => {
                    entry.get_mut().keep(posit_kept, assertion_kept)
                },
                anymap::Entry::Vacant(entry) => {
                    entry.insert(Lookup::new(posit_index.clone(), assertion_index.clone())).keep(posit_kept, assertion_kept)
                }
            };
            assertion_kept
        }
    }

} // end of mod


// --------------------- TESTING ----------------------

fn main() {

    use bareclad::*;
    let mut posy = Database::new();

    loop {
        // ------------- OPTIONS --------------
        println!("Select an option:");
        println!("0. Quit!");
        println!("1. Generate an identity.");
        if posy.current() > 0 {
            println!("   {}", posy.current());
        }
        println!("2. Add a role.");
        if posy.role_index.borrow().count() > 0 {
            println!("   {:?}", posy.role_index);
        }
        println!("3. Create a AppearanceSet.");
        if posy.identity_to_appearance.count() > 0 {
            println!("   i2a: {:?}", posy.identity_to_appearance);
        }
        if posy.appearance_to_AppearanceSet.count() > 0 {
            println!("   a2d: {:?}", posy.appearance_to_AppearanceSet);
        }
        println!("4. Create a posit.");
        println!("   {:?}", posy.posit_index);
        println!("   d2p: {:?}", posy.AppearanceSet_to_posit);
        println!("5. Create an assertion.");
        println!("   {:?}", posy.assertion_index);
        println!("   p2a: {:?}", posy.posit_to_assertion);
        println!("6. Add a tag.");
        if posy.tag_index.borrow().count() > 0 {
            println!("   {:?}", posy.tag_index);
        }
        // ------------------------------------

        let mut entered = String::new();
        io::stdin().read_line(&mut entered).expect("Failed to read line");

        let entered: u8 = match entered.trim().parse() {
            Ok(num) => num,
            Err(_)  => continue,
        };

        match entered {
            0 => break,
            1 => loop {
                posy.generate();
                break;
            },
            2 => loop {
                println!("Please enter a role:");
                let mut entered = String::new();
                io::stdin().read_line(&mut entered).expect("Failed to read line");

                posy.add_role(entered.trim().into());
                break;
            },
            3 => loop {
                println!("How many appearances are there in the dereferencing set?");
                let mut entered = String::new();
                io::stdin().read_line(&mut entered).expect("Failed to read line");

                let entered: u8 = match entered.trim().parse() {
                    Ok(num) => num,
                    Err(_)  => continue,
                };
                let mut appearances = Vec::new();
                for i in 0..entered {
                    println!("({}) Please enter an identity:", i+1);
                    let mut entered = String::new();
                    io::stdin().read_line(&mut entered).expect("Failed to read line");

                    let entered: usize = match entered.trim().parse() {
                        Ok(num) => num,
                        Err(_)  => continue,
                    };
                    let id = entered;
                    println!("({}) Please enter a role:", i+1);
                    let mut entered = String::new();
                    io::stdin().read_line(&mut entered).expect("Failed to read line");

                    let role = posy.add_role(entered.trim().into());
                    let a = Appearance::new(role, id);
                    let a_kept = posy.add_appearance(a);
                    appearances.push(posy.appearance_index.borrow().find(a_kept).unwrap());
                }

                let d = AppearanceSet::new(appearances).unwrap();
                let d_kept = posy.add_AppearanceSet(d);
                break;
            },
            4 => loop {
                println!("Please enter a AppearanceSet number:");
                let mut entered = String::new();
                io::stdin().read_line(&mut entered).expect("Failed to read line");

                let entered: usize = match entered.trim().parse() {
                    Ok(num) => num,
                    Err(_)  => continue,
                };
                let d_kept = entered;
                println!("Please enter the unix time:");
                let mut entered = String::new();
                io::stdin().read_line(&mut entered).expect("Failed to read line");

                let entered: i64 = match entered.trim().parse() {
                    Ok(num) => num,
                    Err(_)  => continue,
                };
                let t = entered;

                println!("Please enter a string value:");
                let mut entered = String::new();
                io::stdin().read_line(&mut entered).expect("Failed to read line");

                let d = posy.AppearanceSet_index.borrow().find(d_kept).unwrap();
                let p = Posit::new(entered.trim().into(), t, d);
                let p_kept = posy.add_posit::<String>(p);
                break;
            },
            5 => loop {
                println!("Please enter a posit number:");
                let mut entered = String::new();
                io::stdin().read_line(&mut entered).expect("Failed to read line");

                let entered: usize = match entered.trim().parse() {
                    Ok(num) => num,
                    Err(_)  => continue,
                };
                let p_kept = entered;
                println!("Please enter the unix time:");
                let mut entered = String::new();
                io::stdin().read_line(&mut entered).expect("Failed to read line");

                let entered: i64 = match entered.trim().parse() {
                    Ok(num) => num,
                    Err(_)  => continue,
                };
                let t = entered;

                println!("Please enter a reliability in the range -1 to 1:");
                let mut entered = String::new();
                io::stdin().read_line(&mut entered).expect("Failed to read line");

                let entered: f64 = match entered.trim().parse() {
                    Ok(num) => num,
                    Err(_)  => continue,
                };
                let r = entered;

                println!("Please enter the identity of the positor:");
                let mut entered = String::new();
                io::stdin().read_line(&mut entered).expect("Failed to read line");

                let entered: usize = match entered.trim().parse() {
                    Ok(num) => num,
                    Err(_)  => continue,
                };
                let id = entered;

                let p = posy.get_posit::<String>(p_kept).unwrap();
                let a = Assertion::new(id, r, t, p);
                let a_kept = posy.add_assertion::<String>(a);
                break;
            },
            _ => continue
        }
    }
}
    */
