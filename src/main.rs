///
/// Implements a database using posits from Transitional Modeling.
///
extern crate chrono;
use chrono::{DateTime, Utc};

use std::io;


// we will use keepers as a pattern to own some things
extern crate bimap;

mod bareclad {
    use std::sync::Arc;

    use bimap::BiMap;

    use std::collections::hash_map::Entry::{Occupied, Vacant};
    use std::collections::{HashMap, HashSet};
    use std::hash::{Hash};
    use std::cell::RefCell;
    use std::ops;
    use std::fmt;

    static GENESIS: usize = 0;

    pub type Identity = usize;

    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct IdentityGenerator {
        current: Identity,
        released: Vec<Identity>
    }

    impl IdentityGenerator {
        pub fn new() -> IdentityGenerator {
            IdentityGenerator {
                current: GENESIS,
                released: Vec::new()
            }
        }
        pub fn release(&mut self, g: Identity) {
            self.released.push(g);
        }
        pub fn generate(&mut self) -> Identity {
            if self.released.len() > 0 {
                return self.released.pop().unwrap()
            }
            self.current += 1;
            self.current
        }
        pub fn current(&self) -> usize {
            self.current - self.released.len()
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

    pub struct RoleKeeper {
        kept: BiMap<&'static str, Arc<Role>>
    }
    impl RoleKeeper {
        pub fn new() -> RoleKeeper {
            RoleKeeper {
                kept: BiMap::new()
            }
        }
        pub fn keep(&mut self, role: Role) -> Arc<Role> {
            let name = role.get_name();
            self.kept.insert(name, Arc::new(role));
            self.kept.get_by_left(&name).unwrap().clone()
        }
    }

    // ------------- Appearance -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Appearance {
        role:           Arc<Role>,
        identity:       Arc<Identity>
    }
    impl Appearance {
        pub fn new(role: &Arc<Role>, identity: &Arc<Identity>) -> Appearance {
            Appearance {
                role: Arc::clone(role),
                identity: Arc::clone(identity)
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
        kept: HashSet<Arc<Appearance>>
    }
    impl AppearanceKeeper {
        pub fn new() -> AppearanceKeeper {
            AppearanceKeeper {
                kept: HashSet::new()
            }
        }
        pub fn keep(&mut self, appearance: Appearance) -> Arc<Appearance> {
            let a = Arc::new(appearance);
            self.kept.insert(a.clone());
            self.kept.get(&a).unwrap().clone()
        }
    }

    // ------------- Appearance -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Dereference {
        set: Vec<Arc<Appearance>>
    }
    impl Dereference {
        pub fn new(mut s: Vec<Arc<Appearance>>) -> Option<Dereference> {
            s.sort();
            if s.len() > 1 {
                for i in 1..s.len() {
                    if s[i].role == s[i-1].role { return None };
                }
            }
            Some(Dereference{ set: s })
        }
        pub fn get_set(&self) -> &Vec<Arc<Appearance>> {
            &self.set
        }
    }
} // end of mod

use std::sync::Arc;
use bareclad::{Identity, IdentityGenerator, Role, RoleKeeper, Appearance, AppearanceKeeper, Dereference};

pub fn main() {
    let mut generator = IdentityGenerator::new();
    let mut role_keeper = RoleKeeper::new();
    let i: Arc<Identity> = Arc::new(generator.generate());
    let r = Role::new(&String::from("color"), false);
    let kept_r = role_keeper.keep(r);
    // drop(r); // just to make sure it moved
    let mut appearance_keeper = AppearanceKeeper::new();
    let a1 = Appearance::new(&kept_r, &i);
    let kept_a1 = appearance_keeper.keep(a1); // transfer ownership to the keeper
    let a2 = Appearance::new(&kept_r, &i);
    let kept_a2 = appearance_keeper.keep(a2);
    println!("{} {}", kept_a1.get_role().get_name(), kept_a1.get_identity());
    println!("{} {}", kept_a2.get_role().get_name(), kept_a2.get_identity());
    let d1 = Dereference::new([kept_a1].to_vec());
    println!("{:?}", appearance_keeper);
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



    // ------------- Dereference ------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Dereference {
        set: Vec<Rc<Appearance>>
    }
    impl Dereference {
        pub fn new(mut s: Vec<Rc<Appearance>>) -> Option<Dereference> {
            s.sort();
            if s.len() > 1 {
                for i in 1..s.len() {
                    if s[i].role == s[i-1].role { return None };
                }
            }
            Some(Dereference{ set: s })
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
        dereference:    Rc<Dereference>
    }
    impl<T> Posit<T> {
        pub fn new(value: T, time: i64, dereference: Rc<Dereference>) -> Posit<T> {
            Posit {
                value: value,
                time: time,
                dereference: dereference
            }
        }
        pub fn get_value(&self) -> &T {
            &self.value
        }
        pub fn get_time(&self) -> i64 {
            self.time
        }
        pub fn get_dereference(&self) -> &Dereference {
            &self.dereference
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
        pub dereference_index:          KeptIndex<Dereference>,
        pub posit_index:                AnyMap,
        pub assertion_index:            AnyMap,
        // lends their references to lookups
        pub role_to_identity:           Lookup<String,()>,
        pub identity_to_appearance:     Lookup<(), Appearance>,
        pub appearance_to_dereference:  Lookup<Appearance, Dereference>,
        pub dereference_to_posit:       AnyMap,
        pub posit_to_assertion:         AnyMap
    }

    impl Database {
        pub fn new() -> Database {
            let id_generator = Generator::new();
            // indexes
            let role_index: KeptIndex<String> = Rc::new(RefCell::new(Index::new()));
            let tag_index: KeptIndex<String> = Rc::new(RefCell::new(Index::new()));
            let appearance_index: KeptIndex<Appearance> = Rc::new(RefCell::new(Index::new()));
            let dereference_index: KeptIndex<Dereference> = Rc::new(RefCell::new(Index::new()));
            let posit_index = AnyMap::new();
            let assertion_index = AnyMap::new();
            // lookups
            let role_to_identity: Lookup<String, ()> = Lookup::new_with_source(role_index.clone());
            let identity_to_appearance: Lookup<(), Appearance> = Lookup::new_with_target(appearance_index.clone());
            let appearance_to_dereference: Lookup<Appearance, Dereference> = Lookup::new(appearance_index.clone(), dereference_index.clone());
            let dereference_to_posit = AnyMap::new();
            let posit_to_assertion = AnyMap::new();

            Database {
                id_generator:               id_generator,
                role_index:                 role_index,
                tag_index:                  tag_index,
                appearance_index:           appearance_index,
                dereference_index:          dereference_index,
                posit_index:                posit_index,
                assertion_index:            assertion_index,
                role_to_identity:           role_to_identity,
                identity_to_appearance:     identity_to_appearance,
                appearance_to_dereference:  appearance_to_dereference,
                dereference_to_posit:       dereference_to_posit,
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
        pub fn add_dereference(&mut self, dereference: Dereference) -> usize {
            let dereference_kept = self.dereference_index.borrow_mut().keep(dereference);
            for a in &self.dereference_index.borrow().find(dereference_kept).unwrap().set {
                self.appearance_to_dereference.keep(
                    self.appearance_index.borrow().index_of(a).unwrap(),
                    dereference_kept
                );
            }
            dereference_kept
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
            let dereference: &Dereference = &posit_index.borrow().find(posit_kept).unwrap().dereference;
            // TODO: Dangerous unwrap below (if the dereference has not been added)
            let dereference_kept = self.dereference_index.borrow().index_of(dereference).unwrap();
            match self.dereference_to_posit.entry::<Lookup<Dereference, Posit<T>>>() {
                anymap::Entry::Occupied(mut entry) => {
                    entry.get_mut().keep(dereference_kept, posit_kept)
                },
                anymap::Entry::Vacant(entry) => {
                    entry.insert(Lookup::new(self.dereference_index.clone(), posit_index.clone())).keep(dereference_kept, posit_kept)
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
        println!("3. Create a dereference.");
        if posy.identity_to_appearance.count() > 0 {
            println!("   i2a: {:?}", posy.identity_to_appearance);
        }
        if posy.appearance_to_dereference.count() > 0 {
            println!("   a2d: {:?}", posy.appearance_to_dereference);
        }
        println!("4. Create a posit.");
        println!("   {:?}", posy.posit_index);
        println!("   d2p: {:?}", posy.dereference_to_posit);
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

                let d = Dereference::new(appearances).unwrap();
                let d_kept = posy.add_dereference(d);
                break;
            },
            4 => loop {
                println!("Please enter a dereference number:");
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

                let d = posy.dereference_index.borrow().find(d_kept).unwrap();
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
