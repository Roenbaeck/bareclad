///
/// Implements a database using posits from Transitional Modeling.
///
extern crate chrono;
use chrono::{DateTime, Utc};

use std::io;


// we will use keepers as a pattern to own some things
extern crate anymap;

mod bareclad {
    use anymap;
    use anymap::AnyMap;

    use std::collections::hash_map::Entry::{Occupied, Vacant};
    use std::collections::{HashMap, HashSet};
    use std::hash::{Hash};
    use std::rc::Rc;
    use std::cell::RefCell;
    use std::ops;
    use std::fmt;

    static GENESIS: usize = 0;
    pub struct Generator {
        current: usize,
        released: Vec<usize>
    }

    impl Generator {
        pub fn new() -> Generator {
            Generator {
                current: GENESIS,
                released: Vec::new()
            }
        }
        pub fn release(&mut self, g: usize) {
            self.released.push(g);
        }
        pub fn generate(&mut self) -> usize {
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

    #[derive(Debug)]
    pub struct Index<T: Eq + Hash> {
        index:  Vec<Rc<T>>,
        kept:   HashMap<Rc<T>, usize>
    }
    impl<T> Index<T> where T: Eq + Hash {
        pub fn new() -> Index<T> {
            Index {
                index: Vec::new(),
                kept:  HashMap::new()
            }
        }
        pub fn keep(&mut self, keepsake: T) -> usize {
            let k = Rc::new(keepsake);
            self.index.push(k.clone());
            match self.kept.entry(k) {
                Occupied(entry) => *entry.get(),
                Vacant(entry)   => *entry.insert(self.index.len() - 1)
            }
        }
        // TODO: Really bad to index using [] since it panics if out of bounds
        pub fn find(&self, i:usize) -> Option<Rc<T>> {
            match self.index.get(i) {
                Some(kept) => Some(kept.clone()),
                None => None
            }
        }
        pub fn index_of(&self, k:&T) -> Option<usize> {
            match self.kept.get(k) {
                Some(i) => Some(*i),
                None => None
            }
        }
        pub fn count(&self) -> usize {
            self.index.len()
        }
    }

    type KeptIndex<T> = Rc<RefCell<Index<T>>>;

    #[derive(Debug)]
    pub struct Lookup<S:Hash + Eq, T:Hash + Eq> {
        source: KeptIndex<S>,
        target: KeptIndex<T>,
        lookup: HashMap<usize, HashSet<usize>>
    }
    impl<S, T> Lookup<S, T> where S: Hash + Eq, T: Hash + Eq {
        pub fn new(s: KeptIndex<S>, t: KeptIndex<T>) -> Lookup<S, T> {
            Lookup {
                source: s,
                target: t,
                lookup: HashMap::new()
            }
        }
        pub fn keep(&mut self, key: usize, value: usize) -> bool {
            match self.lookup.entry(key) {
                Occupied(mut entry) => entry.get_mut().insert(value),
                Vacant(entry) => entry.insert(HashSet::new()).insert(value)
            }
        }
        pub fn find(&self, key: usize) -> Option<&HashSet<usize>> {
            self.lookup.get(&key)
        }
        pub fn count(&self) -> usize {
            self.lookup.len()
        }
    }
    impl<S> Lookup<S, ()> where S: Hash + Eq {
        pub fn new_with_source(s: KeptIndex<S>) -> Lookup<S, ()> {
            let t: Index<()> = Index::new();
            Lookup {
                source: s,
                target: Rc::new(RefCell::new(t)),
                lookup: HashMap::new()
            }
        }
    }
    impl<T> Lookup<(), T> where T: Hash + Eq {
        pub fn new_with_target(t: KeptIndex<T>) -> Lookup<(), T> {
            let s: Index<()> = Index::new();
            Lookup {
                source: Rc::new(RefCell::new(s)),
                target: t,
                lookup: HashMap::new()
            }
        }
    }
    impl Lookup<(), ()> {
        pub fn new_with_nothing() -> Lookup<(), ()> {
            let s: Index<()> = Index::new();
            let t: Index<()> = Index::new();
            Lookup {
                source: Rc::new(RefCell::new(s)),
                target: Rc::new(RefCell::new(t)),
                lookup: HashMap::new()
            }
        }
    }


    /*
    The master will certainly win.
    The master will probably win.
    The master will likely win.
    The master may win.
    The master is unlikely to win.
    The master has a small chance of winning.
    */

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


    // ------------- Appearance -------------
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    pub struct Appearance {
        role:           usize,  // borrowed in theory
        identity:       usize   // borrowed in theory
    }
    impl Appearance {
        pub fn new(role: usize, identity: usize) -> Appearance {
            Appearance {
                role: role,
                identity: identity
            }
        }
        pub fn get_role(&self) -> usize {
            self.role
        }
        pub fn get_identity(&self) -> usize {
            self.identity
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
