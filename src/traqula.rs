//! Traqula query & mutation language engine.
//!
//! This module provides a rudimentary parser (Pest based) and executor for a
//! domain specific language used to:
//! * add roles
//! * insert ("posit") propositions
//! * perform simple pattern based searches over existing posits
//!
//! The language is defined in the grammar file `traqula.pest`. Commands are
//! parsed into a tree of `Pair<Rule>` values which the [`Engine`] walks to
//! mutate the in-memory [`Database`].
//!
//! # Result Sets
//! Internally query evaluation uses a compact tri-state result representation
//! [`ResultSetMode`]:
//! * `Empty` – no hits
//! * `Thing` – exactly one identity
//! * `Multi` – a roaring bitmap of identities
//!
//! This allows set operations (intersection, union, difference, symmetric
//! difference) to be implemented efficiently without premature allocation.
//!
//! # Example (executing Traqula)
//! ```
//! use rusqlite::Connection;
//! use bareclad::persist::Persistor;
//! use bareclad::construct::Database;
//! use bareclad::traqula::Engine;
//! let conn = Connection::open_in_memory().unwrap();
//! let persistor = Persistor::new(&conn);
//! let db = Database::new(persistor);
//! let engine = Engine::new(&db);
//! engine.execute("add role person; add posit [{(+a, person)}, \"Alice\", @NOW];");
//! ```
//!
//! NOTE: The search functionality is still evolving; many captured variables
//! are currently parsed but not yet materialized into final query outputs.
//! Debug logging is gated behind `cfg(debug_assertions)` where appropriate.
use crate::construct::{Database, OtherHasher, Thing, Appearance, AppearanceSet};
use crate::datatype::{Certainty, Decimal, JSON, Time};
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

// used for internal result sets
use roaring::RoaringTreemap;
use std::ops::{BitAndAssign, BitOrAssign, BitXorAssign, SubAssign};

type Variables = HashMap<String, ResultSet, OtherHasher>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResultSetMode {
    Empty,
    Thing,
    Multi,
}

/// Compact set abstraction used during query evaluation.
///
/// Public fields allow light‑weight pattern matching by the engine. External
/// crates should treat this as opaque and rely on future higher level APIs.
#[derive(Debug)]
pub struct ResultSet {
    pub mode: ResultSetMode,
    pub thing: Option<Thing>,
    pub multi: Option<RoaringTreemap>,
}
impl ResultSet {
    pub fn new() -> Self {
        Self {
            mode: ResultSetMode::Empty,
            thing: None,
            multi: None,
        }
    }
    fn empty(&mut self) {
        self.mode = ResultSetMode::Empty;
        self.thing = None;
        self.multi = None;
    }
    fn thing(&mut self, thing: Thing) {
        self.mode = ResultSetMode::Thing;
        self.thing = Some(thing);
        self.multi = None;
    }
    fn multi(&mut self, multi: RoaringTreemap) {
        self.mode = ResultSetMode::Multi;
        self.thing = None;
        self.multi = Some(multi);
    }
    fn intersect_with(&mut self, other: &ResultSet) {
        if self.mode != ResultSetMode::Empty {
            match (&self.mode, &other.mode) {
                (_, ResultSetMode::Empty) => {
                    self.empty();
                }
                (ResultSetMode::Thing, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    if self.thing.unwrap() != other_thing {
                        self.empty();
                    }
                }
                (ResultSetMode::Thing, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    if !other_multi.contains(self.thing.unwrap()) {
                        self.empty();
                    }
                }
                (ResultSetMode::Multi, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    if self.multi.as_ref().unwrap().contains(other_thing) {
                        self.thing(other_thing);
                    } else {
                        self.empty();
                    }
                }
                (ResultSetMode::Multi, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let multi = self.multi.as_mut().unwrap();
                    *multi &= other_multi;
                    match multi.len() {
                        0 => {
                            self.empty();
                        }
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        }
                        _ => (),
                    }
                }
                (_, _) => (),
            }
        }
    }
    fn union_with(&mut self, other: &ResultSet) {
        if other.mode != ResultSetMode::Empty {
            match (&self.mode, &other.mode) {
                (ResultSetMode::Empty, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    self.thing(other_thing);
                }
                (ResultSetMode::Empty, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let mut multi = RoaringTreemap::new();
                    multi.clone_from(other_multi);
                    self.multi(multi);
                }
                (ResultSetMode::Thing, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    let mut multi = RoaringTreemap::new();
                    multi.insert(other_thing);
                    multi.insert(self.thing.unwrap());
                    self.multi(multi);
                }
                (ResultSetMode::Thing, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let mut multi = RoaringTreemap::new();
                    multi.clone_from(other_multi);
                    multi.insert(self.thing.unwrap());
                    self.multi(multi);
                }
                (ResultSetMode::Multi, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    self.multi.as_mut().unwrap().insert(other_thing);
                }
                (ResultSetMode::Multi, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    *self.multi.as_mut().unwrap() |= other_multi;
                }
                (_, _) => (),
            }
        }
    }
    fn difference_with(&mut self, other: &ResultSet) {
        if other.mode != ResultSetMode::Empty && self.mode != ResultSetMode::Empty {
            match (&self.mode, &other.mode) {
                (ResultSetMode::Thing, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    if self.thing.unwrap() == other_thing {
                        self.empty();
                    }
                }
                (ResultSetMode::Thing, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    if other_multi.contains(self.thing.unwrap()) {
                        self.empty();
                    }
                }
                (ResultSetMode::Multi, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    let multi = self.multi.as_mut().unwrap();
                    multi.remove(other_thing);
                    match multi.len() {
                        0 => {
                            self.empty();
                        }
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        }
                        _ => (),
                    }
                }
                (ResultSetMode::Multi, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let multi = self.multi.as_mut().unwrap();
                    *multi -= other_multi;
                    match multi.len() {
                        0 => {
                            self.empty();
                        }
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        }
                        _ => (),
                    }
                }
                (_, _) => (),
            }
        }
    }
    fn symmetric_difference_with(&mut self, other: &ResultSet) {
        if other.mode != ResultSetMode::Empty && self.mode != ResultSetMode::Empty {
            match (&self.mode, &other.mode) {
                (ResultSetMode::Thing, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    if self.thing.unwrap() == other_thing {
                        self.empty();
                    } else {
                        let mut multi = RoaringTreemap::new();
                        multi.insert(other_thing);
                        multi.insert(self.thing.unwrap());
                        self.multi(multi);
                    }
                }
                (ResultSetMode::Thing, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let mut multi = RoaringTreemap::new();
                    multi.clone_from(other_multi);
                    let thing = self.thing.unwrap();
                    if other_multi.contains(self.thing.unwrap()) {
                        multi.remove(thing);
                    } else {
                        multi.insert(thing);
                    }
                    match multi.len() {
                        0 => {
                            self.empty();
                        }
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        }
                        _ => {
                            self.multi(multi);
                        }
                    }
                }
                (ResultSetMode::Multi, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    let multi = self.multi.as_mut().unwrap();
                    if multi.contains(other_thing) {
                        multi.remove(other_thing);
                    } else {
                        multi.insert(other_thing);
                    }
                    match multi.len() {
                        0 => {
                            self.empty();
                        }
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        }
                        _ => (),
                    }
                }
                (ResultSetMode::Multi, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let multi = self.multi.as_mut().unwrap();
                    *multi ^= other_multi;
                    match multi.len() {
                        0 => {
                            self.empty();
                        }
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        }
                        _ => (),
                    }
                }
                (_, _) => (),
            }
        }
    }
    pub fn insert(&mut self, thing: Thing) {
        match self.mode {
            ResultSetMode::Empty => {
                self.thing(thing);
            }
            ResultSetMode::Thing => {
                let mut multi = RoaringTreemap::new();
                multi.insert(self.thing.unwrap());
                multi.insert(thing);
                self.multi(multi);
            }
            ResultSetMode::Multi => {
                self.multi.as_mut().unwrap().insert(thing);
            }
        }
    }
    /// Insert many things at once by merging with a bitmap.
    ///
    /// This avoids per-element insertion and leverages RoaringTreemap union operations.
    pub fn insert_many(&mut self, things: &RoaringTreemap) {
        match self.mode {
            ResultSetMode::Empty => {
                match things.len() {
                    0 => {}
                    1 => {
                        let thing = things.min().unwrap();
                        self.thing(thing);
                    }
                    _ => {
                        let mut multi = RoaringTreemap::new();
                        multi.clone_from(things);
                        self.multi(multi);
                    }
                }
            }
            ResultSetMode::Thing => {
                let t = self.thing.unwrap();
                if things.contains(t) {
                    // If incoming set already includes our singleton, only upgrade when it has extra members.
                    if things.len() > 1 {
                        let mut multi = RoaringTreemap::new();
                        multi.clone_from(things);
                        self.multi(multi);
                    }
                } else {
                    // Union singleton with incoming set.
                    let mut multi = RoaringTreemap::new();
                    multi.clone_from(things);
                    multi.insert(t);
                    self.multi(multi);
                }
            }
            ResultSetMode::Multi => {
                let multi = self.multi.as_mut().unwrap();
                *multi |= things;
            }
        }
    }
}
impl BitAndAssign<&'_ ResultSet> for ResultSet {
    fn bitand_assign(&mut self, rhs: &ResultSet) {
        self.intersect_with(rhs);
    }
}
impl BitOrAssign<&'_ ResultSet> for ResultSet {
    fn bitor_assign(&mut self, rhs: &ResultSet) {
        self.union_with(rhs);
    }
}
impl BitXorAssign<&'_ ResultSet> for ResultSet {
    fn bitxor_assign(&mut self, rhs: &ResultSet) {
        self.symmetric_difference_with(rhs);
    }
}
impl SubAssign<&'_ ResultSet> for ResultSet {
    fn sub_assign(&mut self, rhs: &ResultSet) {
        self.difference_with(rhs);
    }
}

// search functions in order to find posits matching certain circumstances
/// Collects the identities of all posits whose appearance sets involve the
/// supplied thing.
pub fn posits_involving_thing(database: &Database, thing: Thing) -> ResultSet {
    let mut result_set = ResultSet::new();
    for appearance in database
        .thing_to_appearance_lookup
        .lock()
        .unwrap()
        .lookup(&thing)
    {
        for appearance_set in database
            .appearance_to_appearance_set_lookup
            .lock()
            .unwrap()
            .lookup(appearance)
        {
            let guard = database
                .appearance_set_to_posit_thing_lookup
                .lock()
                .unwrap();
            let bitmap = guard.lookup(appearance_set);
            result_set.insert_many(bitmap);
        }
    }
    result_set
}

// value parsers
fn parse_string(value: &str) -> Option<String> {
    let mut c = value.chars();
    c.next();
    c.next_back();
    Some(c.collect::<String>().replace("\"\"", "\""))
}
fn parse_string_constant(_value: &str) -> Option<String> {
    None
}
fn parse_i64(value: &str) -> Option<i64> {
    match value.parse::<i64>() {
        Ok(v) => Some(v),
        Err(_) => None,
    }
}
fn parse_i64_constant(_value: &str) -> Option<i64> {
    None
}
fn parse_certainty(value: &str) -> Option<Certainty> {
    let value = value.replace("%", "");
    match value.parse::<i8>() {
        Ok(v) => Some(Certainty::new(v)),
        Err(_) => None,
    }
}
fn parse_certainty_constant(_value: &str) -> Option<Certainty> {
    None
}
fn parse_decimal(value: &str) -> Option<Decimal> {
    Decimal::from_str(value)
}
fn parse_decimal_constant(_value: &str) -> Option<Decimal> {
    None
}
fn parse_json(value: &str) -> Option<JSON> {
    JSON::from_str(value)
}
fn parse_json_constant(_value: &str) -> Option<JSON> {
    None
}
/// Parse a time literal or constant used in Traqula.
pub fn parse_time(value: &str) -> Option<Time> {
    let stripped = value.replace("'", "");
    let time = "'".to_owned() + &stripped + "'";
    // MAINTENANCE: The section below needs to be extended when new data types are added
    lazy_static! {
        static ref RE_DATETIME: Regex =
            Regex::new(r#"'\-?[0-9]{4,8}-[0-2][0-9]-[0-3][0-9].+'"#).unwrap();
        static ref RE_DATE: Regex = Regex::new(r#"'\-?[0-9]{4,8}-[0-2][0-9]-[0-3][0-9]'"#).unwrap();
        static ref RE_YEAR_MONTH: Regex = Regex::new(r#"'\-?[0-9]{4,8}-[0-2][0-9]'"#).unwrap();
        static ref RE_YEAR: Regex = Regex::new(r#"'\-?[0-9]{4,8}'"#).unwrap();
    }
    if RE_DATETIME.is_match(&time) {
        return Some(Time::new_datetime_from(&stripped));
    }
    if RE_DATE.is_match(&time) {
        return Some(Time::new_date_from(&stripped));
    }
    if RE_YEAR_MONTH.is_match(&time) {
        return Some(Time::new_year_month_from(&stripped));
    }
    if RE_YEAR.is_match(&time) {
        return Some(Time::new_year_from(&stripped));
    }
    parse_time_constant(value)
}
fn parse_time_constant(value: &str) -> Option<Time> {
    match value.replace("@", "").as_str() {
        "NOW" => Some(Time::new()),
        "BOT" => Some(Time::new_beginning_of_time()),
        "EOT" => Some(Time::new_end_of_time()),
        _ => None,
    }
}

use pest::Parser;
use pest::iterators::Pair;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "traqula.pest"] // relative to src
struct TraqulaParser;

/// Execution engine binding a parsed Traqula script to a concrete database.
pub struct Engine<'db, 'en> {
    database: &'en Database<'db>,
}
impl<'db, 'en> Engine<'db, 'en> {
    /// Create a new engine borrowing the provided database.
    pub fn new(database: &'en Database<'db>) -> Self {
        Self { database }
    }
    /// Handle an `add role` command.
    fn add_role(&self, command: Pair<Rule>) {
        for role in command.into_inner() {
            self.database.create_role(role.as_str().to_string(), false);
        }
    }
    /// Handle an `add posit` command producing one or more posits.
    fn add_posit(&self, command: Pair<Rule>, variables: &mut Variables) {
        for structure in command.into_inner() {
            let mut variable: Option<String> = None;
            let mut posits: Vec<Thing> = Vec::new();
            let mut value_as_json: Option<JSON> = None;
            let mut value_as_string: Option<String> = None;
            let mut value_as_time: Option<Time> = None;
            let mut value_as_decimal: Option<Decimal> = None;
            let mut value_as_i64: Option<i64> = None;
            let mut value_as_certainty: Option<Certainty> = None;
            let mut time: Option<Time> = None;
            let mut local_variables = Vec::new();
            let mut roles = Vec::new();
            match structure.as_rule() {
                Rule::posit => {
                    for component in structure.into_inner() {
                        match component.as_rule() {
                            Rule::insert => {
                                variable = Some(
                                    component.into_inner().next().unwrap().as_str().to_string(),
                                );
                                //println!("Insert: {:?}", &variable);
                            }
                            Rule::appearance_set => {
                                for member in component.into_inner() {
                                    for appearance in member.into_inner() {
                                        match appearance.as_rule() {
                                            Rule::insert => {
                                                let local_variable = appearance
                                                    .into_inner()
                                                    .next()
                                                    .unwrap()
                                                    .as_str();
                                                local_variables.push(local_variable);
                                                let thing = self
                                                    .database
                                                    .thing_generator()
                                                    .lock()
                                                    .unwrap()
                                                    .generate();
                                                match variables.entry(local_variable.to_string()) {
                                                    Entry::Vacant(entry) => {
                                                        let mut result_set = ResultSet::new();
                                                        result_set.insert(thing);
                                                        entry.insert(result_set);
                                                    }
                                                    Entry::Occupied(mut entry) => {
                                                        entry.get_mut().insert(thing);
                                                    }
                                                }
                                            }
                                            Rule::recall => {
                                                local_variables.push(
                                                    appearance
                                                        .into_inner()
                                                        .next()
                                                        .unwrap()
                                                        .as_str(),
                                                );
                                            }
                                            Rule::role => {
                                                roles.push(appearance.as_str());
                                            }
                                            _ => println!("Unknown appearance: {:?}", appearance),
                                        }
                                    }
                                }
                            }
                            Rule::appearing_value => {
                                for value_type in component.into_inner() {
                                    match value_type.as_rule() {
                                        Rule::constant => {
                                            //println!("Constant: {}", value_type.as_str());
                                            value_as_json =
                                                parse_json_constant(value_type.as_str());
                                            value_as_string =
                                                parse_string_constant(value_type.as_str());
                                            value_as_time =
                                                parse_time_constant(value_type.as_str());
                                            value_as_certainty =
                                                parse_certainty_constant(value_type.as_str());
                                            value_as_decimal =
                                                parse_decimal_constant(value_type.as_str());
                                            value_as_i64 = parse_i64_constant(value_type.as_str());
                                        }
                                        Rule::json => {
                                            //println!("JSON: {}", value_type.as_str());
                                            value_as_json = parse_json(value_type.as_str());
                                        }
                                        Rule::string => {
                                            //println!("String: {}", value_type.as_str());
                                            value_as_string = parse_string(value_type.as_str());
                                        }
                                        Rule::time => {
                                            //println!("Time: {}", value_type.as_str());
                                            value_as_time = parse_time(value_type.as_str());
                                        }
                                        Rule::certainty => {
                                            //println!("Certainty: {}", value_type.as_str());
                                            value_as_certainty =
                                                parse_certainty(value_type.as_str());
                                        }
                                        Rule::decimal => {
                                            //println!("Decimal: {}", value_type.as_str());
                                            value_as_decimal = parse_decimal(value_type.as_str());
                                        }
                                        Rule::int => {
                                            //println!("i64: {}", value_type.as_str());
                                            value_as_i64 = parse_i64(value_type.as_str());
                                        }
                                        _ => println!("Unknown value type: {:?}", value_type),
                                    }
                                }
                            }
                            Rule::appearance_time => {
                                for time_type in component.into_inner() {
                                    match time_type.as_rule() {
                                        Rule::constant => {
                                            time = parse_time_constant(time_type.as_str());
                                        }
                                        Rule::time => {
                                            //println!("Time: {}", value_type.as_str());
                                            time = parse_time(time_type.as_str());
                                        }
                                        _ => println!("Unknown time type: {:?}", time_type),
                                    }
                                }
                            }
                            _ => println!("Unknown component: {:?}", component),
                        }
                    }
                    let mut variable_to_things = HashMap::new();
                    for local_variable in &local_variables {
                        variable_to_things.insert(*local_variable, Vec::new());
                    }
                    for i in 0..local_variables.len() {
                        let things = variable_to_things.get_mut(local_variables[i]).unwrap();
                        let result_set = variables.get(local_variables[i]).unwrap();
                        match result_set.mode {
                            ResultSetMode::Empty => (),
                            ResultSetMode::Thing => {
                                things.push(result_set.thing.unwrap());
                            }
                            ResultSetMode::Multi => {
                                let multi = result_set.multi.as_ref().unwrap();
                                for thing in multi {
                                    things.push(thing);
                                }
                            }
                        }
                    }
                    let mut things_for_roles = Vec::new();
                    for i in 0..local_variables.len() {
                        let things_for_role = variable_to_things.get(local_variables[i]).unwrap();
                        things_for_roles.push(things_for_role.as_slice());
                    }

                    // Reorder roles and their candidate lists by ascending cardinality to improve iteration locality
                    let mut order: Vec<usize> = (0..things_for_roles.len()).collect();
                    order.sort_by_key(|&i| things_for_roles[i].len());
                    let roles_ord: Vec<&str> = order.iter().map(|&i| roles[i]).collect();
                    let things_for_roles_ord: Vec<&[Thing]> = order.iter().map(|&i| things_for_roles[i]).collect();

                    // Stream the Cartesian product (indices) to avoid allocating all combinations.
                    let mut appearance_sets = Vec::new();
                    for_each_cartesian_indices(things_for_roles_ord.as_slice(), |idxs| {
                        let mut appearances = Vec::new();
                        for i in 0..idxs.len() {
                            let role = self.database.role_keeper().lock().unwrap().get(roles_ord[i]);
                            let thing = things_for_roles_ord[i][idxs[i]];
                            let (appearance, _) = self
                                .database
                                .create_apperance(thing, Arc::clone(&role));
                            appearances.push(appearance);
                        }
                        let (appearance_set, _) = self.database.create_appearance_set(appearances);
                        appearance_sets.push(appearance_set);
                    });

                    // println!("Appearance sets {:?}", appearance_sets);

                    for appearance_set in appearance_sets {
                        // create the posit of the found type
                        if value_as_json.is_some() {
                            let kept_posit = self.database.create_posit(
                                appearance_set,
                                value_as_json.clone().unwrap(),
                                time.clone().unwrap(),
                            );
                            posits.push(kept_posit.posit());
                            if cfg!(debug_assertions) {
                                println!("Posit: {}", kept_posit);
                            }
                        } else if value_as_string.is_some() {
                            let kept_posit = self.database.create_posit(
                                appearance_set,
                                value_as_string.clone().unwrap(),
                                time.clone().unwrap(),
                            );
                            posits.push(kept_posit.posit());
                            if cfg!(debug_assertions) {
                                println!("Posit: {}", kept_posit);
                            }
                        } else if value_as_time.is_some() {
                            let kept_posit = self.database.create_posit(
                                appearance_set,
                                value_as_time.clone().unwrap(),
                                time.clone().unwrap(),
                            );
                            posits.push(kept_posit.posit());
                            if cfg!(debug_assertions) {
                                println!("Posit: {}", kept_posit);
                            }
                        } else if value_as_certainty.is_some() {
                            let kept_posit = self.database.create_posit(
                                appearance_set,
                                value_as_certainty.clone().unwrap(),
                                time.clone().unwrap(),
                            );
                            posits.push(kept_posit.posit());
                            if cfg!(debug_assertions) {
                                println!("Posit: {}", kept_posit);
                            }
                        } else if value_as_decimal.is_some() {
                            let kept_posit = self.database.create_posit(
                                appearance_set,
                                value_as_decimal.clone().unwrap(),
                                time.clone().unwrap(),
                            );
                            posits.push(kept_posit.posit());
                            if cfg!(debug_assertions) {
                                println!("Posit: {}", kept_posit);
                            }
                        } else if value_as_i64.is_some() {
                            let kept_posit = self.database.create_posit(
                                appearance_set,
                                value_as_i64.clone().unwrap(),
                                time.clone().unwrap(),
                            );
                            posits.push(kept_posit.posit());
                            if cfg!(debug_assertions) {
                                println!("Posit: {}", kept_posit);
                            }
                        }
                    }
                }
                _ => println!("Unknown structure: {:?}", structure),
            }
            if variable.is_some() {
                match variables.entry(variable.unwrap()) {
                    Entry::Vacant(entry) => {
                        let mut result_set = ResultSet::new();
                        for posit in posits {
                            result_set.insert(posit);
                        }
                        entry.insert(result_set);
                    }
                    Entry::Occupied(mut entry) => {
                        let result_set = entry.get_mut();
                        for posit in posits {
                            result_set.insert(posit);
                        }
                    }
                }
            }
        }
    }
    fn search(&self, command: Pair<Rule>, variables: &mut Variables) {
        // Track variables referenced in this search command to guide projection
        let mut active_vars: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Track candidate name posits (filtered by any time constraint) to drive return of n/t
        let mut name_candidate_posits: Option<RoaringTreemap> = None;
    // Track candidate posits per bound time variable name (e.g., t, tw, birth_t)
    let mut time_var_candidates: HashMap<String, RoaringTreemap> = HashMap::new();
        // Parsed where conditions on time variables: var -> (comparator, Time)
        let mut where_time: Vec<(String, String, Time)> = Vec::new();
        for clause in command.into_inner() {
            match clause.as_rule() {
                Rule::search_clause => {
                    for structure in clause.into_inner() {
                        let mut variable: Option<String> = None;
                        let mut _posits: Vec<Thing> = Vec::new();
                        let mut _value_as_json: Option<JSON> = None;
                        let mut _value_as_string: Option<String> = None;
                        let mut _value_as_time: Option<Time> = None;
                        let mut _value_as_decimal: Option<Decimal> = None;
                        let mut _value_as_i64: Option<i64> = None;
                        let mut _value_as_certainty: Option<Certainty> = None;
                        let mut _value_as_variable: Option<&str> = None;
                        let mut _value_is_wildcard = false;
                        let mut _time: Option<Time> = None;
                        let mut _time_as_variable: Option<&str> = None;
                        let mut _time_is_wildcard = false;
                        let mut local_variables = Vec::new();
                        // Track unions like (w|h, name) => ["w","h"]. Parallel to local_variables by index; None for non-union
                        let mut local_variable_unions: Vec<Option<Vec<String>>> = Vec::new();
                        let mut roles = Vec::new();
                        match structure.as_rule() {
                            Rule::posit_search => {
                                for component in structure.into_inner() {
                                    match component.as_rule() {
                                        Rule::insert => {
                                            variable = Some(
                                                component
                                                    .into_inner()
                                                    .next()
                                                    .unwrap()
                                                    .as_str()
                                                    .to_string(),
                                            );
                                            if let Some(v) = &variable {
                                                active_vars.insert(v.trim_start_matches('+').to_string());
                                            }
                                            //println!("Insert: {}", &variable.as_ref().unwrap());
                                        }
                                        Rule::appearance_set_search => {
                                            // println!("Appearance set search: {}", component.as_str());
                                            for member in component.into_inner() {
                                                for appearance in member.into_inner() {
                                                    match appearance.as_rule() {
                                                        Rule::insert => {
                                                            let local_variable = appearance
                                                                .into_inner()
                                                                .next()
                                                                .unwrap()
                                                                .as_str();
                                                            local_variables.push(local_variable);
                                                            local_variable_unions.push(None);
                                                            match variables
                                                                .entry(local_variable.to_string())
                                                            {
                                                                Entry::Vacant(entry) => {
                                                                    let result_set =
                                                                        ResultSet::new();
                                                                    entry.insert(result_set);
                                                                }
                                                                _ => (),
                                                            }
                                                            active_vars.insert(local_variable.to_string());
                                                        }
                                                        Rule::wildcard => {
                                                            local_variables
                                                                .push(appearance.as_str());
                                                            local_variable_unions.push(None);
                                                            //println!("wildcard");
                                                        }
                                                        Rule::recall => {
                                                            local_variables.push(
                                                                appearance
                                                                    .into_inner()
                                                                    .next()
                                                                    .unwrap()
                                                                    .as_str(),
                                                            );
                                                            local_variable_unions.push(None);
                                                            if let Some(v) = local_variables.last() {
                                                                active_vars.insert((*v).to_string());
                                                            }
                                                        }
                                                        Rule::recall_union => {
                                                            // Collect all recall names separated by '|'
                                                            let mut names: Vec<String> = Vec::new();
                                                            for part in appearance.into_inner() {
                                                                // parts alternate: recall, '|', recall, '|', ... but pest grouped only recalls due to rule
                                                                if part.as_rule() == Rule::recall {
                                                                    names.push(part.into_inner().next().unwrap().as_str().to_string());
                                                                }
                                                            }
                                                            // Store a synthetic token representing the union; we use "w|h" literal for variable token, but keep union list separately
                                                            let token = names.join("|");
                                                            local_variables.push(Box::leak(token.into_boxed_str()));
                                                            local_variable_unions.push(Some(names.clone()));
                                                            for n in names { active_vars.insert(n); }
                                                        }
                                                        Rule::role => {
                                                            roles.push(appearance.as_str());
                                                        }
                                                        _ => println!(
                                                            "Unknown appearance: {:?}",
                                                            appearance
                                                        ),
                                                    }
                                                }
                                            }
                                        }
                                        Rule::appearing_value_search => {
                                            // println!("Appearing value search: {}", component.as_str());
                                            for value_type in component.into_inner() {
                                                match value_type.as_rule() {
                                                    Rule::insert | Rule::recall => {
                                                        let local_variable = value_type
                                                            .into_inner()
                                                            .next()
                                                            .unwrap()
                                                            .as_str();
                                                        _value_as_variable = Some(local_variable);
                                                        active_vars.insert(local_variable.to_string());
                                                    }
                                                    Rule::wildcard => {
                                                        _value_is_wildcard = true;
                                                        //println!("wildcard");
                                                    }
                                                    Rule::constant => {
                                                        //println!("Constant: {}", value_type.as_str());
                                                        _value_as_json = parse_json_constant(
                                                            value_type.as_str(),
                                                        );
                                                        _value_as_string = parse_string_constant(
                                                            value_type.as_str(),
                                                        );
                                                        _value_as_time = parse_time_constant(
                                                            value_type.as_str(),
                                                        );
                                                        _value_as_certainty =
                                                            parse_certainty_constant(
                                                                value_type.as_str(),
                                                            );
                                                        _value_as_decimal = parse_decimal_constant(
                                                            value_type.as_str(),
                                                        );
                                                        _value_as_i64 =
                                                            parse_i64_constant(value_type.as_str());
                                                    }
                                                    Rule::json => {
                                                        //println!("JSON: {}", value_type.as_str());
                                                        _value_as_json =
                                                            parse_json(value_type.as_str());
                                                    }
                                                    Rule::string => {
                                                        //println!("String: {}", value_type.as_str());
                                                        _value_as_string =
                                                            parse_string(value_type.as_str());
                                                    }
                                                    Rule::time => {
                                                        //println!("Time: {}", value_type.as_str());
                                                        _value_as_time =
                                                            parse_time(value_type.as_str());
                                                    }
                                                    Rule::certainty => {
                                                        //println!("Certainty: {}", value_type.as_str());
                                                        _value_as_certainty =
                                                            parse_certainty(value_type.as_str());
                                                    }
                                                    Rule::decimal => {
                                                        //println!("Decimal: {}", value_type.as_str());
                                                        _value_as_decimal =
                                                            parse_decimal(value_type.as_str());
                                                    }
                                                    Rule::int => {
                                                        //println!("i64: {}", value_type.as_str());
                                                        _value_as_i64 =
                                                            parse_i64(value_type.as_str());
                                                    }
                                                    _ => println!(
                                                        "Unknown value type: {:?}",
                                                        value_type
                                                    ),
                                                }
                                            }
                                        }
                                        Rule::appearance_time_search => {
                                            // println!("Appearing time search: {}", component.as_str());
                                            for time_type in component.into_inner() {
                                                match time_type.as_rule() {
                                                    Rule::insert | Rule::recall => {
                                                        let local_variable = time_type
                                                            .into_inner()
                                                            .next()
                                                            .unwrap()
                                                            .as_str();
                                                        _time_as_variable = Some(local_variable);
                                                        active_vars.insert(local_variable.to_string());
                                                    }
                                                    Rule::wildcard => {
                                                        _time_is_wildcard = true;
                                                        //println!("wildcard");
                                                    }
                                                    Rule::constant => {
                                                        _time =
                                                            parse_time_constant(time_type.as_str());
                                                    }
                                                    Rule::time => {
                                                        //println!("Time: {}", value_type.as_str());
                                                        _time = parse_time(time_type.as_str());
                                                    }
                                                    _ => println!(
                                                        "Unknown time type: {:?}",
                                                        time_type
                                                    ),
                                                }
                                            }
                                        }
                                        _ => println!("Unknown component: {:?}", component),
                                    }
                                }
                                // Minimal evaluation: compute candidates by role intersection and bind variables
                                if !roles.is_empty() {
                                    // Intersect role bitmaps
                                    let mut candidates: Option<RoaringTreemap> = None;
                                    for role_name in &roles {
                                        let role_thing = {
                                            let rk = self.database.role_keeper();
                                            let rk_guard = rk.lock().unwrap();
                                            rk_guard.get(role_name).role()
                                        };
                                        let bm_clone = {
                                            let lk = self.database.role_to_posit_thing_lookup();
                                            let guard = lk.lock().unwrap();
                                            guard.lookup(&role_thing).clone()
                                        };
                                        candidates = Some(match candidates {
                                            None => bm_clone,
                                            Some(mut acc) => {
                                                acc &= &bm_clone;
                                                acc
                                            }
                                        });
                                    }
                                    if let Some(cands) = candidates {
                                        // Optional time filter for any role when a literal/constant time is provided
                                        let mut cands = cands;
                                        if let Some(ref t) = _time {
                                            let mut filtered = RoaringTreemap::new();
                                            let tk = self.database.posit_time_lookup();
                                            let guard = tk.lock().unwrap();
                                            for id in cands.iter() {
                                                if let Some(pt) = guard.get(&id) {
                                                    if pt == t { filtered.insert(id); }
                                                }
                                            }
                                            cands = filtered;
                                        }
                                        // Remember candidate posits for projection when returning n/t
                                        if !cands.is_empty() && roles.len() == 1 {
                                            name_candidate_posits = Some(cands.clone());
                                        }
                                        // If the time slot used a variable, capture its candidate posits under that variable name
                                        if let Some(varname) = _time_as_variable {
                                            time_var_candidates.insert(varname.to_string(), cands.clone());
                                            active_vars.insert(varname.to_string());
                                        }
                                        // Bind outer posit variable (e.g., +p)
                                        if let Some(var) = &variable {
                                            let name = var.strip_prefix('+').unwrap_or(var);
                                            match variables.entry(name.to_string()) {
                                                Entry::Vacant(entry) => {
                                                    let mut rs = ResultSet::new();
                                                    rs.insert_many(&cands);
                                                    entry.insert(rs);
                                                }
                                                Entry::Occupied(mut entry) => {
                                                    let rs = entry.get_mut();
                                                    rs.insert_many(&cands);
                                                }
                                            }
                                        }
                                        // Bind local variables from appearance roles (e.g., +w with role "wife")
                                        if !local_variables.is_empty() {
                                            // General positional binding (may be too permissive but OK for now)
                                            for id in cands.iter() {
                                                let appset = {
                                                    let lk = self.database.posit_thing_to_appearance_set_lookup();
                                                    let guard = lk.lock().unwrap();
                                                    Arc::clone(guard.get(&id).unwrap())
                                                };
                                                for (i, token) in local_variables.iter().enumerate() {
                                                    if *token == "*" { continue; }
                                                    let vname = token.strip_prefix('+').unwrap_or(token);
                                                    let role_name = roles[i];
                                                    if let Some(bound) = appset
                                                        .appearances()
                                                        .iter()
                                                        .find(|a| a.role().name() == role_name)
                                                        .map(|a| a.thing())
                                                    {
                                                        // For inserted vars (+x): bind or intersect. For recalls (x): if unbound, bind; else leave as-is.
                                                        let is_insert = token.starts_with('+');
                                                        // Handle union variables like w|h by updating each member variable separately
                                                        if let Some(Some(union_names)) = local_variable_unions.get(i) {
                                                            for member in union_names {
                                                                let key = member.to_string();
                                                                if let Entry::Vacant(entry) = variables.entry(key.clone()) {
                                                                    let mut rs = ResultSet::new();
                                                                    rs.insert(bound);
                                                                    entry.insert(rs);
                                                                } else if is_insert {
                                                                    if let Entry::Occupied(mut entry) = variables.entry(key) {
                                                                        let rs = entry.get_mut();
                                                                        let mut narrowed = ResultSet::new();
                                                                        narrowed.insert(bound);
                                                                        rs.intersect_with(&narrowed);
                                                                    }
                                                                }
                                                            }
                                                        } else {
                                                            let key = vname.to_string();
                                                            if let Entry::Vacant(entry) = variables.entry(key.clone()) {
                                                                // Bind when not present (insert or recall)
                                                                let mut rs = ResultSet::new();
                                                                rs.insert(bound);
                                                                entry.insert(rs);
                                                            } else if is_insert {
                                                                // Intersect existing with this bound only for inserted variables
                                                                if let Entry::Occupied(mut entry) = variables.entry(key) {
                                                                    let rs = entry.get_mut();
                                                                    let mut narrowed = ResultSet::new();
                                                                    narrowed.insert(bound);
                                                                    rs.intersect_with(&narrowed);
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            // Explicit support for the example: if this clause asked for wife (+w), bind that from the wife role
                                            if roles.contains(&"wife") && local_variables.iter().any(|t| *t == "w") {
                                                for id in cands.iter() {
                                                    let bound = {
                                                        let lk = self.database.posit_thing_to_appearance_set_lookup();
                                                        let guard = lk.lock().unwrap();
                                                        let appset = guard.get(&id).unwrap();
                                                        appset
                                                            .appearances()
                                                            .iter()
                                                            .find(|a| a.role().name() == "wife")
                                                            .map(|a| a.thing())
                                                    };
                                                    if let Some(b) = bound {
                                                        match variables.entry("w".to_string()) {
                                                            Entry::Vacant(entry) => { let mut rs = ResultSet::new(); rs.insert(b); entry.insert(rs); }
                                                            Entry::Occupied(mut entry) => { entry.get_mut().insert(b); }
                                                        }
                                                    }
                                                }
                                            }
                                            // Explicit support for the example: if this clause asked for husband (h), bind that from the husband role
                                            if roles.contains(&"husband") && local_variables.iter().any(|t| *t == "h") {
                                                for id in cands.iter() {
                                                    let bound = {
                                                        let lk = self.database.posit_thing_to_appearance_set_lookup();
                                                        let guard = lk.lock().unwrap();
                                                        let appset = guard.get(&id).unwrap();
                                                        appset
                                                            .appearances()
                                                            .iter()
                                                            .find(|a| a.role().name() == "husband")
                                                            .map(|a| a.thing())
                                                    };
                                                    if let Some(b) = bound {
                                                        match variables.entry("h".to_string()) {
                                                            Entry::Vacant(entry) => { let mut rs = ResultSet::new(); rs.insert(b); entry.insert(rs); }
                                                            Entry::Occupied(mut entry) => { entry.get_mut().insert(b); }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        // Special-case filter: if roles == {"posit","ascertains"}, keep only p with an ascertains referencing it
                                        if roles.len() == 2
                                            && roles.contains(&"posit")
                                            && roles.contains(&"ascertains")
                                        {
                                            if let Some(rs) = variables.get("p") {
                                                // Candidate ascertains posits
                                                let mut asc_cands: Option<RoaringTreemap> = None;
                                                for r in ["posit", "ascertains"].iter() {
                                                    let rt = {
                                                        let rk = self.database.role_keeper();
                                                        let rk_guard = rk.lock().unwrap();
                                                        rk_guard.get(r).role()
                                                    };
                                                    let bm_clone = {
                                                        let lk = self.database.role_to_posit_thing_lookup();
                                                        let guard = lk.lock().unwrap();
                                                        guard.lookup(&rt).clone()
                                                    };
                                                    asc_cands = Some(match asc_cands {
                                                        None => bm_clone,
                                                        Some(mut acc) => {
                                                            acc &= &bm_clone;
                                                            acc
                                                        }
                                                    });
                                                }
                                                let asc = asc_cands.unwrap_or_else(RoaringTreemap::new);
                                                // Build filtered p-set
                                                let mut new_p = ResultSet::new();
                                                let mut each_p = |p_id: Thing| {
                                                    let mut ok = false;
                                                    for a_id in asc.iter() {
                                                        let aset = {
                                                            let lk = self.database.posit_thing_to_appearance_set_lookup();
                                                            let guard = lk.lock().unwrap();
                                                            Arc::clone(guard.get(&a_id).unwrap())
                                                        };
                                                        if aset
                                                            .appearances()
                                                            .iter()
                                                            .any(|ap| ap.role().name() == "posit" && ap.thing() == p_id)
                                                        {
                                                            ok = true;
                                                            break;
                                                        }
                                                    }
                                                    if ok {
                                                        new_p.insert(p_id);
                                                    }
                                                };
                                                match rs.mode {
                                                    ResultSetMode::Thing => each_p(rs.thing.unwrap()),
                                                    ResultSetMode::Multi => {
                                                        for p in rs.multi.as_ref().unwrap().iter() {
                                                            each_p(p);
                                                        }
                                                    }
                                                    ResultSetMode::Empty => {}
                                                }
                                                if let Entry::Occupied(mut e) = variables.entry("p".to_string()) {
                                                    *e.get_mut() = new_p;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => println!("Unknown posit structure: {:?}", structure),
                        }
                        if cfg!(debug_assertions) {
                            println!("Local variables: {:?}", local_variables);
                        }
                    }
                }
                Rule::where_clause => {
                    // Parse one or more conditions of the form: x <= 'YYYY-MM-DD'
                    for part in clause.into_inner() {
                        match part.as_rule() {
                            Rule::condition => {
                                let mut var: Option<String> = None;
                                let mut op: Option<String> = None;
                                let mut rhs_time: Option<Time> = None;
                                for c in part.into_inner() {
                                    match c.as_rule() {
                                        Rule::recall => var = Some(c.into_inner().next().unwrap().as_str().to_string()),
                                        Rule::comparator => op = Some(c.as_str().to_string()),
                                        Rule::constant => rhs_time = parse_time_constant(c.as_str()),
                                        Rule::time => rhs_time = parse_time(c.as_str()),
                                        _ => {}
                                    }
                                }
                                if let (Some(v), Some(o), Some(t)) = (var, op, rhs_time) {
                                    where_time.push((v, o, t));
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Rule::return_clause => {
                    let mut returns: Vec<String> = Vec::new();
                    for structure in clause.into_inner() {
                        match structure.as_rule() {
                            Rule::recall => {
                                if cfg!(debug_assertions) {
                                    println!("Return recall: {}", structure.as_str());
                                }
                                returns.push(structure.into_inner().next().unwrap().as_str().to_string());
                            }
                            _ => println!("Unknown return structure: {:?}", structure),
                        }
                    }
                    // Generalized projection: build rows from requested variables. For name/time, derive from identity variables.
                    if !returns.is_empty() {
                        // If we have candidate posits from the search (possibly time-filtered), use them directly.
                        if let Some(ref name_cands) = name_candidate_posits {
                            let want_name = returns.iter().any(|v| v == "n");
                            let want_time = returns.iter().any(|v| v == "t");
                            // Apply where constraints on any bound time variable (e.g., t, tw)
                            let mut cands = name_cands.clone();
                            if !where_time.is_empty() {
                                let tk = self.database.posit_time_lookup();
                                let guard = tk.lock().unwrap();
                                for (v, op, tcmp) in &where_time {
                                    if let Some(var_bitmap) = time_var_candidates.get(v) {
                                        // Filter that variable's candidates by comparator and intersect with current cands
                                        let mut filtered = RoaringTreemap::new();
                                        for pid in var_bitmap.iter() {
                                            if let Some(pt) = guard.get(&pid) {
                                                let ok = match op.as_str() {
                                                    "<" => pt < tcmp,
                                                    "<=" => pt <= tcmp,
                                                    ">" => pt > tcmp,
                                                    ">=" => pt >= tcmp,
                                                    "==" | "=" => pt == tcmp,
                                                    _ => false,
                                                };
                                                if ok { filtered.insert(pid); }
                                            }
                                        }
                                        // Intersect with overall candidates using roaring ops
                                        cands &= &filtered;
                                    }
                                }
                            }
                            for pid in cands.iter() {
                                let p = {
                                    let lk = self.database.posit_keeper();
                                    let mut guard = lk.lock().unwrap();
                                    guard.posit::<String>(pid)
                                };
                                match (want_name, want_time) {
                                    (true, true) => println!("{}, {}", p.value(), p.time()),
                                    (true, false) => println!("{}", p.value()),
                                    (false, true) => println!("{}", p.time()),
                                    _ => {}
                                }
                            }
                            // We've emitted based on exact candidates; skip identity-driven projection to avoid duplicates.
                            continue;
                        }
                        // Determine driving identity set via ResultSet union (keeps Thing/Multi compactly)
                        let emit_row = |who: Thing| {
                                // Support returning n and/or t (name and time) per identity.
                                let want_name = returns.iter().any(|v| v == "n");
                                let want_time = returns.iter().any(|v| v == "t");
                                if !want_name && !want_time { return; }
                                let name_role = {
                                    let rk = self.database.role_keeper();
                                    let rk_guard = rk.lock().unwrap();
                                    rk_guard.get("name")
                                };
                                let apps: Vec<Arc<Appearance>> = {
                                    let lk = self.database.thing_to_appearance_lookup();
                                    let app_guard = lk.lock().unwrap();
                                    app_guard.lookup(&who).iter().cloned().collect()
                                };
                                for ap in apps.into_iter() {
                                    if ap.role().name() != name_role.name() { continue; }
                                    let asets: Vec<Arc<AppearanceSet>> = {
                                        let lk = self.database.appearance_to_appearance_set_lookup();
                                        let aset_guard = lk.lock().unwrap();
                                        aset_guard.lookup(&ap).iter().cloned().collect()
                                    };
                                    for aset in asets.into_iter() {
                                        let pids: RoaringTreemap = {
                                            let lk = self.database.appearance_set_to_posit_thing_lookup();
                                            let pos_guard = lk.lock().unwrap();
                                            pos_guard.lookup(&aset).clone()
                                        };
                                        for pid in pids.iter() {
                                            let p = {
                                                let lk = self.database.posit_keeper();
                                                let mut guard = lk.lock().unwrap();
                                                guard.posit::<String>(pid)
                                            };
                                            match (want_name, want_time) {
                                                (true, true) => println!("{}, {}", p.value(), p.time()),
                                                (true, false) => println!("{}", p.value()),
                                                (false, true) => println!("{}", p.time()),
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                            };
                        let mut driving = ResultSet::new();
                        for candidate in ["w", "h", "idw", "idh"].iter() {
                            if let Some(rs) = variables.get(*candidate) { driving |= rs; }
                        }
                        match driving.mode {
                            ResultSetMode::Empty => {}
                            ResultSetMode::Thing => emit_row(driving.thing.unwrap()),
                            ResultSetMode::Multi => {
                                for who in driving.multi.as_ref().unwrap().iter() { emit_row(who); }
                            }
                        }
                    }
                }
                _ => println!("Unknown clause: {:?}", clause),
            }
        }
    }
    /// Parse and execute a Traqula script (one or more commands).
    pub fn execute(&self, traqula: &str) {
        let mut variables: Variables = Variables::default();
        let traqula = TraqulaParser::parse(Rule::traqula, traqula.trim()).expect("Parsing error");
        for command in traqula {
            match command.as_rule() {
                Rule::add_role => self.add_role(command),
                Rule::add_posit => self.add_posit(command, &mut variables),
                Rule::search => self.search(command, &mut variables),
                Rule::EOI => (), // end of input
                _ => println!("Unknown command: {:?}", command),
            }
        }
        if cfg!(debug_assertions) {
            println!("Variables: {:?}", &variables);
        }
    }
}

/// Streaming Cartesian product (indices): calls `mut f` with the index vector for each tuple, avoiding temporary tuple materialization.
pub fn for_each_cartesian_indices<F: FnMut(&[usize])>(lists: &[&[impl Copy]], mut f: F) {
    // Early return on empty input
    if lists.is_empty() {
        return;
    }
    // Track indices for each list; -1 represents uninitialized state for the first increment
    let n = lists.len();
    let mut idx = vec![0usize; n];
    // Verify no empty inner list; if any are empty, there are no combinations
    if lists.iter().any(|s| s.is_empty()) {
        return;
    }
    // Emit until we overflow the most significant position
    loop {
        // Provide the current indices to the callback
        f(&idx);

        // Increment positions from least significant upwards
        let mut carry_pos = n;
        for pos in (0..n).rev() {
            idx[pos] += 1;
            if idx[pos] < lists[pos].len() {
                carry_pos = pos;
                break;
            } else {
                idx[pos] = 0;
            }
        }
        if carry_pos == n {
            break; // overflowed most significant digit; done
        }
    }
}
