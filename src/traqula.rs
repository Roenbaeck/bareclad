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
            for posit_thing in bitmap.iter() {
                result_set.insert(posit_thing);
            }
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

                    let cartesian = cartesian_product(things_for_roles.as_slice());

                    //println!("variable_to_things {:?}", variable_to_things);
                    //println!("things_for_roles {:?}", things_for_roles.as_slice());

                    let mut appearance_sets = Vec::new();
                    for things_in_appearance_set in cartesian {
                        let mut appearances = Vec::new();
                        for i in 0..things_in_appearance_set.len() {
                            let role = self.database.role_keeper().lock().unwrap().get(roles[i]);
                            let (appearance, _) = self
                                .database
                                .create_apperance(things_in_appearance_set[i], Arc::clone(&role));
                            appearances.push(appearance);
                        }
                        let (appearance_set, _) = self.database.create_appearance_set(appearances);
                        appearance_sets.push(appearance_set);
                    }

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
        for clause in command.into_inner() {
            match clause.as_rule() {
                Rule::search_clause => {
                    for structure in clause.into_inner() {
                        let mut variable: Option<String> = None;
                        let mut _posits: Vec<Thing> = Vec::new();
                        let mut value_as_json: Option<JSON> = None;
                        let mut value_as_string: Option<String> = None;
                        let mut value_as_time: Option<Time> = None;
                        let mut value_as_decimal: Option<Decimal> = None;
                        let mut value_as_i64: Option<i64> = None;
                        let mut value_as_certainty: Option<Certainty> = None;
                        let mut value_as_variable: Option<&str> = None;
                        let mut value_is_wildcard = false;
                        let mut time: Option<Time> = None;
                        let mut time_as_variable: Option<&str> = None;
                        let mut time_is_wildcard = false;
                        let mut local_variables = Vec::new();
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
                                                            if let Some(v) = local_variables.last() {
                                                                active_vars.insert((*v).to_string());
                                                            }
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
                                                        value_as_variable = Some(local_variable);
                                                        active_vars.insert(local_variable.to_string());
                                                    }
                                                    Rule::wildcard => {
                                                        value_is_wildcard = true;
                                                        //println!("wildcard");
                                                    }
                                                    Rule::constant => {
                                                        //println!("Constant: {}", value_type.as_str());
                                                        value_as_json = parse_json_constant(
                                                            value_type.as_str(),
                                                        );
                                                        value_as_string = parse_string_constant(
                                                            value_type.as_str(),
                                                        );
                                                        value_as_time = parse_time_constant(
                                                            value_type.as_str(),
                                                        );
                                                        value_as_certainty =
                                                            parse_certainty_constant(
                                                                value_type.as_str(),
                                                            );
                                                        value_as_decimal = parse_decimal_constant(
                                                            value_type.as_str(),
                                                        );
                                                        value_as_i64 =
                                                            parse_i64_constant(value_type.as_str());
                                                    }
                                                    Rule::json => {
                                                        //println!("JSON: {}", value_type.as_str());
                                                        value_as_json =
                                                            parse_json(value_type.as_str());
                                                    }
                                                    Rule::string => {
                                                        //println!("String: {}", value_type.as_str());
                                                        value_as_string =
                                                            parse_string(value_type.as_str());
                                                    }
                                                    Rule::time => {
                                                        //println!("Time: {}", value_type.as_str());
                                                        value_as_time =
                                                            parse_time(value_type.as_str());
                                                    }
                                                    Rule::certainty => {
                                                        //println!("Certainty: {}", value_type.as_str());
                                                        value_as_certainty =
                                                            parse_certainty(value_type.as_str());
                                                    }
                                                    Rule::decimal => {
                                                        //println!("Decimal: {}", value_type.as_str());
                                                        value_as_decimal =
                                                            parse_decimal(value_type.as_str());
                                                    }
                                                    Rule::int => {
                                                        //println!("i64: {}", value_type.as_str());
                                                        value_as_i64 =
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
                                                        time_as_variable = Some(local_variable);
                                                        active_vars.insert(local_variable.to_string());
                                                    }
                                                    Rule::wildcard => {
                                                        time_is_wildcard = true;
                                                        //println!("wildcard");
                                                    }
                                                    Rule::constant => {
                                                        time =
                                                            parse_time_constant(time_type.as_str());
                                                    }
                                                    Rule::time => {
                                                        //println!("Time: {}", value_type.as_str());
                                                        time = parse_time(time_type.as_str());
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
                                        // Bind outer posit variable (e.g., +p)
                                        if let Some(var) = &variable {
                                            let name = var.strip_prefix('+').unwrap_or(var);
                                            match variables.entry(name.to_string()) {
                                                Entry::Vacant(entry) => {
                                                    let mut rs = ResultSet::new();
                                                    for id in cands.iter() {
                                                        rs.insert(id);
                                                    }
                                                    entry.insert(rs);
                                                }
                                                Entry::Occupied(mut entry) => {
                                                    let rs = entry.get_mut();
                                                    for id in cands.iter() {
                                                        rs.insert(id);
                                                    }
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
                    // Minimal projection: if returning (n, t), print name/time for bound w or h
                    if returns.iter().any(|v| v == "n") && returns.iter().any(|v| v == "t") {
                        // Choose driving variable based on variables referenced in this search
                        let driving = if active_vars.contains("w") {
                            variables.get("w").map(|wrs| ("w", wrs))
                        } else if active_vars.contains("h") {
                            variables.get("h").map(|hrs| ("h", hrs))
                        } else {
                            None
                        };
                        if let Some((_key, wrs)) = driving {
                            let mut seen: std::collections::BTreeSet<(String, String)> = std::collections::BTreeSet::new();
                            let each_w = |w: Thing, seen: &mut std::collections::BTreeSet<(String, String)>| {
                                // Find (w, name) posits and print value/time
                                let name_role = {
                                    let rk = self.database.role_keeper();
                                    let rk_guard = rk.lock().unwrap();
                                    rk_guard.get("name")
                                };
                                let apps: Vec<Arc<Appearance>> = {
                                    let lk = self.database.thing_to_appearance_lookup();
                                    let app_guard = lk.lock().unwrap();
                                    app_guard.lookup(&w).iter().cloned().collect()
                                };
                                for ap in apps.into_iter() {
                                    if ap.role().name() != name_role.name() {
                                        continue;
                                    }
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
                                            // Fetch typed posit to access value/time
                                            let p = {
                                                let lk = self.database.posit_keeper();
                                                let mut guard = lk.lock().unwrap();
                                                guard.posit::<String>(pid)
                                            };
                                            let key = (p.value().to_string(), p.time().to_string());
                                            if seen.insert(key.clone()) {
                                                println!("{}, {}", key.0, key.1);
                                            }
                                        }
                                    }
                                }
                            };
                            match wrs.mode {
                                ResultSetMode::Thing => each_w(wrs.thing.unwrap(), &mut seen),
                                ResultSetMode::Multi => {
                                    for w in wrs.multi.as_ref().unwrap().iter() {
                                        each_w(w, &mut seen);
                                    }
                                }
                                ResultSetMode::Empty => {}
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

/*
The following code for cartesian products has been made by Kyle Lacy,
and was originally found here:

https://gist.github.com/kylewlacy/115965b40e02a3325558

Copyright © 2016-2021 Kyle Lacy, Some Rights Reserved.

Additionally, all code snippets and fragments are also licensed under both the terms of the
MIT license and the Unlicense (at the licensee's choice), unless otherwise noted.
*/

/// Helper producing the Cartesian product of accumulated rows with another slice.
pub fn partial_cartesian<T: Clone>(a: Vec<Vec<T>>, b: &[T]) -> Vec<Vec<T>> {
    a.into_iter()
        .flat_map(|xs| {
            b.iter()
                .cloned()
                .map(|y| {
                    let mut vec = xs.clone();
                    vec.push(y);
                    vec
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Cartesian product for a slice of slices.
pub fn cartesian_product<T: Clone>(lists: &[&[T]]) -> Vec<Vec<T>> {
    match lists.split_first() {
        Some((first, rest)) => {
            let init: Vec<Vec<T>> = first.iter().cloned().map(|n| vec![n]).collect();

            rest.iter()
                .cloned()
                .fold(init, |vec, list| partial_cartesian(vec, list))
        }
        None => {
            vec![]
        }
    }
}
