
use regex::{Regex};
use lazy_static::lazy_static;
use crate::construct::{Database, Thing, OtherHasher};
use crate::datatype::{Decimal, JSON, Time};
//use logos::{Logos, Lexer};
use std::collections::{HashMap};

// used for internal result sets
use roaring::RoaringTreemap;
use std::ops::{BitAndAssign, BitOrAssign, SubAssign, BitXorAssign};

type Variables = HashMap<String, ResultSet, OtherHasher>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResultSetMode {
    Empty,
    Thing, 
    Multi
}

#[derive(Debug)]
pub struct ResultSet {
    pub mode: ResultSetMode,
    pub thing: Option<Thing>,
    pub multi: Option<RoaringTreemap>
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
                }, 
                (ResultSetMode::Thing, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    if self.thing.unwrap() != other_thing {
                        self.empty();
                    }
                },
                (ResultSetMode::Thing, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    if !other_multi.contains(self.thing.unwrap()) {
                        self.empty();
                    }
                },
                (ResultSetMode::Multi, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    if self.multi.as_ref().unwrap().contains(other_thing) {
                        self.thing(other_thing);
                    }
                    else {
                        self.empty();
                    }
                },
                (ResultSetMode::Multi, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let multi = self.multi.as_mut().unwrap();
                    *multi &= other_multi; 
                    match multi.len() {
                        0 => {
                            self.empty();
                        },
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        },
                        _ => ()
                    }
                },
                (_, _) => ()
            }
        }
    }
    fn union_with(&mut self, other: &ResultSet) {
        if other.mode != ResultSetMode::Empty {
            match (&self.mode, &other.mode) {
                (ResultSetMode::Empty, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    self.thing(other_thing);
                },
                (ResultSetMode::Empty, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let mut multi = RoaringTreemap::new(); 
                    multi.clone_from(other_multi);
                    self.multi(multi);
                },
                (ResultSetMode::Thing, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    let mut multi = RoaringTreemap::new(); 
                    multi.insert(other_thing);
                    multi.insert(self.thing.unwrap());
                    self.multi(multi);
                },
                (ResultSetMode::Thing, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let mut multi = RoaringTreemap::new(); 
                    multi.clone_from(other_multi);
                    multi.insert(self.thing.unwrap());
                    self.multi(multi);
                },
                (ResultSetMode::Multi, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    self.multi.as_mut().unwrap().insert(other_thing);
                },
                (ResultSetMode::Multi, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    *self.multi.as_mut().unwrap() |= other_multi;
                }, 
                (_, _) => ()
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
                },
                (ResultSetMode::Thing, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    if other_multi.contains(self.thing.unwrap()) {
                        self.empty();
                    }
                },
                (ResultSetMode::Multi, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    let multi = self.multi.as_mut().unwrap();
                    multi.remove(other_thing);
                    match multi.len() {
                        0 => {
                            self.empty();
                        },
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        },
                        _ => ()
                    }
                },
                (ResultSetMode::Multi, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let multi = self.multi.as_mut().unwrap();
                    *multi -= other_multi;
                    match multi.len() {
                        0 => {
                            self.empty();
                        },
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        },
                        _ => ()
                    }
                }, 
                (_, _) => ()
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
                    }
                    else {
                        let mut multi = RoaringTreemap::new(); 
                        multi.insert(other_thing);
                        multi.insert(self.thing.unwrap());
                        self.multi(multi);
                    }
                },
                (ResultSetMode::Thing, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let mut multi = RoaringTreemap::new(); 
                    multi.clone_from(other_multi);
                    let thing = self.thing.unwrap();
                    if other_multi.contains(self.thing.unwrap()) {
                        multi.remove(thing);
                    }
                    else {
                        multi.insert(thing);
                    }
                    match multi.len() {
                        0 => {
                            self.empty();
                        },
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        },
                        _ => {
                            self.multi(multi);
                        }
                    }
                },
                (ResultSetMode::Multi, ResultSetMode::Thing) => {
                    let other_thing = other.thing.unwrap();
                    let multi = self.multi.as_mut().unwrap();
                    if multi.contains(other_thing) {
                        multi.remove(other_thing);
                    }
                    else {
                        multi.insert(other_thing);
                    }
                    match multi.len() {
                        0 => {
                            self.empty();
                        },
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        },
                        _ => ()
                    }
                },
                (ResultSetMode::Multi, ResultSetMode::Multi) => {
                    let other_multi = other.multi.as_ref().unwrap();
                    let multi = self.multi.as_mut().unwrap();
                    *multi ^= other_multi;
                    match multi.len() {
                        0 => {
                            self.empty();
                        },
                        1 => {
                            let thing = multi.min().unwrap();
                            self.thing(thing);
                        },
                        _ => ()
                    }
                }, 
                (_, _) => ()
            }
        }
    }
    pub fn insert(&mut self, thing: Thing) {
        match self.mode {
            ResultSetMode::Empty => {
                self.thing(thing);
            }, 
            ResultSetMode::Thing => {
                let mut multi = RoaringTreemap::new(); 
                multi.insert(self.thing.unwrap());
                multi.insert(thing);
                self.multi(multi);
            },   
            ResultSetMode::Multi => {
                self.multi.as_mut().unwrap().insert(thing);
            }    
        }
    }
    pub fn one(&self) -> Option<Thing> {
        match self.mode {
            ResultSetMode::Empty => None,
            ResultSetMode::Thing => self.thing,
            ResultSetMode::Multi => self.multi.as_ref().unwrap().min()
        } 
    }
}
impl BitAndAssign<&'_ ResultSet> for ResultSet  {
    fn bitand_assign(&mut self, rhs: &ResultSet) {
        self.intersect_with(rhs);
    }
}
impl BitOrAssign<&'_ ResultSet> for ResultSet  {
    fn bitor_assign(&mut self, rhs: &ResultSet) {
        self.union_with(rhs);
    }
}
impl BitXorAssign<&'_ ResultSet> for ResultSet  {
    fn bitxor_assign(&mut self, rhs: &ResultSet) {
        self.symmetric_difference_with(rhs);
    }
}
impl SubAssign<&'_ ResultSet> for ResultSet  {
    fn sub_assign(&mut self, rhs: &ResultSet) {
        self.difference_with(rhs);
    }
}


// search functions in order to find posits matching certain circumstances
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
            for posit_thing in database
                .appearance_set_to_posit_thing_lookup
                .lock()
                .unwrap()
                .lookup(appearance_set)
            {
                result_set.insert(*posit_thing);
            }
        }
    }
    result_set
}

// value parsers
fn parse_string(value: &str) -> String {
    let mut c = value.chars();
    c.next();
    c.next_back();
    c.collect::<String>().replace("\"\"", "\"")
}
fn parse_i64(value: &str) -> i64 {
    value.parse::<i64>().unwrap()
}
fn parse_decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}
fn parse_json(value: &str) -> JSON {
    JSON::from_str(value).unwrap()
}
pub fn parse_time(value: &str) -> Time {
    let stripped = value.replace("'", "");
    let time = "'".to_owned() + &stripped + "'";
    // MAINTENANCE: The section below needs to be extended when new data types are added
    lazy_static! {
        static ref RE_NAIVE_DATE: Regex = Regex::new(r#"'[0-9]{4}-[0-2][0-9]-[0-3][0-9]'"#).unwrap();
    }
    if RE_NAIVE_DATE.is_match(&time) {
        return Time::new_date_from(&stripped)
    }
    Time::new()
}

use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "traqula.pest"] // relative to src
struct TraqulaParser;

pub struct Engine<'db, 'en> {
    database: &'en Database<'db>, 
}
impl<'db, 'en> Engine<'db, 'en> {
    pub fn new(database: &'en Database<'db>) -> Self {
        Self {
            database
        }
    }
    pub fn execute(&self, traqula: &str) {
        let mut variables: Variables = Variables::default();
        let traqula = TraqulaParser::parse(Rule::traqula, traqula.trim()).expect("Parsing error");
        for command in traqula {
            match command.as_rule() {
                Rule::add_role => { 
                    for role in command.into_inner() {
                        self.database.create_role(role.as_str().to_string(), false);
                    }
                }
                Rule::add_posit => { 
                    for optional_recollection in command.into_inner() {
                        let mut variable: Option<String> = None;
                        let mut value_as_json: Option<JSON> = None;
                        let mut value_as_string: Option<String> = None;
                        let mut value_as_time: Option<Time> = None;
                        let mut value_as_decimal: Option<Decimal> = None;
                        let mut value_as_i64: Option<i64> = None;
                        let mut appearance_time: Option<Time> = None;
                        let mut things = Vec::new();
                        let mut roles = Vec::new();
                        match optional_recollection.as_rule() {
                            Rule::recollect => {
                                variable = Some(optional_recollection.into_inner().next().unwrap().as_str().to_string()); 
                                println!("Recollect: {}", &variable.unwrap());
                            }
                            Rule::posit => {
                                for component in optional_recollection.into_inner() {
                                    match component.as_rule() {
                                        Rule::appearance_set => {
                                            for member in component.into_inner() {
                                                for appearance in member.into_inner() {
                                                    match appearance.as_rule() {
                                                        Rule::generate => {
                                                            let t = self.database.thing_generator().lock().unwrap().generate();
                                                            let mut result_set = ResultSet::new();
                                                            result_set.insert(t);
                                                            variables.insert(appearance.into_inner().next().unwrap().as_str().to_string(), result_set);
                                                            // println!("Variables: {:?}", variables);
                                                            things.push(t);
                                                        }
                                                        Rule::recollect => {
                                                            let result_set = variables.get(appearance.into_inner().next().unwrap().as_str()).unwrap();
                                                            let t = result_set.one().unwrap();
                                                            things.push(t);
                                                        }
                                                        Rule::role => {
                                                            roles.push(appearance.as_str());
                                                        },
                                                        _ => ()
                                                    }
                                                }
                                            }
                                        }
                                        Rule::appearing_value => {
                                            for value_type in component.into_inner() {
                                                match value_type.as_rule() {
                                                    Rule::json => {
                                                        //println!("JSON: {}", value_type.as_str());
                                                        value_as_json = Some(parse_json(value_type.as_str()))
                                                    }
                                                    Rule::string => {
                                                        //println!("String: {}", value_type.as_str());
                                                        value_as_string = Some(parse_string(value_type.as_str()));  
                                                    }
                                                    Rule::time => {
                                                        //println!("Time: {}", value_type.as_str());
                                                        value_as_time = Some(parse_time(value_type.as_str()));
                                                    }
                                                    Rule::decimal => {
                                                        //println!("Decimal: {}", value_type.as_str());
                                                        value_as_decimal = Some(parse_decimal(value_type.as_str()));
                                                    }
                                                    Rule::int => {
                                                        //println!("i64: {}", value_type.as_str());
                                                        value_as_i64 = Some(parse_i64(value_type.as_str()));
                                                    }, 
                                                    _ => ()
                                                }
                                            }
                                        }
                                        Rule::appearance_time => {
                                            appearance_time = Some(parse_time(component.as_str()));
                                        }
                                        _ => ()
                                    }
                                }
                                let mut appearances = Vec::new();
                                for i in 0..things.len() {
                                    let role = self.database.role_keeper().lock().unwrap().get(roles[i]);
                                    let (kept_appearance, previously_known) = self.database.create_apperance(things[i], role);
                                    appearances.push(kept_appearance);
                                    //println!("({}, {})", things[i], roles[i]);
                                }
                                let (kept_appearance_set, previously_known) = self.database.create_appearance_set(appearances);

                                if value_as_json.is_some() {
                                    let kept_posit = self.database.create_posit(kept_appearance_set, value_as_json.unwrap(), appearance_time.unwrap());
                                    println!("Posit: {}", kept_posit);
                                }
                                else if value_as_string.is_some() {
                                    let kept_posit = self.database.create_posit(kept_appearance_set, value_as_string.unwrap(), appearance_time.unwrap());
                                    println!("Posit: {}", kept_posit);
                                }
                                else if value_as_time.is_some() {
                                    let kept_posit = self.database.create_posit(kept_appearance_set, value_as_time.unwrap(), appearance_time.unwrap());
                                    println!("Posit: {}", kept_posit);
                                }
                                else if value_as_decimal.is_some() {
                                    let kept_posit = self.database.create_posit(kept_appearance_set, value_as_decimal.unwrap(), appearance_time.unwrap());
                                    println!("Posit: {}", kept_posit);
                                }
                                else if value_as_i64.is_some() {
                                    let kept_posit = self.database.create_posit(kept_appearance_set, value_as_i64.unwrap(), appearance_time.unwrap());
                                    println!("Posit: {}", kept_posit);
                                }
                            }
                            _ => ()
                        }
                    }
                }
                _ => ()
            }
        }
    }  
}

