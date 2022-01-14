
use regex::{Regex};
use lazy_static::lazy_static;
use std::sync::Arc;
use crate::construct::{Database, Appearance, AppearanceSet, Thing, OtherHasher};
use crate::datatype::{DataType, Decimal, JSON, Time};
use logos::{Logos, Lexer};
use std::collections::{HashMap};
use chrono::{NaiveDate, NaiveDateTime};

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

#[derive(Logos, Debug, PartialEq)]
enum Command {
    #[error]
    #[regex(r"[\t\n\r\f]+", logos::skip)] 
    Error,

    #[regex(r"add role [^;]+")]
    AddRole,

    #[regex(r"add posit [^;]+")]
    AddPosit,

    #[regex(r"search [^;]+")]
    Search,

    #[token(";")]
    CommandTerminator,
} 
fn parse_command(mut command: Lexer<Command>, database: &Database, variables: &mut Variables, strips: &Vec<String>) {
    while let Some(token) = command.next() {
        match token {
            Command::AddRole => {
                println!("Adding roles...");
                let trimmed_command = command.slice().trim().replacen("add role ", "", 1);
                let add_role_result_set = parse_add_role(AddRole::lexer(&trimmed_command), database);
                println!("{:?}", add_role_result_set);
            }, 
            Command::AddPosit => {
                println!("Adding posits...");
                let trimmed_command = command.slice().trim().replacen("add posit ", "", 1);
                parse_add_posit(AddPosit::lexer(&trimmed_command), database, variables, strips);
            }, 
            Command::Search => {
                println!("Search: {}", command.slice());
            }, 
            Command::CommandTerminator => (), 
            _ => {
                println!("Unrecognized command: {}", command.slice());
            }
        }
    }
}

#[derive(Logos, Debug, PartialEq)]
enum AddRole {
    #[error]
    #[regex(r"[\t\n\r\f]+", logos::skip, priority = 2)] 
    Error,

    #[regex(r"[^,]+")]
    Role,

    #[token(",")]
    ItemSeparator,
}

fn parse_add_role(mut add_role: Lexer<AddRole>, database: &Database) -> ResultSet {
    let mut add_roles_result_set = ResultSet::new();
    while let Some(token) = add_role.next() {
        match token {
            AddRole::Role => {
                let role_name = String::from(add_role.slice().trim());
                let (role, previously_known) = database.create_role(role_name, false);
                add_roles_result_set.insert(role.role());
            },
            AddRole::ItemSeparator => (), 
            _ => {
                println!("Unrecognized role: {}", add_role.slice());
            }
        } 
    }
    //println!("Added roles {:?}", add_roles_results);
    add_roles_result_set
}

#[derive(Logos, Debug, PartialEq)]
enum AddPosit {
    #[error]
    #[regex(r"[\t\n\r\f]+", logos::skip)] 
    Error,

    #[regex(r"\{[^\}]+\},([^,]+|\{[^\}]*\}),'[^']+'")]
    Posit,

    #[token("[")]
    StartPosit,

    #[token("]")]
    EndPosit,

    #[token(",")]
    ItemSeparator,
}

fn parse_add_posit(mut add_posit: Lexer<AddPosit>, database: &Database, variables: &mut Variables, strips: &Vec<String>) -> ResultSet {
    let mut add_posit_result_set = ResultSet::new();
    while let Some(token) = add_posit.next() {
        match token {
            AddPosit::Posit => {
                lazy_static! {
                    static ref RE_POSIT_ENCLOSURE: Regex = Regex::new(r"\[|\]").unwrap();
                }
                let posit = RE_POSIT_ENCLOSURE.replace_all(add_posit.slice().trim(), "");
                match parse_posit(&posit, database, variables, strips) {
                    Some(posit_thing) => add_posit_result_set.insert(posit_thing), 
                    None => ()
                }    
            },
            AddPosit::ItemSeparator => (), 
            AddPosit::StartPosit => (),
            AddPosit::EndPosit => (),
            _ => {
                println!("Unrecognized posit: {}", add_posit.slice());
            }
        }
    }
    add_posit_result_set
    
}

// these functions will provide "look-alike" data types
fn parse_value_type(value: &str) -> &'static str {
    // MAINTENANCE: The section below needs to be extended when new data types are added
    if value.chars().nth(0).unwrap() == Engine::STRIPMARK {
        return String::DATA_TYPE;
    }
    if value.chars().nth(0).unwrap() == '{' {
        return JSON::DATA_TYPE; 
    }
    if value.chars().nth(0).unwrap() == '\'' {
        return Time::DATA_TYPE;
    }
    if value.parse::<i64>().is_ok() {
        return i64::DATA_TYPE;
    }
    if Decimal::from_str(value).is_some() {
        return Decimal::DATA_TYPE;
    }
    "Unknown"
}

// value parsers
fn parse_string(value: &str, strips: &Vec<String>) -> String {
    let strip = value.replace(Engine::STRIPMARK, "").parse::<usize>().unwrap() - 1;
    strips[strip].clone()
}
fn parse_i64(value: &str) -> i64 {
    value.parse::<i64>().unwrap()
}
fn parse_decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}
fn parse_json(value: &str, strips: &Vec<String>) -> JSON {
    lazy_static! {
        static ref RE_STRIPMARKED: Regex = {
            let mut pattern = "".to_owned();
            pattern.push(Engine::STRIPMARK);
            pattern.push_str(r"\d+");
            Regex::new(&pattern).unwrap()
        };
    }
    let mut v = String::from(value);
    for m in RE_STRIPMARKED.find_iter(value) {
        let strip = m.as_str().replace(Engine::STRIPMARK, "").parse::<usize>().unwrap() - 1;
        v = v.replace(m.as_str(), &("\"".to_owned() + &strips[strip] + "\""));
    }
    JSON::from_str(&v).unwrap()
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

fn parse_posit(posit: &str, database: &Database, variables: &mut Variables, strips: &Vec<String>) -> Option<Thing> {
    println!("Parsing posit: {}", posit);
    lazy_static! {
        static ref RE_POSIT_COMPONENTS: Regex = Regex::new(r#"\{([^\}]+)\},(.*),('.*')"#).unwrap();
    }
    let captures = RE_POSIT_COMPONENTS.captures(posit).unwrap();
    let appearance_set = captures.get(1).unwrap().as_str();
    let appearance_set = parse_appearance_set(LexicalAppearanceSet::lexer(&appearance_set), database, variables);
    let value = captures.get(2).unwrap().as_str();
    let time = captures.get(3).unwrap().as_str();

    // MAINTENANCE: The section below needs to be extended when new data types are added
    match parse_value_type(value) {
        String::DATA_TYPE => {
            let value = parse_string(value, strips);
            let time = parse_time(time);
            let posit = database.create_posit(appearance_set, value, time);
            Some(posit.posit())
        }, 
        i64::DATA_TYPE => {
            let value = parse_i64(value);
            let time = parse_time(time);
            let posit = database.create_posit(appearance_set, value, time);
            Some(posit.posit())
        }, 
        Decimal::DATA_TYPE => {
            let value = parse_decimal(value);
            let time = parse_time(time);
            let posit = database.create_posit(appearance_set, value, time);
            Some(posit.posit())
        }, 
        Time::DATA_TYPE => {
            let value = parse_time(value);
            let time = parse_time(time);
            let posit = database.create_posit(appearance_set, value, time);
            Some(posit.posit())
        }, 
        JSON::DATA_TYPE => {
            let value = parse_json(value, strips);
            let time = parse_time(time);
            let posit = database.create_posit(appearance_set, value, time);
            Some(posit.posit())
        },
        v => {
            println!(">>> Unhandled value type: {} ({})", v, value);
            None
        }
    }
}

#[derive(Logos, Debug, PartialEq)]
enum LexicalAppearanceSet {
    #[error]
    #[regex(r"[\t\n\r\f]+", logos::skip)] 
    Error,

    #[regex(r"\([^\)]+\)")]
    Appearance,

    #[token(",")]
    ItemSeparator,
}

fn parse_appearance_set(mut appearance_set: Lexer<LexicalAppearanceSet>, database: &Database, variables: &mut Variables) -> Arc<AppearanceSet> {
    let mut appearances = Vec::new();
    while let Some(token) = appearance_set.next() {
        match token {
            LexicalAppearanceSet::Appearance => {
                lazy_static! {
                    static ref RE_APPEARANCE_ENCLOSURE: Regex = Regex::new(r"\(|\)").unwrap();
                }
                let appearance = RE_APPEARANCE_ENCLOSURE.replace_all(appearance_set.slice().trim(), "");
                // println!("\tParsing appearance: {}", appearance);
                let appearance = parse_appearance(&appearance, database, variables);
                appearances.push(appearance);
            },
            LexicalAppearanceSet::ItemSeparator => (),
            _ => {
                println!("Unrecognized appearance: {}", appearance_set.slice());
            }
        } 
    }
    let (kept_appearance_set, previously_known) = database.create_appearance_set(appearances);
    kept_appearance_set
}

fn parse_appearance(appearance: &str, database: &Database, variables: &mut Variables) -> Arc<Appearance> {
    lazy_static! {
        static ref RE_APPEARANCE_COMPONENTS: Regex = Regex::new(r#"([^,]+),(.+)"#).unwrap();
    }
    let captures = RE_APPEARANCE_COMPONENTS.captures(appearance).unwrap();
    let qualified_thing = captures.get(1).unwrap().as_str();
    let role_name = captures.get(2).unwrap().as_str();
    let (qualifier, thing_or_variable) = if qualified_thing.parse::<Thing>().is_ok() {
        ('#', qualified_thing)
    }
    else {
        let mut chars = qualified_thing.chars();
        (chars.next().unwrap(), chars.as_str())
    };
    let thing = match qualifier {
        '#' => { 
            // println!("\tNumeric value"); 
            let t = thing_or_variable.parse::<Thing>().unwrap();
            database.thing_generator().lock().unwrap().check(t).unwrap(); // error if the thing is unknown
            Some(t)
        },
        '+' => { 
            // println!("\tGenerate identity"); 
            let t = database.thing_generator().lock().unwrap().generate();
            let mut result_set = ResultSet::new();
            result_set.insert(t);
            variables.insert(thing_or_variable.to_string(), result_set);
            Some(t)
        },
        '$' => { 
            // println!("\tFetch identity"); 
            let result_set = variables.get(thing_or_variable).unwrap();
            let t = result_set.one().unwrap();
            Some(t)
        },
        _ => None
    };
    let role = database.role_keeper().lock().unwrap().get(role_name);
    let (kept_appearance, previously_known) = database.create_apperance(thing.unwrap(), role);
    kept_appearance
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

pub struct Engine<'db, 'en> {
    database: &'en Database<'db>, 
}
impl<'db, 'en> Engine<'db, 'en> {
    const SUBSTITUTE: char = 26 as char;
    const STRIPMARK: char = 15 as char;
    pub fn new(database: &'en Database<'db>) -> Self {
        Self {
            database
        }
    }
    pub fn execute(&self, traqula: &str) {
        let mut in_string = false;
        let mut in_comment = false;
        let mut previous_c = Engine::SUBSTITUTE;
        let mut stripped = String::new();
        let mut strip = String::new();
        let mut strips: Vec<String> = Vec::new();
        for c in traqula.chars() {
            // first determine mode
            if c == '#' && !in_string {
                in_comment = true;
            }
            else if c == '\n' && !in_string {
                in_comment = false;
            }
            else if c == '"' && !in_string {
                in_string = true;
            }
            else if c == '"' && previous_c != '"' && in_string {
                in_string = false;
            }
            // mode dependent push
            if in_string {
                if c == '"' && previous_c == '"' {
                    strip.push('"');
                    previous_c = Engine::SUBSTITUTE;
                }
                else {
                    if c != '"' { strip.push(c); }
                    previous_c = c;
                }
            }
            else if !in_comment {
                if c == '\n' || c == '\r' {
                    if !previous_c.is_whitespace() && previous_c != ',' && previous_c != ';' { 
                        stripped.push(' '); 
                    }
                    previous_c = ' ';
                }
                else if c.is_whitespace() && (previous_c.is_whitespace() || previous_c == ',' || previous_c == ';') {
                    previous_c = c;
                }     
                else {
                    if previous_c == '"' {
                        strips.push(strip);
                        strip = String::new();
                        stripped += &(Engine::STRIPMARK.to_string() + &strips.len().to_string());
                    }
                    if c != '"' { stripped.push(c); }
                    previous_c = c;
                }           
            }
        }
        let mut variables: Variables = Variables::default();
        println!("Stripped:\n{}\nStrips:\n{:?}", &stripped.trim(), strips);
        parse_command(Command::lexer(&stripped.trim()), &self.database, &mut variables, &strips);  
    }  
}

