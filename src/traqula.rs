
use regex::{Regex};
use std::sync::Arc;
use crate::construct::{Database, Appearance, AppearanceSet, Thing, OtherHasher, DataType};
use logos::{Logos, Lexer};
use std::collections::{HashMap, HashSet};
use chrono::NaiveDate;

// used for internal result sets
use roaring::RoaringTreemap;
use roaring::treemap::IntoIter;
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

    #[regex(r"\{[^\}]+\},[^,]+,'[^']+'")]
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
                let posit_enclosure = Regex::new(r"\[|\]").unwrap();
                let posit = posit_enclosure.replace_all(add_posit.slice().trim(), "");
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

// this function will provide "look-alike" data types
fn parse_data_type(value: &str, time: &str) -> (&'static str, &'static str) {
    if value.chars().nth(0).unwrap() == Engine::STRIPMARK {
        return (String::DATA_TYPE, NaiveDate::DATA_TYPE)
    }
    ("Unknown", "Unknown")
}

fn parse_posit(posit: &str, database: &Database, variables: &mut Variables, strips: &Vec<String>) -> Option<Thing> {
    println!("Parsing posit: {}", posit);
    let component_regex = Regex::new(r#"\{([^\}]+)\},(.*),'(.*)'"#).unwrap();
    let captures = component_regex.captures(posit).unwrap();
    let appearance_set = captures.get(1).unwrap().as_str();
    let appearance_set = parse_appearance_set(LexicalAppearanceSet::lexer(&appearance_set), database, variables);
    let value = captures.get(2).unwrap().as_str();
    let time = captures.get(3).unwrap().as_str();
    match parse_data_type(value, time) {
        (String::DATA_TYPE, NaiveDate::DATA_TYPE) => {
            let value = strips[value.replace(Engine::STRIPMARK, "").parse::<usize>().unwrap() - 1].clone();
            let time = NaiveDate::parse_from_str(time, "%Y-%m-%d").unwrap();
            let posit = database.create_posit(appearance_set, value, time);
            return Some(posit.posit())
        }
        (_, _) => None
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
                let appearance_enclosure = Regex::new(r"\(|\)").unwrap();
                let appearance = appearance_enclosure.replace_all(appearance_set.slice().trim(), "");
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
    let component_regex = Regex::new(r#"([^,]+),(.+)"#).unwrap();
    let captures = component_regex.captures(appearance).unwrap();
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

