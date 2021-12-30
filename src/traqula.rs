
use regex::Regex;
use std::sync::Arc;
use crate::bareclad::{Database, Role, Appearance, AppearanceSet, Thing};
use logos::{Logos, Lexer};
use std::collections::HashMap;
use chrono::NaiveDate;

type Variables = HashMap<String, Arc<Thing>>;

#[derive(Logos, Debug, PartialEq)]
enum Command {
    #[error]
    #[regex(r"[\t\n\r\f]+", logos::skip)] 
    Error,

    #[regex(r"add role ([a-z A-Z]+[,]?)+")]
    AddRole,

    #[regex(r"add posit (\[[^\]]*\][,]?)+")]
    AddPosit,

    #[regex(r"search [^;]+")]
    Search,

    #[token(";")]
    CommandTerminator,
} 
fn parse_command(mut command: Lexer<Command>, database: &Database, variables: &mut Variables) {
    while let Some(token) = command.next() {
        match token {
            Command::AddRole => {
                println!("Adding roles...");
                let trimmed_command = command.slice().trim().replacen("add role ", "", 1);
                for add_role_result in parse_add_role(AddRole::lexer(&trimmed_command), database, variables) {
                    println!("{: >15} -> known: {}", add_role_result.role.name(), add_role_result.known);
                }
            }, 
            Command::AddPosit => {
                println!("Adding posits...");
                let trimmed_command = command.slice().trim().replacen("add posit ", "", 1);
                parse_add_posit(AddPosit::lexer(&trimmed_command), database, variables);
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
struct AddRoleResult {
    role: Arc<Role>,
    known: bool
}
fn parse_add_role(mut add_role: Lexer<AddRole>, database: &Database, variables: &mut Variables) -> Vec<AddRoleResult> {
    let mut roles: Vec<AddRoleResult> = Vec::new();
    while let Some(token) = add_role.next() {
        match token {
            AddRole::Role => {
                let role_name = String::from(add_role.slice().trim());
                let (role, previously_known) = database.create_role(role_name, false);
                roles.push(AddRoleResult { role: role, known: previously_known });
            },
            AddRole::ItemSeparator => (), 
            _ => {
                println!("Unrecognized role: {}", add_role.slice());
            }
        } 
    }
    roles
}

#[derive(Logos, Debug, PartialEq)]
enum AddPosit {
    #[error]
    #[regex(r"[\t\n\r\f]+", logos::skip)] 
    Error,

    #[regex(r"\[[^\]]+\]")]
    Posit,

    #[token(",")]
    ItemSeparator,
}

fn parse_add_posit(mut add_posit: Lexer<AddPosit>, database: &Database, variables: &mut Variables) {
    while let Some(token) = add_posit.next() {
        match token {
            AddPosit::Posit => {
                let posit_enclosure = Regex::new(r"\[|\]").unwrap();
                let posit = posit_enclosure.replace_all(add_posit.slice().trim(), "");
                parse_posit(&posit, database, variables);
            },
            AddPosit::ItemSeparator => (), 
            _ => {
                println!("Unrecognized posit: {}", add_posit.slice());
            }
        }
    }
}

fn parse_posit(posit: &str, database: &Database, variables: &mut Variables) {
    // println!("\t[{}]", posit);
    let component_regex = Regex::new(r#"\{([^\}]+)\},(.*),'(.*)'"#).unwrap();
    let captures = component_regex.captures(posit).unwrap();
    let appearance_set = captures.get(1).unwrap().as_str();
    let appearance_set_result = parse_appearance_set(LexicalAppearanceSet::lexer(&appearance_set), database, variables);
    let value = captures.get(2).unwrap().as_str();
    let time = captures.get(3).unwrap().as_str();
    let naive_date = NaiveDate::parse_from_str(time, "%Y-%m-%d").unwrap();
    // determine type of value
    if value.chars().nth(0).unwrap() == '"' {
        let string_value = value.replace("\"", "").replace(Engine::substitute, "\"");
        database.create_posit(appearance_set_result.appearance_set, string_value, naive_date);
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
struct AppearanceSetResult {
    appearance_results: Vec<AppearanceResult>,
    appearance_set: Arc<AppearanceSet>,
    known: bool
}
fn parse_appearance_set(mut appearance_set: Lexer<LexicalAppearanceSet>, database: &Database, variables: &mut Variables) -> AppearanceSetResult {
    let mut appearances = Vec::new();
    let mut appearance_results = Vec::new();
    while let Some(token) = appearance_set.next() {
        match token {
            LexicalAppearanceSet::Appearance => {
                let appearance_enclosure = Regex::new(r"\(|\)").unwrap();
                let appearance = appearance_enclosure.replace_all(appearance_set.slice().trim(), "");
                // println!("\tParsing appearance: {}", appearance);
                let appearance_result = parse_appearance(&appearance, database, variables);
                appearances.push(appearance_result.appearance.clone());
                appearance_results.push(appearance_result);
            },
            LexicalAppearanceSet::ItemSeparator => (),
            _ => {
                println!("Unrecognized appearance: {}", appearance_set.slice());
            }
        } 
    }
    let (kept_appearance_set, previously_known) = database.create_appearance_set(appearances);
    AppearanceSetResult {
        appearance_results: appearance_results,
        appearance_set: kept_appearance_set,
        known: previously_known
    }
}

struct AppearanceResult {
    appearance: Arc<Appearance>,
    known: bool
}
fn parse_appearance(appearance: &str, database: &Database, variables: &mut Variables) -> AppearanceResult {
    let component_regex = Regex::new(r#"([^,]+),(.+)"#).unwrap();
    let captures = component_regex.captures(appearance).unwrap();
    let qualified_thing = captures.get(1).unwrap().as_str();
    let role_name = captures.get(2).unwrap().as_str();
    let (qualifier, thing_or_variable) = if qualified_thing.parse::<usize>().is_ok() {
        ('#', qualified_thing)
    }
    else {
        let mut chars = qualified_thing.chars();
        (chars.next().unwrap(), chars.as_str())
    };
    let thing = match qualifier {
        '#' => { 
            // println!("\tNumeric value"); 
            let t = thing_or_variable.parse::<usize>().unwrap();
            database.thing_generator().lock().unwrap().retain(t);
            Some(Arc::new(t))
        },
        '+' => { 
            // println!("\tGenerate identity"); 
            let t = Arc::new(database.thing_generator().lock().unwrap().generate());
            variables.insert(thing_or_variable.to_string(), t.clone());
            Some(t)
        },
        '$' => { 
            // println!("\tFetch identity"); 
            let t = variables.get(thing_or_variable).unwrap().clone();
            Some(t)
        },
        _ => None
    };
    let role = database.role_keeper().lock().unwrap().get(role_name);
    let (kept_appearance, previously_known) = database.create_apperance(thing.unwrap(), role);
    AppearanceResult {
        appearance: kept_appearance,
        known: previously_known
    } 
}
pub struct Engine<'db> {
    database: Database<'db>, 
}
impl<'db> Engine<'db> {
    const substitute: char = 26 as char;
    pub fn new(database: Database<'db>) -> Self {
        Self {
            database
        }
    }
    pub fn execute(&self, traqula: &str) {
        let mut in_string = false;
        let mut in_comment = false;
        let mut previous_c = Engine::substitute;
        let mut oneliner = String::new();
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
            if c == '"' && previous_c == '"' && in_string {
                oneliner.pop();
                oneliner.push(Engine::substitute);
                previous_c = Engine::substitute;
            }
            else if (c == '\n' || c == '\r') && !in_string {
                if !previous_c.is_whitespace() && previous_c != ',' && previous_c != ';' { 
                    oneliner.push(' '); 
                }
                previous_c = ' ';
            }
            else if c.is_whitespace() && (previous_c.is_whitespace() || previous_c == ',' || previous_c == ';') && !in_string {
                previous_c = c;
            }
            else if !in_comment {
                oneliner.push(c);
                previous_c = c;
            }
        }
        let mut variables: Variables = Variables::new();
        //println!("Traqula:\n{}", &oneliner.trim());
        parse_command(Command::lexer(&oneliner.trim()), &self.database, &mut variables);  
    }  
}

