// used for persistence
use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, ValueRef};

// used for timestamps in the database
use chrono::{NaiveDateTime, NaiveDate, Utc, Datelike};
// used for decimal numbers
use bigdecimal::BigDecimal;
// used for JSON
use jsondata::Json;

// used when parsing a string to a DateTime<Utc>
use std::str::FromStr;
// used to print out readable forms of a data type
use std::fmt;
// used to indicate that data types need to be hashable
use std::hash::{Hash, Hasher};
// used to overload common operations for datatypes
use std::ops;
// used when implementing PartialOrd for Time
use std::cmp::Ordering;

use crate::traqula::parse_time;

pub trait DataType: fmt::Display + Eq + Hash + Send + Sync + ToSql  {
    // static stuff which needs to be implemented downstream
    const UID: u8;
    const DATA_TYPE: &'static str;
    fn convert(value: &ValueRef) -> Self;
    // instance callable with pre-made implementation
    fn data_type(&self) -> &'static str {
        Self::DATA_TYPE
    }
    fn identifier(&self) -> u8 {
        Self::UID
    }
}

// ------------- Data Types --------------
impl DataType for Certainty {
    const UID: u8 = 1; 
    const DATA_TYPE: &'static str = "Certainty";
    fn convert(value: &ValueRef) -> Certainty {
        Certainty {
            alpha: i8::try_from(value.as_i64().unwrap()).unwrap(),
        }
    }
}
impl DataType for String {
    const UID: u8 = 2;
    const DATA_TYPE: &'static str = "String";
    fn convert(value: &ValueRef) -> String {
        String::from(value.as_str().unwrap())
    }
}
impl DataType for NaiveDateTime {
    const UID: u8 = 3;
    const DATA_TYPE: &'static str = "NaiveDateTime";
    fn convert(value: &ValueRef) -> NaiveDateTime {
        NaiveDateTime::from_str(value.as_str().unwrap()).unwrap()
    }
}
impl DataType for NaiveDate {
    const UID: u8 = 4;
    const DATA_TYPE: &'static str = "NaiveDate";
    fn convert(value: &ValueRef) -> NaiveDate {
        NaiveDate::from_str(value.as_str().unwrap()).unwrap()
    }
}
impl DataType for i64 {
    const UID: u8 = 5;
    const DATA_TYPE: &'static str = "i64";
    fn convert(value: &ValueRef) -> i64 {
        value.as_i64().unwrap()
    }
}
impl DataType for Decimal {
    const UID: u8 = 6;
    const DATA_TYPE: &'static str = "Decimal";
    fn convert(value: &ValueRef) -> Decimal {
        Decimal (BigDecimal::from_str(value.as_str().unwrap()).unwrap())
    }
}
impl DataType for JSON {
    const UID: u8 = 7;
    const DATA_TYPE: &'static str = "JSON";
    fn convert(value: &ValueRef) -> JSON {
        JSON (Json::from_str(value.as_str().unwrap()).unwrap())
    }
}
impl DataType for Time {
    const UID: u8 = 8;
    const DATA_TYPE: &'static str = "Time";
    fn convert(value: &ValueRef) -> Time {
        parse_time(value.as_str().unwrap()).unwrap()
    }   
}

// Special types below
#[derive(Eq, PartialEq, PartialOrd, Ord, Clone)]
pub struct JSON (Json);

impl JSON {
    pub fn from_str(s: &str) -> Option<JSON> {
        match Json::from_str(s) {
            Ok(json) => Some(JSON (json)),
            _ => None
        }
    }
}
impl ToSql for JSON {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.0.to_string()))
    }
}
impl FromSql for JSON {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        rusqlite::Result::Ok(JSON (Json::from_str(value.as_str().unwrap()).unwrap()))
    }
}
impl Hash for JSON {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_string().hash(state);
    }
}
impl fmt::Display for JSON {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl ops::Deref for JSON {
    type Target = Json;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
/* 
// all our values are immutable
impl ops::DerefMut for JSON {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
*/


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
    pub fn new<T: Into<f64>>(a: T) -> Self {
        let a = a.into();
        let a = if a < -1. {
            -1.
        } else if a > 1. {
            1.
        } else {
            a
        };
        Self {
            alpha: (100f64 * a) as i8,
        }
    }
    pub fn consistent(rs: &[Certainty]) -> bool {
        let r_total = rs
            .iter()
            .map(|r: &Certainty| r.alpha as i32)
            .filter(|i| *i != 0)
            .fold(0, |sum, i| sum + 100 * (1 - i.signum()))
            / 2
            + rs.iter()
                .map(|r: &Certainty| r.alpha as i32)
                .filter(|i| *i != 0)
                .sum::<i32>();

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
            -100 => write!(f, "-1"),
            -99..=-1 => write!(f, "-0.{}", -self.alpha),
            0 => write!(f, "0"),
            1..=99 => write!(f, "0.{}", self.alpha),
            100 => write!(f, "1"),
            _ => write!(f, "?"),
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
impl ToSql for Certainty {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.alpha))
    }
}
impl FromSql for Certainty {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        rusqlite::Result::Ok(Certainty {
            alpha: i8::try_from(value.as_i64().unwrap()).ok().unwrap(),
        })
    }
}

#[derive(Eq, PartialEq, Hash, PartialOrd, Ord, Clone)]
pub struct Decimal (BigDecimal);

impl Decimal {
    pub fn from_str(s: &str) -> Option<Decimal> {
        match BigDecimal::from_str(s) {
            Ok(decimal) => Some(Decimal (decimal)),
            _ => None
        }
    }
}
impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl FromSql for Decimal {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        rusqlite::Result::Ok(Decimal (BigDecimal::from_str(value.as_str().unwrap()).unwrap()))
    }
}
impl ToSql for Decimal {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.0.to_string()))
    }
}
impl ops::Deref for Decimal {
    type Target = BigDecimal;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl ops::DerefMut for Decimal {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// TODO: We will use a specialized time type instead of the 
// trait constrained generic
#[derive(Eq, PartialEq, Ord, Debug, Hash, Clone)]
pub enum TimeType {
    // abstract time points
    BeginningOfTime,
    EndOfTime,
    // concrete time points
    Year(i32),
    YearMonth(i32,u8),
    Date(NaiveDate), 
    DateTime(NaiveDateTime)
}

impl PartialOrd for TimeType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // abstact type combinations
            (TimeType::BeginningOfTime, TimeType::BeginningOfTime) | (TimeType::EndOfTime, TimeType::EndOfTime) => Some(Ordering::Equal),
            (TimeType::BeginningOfTime, _) | (_, TimeType::EndOfTime) => Some(Ordering::Less),
            (_, TimeType::BeginningOfTime) | (TimeType::EndOfTime, _) => Some(Ordering::Greater),
            // concrete type combinations
            (TimeType::Year(y_self), type_other) => {
                match type_other {
                    TimeType::Year(y) => y_self.partial_cmp(y),
                    TimeType::YearMonth(y, _) => y_self.partial_cmp(y),
                    TimeType::Date(d) => y_self.partial_cmp(&d.year()),
                    TimeType::DateTime(d) => y_self.partial_cmp(&d.year()),
                    _ => None
                }
            },
            (TimeType::YearMonth(y_self, m_self), type_other) => {
                match type_other {
                    TimeType::Year(y) => y_self.partial_cmp(y),
                    TimeType::YearMonth(y, m) => {
                        match y_self.partial_cmp(y) {
                            Some(Ordering::Equal) => m_self.partial_cmp(m),
                            _ => y_self.partial_cmp(y)
                        }
                    },
                    TimeType::Date(d) => {
                        match y_self.partial_cmp(&d.year()) {
                            Some(Ordering::Equal) => m_self.partial_cmp(&(d.month() as u8)),
                            _ => y_self.partial_cmp(&d.year())
                        }
                    },
                    TimeType::DateTime(d) => {
                        match y_self.partial_cmp(&d.year()) {
                            Some(Ordering::Equal) => m_self.partial_cmp(&(d.month() as u8)),
                            _ => y_self.partial_cmp(&d.year())
                        }
                    },
                    _ => None
                }
            }, 
            (TimeType::Date(d_self), type_other) => {
                match type_other {
                    TimeType::Year(y) => d_self.year().partial_cmp(y),
                    TimeType::YearMonth(y, m) => {
                        match d_self.year().partial_cmp(y) {
                            Some(Ordering::Equal) => (d_self.month() as u8).partial_cmp(m),
                            _ => d_self.year().partial_cmp(y)
                        }
                    },
                    TimeType::Date(d) => d_self.partial_cmp(d),
                    TimeType::DateTime(d) => d_self.partial_cmp(&d.date()),
                    _ => None
                }
            },
            (TimeType::DateTime(d_self), type_other) => {
                match type_other {
                    TimeType::Year(y) => d_self.year().partial_cmp(y),
                    TimeType::YearMonth(y, m) => {
                        match d_self.year().partial_cmp(y) {
                            Some(Ordering::Equal) => (d_self.month() as u8).partial_cmp(m),
                            _ => d_self.year().partial_cmp(y)
                        }
                    },
                    TimeType::Date(d) => d_self.date().partial_cmp(d),
                    TimeType::DateTime(d) => d_self.partial_cmp(d),
                    _ => None
                }
            },
        } 
    }
}


#[derive(Eq, PartialEq, PartialOrd, Ord, Debug, Hash, Clone)]
pub struct Time {
    moment: TimeType
}
impl Time {
    pub fn new() -> Time {
        Time { moment: TimeType::DateTime(Utc::now().naive_utc()) }
    }
    pub fn new_beginning_of_time() -> Time {
        Time { moment: TimeType::BeginningOfTime }
    }
    pub fn new_end_of_time() -> Time {
        Time { moment: TimeType::EndOfTime }
    }
    // TODO: may panic
    pub fn new_year_from(d: &str) -> Time {
        Time { moment: TimeType::Year(d.parse::<i32>().unwrap()) }
    }
    pub fn new_year_month_from(d: &str) -> Time {
        let mut year = String::new();
        let mut month = String::new();
        for c in d.chars() {
            if c != '-' {
                if year.len() < 4 {
                    year.push(c);
                }
                else {
                    month.push(c);
                }
            }
        }
        Time { moment: TimeType::YearMonth(year.parse::<i32>().unwrap(), month.parse::<u8>().unwrap()) }
    }
    pub fn new_date_from(d: &str) -> Time {
        Time { moment: TimeType::Date(NaiveDate::from_str(d).unwrap()) } 
    }
    pub fn new_datetime_from(d: &str) -> Time {
        Time { moment: TimeType::DateTime(NaiveDateTime::from_str(d).unwrap()) } 
    }
}
impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.moment {
            TimeType::BeginningOfTime => {
                write!(f, "BOT")
            }
            TimeType::EndOfTime => {
                write!(f, "EOT")
            }
            TimeType::Year(y) => {
                write!(f, "{}", y)
            }
            TimeType::YearMonth(y, m) => {
                write!(f, "{}-{}", y, m)
            }
            TimeType::Date(d) => {
                write!(f, "{}", d)
            }
            TimeType::DateTime(d) => {
                write!(f, "{}", d)
            }
        }
    }
}
impl ToSql for Time {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}
