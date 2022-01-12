// used for persistence
use rusqlite::types::{FromSql, FromSqlResult, ToSql, ToSqlOutput, Value, ValueRef};

// used for timestamps in the database
use chrono::{DateTime, Utc, NaiveDate};
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

pub trait DataType: fmt::Display + Eq + Hash + Send + Sync + ToSql + FromSql {
    // static stuff which needs to be implemented downstream
    type TargetType;
    const UID: u8;
    const DATA_TYPE: &'static str;
    fn convert(value: &ValueRef) -> Self::TargetType;
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
    type TargetType = Certainty;
    const UID: u8 = 1; // needs to be unique
    const DATA_TYPE: &'static str = "Certainty";
    fn convert(value: &ValueRef) -> Self::TargetType {
        Certainty {
            alpha: i8::try_from(value.as_i64().unwrap()).unwrap(),
        }
    }
}
impl DataType for String {
    type TargetType = String;
    const UID: u8 = 2;
    const DATA_TYPE: &'static str = "String";
    fn convert(value: &ValueRef) -> Self::TargetType {
        String::from(value.as_str().unwrap())
    }
}
impl DataType for DateTime<Utc> {
    type TargetType = DateTime<Utc>;
    const UID: u8 = 3;
    const DATA_TYPE: &'static str = "DateTime::<Utc>";
    fn convert(value: &ValueRef) -> Self::TargetType {
        DateTime::<Utc>::from_str(value.as_str().unwrap()).unwrap()
    }
}
impl DataType for NaiveDate {
    type TargetType = NaiveDate;
    const UID: u8 = 4;
    const DATA_TYPE: &'static str = "NaiveDate";
    fn convert(value: &ValueRef) -> Self::TargetType {
        NaiveDate::from_str(value.as_str().unwrap()).unwrap()
    }
}
impl DataType for i64 {
    type TargetType = i64;
    const UID: u8 = 5;
    const DATA_TYPE: &'static str = "i64";
    fn convert(value: &ValueRef) -> Self::TargetType {
        value.as_i64().unwrap()
    }
}
impl DataType for Decimal {
    type TargetType = Decimal;
    const UID: u8 = 6;
    const DATA_TYPE: &'static str = "Decimal";
    fn convert(value: &ValueRef) -> Self::TargetType {
        Decimal (BigDecimal::from_str(value.as_str().unwrap()).unwrap())
    }
}
impl DataType for JSON {
    type TargetType = JSON;
    const UID: u8 = 7;
    const DATA_TYPE: &'static str = "JSON";
    fn convert(value: &ValueRef) -> Self::TargetType {
        JSON (Json::from_str(value.as_str().unwrap()).unwrap())
    }
}

// Special types below
#[derive(Eq, PartialEq, PartialOrd, Ord)]
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
        let v = Value::Text(self.0.to_string());
        let output = ToSqlOutput::Owned(v);
        Ok(output)
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

#[derive(Eq, PartialEq, Hash, PartialOrd, Ord)]
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
        let v = Value::Text(self.0.to_string());
        let output = ToSqlOutput::Owned(v);
        Ok(output)
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
