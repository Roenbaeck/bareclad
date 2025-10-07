
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BarecladError {
    #[error("Config error: {0}")] 
    Config(String),
    #[error("Persistence error: {0}")] 
    Persistence(String),
    #[error("Data corruption: {message}")] 
    DataCorruption { message: String },
    #[error("Parse error: {message}")] 
    Parse { message: String, line: Option<usize>, col: Option<usize> },
    #[error("Execution error: {0}")] 
    Execution(String),
    #[error("Internal invariant violated: {0}")] 
    Invariant(String),
    #[error("Lock poisoned: {0}")] 
    Lock(String),
}

pub type Result<T> = std::result::Result<T, BarecladError>;

// Helper conversions
impl From<rusqlite::Error> for BarecladError {
    fn from(e: rusqlite::Error) -> Self { Self::Persistence(e.to_string()) }
}
