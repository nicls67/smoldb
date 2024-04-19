//!
//! Database Model definition
//!

mod db_table;
mod db_entry;
mod db_type;

pub use db_table::DbTable;


/// Database model
pub struct DbModel {
    name: String,
    version: [u8; 3],
}