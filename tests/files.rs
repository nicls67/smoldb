//!
//! Databse files integration tests for `SmolDB` crate
//!

use std::{fs::remove_file, path::Path};

use rusttests::{check_option, check_value};
use smoldb::SmolDb;

/// Create a database, save it and load values from file
#[test]
fn file_ops_1() -> Result<(), String> {
    let mut db = SmolDb::init("Database test".to_string());

    db.set_database_file(Path::new("test.json"));

    // Define keys name and type and create table
    let keys = Some(vec![
        ("key1".to_string(), "String".to_string()),
        ("key2".to_string(), "Integer".to_string()),
        ("key3".to_string(), "Float".to_string()),
    ]);

    db.database().create_table(&"Table test 1".to_string(), keys)?;

    let table = db.database().table(&"Table test 1".to_string())?;

    // Add entries, 2nd has all keys set to None
    let mut values = vec![Some("hey".to_string()), None, Some("2.23".to_string())];
    table.add_entry(&"entry1".to_string(), Some(&mut values))?;
    table.add_entry(&"entry2".to_string(), None)?;

    // Save database
    db.save()?;

    let mut new_db = SmolDb::load(Path::new("test.json"))?;

    remove_file("test.json").unwrap_or(());

    check_value((1,1), new_db.database().table(&"Table test 1".to_string())?.get_entry_value_float(&"entry1".to_string(), &"key3".to_string())?.unwrap(), &2.23, rusttests::CheckType::Equal)?;

    check_option((1,2), new_db.database().table(&"Table test 1".to_string())?.get_entry_value_integer(&"entry2".to_string(), &"key2".to_string())?, false)?;

    Ok(())
}