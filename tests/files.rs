//!
//! Database files integration tests for `SmolDB` crate
//!

use std::fs::remove_file;
use std::path::PathBuf;

use rusttests::{check_option, check_value};

use smoldb::SmolDb;

/// Create a database, save it and load values from file
#[test]
fn file_ops_1() -> Result<(), String> {
    let mut l_db = SmolDb::init("Database test");

    l_db.set_database_file(PathBuf::from("test.json"));

    // Define keys name and type and create table
    let l_keys = Some(vec![
        ("key1".to_string(), "String".to_string()),
        ("key2".to_string(), "Integer".to_string()),
        ("key3".to_string(), "Float".to_string()),
    ]);

    l_db.database().create_table("Table test 1", l_keys)?;

    let l_table = l_db.database().table(&"Table test 1".to_string())?;

    // Add entries, 2nd has all keys set to None
    let mut l_values = vec![Some("hey".to_string()), None, Some("2.23".to_string())];
    l_table.add_entry(&"entry1".to_string(), Some(&mut l_values))?;
    l_table.add_entry(&"entry2".to_string(), None)?;

    // Save database
    l_db.save()?;

    let mut l_new_db = SmolDb::load(PathBuf::from("test.json"))?;

    remove_file("test.json").unwrap_or(());

    check_value(
        (1, 1),
        l_new_db
            .database()
            .table(&"Table test 1".to_string())?
            .get_entry_value_float(&"entry1".to_string(), &"key3".to_string())?
            .unwrap(),
        &2.23,
        rusttests::CheckType::Equal,
    )?;

    check_option(
        (1, 2),
        l_new_db
            .database()
            .table(&"Table test 1".to_string())?
            .get_entry_value_integer(&"entry2".to_string(), &"key2".to_string())?,
        false,
    )?;

    Ok(())
}
