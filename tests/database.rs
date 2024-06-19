//!
//! Database integration tests for `SmolDB` crate
//!

use rusttests::{check_option, check_result, check_value};
use smoldb::SmolDb;

/// Create a database, a table, add entries and get some values
#[test]
fn basic_ops_1() -> Result<(), String> {
    let mut db = SmolDb::init("Database test".to_string());

    check_value(
        (1, 1),
        &db.database().tables_count(),
        &0,
        rusttests::CheckType::Equal,
    )?;

    // Define keys name and type and create table
    let keys = Some(vec![
        ("key1".to_string(), "String".to_string()),
        ("key2".to_string(), "Integer".to_string()),
        ("key3".to_string(), "Float".to_string()),
    ]);
    db.database()
        .create_table(&"Table test 1".to_string(), keys)?;

    let table = db.database().table(&"Table test 1".to_string())?;

    // Add entries, 2nd has all keys set to None
    let mut values = vec![Some("hey".to_string()), None, Some("2.23".to_string())];
    table.add_entry(&"entry1".to_string(), Some(&mut values))?;
    table.add_entry(&"entry2".to_string(), None)?;

    // Get an entry value using type-specific method
    let val = check_option(
        (2, 1),
        table.get_entry_value_float(&"entry1".to_string(), &"key3".to_string())?,
        true,
    )?.unwrap();
    check_value((2,2), val, &2.23, rusttests::CheckType::Equal)?;

    // Get an entry value using String method
    let val = check_option(
        (2, 3),
        table.get_entry_value_string(&"entry1".to_string(), &"key3".to_string())?,
        true,
    )?.unwrap();
    check_value((2,4), &val, &format!("2.23"), rusttests::CheckType::Equal)?;

    // Get entry None value
    check_option((2,5), table.get_entry_value_string(&"entry2".to_string(), &"key1".to_string())?, false)?;

    // Get entries count
    check_value((2,6), &table.entries_count(), &2, rusttests::CheckType::Equal)?;

    // Update an entry with None value, any update method can be used
    table.update_entry_integer(&"entry1".to_string(), &"key3".to_string(), None)?;
    check_option((3,1), table.get_entry_value_float(&"entry1".to_string(), &"key3".to_string())?, false)?;

    // Update an entry value using type-specific method
    table.update_entry_integer(&"entry1".to_string(), &"key2".to_string(), Some(23))?;
    let val = check_option((3,2), table.get_entry_value_integer(&"entry1".to_string(), &"key2".to_string())?, true)?.unwrap();
    check_value((3,3), val, &23, rusttests::CheckType::Equal)?;

    // Update again
    table.update_entry_integer(&"entry1".to_string(), &"key2".to_string(), Some(-115))?;
    let val = check_option((3,4), table.get_entry_value_integer(&"entry1".to_string(), &"key2".to_string())?, true)?.unwrap();
    check_value((3,5), val, &-115, rusttests::CheckType::Equal)?;

    // Add a 3rd entry
    let mut values = vec![Some("what".to_string()), None, Some("-102.56".to_string())];
    table.add_entry(&"entry3".to_string(), Some(&mut values))?;

    // Get entries count
    check_value((4,1), &table.entries_count(), &3, rusttests::CheckType::Equal)?;

    // Get entries name
    check_value((4,2), &table.get_all_entries(), &Some(vec![&"entry1".to_string(), &"entry2".to_string(), &"entry3".to_string()]), rusttests::CheckType::Equal)?;

    // Remove an entry from the table
    table.remove_entry(&"entry1".to_string())?;

    // Get value from deleted entry
    check_result((5,1), table.get_entry_value_float(&"entry1".to_string(), &"key3".to_string()), false)?;

    // Get entries count
    check_value((5,2), &table.entries_count(), &2, rusttests::CheckType::Equal)?;

    // Rename entry
    table.rename_entry(&"entry2".to_string(), &"new_entry_name".to_string())?;

    // Get entry None value
    check_option((6,1), table.get_entry_value_string(&"new_entry_name".to_string(), &"key1".to_string())?, false)?;

    // Get table count
    if db.database().tables_count() != 1 {
        return Err(format!("Tables count should be 1"));
    }
    check_value((7,1), &db.database().tables_count(), &1, rusttests::CheckType::Equal)?;

    Ok(())
}

/// Error cases
#[test]
fn error_ops_1() -> Result<(), String> {
    let mut db = SmolDb::init("Database test".to_string());

    // Unknown table
    check_result((1,1), db.database().table(&"Table test 1".to_string()), false)?;

    // Define keys name and type and create table
    let keys = Some(vec![
        ("key1".to_string(), "String".to_string()),
        ("key2".to_string(), "Integer".to_string()),
        ("key3".to_string(), "Float".to_string()),
    ]);
    db.database()
        .create_table(&"Table test 1".to_string(), keys)?;

    let table = db.database().table(&"Table test 1".to_string())?;

    // Add entries, 2nd has all keys set to None
    let mut values = vec![Some("hey".to_string()), None, Some("2.23".to_string())];
    table.add_entry(&"entry1".to_string(), Some(&mut values))?;
    table.add_entry(&"entry2".to_string(), None)?;

    // Unknown entry
    check_result((2,1), table.get_entry_value_float(&"entryX".to_string(), &"key3".to_string()), false)?;

    // Correct entry but unknown key
    check_result((2,2), table.get_entry_value_float(&"entry1".to_string(), &"key4".to_string()), false)?;

    // Correct entry and key but wrong type
    check_result((2,3), table.get_entry_value_float(&"entry1".to_string(), &"key2".to_string()), false)?;

    Ok(())
}
