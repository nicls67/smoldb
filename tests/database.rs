//!
//! Database integration tests for `SmolDB` crate
//!

use smoldb::SmolDb;

/// Create a database, a table, add entries and get some values
#[test]
fn basic_ops_1() -> Result<(), String> {
    let mut db = SmolDb::init("Database test".to_string());

    // Get table count
    if db.get_database().tables_count() != 0 {
        return Err(format!("Tables count should be 0"));
    }

    // Define keys name and type and create table
    let keys = Some(vec![
        ("key1".to_string(), "String".to_string()),
        ("key2".to_string(), "Integer".to_string()),
        ("key3".to_string(), "Float".to_string()),
    ]);
    db.get_database().create_table(&"Table test 1".to_string(), keys)?;

    let table = db.get_database().table(&"Table test 1".to_string())?;

    // Add entries, 2nd has all keys set to None
    let mut values = vec![Some("hey".to_string()), None, Some("2.23".to_string())];
    table.add_entry(&"entry1".to_string(), Some(&mut values))?;
    table.add_entry(&"entry2".to_string(), None)?;

    // Get an entry value using type-specific method
    match table.get_entry_value_float(&"entry1".to_string(), &"key3".to_string())? {
        Some(val) => {
            if *val != 2.23 {
                return Err(format!("Value should be 2.23"))
            }
        },
        None => return Err(format!("Value should be Some")),
    };

    // Get an entry value using String method
    match table.get_entry_value_string(&"entry1".to_string(), &"key3".to_string())? {
        Some(val) => {
            if val != "2.23" {
                return Err(format!("Value should be 2.23"))
            }
        },
        None => return Err(format!("Value should be Some")),
    };

    // Get entry None value
    match table.get_entry_value_string(&"entry2".to_string(), &"key1".to_string())? {
        Some(_) => return Err(format!("Entry value should be None")),
        None => (),
    }

    // Get entries count
    if table.entries_count() != 2 {
        return Err(format!("Entries count should be 2"));
    }

    // Update an entry with None value, any update method can be used
    table.update_entry_integer(&"entry1".to_string(), &"key3".to_string(), None)?;
    match table.get_entry_value_float(&"entry1".to_string(), &"key3".to_string())? {
        Some(_) => return Err(format!("Entry value should be None")),
        None => (),
    }

    // Update an entry value using type-specific method
    table.update_entry_integer(&"entry1".to_string(), &"key2".to_string(), Some(23))?;
    match table.get_entry_value_integer(&"entry1".to_string(), &"key2".to_string())? {
        Some(val) => {
            if *val != 23 {
                return Err(format!("Value should be 23"))
            }
        },
        None => return Err(format!("Value should be Some")),
    };

    // Update again
    table.update_entry_integer(&"entry1".to_string(), &"key2".to_string(), Some(-115))?;
    match table.get_entry_value_integer(&"entry1".to_string(), &"key2".to_string())? {
        Some(val) => {
            if *val != -115 {
                return Err(format!("Value should be 23"))
            }
        },
        None => return Err(format!("Value should be Some")),
    };

    // Remove an entry from the table
    table.remove_entry(&"entry1".to_string())?;

    // Get value from deleted entry
    match table.get_entry_value_float(&"entry1".to_string(), &"key3".to_string()) {
        Ok(_) => return Err(format!("Value should be Err")),
        Err(_) => (),
    }

    // Get entries count
    if table.entries_count() != 1 {
        return Err(format!("Entries count should be 1"));
    }

    // Get table count
    if db.get_database().tables_count() != 1 {
        return Err(format!("Tables count should be 1"));
    }

    Ok(())
}

/// Error cases
#[test]
fn error_ops_1() -> Result<(), String> {
    let mut db = SmolDb::init("Database test".to_string());

    // Unknown table
    match db.get_database().table(&"Table test 1".to_string()) {
        Ok(_) => return Err(format!("Result should be Err")),
        Err(_) => (),
    }

    // Define keys name and type and create table
    let keys = Some(vec![
        ("key1".to_string(), "String".to_string()),
        ("key2".to_string(), "Integer".to_string()),
        ("key3".to_string(), "Float".to_string()),
    ]);
    db.get_database().create_table(&"Table test 1".to_string(), keys)?;

    let table = db.get_database().table(&"Table test 1".to_string())?;

    // Add entries, 2nd has all keys set to None
    let mut values = vec![Some("hey".to_string()), None, Some("2.23".to_string())];
    table.add_entry(&"entry1".to_string(), Some(&mut values))?;
    table.add_entry(&"entry2".to_string(), None)?;

    // Unknown entry
    match table.get_entry_value_float(&"entryX".to_string(), &"key3".to_string()) {
        Ok(_) => return Err(format!("Result should be Err")),
        Err(_) => (),
    }

    // Correct entry but unknown key
    match table.get_entry_value_float(&"entry1".to_string(), &"key4".to_string()) {
        Ok(_) => return Err(format!("Result should be Err")),
        Err(_) => (),
    }

    // Correct entry and key but wrong type
    match table.get_entry_value_float(&"entry1".to_string(), &"key2".to_string()) {
        Ok(_) => return Err(format!("Result should be Err")),
        Err(_) => (),
    }

    Ok(())
}
