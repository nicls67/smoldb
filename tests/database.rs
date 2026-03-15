//!
//! Database integration tests for `SmolDB` crate
//!

use chrono::NaiveDate;
use rusttests::{check_option, check_result, check_value, CheckType};

use smoldb::{MatchingCriteria, SmolDb};

/// Create a database, a table, add entries and get some values
#[test]
fn basic_ops_1() -> Result<(), String> {
    let mut l_db = SmolDb::init("Database test");

    check_value(
        (1, 1),
        &l_db.database().tables_count(),
        &0,
        rusttests::CheckType::Equal,
    )?;

    // Define keys name and type and create table
    let l_keys = Some(vec![
        ("key1".to_string(), "String".to_string()),
        ("key2".to_string(), "Integer".to_string()),
        ("key3".to_string(), "Float".to_string()),
        ("key4".to_string(), "Date".to_string()),
    ]);
    l_db.database().create_table("Table test 1", l_keys)?;

    let l_table = l_db.database().table(&"Table test 1".to_string())?;

    // Get unique integer values for key2 on empty table
    check_option(
        (1, 2),
        l_table.get_unique_integer_values_for_key(None, &"key2".to_string())?,
        false,
    )?;

    // Add entries, 2nd has all keys set to None
    let mut l_values = vec![
        Some("hey".to_string()),
        None,
        Some("2.23".to_string()),
        Some("30/04/2020".to_string()),
    ];
    l_table.add_entry(&"entry1".to_string(), Some(&mut l_values))?;
    l_table.add_entry(&"entry2".to_string(), None)?;

    // Get an entry value using type-specific method
    let l_val = check_option(
        (2, 1),
        l_table.get_entry_value_float(&"entry1".to_string(), &"key3".to_string())?,
        true,
    )?
    .unwrap();
    check_value((2, 2), l_val, &2.23, rusttests::CheckType::Equal)?;

    // Get an entry value using String method
    let l_val = check_option(
        (2, 3),
        l_table.get_entry_value_string(&"entry1".to_string(), &"key1".to_string())?,
        true,
    )?
    .unwrap();
    check_value(
        (2, 4),
        &l_val,
        &"hey".to_string(),
        rusttests::CheckType::Equal,
    )?;

    // Get entry None value
    check_option(
        (2, 5),
        l_table.get_entry_value_string(&"entry2".to_string(), &"key1".to_string())?,
        false,
    )?;

    // Get entries count
    check_value(
        (2, 6),
        &l_table.entries_count(),
        &2,
        rusttests::CheckType::Equal,
    )?;

    // Update an entry with None value, any update method can be used
    l_table.update_entry_integer(&"entry1".to_string(), &"key3".to_string(), None)?;
    check_option(
        (3, 1),
        l_table.get_entry_value_float(&"entry1".to_string(), &"key3".to_string())?,
        false,
    )?;

    // Update an entry value using type-specific method
    l_table.update_entry_integer(&"entry1".to_string(), &"key2".to_string(), Some(23))?;
    let l_val = check_option(
        (3, 2),
        l_table.get_entry_value_integer(&"entry1".to_string(), &"key2".to_string())?,
        true,
    )?
    .unwrap();
    check_value((3, 3), l_val, &23, rusttests::CheckType::Equal)?;

    // Update again
    l_table.update_entry_integer(&"entry1".to_string(), &"key2".to_string(), Some(-115))?;
    let l_val = check_option(
        (3, 4),
        l_table.get_entry_value_integer(&"entry1".to_string(), &"key2".to_string())?,
        true,
    )?
    .unwrap();
    check_value((3, 5), l_val, &-115, rusttests::CheckType::Equal)?;

    // Get unique integer values for key2
    let l_unique_vals = check_option(
        (3, 6),
        l_table.get_unique_integer_values_for_key(None, &"key2".to_string())?,
        true,
    )?
    .unwrap();
    check_value(
        (3, 7),
        &l_unique_vals,
        &vec![-115],
        rusttests::CheckType::Equal,
    )?;

    // Add a 3rd entry
    let mut l_values = vec![
        Some("what".to_string()),
        None,
        Some("-102.56".to_string()),
        Some("14/07/2022".to_string()),
    ];
    l_table.add_entry(&"entry3".to_string(), Some(&mut l_values))?;

    // Get entries count
    check_value(
        (4, 1),
        &l_table.entries_count(),
        &3,
        rusttests::CheckType::Equal,
    )?;

    // Get entries name
    check_value(
        (4, 2),
        &l_table.get_all_entries(),
        &Some(vec![
            "entry1".to_string(),
            "entry2".to_string(),
            "entry3".to_string(),
        ]),
        rusttests::CheckType::Equal,
    )?;

    // Get entries with key4 value equal to 30/04/2020
    check_value(
        (4, 3),
        &l_table
            .get_matching_entries_date(
                None,
                &"key4".to_string(),
                MatchingCriteria::Equal,
                NaiveDate::from_ymd_opt(2020, 4, 30).unwrap(),
                None,
            )
            .unwrap()
            .unwrap(),
        &vec!["entry1".to_string()],
        CheckType::Equal,
    )?;

    // Remove an entry from the table
    l_table.remove_entry(&"entry1".to_string())?;

    // Get value from deleted entry
    check_result(
        (5, 1),
        l_table.get_entry_value_float(&"entry1".to_string(), &"key3".to_string()),
        false,
    )?;

    // Get entries count
    check_value(
        (5, 2),
        &l_table.entries_count(),
        &2,
        rusttests::CheckType::Equal,
    )?;

    // Rename entry
    l_table.rename_entry(&"entry2".to_string(), "new_entry_name")?;

    // Get entry None value
    check_option(
        (6, 1),
        l_table.get_entry_value_string(&"new_entry_name".to_string(), &"key1".to_string())?,
        false,
    )?;

    // Get table count
    if l_db.database().tables_count() != 1 {
        return Err("Tables count should be 1".to_string());
    }
    check_value(
        (7, 1),
        &l_db.database().tables_count(),
        &1,
        rusttests::CheckType::Equal,
    )?;

    Ok(())
}

/// Error cases
#[test]
fn error_ops_1() -> Result<(), String> {
    let mut l_db = SmolDb::init("Database test");

    // Unknown table
    check_result(
        (1, 1),
        l_db.database().table(&"Table test 1".to_string()),
        false,
    )?;

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

    // Unknown entry
    check_result(
        (2, 1),
        l_table.get_entry_value_float(&"entryX".to_string(), &"key3".to_string()),
        false,
    )?;

    // Correct entry but unknown key
    check_result(
        (2, 2),
        l_table.get_entry_value_float(&"entry1".to_string(), &"key4".to_string()),
        false,
    )?;

    // Correct entry and key but wrong type
    check_result(
        (2, 3),
        l_table.get_entry_value_float(&"entry1".to_string(), &"key2".to_string()),
        false,
    )?;

    Ok(())
}
