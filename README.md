# SmolDB

A small database library written in Rust

## Principle

This library is meant to be used inside a Rust binary application. Database is stored inside a JSON file (the extension
can be anything else than `.json`).

**SmolDB** only requires a few accesses to disk as database file is loaded once during initialization, and then all
requests are handled from RAM (read and write accesses). The database file on disk is updated when `save` method is
called.

A database contains one or many tables, each tables containing one or many database fields. Tables and fields name (
keys) are defined during initialization process. Each field has a designated type among the followings :

* **Integer**
* **Unsigned Integer**
* **Float**
* **Boolean**
* **String**
* **Date**

A database entry is linked to a table and has its fields filled with a value. It is possible to leave a field empty.

It is possible to add tables and/or keys after initialization, when a key is added to a table, all entries inside the
table will have a `None` value for the new key.

To get or update an entry value, either call the method corresponding to the data type, like `update_entry_integer`
or `get_entry_value_float`, or call `update_entry_string` or `get_entry_value_string` to use strings instead. Please
note that `update_entry_string` will return `Err` if the passed string doesn't match the data type.

To get names of all entries inside a table, use `get_all_entries` method.

## Usage

### Database creation, saving and loading

When an empty database is created, no JSON file is associated, it must be configured manually using `set_database_file`
before any save attempt.

When an existing database is loaded, the JSON file is linked to the database, thus any `save` call will write to the
original file. The linked file can be updated using `set_database_file`.

```rust
use smoldb::SmolDb;
use std::path::PathBuf;
use std::fs::remove_file;

// Create new empty database
let mut db = SmolDb::init("Database name");

// Configure database file
db.set_database_file(PathBuf::from("file.json"));

// Save database to file
db.save().unwrap();

// Load existing database
let new_db = SmolDb::load(PathBuf::from("file.json")).unwrap();

// Delete base
remove_file("file.json").unwrap_or(());
```

### Table management

```rust
use smoldb::SmolDb;
let mut db = SmolDb::init("Database name");

// Define keys name and type and create table
let keys = Some(vec![
    ("key1".to_string(), "String".to_string()),
    ("key2".to_string(), "Integer".to_string()),
]);
db.database().create_table( & "Table name".to_string(), keys).unwrap();

// Keys can also be left empty
db.database().create_table( & "Table name 2".to_string(), None).unwrap();

// Delete the table
db.database().delete_table( & "Table name".to_string()).unwrap();
```

### Basic table usage

```rust
use smoldb::SmolDb;

// Create database and table
let mut db = SmolDb::init("Database name");
let keys = Some(vec![
    ("key1".to_string(), "String".to_string()),
    ("key2".to_string(), "Integer".to_string()),
    ("key3".to_string(), "Float".to_string()),
]);
db.database().create_table( & "Table name".to_string(), keys).unwrap();

// Get table reference
let mut table = db.database().table( & "Table name".to_string()).unwrap();

// Add a new entry to the table with values
// Values for the new entry must be provided as String, conversion will be done by the database
// Any value can be set to None
let mut values = vec![Some("hey".to_string()), None, Some("2.23".to_string())];
table.add_entry( & "entry1".to_string(), Some( & mut values)).unwrap();

// Add another entry with all values set to None
table.add_entry( & "entry2".to_string(), None).unwrap();

// Update an entry value using string
table.update_entry_string( & "entry1".to_string(), & "key2".to_string(), Some("12".to_string()));
// Update an entry value using type-specific method
table.update_entry_float( & "entry1".to_string(), & "key3".to_string(), Some(1.23));
// Update an entry with None value, any update method can be used
table.update_entry_integer( & "entry1".to_string(), & "key3".to_string(), None);

// Get an entry value using string
let value: Option<String> = table.get_entry_value_string( & "entry1".to_string(), & "key2".to_string()).unwrap();
// Get an entry value using type-specific method
let value: Option< & f32> = table.get_entry_value_float( & "entry1".to_string(), & "key3".to_string()).unwrap();

// Get entries count inside a table
let count = table.entries_count();

// Get vector with all entries names
let entries: Option<Vec<String> > = table.get_all_entries();

// Add a new key to a table ()existing entries will get a None value for this key)
table.add_key( & "new_key".to_string(), & "Integer".to_string()).unwrap();

// Remove an entry from the table
table.remove_entry( & "entry1".to_string()).unwrap();

// Rename an entry
table.rename_entry( & "entry2".to_string(), & "new_name".to_string()).unwrap();
```

### Find entries by key values

It is possible to find the entries corresponding to a specific key value. The following comparison criteria are
available :

* **More**: value is higher than the reference (or **after** for date, not available for String and Boolean types)
* **Less**: value is lower than the reference (or **before** for date, not available for String and Boolean types)
* **Equal**: value is equal than the reference
* **Different**: value is different of the reference
* **Between**: value is between the 1st and the 2nd references (not available for String and Boolean types)

```rust
use smoldb::SmolDb;
use chrono::NaiveDate;
use smoldb::MatchingCriteria;

// Create database and table
let mut db = SmolDb::init("Database name");
let keys = Some(vec![
    ("key1".to_string(), "Date".to_string()),
    ("key2".to_string(), "String".to_string()),
    ("key3".to_string(), "Float".to_string()),
]);
db.database().create_table( & "Table name".to_string(), keys).unwrap();

// Get table reference
let mut table = db.database().table( & "Table name".to_string()).unwrap();

let mut binding = vec![Some("13/03/2014".to_string()), Some("toto".to_string()), Some("2.23".to_string())];
let mut binding2 = vec![Some("14/03/2014".to_string()), Some("tata".to_string()), Some("1.46".to_string())];
let mut binding3 = vec![Some("13/08/2024".to_string()), Some("toto".to_string()), Some("-0.27".to_string())];
let new_entry = Some( & mut binding);
let new_entry2 = Some( & mut binding2);
let new_entry3 = Some( & mut binding3);


table.add_entry( & "entry1".to_string(), new_entry);
table.add_entry( & "entry2".to_string(), new_entry2);
table.add_entry( & "entry3".to_string(), new_entry3);

// Find all entries with date equal to 13/03/2014
let matching_entries = table.get_matching_entries_date(None, & "key1".to_string(), MatchingCriteria::Equal, NaiveDate::from_ymd_opt(2014, 3, 13).unwrap(), None);

// Find all entries with string equal to "toto"
let matching_entries = table.get_matching_entries_string(None, & "key2".to_string(), MatchingCriteria::Equal, & "toto".to_string());

// Find all entries with values between 1.46 and 2.23
let matching_entries = table.get_matching_entries_float(None, & "key3".to_string(), MatchingCriteria::Between, 1.46, Some(2.23));

// Find all entries with None
let none_entries = table.get_entries_none(None, & "key2".to_string());
```

### Get list of unique key values

For a given key it is possible to get the list of unique existing values

```rust
use smoldb::SmolDb;

// Create database and table
let mut db = SmolDb::init("Database name");
let keys = Some(vec![
    ("key1".to_string(), "Date".to_string()),
    ("key2".to_string(), "String".to_string()),
    ("key3".to_string(), "Float".to_string()),
]);
db.database().create_table( & "Table name".to_string(), keys).unwrap();

// Get table reference
let mut table = db.database().table( & "Table name".to_string()).unwrap();

let mut binding = vec![Some("13/03/2014".to_string()), Some("toto".to_string()), Some("2.23".to_string())];
let mut binding2 = vec![Some("14/03/2014".to_string()), Some("tata".to_string()), Some("1.46".to_string())];
let mut binding3 = vec![Some("13/08/2024".to_string()), Some("toto".to_string()), Some("-0.27".to_string())];
let new_entry = Some( & mut binding);
let new_entry2 = Some( & mut binding2);
let new_entry3 = Some( & mut binding3);


table.add_entry( & "entry1".to_string(), new_entry);
table.add_entry( & "entry2".to_string(), new_entry2);
table.add_entry( & "entry3".to_string(), new_entry3);

let float_values = table.get_unique_float_values_for_key(None, & "key3".to_string()).unwrap();
assert_eq!(float_values, Some(vec![2.23, 1.46, -0.27]));
let string_values = table.get_unique_string_values_for_key(None, & "key2".to_string()).unwrap();
assert_eq!(string_values, Some(vec!["toto".to_string(), "tata".to_string()]));
```