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

* Integer
* Unsigned Integer
* Float
* Boolean
* String
* Date

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

// Create new empty database
let mut db = SmolDb::init("Database name".to_string());

// Configure database file
db.set_database_file(PathBuf::from("file.json"));

// Save database to file
db.save().unwrap();

// Load existing database
let new_db = SmolDb::load(PathBuf::from("file.json")).unwrap();
```

### Table management

```rust
use smoldb::SmolDb;
let mut db = SmolDb::init("Database name".to_string());

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
let mut db = SmolDb::init("Database name".to_string());
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
