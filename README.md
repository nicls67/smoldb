# SmolDB

A small database library written in Rust

## Principle

This library is meant to be used inside a Rust binary application. Database is stored inside a `.smoldb` file (or any other extension) which is opened or created during library loading.

**SmolDB** only requires a few accesses to disk as database file is loaded once during initialization, and then all requests are handled from RAM (read and write accesses). The database file on disk is updated when `save` method is called.

A database contains one or many tables, each tables containing one or many database fields. Tables and fields name (keys) are defined during initialization process. Each field has a designated type among the followings :

* Integer
* Unsigned Integer
* Float
* String

A database entry is linked to a table and has its fields filled with a value. It is possible to leave a field empty.

It is possible to add tables and/or keys after initialization, when a key is added to a table, all entries inside the table will have a `None` value for the new key.

To get or update an entry value, either call the method corresponding to the data type, like `update_entry_integer` or `get_entry_value_float`, or call `update_entry_string` or `get_entry_value_string` to use strings instead. Please note that `update_entry_string` will return `Err` if the passed string doesn't match the data type.

## Usage

### Create new database

```rust
use smoldb::SmolDb;

let db = SmolDb::init("Database name".to_string());
```

### Create a new table

```rust
use smoldb::SmolDb;
let mut db = SmolDb::init("Database name".to_string());

// Define keys name and type and create table
let keys = Some(vec![
                ("key1".to_string(), "String".to_string()),
                ("key2".to_string(), "Integer".to_string()),
            ]);
db.get_database().create_table(&"Table name".to_string(), keys).unwrap();

// Keys can also be left empty
db.get_database().create_table(&"Table name 2".to_string(), None).unwrap();
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
db.get_database().create_table(&"Table name".to_string(), keys).unwrap();

// Get table reference
let mut table = db.get_database().table(&"Table name".to_string()).unwrap();

// Add a new entry to the table with values
// Values for the new entry must be provided as String, conversion will be done by the database
// Any value can be set to None
let mut values = vec![Some("hey".to_string()), None, Some("2.23".to_string())];
table.add_entry(&"entry1".to_string(), Some(&mut values)).unwrap();

// Add another entry with all values set to None
table.add_entry(&"entry2".to_string(), None).unwrap();

// Update an entry value using string
table.update_entry_string(&"entry1".to_string(), &"key2".to_string(), Some("12".to_string()));
// Update an entry value using type-specific method
table.update_entry_float(&"entry1".to_string(), &"key3".to_string(), Some(1.23));
// Update an entry with None value, any update method can be used
table.update_entry_integer(&"entry1".to_string(), &"key3".to_string(), None);

// Get an entry value using string
let value: Option<String> = table.get_entry_value_string(&"entry1".to_string(), &"key2".to_string()).unwrap();
// Get an entry value using type-specific method
let value: Option<&f32> = table.get_entry_value_float(&"entry1".to_string(), &"key3".to_string()).unwrap();

// Get entries count inside a table
let count = table.entries_count();

// Add a new key to a table ()existing entries will get a None value for this key)
table.add_key(&"new_key".to_string(), &"Integer".to_string()).unwrap();
```
