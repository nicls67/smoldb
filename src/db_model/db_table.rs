//!
//! Database Table definition
//!

use std::mem::discriminant;

use super::{db_entry::DbEntry, db_type::DbType};

/// Database table
#[derive(PartialEq)]
pub struct DbTable {
    /// Table name
    name: String,
    /// Defines the keys, with name and associated type
    keys: Vec<(String, DbType)>,
    /// Table entries, each entry is a vector of entries
    entries: Vec<DbEntry>,
}

impl DbTable {
    /// Creates a new table with the selected keys. The new table has no entries, the keys can be left empty
    pub(crate) fn new(name: String, keys: Option<Vec<(String, DbType)>>) -> DbTable {
        DbTable {
            name,
            keys: if let Some(k) = keys { k } else { Vec::new() },
            entries: Vec::new(),
        }
    }

    ///
    /// ## Adds a new entry to table.
    /// Entry name and fields values must be provided, values can be set to `None`
    ///
    pub fn add_entry(
        &mut self,
        name: String,
        values: Option<&mut Vec<Option<String>>>,
    ) -> Result<(), String> {
        let new_entry;

        // Check unicity of entry name
        if self.entry_exists(&name) {
            return Err(format!(
                "Cannot create new entry : name {name} already exists in table"
            ));
        }

        if let Some(vals) = values {
            // Check vector size
            if vals.len() != self.keys.len() {
                return Err(format!(
                    "Cannot create new entry : `values` parameter must have a length of {}",
                    self.keys.len()
                ));
            }
            // Store values after conversion
            let mut db_vals = Vec::new();
            for (i, val) in vals.iter().enumerate() {
                if let Some(val_str) = val {
                    let db_val = self.keys.get(i).unwrap().1.convert(val_str)?;
                    db_vals.push(Some(db_val));
                } else {
                    db_vals.push(None);
                }
            }

            new_entry = DbEntry::new(name, self.keys.len(), Some(db_vals.as_mut()))?;
        } else {
            new_entry = DbEntry::new(name, self.keys.len(), None)?;
        }
        self.entries.push(new_entry);
        Ok(())
    }

    ///
    /// ## Updates an entry of the table.
    /// Entry name, key to update and field value must be provided, value can be set to `None`
    ///
    pub fn update_entry_string(
        &mut self,
        entry_name: &String,
        key_name: &String,
        new_value: Option<String>,
    ) -> Result<(), String> {
        let mut db_value = None;
        if let Some(value) = new_value {
            db_value = Some(self.find_key(key_name)?.1.convert(&value)?);
        }
        self.update_entry(entry_name, key_name, db_value)
    }

    ///
    /// ## Gets an entry value.
    /// Entry name, key to get must be provided
    ///
    pub fn get_entry_value_string(&mut self, entry_name: &String, key_name: &String) -> Result<Option<&String>, String> {
        if let Some(value) = self.get_entry_value(entry_name, key_name)? {
            if let DbType::String(s) = value {
                Ok(Some(s))
            } else {
                // Impossible case
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Updates the selected entry
    ///
    /// Private method called by type-specific public methods
    fn update_entry(&mut self, entry_name: &String, key_name: &String, new_value: Option<DbType>) -> Result<(), String> {
        let key = self.find_key(key_name)?;
        let key_index = key.0;

        if let Some(ref db_val) = new_value {
            if discriminant(key.1) != discriminant(db_val) {
                return Err(format!("Type of key {} is not compatible with given type", key_name));
            }
        }

        self.find_entry(entry_name)?.update(key_index, new_value);
        Ok(())
    }

    /// Gets key value for selected entry
    ///
    /// Private method called by type-specific public methods
    fn get_entry_value(&mut self, entry_name: &String, key_name: &String) -> Result<Option<&DbType>, String> {
        let key_index = self.find_key(key_name)?.0;
        Ok(self.find_entry(entry_name)?.get(key_index))
    }

    /// Returns entries count in table
    pub fn entries_count(&self) -> usize {
        self.entries.len()
    }

    /// Search for an entry and returns `Ok` with a reference to it, or `Err` if the entry does not exist
    fn find_entry(&mut self, entry_name: &String) -> Result<&mut DbEntry, String> {
        for entry in self.entries.iter_mut() {
            if entry.name() == entry_name {
                return Ok(entry);
            }
        }
        Err(format!(
            "Entry {} does not exists in table {}",
            entry_name, self.name
        ))
    }

    /// Checks if the entry exists or not
    fn entry_exists(&mut self, entry_name: &String) -> bool {
        match self.find_entry(entry_name) {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    /// Search for a key name and returns `Ok` with its index and type, or `Err` if the key doesn't exist
    fn find_key(&self, key_name: &String) -> Result<(usize, &DbType), String> {
        for (index, key) in self.keys.iter().enumerate() {
            if &key.0 == key_name {
                return Ok((index, &key.1));
            }
        }
        Err(format!(
            "Key {} does not exists in table {}",
            key_name, self.name
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{DbTable, DbType};

    #[test]
    fn new_table_none() -> Result<(), String> {
        let table = DbTable::new("Table".to_string(), None);

        let expected = DbTable {
            name: "Table".to_string(),
            keys: Vec::new(),
            entries: Vec::new(),
        };

        if table == expected {
            Ok(())
        } else {
            Err("New table doesn't match the expected".to_string())
        }
    }

    #[test]
    fn new_table_some() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
        ];
        let table = DbTable::new("Table".to_string(), Some(keys));

        let expected = DbTable {
            name: "Table".to_string(),
            keys: vec![
                ("key1".to_string(), DbType::Integer(0)),
                ("key2".to_string(), DbType::String(" ".to_string())),
            ],
            entries: Vec::new(),
        };

        if table == expected {
            Ok(())
        } else {
            Err("New table doesn't match the expected".to_string())
        }
    }

    #[test]
    fn add_entry() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry("entry1".to_string(), new_entry)?;
        table.add_entry("entry2".to_string(), None)?;

        if table.entries_count() == 2 {
            Ok(())
        } else {
            Err("Table should have 2 entries".to_string())
        }
    }

    #[test]
    fn add_entry_bad_type() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("text".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        match table.add_entry("entry1".to_string(), new_entry) {
            Ok(_) => return Err(format!("Error should be raised because of wrong type")),
            Err(_) => (),
        };
        table.add_entry("entry2".to_string(), None)?;

        if table.entries_count() == 1 {
            Ok(())
        } else {
            Err("Table should have 1 entry".to_string())
        }
    }

    #[test]
    fn add_entry_bad_size() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("value1".to_string()), None];
        let new_entry = Some(&mut binding);

        match table.add_entry("entry1".to_string(), new_entry) {
            Ok(_) => {
                return Err(format!(
                    "Error should be raised because of wrong vector size"
                ))
            }
            Err(_) => (),
        };
        table.add_entry("entry2".to_string(), None)?;

        if table.entries_count() == 1 {
            Ok(())
        } else {
            Err("Table should have 1 entry".to_string())
        }
    }

    #[test]
    fn add_entry_bad_name() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("2".to_string()), None, None];
        let new_entry = Some(&mut binding);

        table.add_entry("entry1".to_string(), new_entry)?;

        match table.add_entry("entry1".to_string(), None) {
            Ok(_) => {
                return Err(format!(
                    "Error should be raised because entry name already exists"
                ))
            }
            Err(_) => (),
        }

        if table.entries_count() == 1 {
            Ok(())
        } else {
            Err("Table should have 1 entry".to_string())
        }
    }

    #[test]
    fn update_entry_nominal() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry("entry1".to_string(), new_entry)?;
        table.add_entry("entry2".to_string(), None)?;

        table.update_entry(&"entry1".to_string(), &"key3".to_string(), Some(DbType::Float(5.98)))?;
        table.update_entry(&"entry2".to_string(), &"key2".to_string(), Some(DbType::String("Some value".to_string())))?;

        if let Some(value) = table.get_entry_value(&"entry1".to_string(), &"key3".to_string())? {
            match value {
                DbType::Float(f) => {
                    if *f == 5.98 {
                        ()
                    }
                    else {
                        return Err(format!("Entry value should be 5.98"));
                    }
                },
                _ => return Err(format!("Entry value should be Float"))
            };
        } else {
            return Err(format!("Entry value should be Some(5.98)"));
        }

        if let Some(value) = table.get_entry_value(&"entry2".to_string(), &"key2".to_string())? {
            match value {
                DbType::String(s) => {
                    if s == "Some value" {
                        return Ok(());
                    }
                    else {
                        return Err(format!("Entry value should be Some value"));
                    }
                },
                _ => return Err(format!("Entry value should be String"))
            };
        } else {
            return Err(format!("Entry value should be Some(Some value)"));
        }
    }

    #[test]
    fn update_entry_none() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry("entry1".to_string(), new_entry)?;
        table.add_entry("entry2".to_string(), None)?;

        table.update_entry(&"entry1".to_string(), &"key1".to_string(), None)?;

        if table.get_entry_value(&"entry1".to_string(), &"key1".to_string())?.is_none() {
            Ok(())
        } else {
            Err(format!("Entry value should be None"))
        }
    }

    #[test]
    fn update_entry_bad_name() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry("entry1".to_string(), new_entry)?;
        table.add_entry("entry2".to_string(), None)?;

        match table.update_entry(&"entry5".to_string(), &"key2".to_string(), None) {
            Ok(_) => Err(format!("Error should be raised because entry name does not exist")),
            Err(_) => Ok(()),
        }
    }

    #[test]
    fn update_entry_bad_key() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry("entry1".to_string(), new_entry)?;
        table.add_entry("entry2".to_string(), None)?;

        match table.update_entry(&"entry2".to_string(), &"key4".to_string(), None) {
            Ok(_) => Err(format!("Error should be raised because key name does not exist")),
            Err(_) => Ok(()),
        }
    }

    #[test]
    fn update_entry_bad_type() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry("entry1".to_string(), new_entry)?;
        table.add_entry("entry2".to_string(), None)?;

        match table.update_entry(&"entry2".to_string(), &"key1".to_string(), Some(DbType::Float(0.25))) {
            Ok(_) => Err(format!("Error should be raised because key type is incompatible")),
            Err(_) => Ok(()),
        }
    }

    #[test]
    fn update_entry_string() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry("entry1".to_string(), new_entry)?;
        table.add_entry("entry2".to_string(), None)?;

        table.update_entry_string(&"entry1".to_string(), &"key2".to_string(), Some("New value".to_string()))?;

        if let Some(value) = table.get_entry_value_string(&"entry1".to_string(), &"key2".to_string())? {
            if value == "New value" {
                Ok(())
            } else {
                Err(format!("String value should be New value"))
            }
        } else {
            Err(format!("Entry value should be Some(New value)"))
        }
    }

    #[test]
    fn update_entry_string_wrong_key() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry("entry1".to_string(), new_entry)?;
        table.add_entry("entry2".to_string(), None)?;

        match table.update_entry_string(&"entry1".to_string(), &"key1".to_string(), Some("New value".to_string())) {
            Ok(_) => return Err(format!("Update result should be Err")),
            Err(_) => (),
        }

        if let Some(value) = table.get_entry_value(&"entry1".to_string(), &"key1".to_string())? {
            match value {
                DbType::Integer(i) => {
                    if *i == 1 {
                        return Ok(());
                    }
                    else {
                        return Err(format!("Entry value should be 1"));
                    }
                },
                _ => return Err(format!("Entry value should be Integer"))
            };
        } else {
            return Err(format!("Entry value should be Some"));
        }
    }

    #[test]
    fn update_entry_string_none() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry("entry1".to_string(), new_entry)?;
        table.add_entry("entry2".to_string(), None)?;

        table.update_entry_string(&"entry1".to_string(), &"key2".to_string(), None)?;

        if table.get_entry_value_string(&"entry1".to_string(), &"key2".to_string())?.is_none() {
            Ok(())
        } else {
            Err(format!("Entry value should be None"))
        }
    }

}
