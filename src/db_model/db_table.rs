//!
//! Database Table definition
//!

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

    /// Adds a new entry to table.
    /// Entry name and fields values must be provided, values can be set to `None`
    pub fn add_entry(
        &mut self,
        name: String,
        values: Option<&mut Vec<Option<String>>>,
    ) -> Result<(), String> {
        let new_entry;

        // Check unicity of entry name
        for entry_name in self.entries.iter() {
            if entry_name.name() == &name {
                return Err(format!("Cannot create new entry : name {name} already exists in table"));
            }
        }

        if let Some(vals) = values {
            // Check vector size
            if vals.len() != self.keys.len() {
                return Err(format!(
                    "Cannot create new entry : `values` parameter must have a length of {}",
                    self.keys.len()
                ));
            }
            // Check values types
            let mut db_vals = Vec::new();
            for (i, val) in vals.iter().enumerate() {
                if let Some(val_str) = val {
                    let db_val = self.keys.get(i).unwrap().1.convert(val_str)?;
                    db_vals.push(Some(db_val));
                }
                else {
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

    //pub fn update_entry(&mut self, entry_name: String, key_name: String, )

    /// Returns entries count in table
    pub fn entries_count(&self) -> usize {
        self.entries.len()
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
            Ok(_) => {
                return Err(format!(
                    "Error should be raised because of wrong type"
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
            Ok(_) => return Err(format!("Error should be raised because entry name already exists")),
            Err(_) => (),
        }

        if table.entries_count() == 1 {
            Ok(())
        } else {
            Err("Table should have 1 entry".to_string())
        }
    }
}
