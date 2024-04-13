//!
//! Database model
//!

/// Database model
pub struct DbModel {
    name: String,
    version: [u8; 3],
}

/// Database table
pub struct DbTable {
    /// Table name
    name: String,
    /// Defines the keys, with name and type
    keys: Vec<(String, String)>,
    /// Table entries, each entry is a vector of entries
    entries: Vec<DbEntry>,
}

/// Database entry
pub struct DbEntry {
    /// Entry name
    name: String,
    /// Fields vector has the size of `fields` vector from upper table
    fields: Vec<Option<String>>,
}

impl DbEntry {
    /// Creates a new entry, number of fields and their values must be provided.
    /// Fields values can be globally empty (parameter `values` equal to `None`)
    /// or one particular field can be empty (one element of vector is `None`)
    pub fn new(
        name: String,
        fields_nb: u16,
        values: Option<&mut Vec<Option<String>>>,
    ) -> Result<DbEntry, String> {
        // Create new vector
        let mut entry: Vec<Option<String>> = Vec::new();
        match values {
            Some(vals) => {
                // Check sizes coherency
                if fields_nb as usize != vals.len() {
                    return Err(format!(
                        "Values given for new entry {} does not have the correct size ({})",
                        name, fields_nb
                    ));
                }
                entry.append(vals);
            }
            None => {
                for _i in 0..fields_nb {
                    entry.push(None);
                }
            }
        }

        Ok(DbEntry {
            name,
            fields: entry,
        })
    }

    /// Updates the designated field of the entry
    pub fn update(&mut self, key_index: u16, value: Option<String>) {
        *self.fields.get_mut(key_index as usize).unwrap() = value;
    }

    /// Gets value of the selected field
    pub fn get(&self, key_index: u16) -> Option<&String> {
        self.fields.get(key_index as usize).unwrap().as_ref()
    }

    /// Adds a new field to the entry with the given value (can be `None`)
    pub fn add_field(&mut self, value: Option<String>) {
        self.fields.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::DbEntry;

    #[test]
    fn new_entry_empty() -> Result<(), String> {
        let name = "entry";
        let none_vec = vec![None, None, None, None];

        match DbEntry::new(name.to_string(), 4, None) {
            Ok(entry) => {
                if entry.name.as_str() != name {
                    Err(format!("Entry name should be {name}"))
                } else {
                    if entry.fields != none_vec {
                        Err("Entry fields should all be None".to_string())
                    } else {
                        Ok(())
                    }
                }
            }
            Err(_) => Err("Result should be Ok".to_string()),
        }
    }

    #[test]
    fn new_entry_not_empty() -> Result<(), String> {
        let name = "entry2";
        let mut some_vec = vec![
            Some("item1".to_string()),
            None,
            Some("item2".to_string()),
            Some("item3".to_string()),
        ];
        let some_vec2 = some_vec.clone();

        match DbEntry::new(name.to_string(), 4, Some(&mut some_vec)) {
            Ok(entry) => {
                if entry.name.as_str() != name {
                    Err(format!("Entry name should be {name}"))
                } else {
                    if entry.fields != some_vec2 {
                        Err(format!(
                            "Entry fields have wrong value : {:?}",
                            entry.fields
                        ))
                    } else {
                        Ok(())
                    }
                }
            }
            Err(_) => Err("Result should be Ok".to_string()),
        }
    }

    #[test]
    fn entry_update() -> Result<(), String> {
        let name = "entry2";
        let mut some_vec = vec![
            Some("item1".to_string()),
            None,
            Some("item2".to_string()),
            Some("item3".to_string()),
        ];

        let new_vec = vec![
            Some("item1".to_string()),
            Some("new_item".to_string()),
            Some("item2_updated".to_string()),
            Some("item3".to_string()),
        ];

        let mut entry = DbEntry::new(name.to_string(), 4, Some(&mut some_vec)).unwrap();
        entry.update(1, Some("new_item".to_string()));
        entry.update(2, Some("item2_updated".to_string()));

        if entry.fields == new_vec {
            Ok(())
        }
        else {
            Err(format!(
                "Entry fields have wrong value : {:?}",
                entry.fields
            ))
        }
    }

    #[test]
    fn entry_get() -> Result<(), String> {
        let name = "entry2";
        let mut some_vec = vec![
            Some("item1".to_string()),
            None,
            Some("item2".to_string()),
            Some("item3".to_string()),
        ];

        let entry = DbEntry::new(name.to_string(), 4, Some(&mut some_vec)).unwrap();

        let val = entry.get(3).unwrap();

        if val.as_str() != "item3" {
            return Err(format!(
                "Entry field have wrong value : {:?}",
                entry.fields
            ));
        }

        let val_none = entry.get(1);
        if val_none.is_some() {
            return Err("Entry field should be None".to_string());
        }

        Ok(())
    }

    #[test]
    fn entry_add_field() -> Result<(), String> {
        let name = "entry2";
        let mut some_vec = vec![
            Some("item1".to_string()),
            None,
            Some("item2".to_string()),
            Some("item3".to_string()),
        ];

        let new_vec = vec![
            Some("item1".to_string()),
            None,
            Some("item2".to_string()),
            Some("item3".to_string()),
            Some("item4".to_string()),
            None
        ];

        let mut entry = DbEntry::new(name.to_string(), 4, Some(&mut some_vec)).unwrap();
        entry.add_field(Some("item4".to_string()));
        entry.add_field(None);

        if entry.fields == new_vec {
            Ok(())
        }
        else {
            Err(format!(
                "Entry fields have wrong value : {:?}",
                entry.fields
            ))
        }
    }
}
