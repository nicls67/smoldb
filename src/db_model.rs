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
    /// Defines the fields, with name and type
    fields: Vec<(String, String)>,
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
}
