//!
//! Database Entry definition
//!

use rustlog::{write_log, LogSeverity};
use serde_derive::{Deserialize, Serialize};

use super::db_type::DbType;

/// Database entry
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct DbEntry {
    /// Entry name
    name: String,
    /// Fields vector has the size of `fields` vector from upper table
    fields: Vec<Option<DbType>>,
}

impl DbEntry {
    /// Create a new `DbEntry`.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the `DbEntry`.
    /// * `fields_nb` - The number of fields in the `DbEntry`.
    /// * `values` - An optional mutable reference to a vector of `DbType` values.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if the number of fields in the `values` vector is not equal to `fields_nb`, with a message indicating the incorrect size.
    ///
    pub fn new(
        name: &String,
        fields_nb: usize,
        values: Option<&mut Vec<Option<DbType>>>,
    ) -> Result<DbEntry, String> {
        // Create new vector
        let mut entry: Vec<Option<DbType>> = Vec::new();
        match values {
            Some(vals) => {
                // Check sizes coherency
                if fields_nb != vals.len() {
                    let msg = format!(
                        "Values given for new entry {} does not have the correct size ({})",
                        name, fields_nb
                    );
                    write_log(
                        LogSeverity::Error,
                        &msg,
                        &env!("CARGO_PKG_NAME").to_string(),
                    );
                    return Err(msg);
                }
                entry.append(vals);
            }
            None => {
                for _i in 0..fields_nb {
                    entry.push(None);
                }
            }
        }

        write_log(
            LogSeverity::Info,
            &format!("CREATE new entry {name}"),
            &env!("CARGO_PKG_NAME").to_string(),
        );
        Ok(DbEntry {
            name: name.clone(),
            fields: entry,
        })
    }

    /// Update the value of a field in the data structure.
    ///
    /// # Arguments
    ///
    /// - `key_index`: The index of the field to be updated.
    /// - `value`: The new value to be assigned to the field. Use `None` to unset the value.
    ///
    /// # Panics
    ///
    /// This method will panic if the `key_index` is out of bounds.
    ///
    /// # Notes
    ///
    /// - This method mutates the data structure in-place.
    /// - If the `key_index` is out of bounds, this method will panic.
    pub fn update(&mut self, key_index: usize, value: Option<DbType>) {
        *self.fields.get_mut(key_index).unwrap() = value;
    }

    /// Retrieves the value associated with the specified key index from the underlying data structure.
    ///
    /// # Arguments
    ///
    /// * `key_index` - The index of the key to retrieve the value for.
    ///
    /// # Returns
    ///
    /// An `Option` containing a reference to the value, if it exists, otherwise `None`.
    pub fn get(&self, key_index: usize) -> Option<&DbType> {
        self.fields.get(key_index)?.as_ref()
    }

    /// Returns a reference to the name of the object.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Adds a field value to the current instance of the struct.
    ///
    /// # Arguments
    ///
    /// * `value` - An optional value of the specified type to be added to the fields.
    ///
    pub fn add_field(&mut self, value: Option<DbType>) {
        self.fields.push(value);
    }

    /// Renames the object.
    ///
    /// # Arguments
    ///
    /// * `new_name` - The new name of the object.
    ///
    pub fn rename(&mut self, new_name: &String) {
        self.name = new_name.clone();
    }
}

#[cfg(test)]
mod tests {
    use rusttests::{check_option, check_result, check_struct, check_value};

    use crate::db_model::db_type::DbType;

    use super::DbEntry;

    #[test]
    fn new_entry_empty() -> Result<(), String> {
        let name = "entry";
        let none_vec = vec![None, None, None, None];

        let val = check_result((1, 1), DbEntry::new(&name.to_string(), 4, None), true)?.unwrap();
        check_value(
            (1, 2),
            &val.name,
            &name.to_string(),
            rusttests::CheckType::Equal,
        )?;
        check_struct((1, 3), &val.fields, &none_vec, rusttests::CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn new_entry_not_empty() -> Result<(), String> {
        let name = "entry2";
        let mut some_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
        ];
        let some_vec2 = some_vec.clone();

        let val = check_result(
            (1, 1),
            DbEntry::new(&name.to_string(), 4, Some(&mut some_vec)),
            true,
        )?
        .unwrap();
        check_value(
            (1, 2),
            &val.name,
            &name.to_string(),
            rusttests::CheckType::Equal,
        )?;
        check_struct((1, 3), &val.fields, &some_vec2, rusttests::CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn entry_update() -> Result<(), String> {
        let name = "entry2";
        let mut some_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
        ];

        let new_vec = vec![
            Some(DbType::String("item1".to_string())),
            Some(DbType::String("new_item".to_string())),
            Some(DbType::UnsignedInt(35)),
            Some(DbType::Integer(12)),
        ];

        let mut entry = DbEntry::new(&name.to_string(), 4, Some(&mut some_vec))?;
        entry.update(1, Some(DbType::String("new_item".to_string())));
        entry.update(2, Some(DbType::UnsignedInt(35)));

        check_struct((1, 1), &entry.fields, &new_vec, rusttests::CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn entry_get() -> Result<(), String> {
        let name = "entry2";
        let mut some_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
        ];

        let entry = DbEntry::new(&name.to_string(), 4, Some(&mut some_vec))?;

        let val = entry.get(2).unwrap();

        if *val != DbType::Float(3.33) {
            return Err(format!("Entry field have wrong value : {:?}", entry.fields));
        }
        check_struct(
            (1, 1),
            val,
            &DbType::Float(3.33),
            rusttests::CheckType::Equal,
        )?;

        let val_none = entry.get(1);
        if val_none.is_some() {
            return Err("Entry field should be None".to_string());
        }
        check_option((1, 2), val_none, false)?;

        Ok(())
    }

    #[test]
    fn entry_add_field() -> Result<(), String> {
        let name = "entry2";
        let mut some_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
        ];

        let new_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
            Some(DbType::String("item4".to_string())),
            None,
        ];

        let mut entry = DbEntry::new(&name.to_string(), 4, Some(&mut some_vec))?;
        entry.add_field(Some(DbType::String("item4".to_string())));
        entry.add_field(None);

        check_struct((1, 1), &entry.fields, &new_vec, rusttests::CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn entry_rename() -> Result<(), String> {
        let name = "entry2";
        let mut some_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
        ];

        let mut entry = DbEntry::new(&name.to_string(), 4, Some(&mut some_vec))?;
        entry.rename(&"new_name".to_string());

        check_struct(
            (1, 1),
            entry.name(),
            &"new_name".to_string(),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }
}
