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
    /// * `p_name` - The name of the `DbEntry`.
    /// * `p_fields_nb` - The number of fields in the `DbEntry`.
    /// * `p_values` - An optional mutable reference to a vector of `DbType` values.
    ///
    /// # Errors
    ///
    /// Returns an `Err` if the number of fields in the `values` vector is not equal to `fields_nb`, with a message indicating the incorrect size.
    ///
    pub fn new(
        p_name: &String,
        p_fields_nb: usize,
        p_values: Option<&mut Vec<Option<DbType>>>,
    ) -> Result<DbEntry, String> {
        // Create new vector
        let mut l_entry: Vec<Option<DbType>> = Vec::new();
        match p_values {
            Some(l_vals) => {
                // Check sizes coherency
                if p_fields_nb != l_vals.len() {
                    let l_msg = format!(
                        "Values given for new entry {} does not have the correct size ({})",
                        p_name, p_fields_nb
                    );
                    write_log(LogSeverity::Error, &l_msg, env!("CARGO_PKG_NAME"));
                    return Err(l_msg);
                }
                l_entry.append(l_vals);
            }
            None => {
                for _l_i in 0..p_fields_nb {
                    l_entry.push(None);
                }
            }
        }

        write_log(
            LogSeverity::Info,
            &format!("CREATE new entry {p_name}"),
            env!("CARGO_PKG_NAME"),
        );
        Ok(DbEntry {
            name: p_name.clone(),
            fields: l_entry,
        })
    }

    /// Update the value of a field in the data structure.
    ///
    /// # Arguments
    ///
    /// - `p_key_index`: The index of the field to be updated.
    /// - `p_value`: The new value to be assigned to the field. Use `None` to unset the value.
    ///
    /// # Panics
    ///
    /// This method will panic if the `key_index` is out of bounds.
    ///
    /// # Notes
    ///
    /// - This method mutates the data structure in-place.
    /// - If the `key_index` is out of bounds, this method will panic.
    pub fn update(&mut self, p_key_index: usize, p_value: Option<DbType>) {
        *self.fields.get_mut(p_key_index).unwrap() = p_value;
    }

    /// Retrieves the value associated with the specified key index from the underlying data structure.
    ///
    /// # Arguments
    ///
    /// * `p_key_index` - The index of the key to retrieve the value for.
    ///
    /// # Returns
    ///
    /// An `Option` containing a reference to the value, if it exists, otherwise `None`.
    pub fn get(&self, p_key_index: usize) -> Option<&DbType> {
        self.fields.get(p_key_index)?.as_ref()
    }

    /// Returns a reference to the name of the object.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Adds a field value to the current instance of the struct.
    ///
    /// # Arguments
    ///
    /// * `p_value` - An optional value of the specified type to be added to the fields.
    ///
    pub fn add_field(&mut self, p_value: Option<DbType>) {
        self.fields.push(p_value);
    }

    /// Renames the object.
    ///
    /// # Arguments
    ///
    /// * `p_new_name` - The new name of the object.
    ///
    pub fn rename(&mut self, p_new_name: &str) {
        self.name = p_new_name.to_owned();
    }
}

#[cfg(test)]
mod tests {
    use rusttests::{check_option, check_result, check_struct, check_value};

    use crate::db_model::db_type::DbType;

    use super::DbEntry;

    #[test]
    fn new_entry_empty() -> Result<(), String> {
        let l_name = "entry";
        let l_none_vec = vec![None, None, None, None];

        let l_val =
            check_result((1, 1), DbEntry::new(&l_name.to_string(), 4, None), true)?.unwrap();
        check_value(
            (1, 2),
            &l_val.name,
            &l_name.to_string(),
            rusttests::CheckType::Equal,
        )?;
        check_struct(
            (1, 3),
            &l_val.fields,
            &l_none_vec,
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn new_entry_not_empty() -> Result<(), String> {
        let l_name = "entry2";
        let mut l_some_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
        ];
        let l_some_vec2 = l_some_vec.clone();

        let l_val = check_result(
            (1, 1),
            DbEntry::new(&l_name.to_string(), 4, Some(&mut l_some_vec)),
            true,
        )?
        .unwrap();
        check_value(
            (1, 2),
            &l_val.name,
            &l_name.to_string(),
            rusttests::CheckType::Equal,
        )?;
        check_struct(
            (1, 3),
            &l_val.fields,
            &l_some_vec2,
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn entry_update() -> Result<(), String> {
        let l_name = "entry2";
        let mut l_some_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
        ];

        let l_new_vec = vec![
            Some(DbType::String("item1".to_string())),
            Some(DbType::String("new_item".to_string())),
            Some(DbType::UnsignedInt(35)),
            Some(DbType::Integer(12)),
        ];

        let mut l_entry = DbEntry::new(&l_name.to_string(), 4, Some(&mut l_some_vec))?;
        l_entry.update(1, Some(DbType::String("new_item".to_string())));
        l_entry.update(2, Some(DbType::UnsignedInt(35)));

        check_struct(
            (1, 1),
            &l_entry.fields,
            &l_new_vec,
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn entry_get() -> Result<(), String> {
        let l_name = "entry2";
        let mut l_some_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
        ];

        let l_entry = DbEntry::new(&l_name.to_string(), 4, Some(&mut l_some_vec))?;

        let l_val = l_entry.get(2).unwrap();

        if *l_val != DbType::Float(3.33) {
            return Err(format!("Entry field have wrong value : {:?}", l_entry.fields));
        }
        check_struct(
            (1, 1),
            l_val,
            &DbType::Float(3.33),
            rusttests::CheckType::Equal,
        )?;

        let l_val_none = l_entry.get(1);
        if l_val_none.is_some() {
            return Err("Entry field should be None".to_string());
        }
        check_option((1, 2), l_val_none, false)?;

        Ok(())
    }

    #[test]
    fn entry_add_field() -> Result<(), String> {
        let l_name = "entry2";
        let mut l_some_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
        ];

        let l_new_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
            Some(DbType::String("item4".to_string())),
            None,
        ];

        let mut l_entry = DbEntry::new(&l_name.to_string(), 4, Some(&mut l_some_vec))?;
        l_entry.add_field(Some(DbType::String("item4".to_string())));
        l_entry.add_field(None);

        check_struct(
            (1, 1),
            &l_entry.fields,
            &l_new_vec,
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn entry_rename() -> Result<(), String> {
        let l_name = "entry2";
        let mut l_some_vec = vec![
            Some(DbType::String("item1".to_string())),
            None,
            Some(DbType::Float(3.33)),
            Some(DbType::Integer(12)),
        ];

        let mut l_entry = DbEntry::new(&l_name.to_string(), 4, Some(&mut l_some_vec))?;
        l_entry.rename("new_name");

        check_struct(
            (1, 1),
            l_entry.name(),
            &"new_name".to_string(),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }
}
