//!
//! Database Table definition
//!

use std::cmp::PartialEq;
use std::mem::discriminant;

use chrono::NaiveDate;
use rustlog::{write_log, LogSeverity};
use serde_derive::{Deserialize, Serialize};

use super::{db_entry::DbEntry, db_type::DbType};

/// Keys values matching criteria
///
/// * `IsMore` and `IsLess` apply to date as "after" and "before" the reference date
/// * Only `Equal` and `Different` can be applied to `String` and `Boolean` types
#[derive(PartialEq, Debug)]
pub enum MatchingCriteria {
    IsMore,
    IsLess,
    Equal,
    Different,
    Between,
}

/// Database table
#[derive(PartialEq, Serialize, Deserialize, Debug)]
pub struct DbTable {
    /// Table name
    name: String,
    /// Defines the keys, with name and associated type
    keys: Vec<(String, DbType)>,
    /// Table entries, each entry is a vector of entries
    entries: Vec<DbEntry>,
}

impl DbTable {
    /// Create a new instance of `DbTable`.
    ///
    /// # Arguments
    ///
    /// * `name` - A `String` representing the name of the table.
    /// * `keys` - An optional `Vec` of tuples containing a `String` representing the name of each key,
    ///            and a `DbType` representing the type of the key.
    ///
    /// # Returns
    ///
    /// Returns a new `DbTable` with the specified name and keys.
    ///
    pub(crate) fn new(name: String, keys: Option<Vec<(String, DbType)>>) -> DbTable {
        DbTable {
            name,
            keys: keys.unwrap_or_default(),
            entries: Vec::new(),
        }
    }

    /// Adds a new entry to the table.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the entry. It must be unique within the table.
    /// * `values` - Optional values for the entry. If provided, the length must be equal to the number of keys in the table.
    ///              Each value should be wrapped in an `Option<String>`.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns an error message as a `Result` if any of the following conditions are met:
    ///
    /// * The entry name already exists in the table.
    /// * The length of the `values` vector is not equal to the number of keys in the table.
    ///
    pub fn add_entry(
        &mut self,
        name: &String,
        values: Option<&mut Vec<Option<String>>>,
    ) -> Result<(), String> {
        let new_entry;

        // Check unicity of entry name
        if self.entry_exists(name) {
            let msg = format!(
                "Cannot create new entry : name {} already exists in table",
                name
            );
            write_log(
                LogSeverity::Error,
                &msg,
                &env!("CARGO_PKG_NAME").to_string(),
            );
            return Err(msg);
        }

        if let Some(vals) = values {
            // Check vector size
            if vals.len() != self.keys.len() {
                let msg = format!(
                    "Cannot create new entry : `values` parameter must have a length of {}",
                    self.keys.len()
                );
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                return Err(msg);
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

    /// Updates the value of a key for a given entry in the database.
    ///
    /// # Arguments
    ///
    /// - `entry_name`: A reference to the name of the entry.
    /// - `key_name`: A reference to the name of the key.
    /// - `new_value`: An optional new value to update the key with. If `None`, the key will be unset.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the update is successful.
    /// - `Err(String)` if an error occurs during the update process.
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

    /// Returns the value of a specific key in an entry as a string.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - A reference to a `String` that represents the name of the entry.
    /// * `key_name` - A reference to a `String` that represents the name of the key.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` - If the key exists in the entry, returns `Some(value.to_string())` where `value` is the value associated with the key.
    /// * `Ok(None)` - If the key does not exist in the entry, returns `None`.
    /// * `Err(message)` - If an error occurs during the retrieval of the value, returns an error message as a `String`.
    ///
    pub fn get_entry_value_string(
        &mut self,
        entry_name: &String,
        key_name: &String,
    ) -> Result<Option<String>, String> {
        if let Some(value) = self.get_entry_value(entry_name, key_name)? {
            Ok(Some(value.to_string()))
        } else {
            Ok(None)
        }
    }

    /// Updates an entry in the database with a new integer value.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - The name of the entry to update.
    /// * `key_name` - The name of the key within the entry to update.
    /// * `new_value` - The new integer value to set. Pass `None` to remove the value.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the update was successful, or an error message as a `String` if there was a problem.
    ///
    pub fn update_entry_integer(
        &mut self,
        entry_name: &String,
        key_name: &String,
        new_value: Option<i32>,
    ) -> Result<(), String> {
        let mut db_value = None;
        if let Some(value) = new_value {
            db_value = Some(DbType::Integer(value));
        }
        self.update_entry(entry_name, key_name, db_value)
    }

    /// Retrieves the value of an entry as an integer.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - The name of the entry to retrieve
    pub fn get_entry_value_integer(
        &mut self,
        entry_name: &String,
        key_name: &String,
    ) -> Result<Option<&i32>, String> {
        // Coherency check
        match self.find_key(key_name)?.1.check_type(&DbType::Integer(0)) {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        if let Some(value) = self.get_entry_value(entry_name, key_name)? {
            if let DbType::Integer(s) = value {
                Ok(Some(s))
            } else {
                // Impossible case
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Update an entry in the database with an unsigned integer value.
    ///
    /// # Arguments
    ///
    /// - `entry_name`: The name of the entry to update.
    /// - `key_name`: The name of the key within the entry to update.
    /// - `new_value`: The new value to set for the key. Use `None` to delete the key.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the update was successful, otherwise returns an error message as a `Result`.
    ///
    pub fn update_entry_unsigned_integer(
        &mut self,
        entry_name: &String,
        key_name: &String,
        new_value: Option<u32>,
    ) -> Result<(), String> {
        let mut db_value = None;
        if let Some(value) = new_value {
            db_value = Some(DbType::UnsignedInt(value));
        }
        self.update_entry(entry_name, key_name, db_value)
    }

    /// Retrieves the value of an entry with an unsigned integer type, given the entry name and key name.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - The name of the entry to retrieve the value from.
    /// * `key_name` - The name of the key within the entry.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating the success of the operation. If successful, it returns an `Option` containing a reference to the unsigned integer value. If the entry or key does not exist, or if the value is not an unsigned integer, it returns `Ok(None)`. If an error occurs during the operation, it returns an `Err` with a descriptive error message.
    ///
    /// # Errors
    ///
    /// Returns an error if a coherency check fails or an error occurs while retrieving the value.
    pub fn get_entry_value_unsigned_integer(
        &mut self,
        entry_name: &String,
        key_name: &String,
    ) -> Result<Option<&u32>, String> {
        // Coherency check
        match self
            .find_key(key_name)?
            .1
            .check_type(&DbType::UnsignedInt(0))
        {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        if let Some(value) = self.get_entry_value(entry_name, key_name)? {
            if let DbType::UnsignedInt(s) = value {
                Ok(Some(s))
            } else {
                // Impossible case
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Updates the entry with a new floating point value.
    ///
    /// # Arguments
    ///
    /// - `entry_name`: A reference to the string containing the name of the entry.
    /// - `key_name`: A reference to the string containing the name of the key in the entry.
    /// - `new_value`: An optional `f32` value containing the new value to be set.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the entry was successfully updated.
    /// - `Err(String)` if an error occurred while updating the entry.
    ///
    pub fn update_entry_float(
        &mut self,
        entry_name: &String,
        key_name: &String,
        new_value: Option<f32>,
    ) -> Result<(), String> {
        let mut db_value = None;
        if let Some(value) = new_value {
            db_value = Some(DbType::Float(value));
        }
        self.update_entry(entry_name, key_name, db_value)
    }

    /// Retrieves the floating-point value of an entry given its name and key name.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - The name of the entry to retrieve from.
    /// * `key_name` - The name of the key to retrieve the value for.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(&f32))` with the value if it exists and is of type `DbType::Float`,
    /// `Ok(None)` if the value doesn't exist, or `Err(String)` if there was an error.
    ///
    pub fn get_entry_value_float(
        &mut self,
        entry_name: &String,
        key_name: &String,
    ) -> Result<Option<&f32>, String> {
        // Coherency check
        match self.find_key(key_name)?.1.check_type(&DbType::Float(0.0)) {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        if let Some(value) = self.get_entry_value(entry_name, key_name)? {
            if let DbType::Float(s) = value {
                Ok(Some(s))
            } else {
                // Impossible case
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Updates the entry with the specified key in the database.
    ///
    /// The `entry_name` parameter is a reference to the name of the entry to be updated.
    /// The `key_name` parameter is a reference to the name of the key in the entry to be updated.
    /// The `new_value` parameter is an optional boolean value to be set as the new value for the key.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - The name of the entry to be updated.
    /// * `key_name` - The name of the key in the entry to be updated.
    /// * `new_value` - An optional boolean value to be set as the new value for the key.
    ///
    /// # Errors
    ///
    /// Returns an error message as a `Result` if the update operation fails.
    ///
    pub fn update_entry_bool(
        &mut self,
        entry_name: &String,
        key_name: &String,
        new_value: Option<bool>,
    ) -> Result<(), String> {
        let mut db_value = None;
        if let Some(value) = new_value {
            db_value = Some(DbType::Bool(value));
        }
        self.update_entry(entry_name, key_name, db_value)
    }

    /// Gets the value of a boolean entry.
    ///
    /// This method checks if the given key exists and has the same type as `DbType::Bool(false)`,
    /// and returns the corresponding value if it exists and is of type `DbType::Bool`.
    /// Otherwise, it returns `None`.
    ///
    /// # Arguments
    ///
    /// - `entry_name`: A reference to a `String` representing the entry name.
    /// - `key_name`: A reference to a `String` representing the key name.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing:
    /// - `Ok(Some(bool))` if the value exists and is of type `DbType::Bool`.
    /// - `Ok(None)` if the value does not exist or is not of type `DbType::Bool`.
    /// - `Err(String)` if an error occurred during the coherency check or while retrieving the value.
    pub fn get_entry_value_bool(
        &mut self,
        entry_name: &String,
        key_name: &String,
    ) -> Result<Option<&bool>, String> {
        // Coherency check
        match self.find_key(key_name)?.1.check_type(&DbType::Bool(false)) {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        if let Some(value) = self.get_entry_value(entry_name, key_name)? {
            if let DbType::Bool(b) = value {
                Ok(Some(b))
            } else {
                // Impossible case
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Updates the entry date for a given entry with a new value.
    ///
    /// # Arguments
    ///
    /// - `entry_name`: The name of the entry to update.
    /// - `key_name`: The name of the key within the entry to update.
    /// - `new_value`: An optional new value for the entry date.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the update was successful.
    /// - `Err(String)` if there was an error updating the entry.
    pub fn update_entry_date(
        &mut self,
        entry_name: &String,
        key_name: &String,
        new_value: Option<NaiveDate>,
    ) -> Result<(), String> {
        let mut db_value = None;
        if let Some(value) = new_value {
            db_value = Some(DbType::Date(value));
        }
        self.update_entry(entry_name, key_name, db_value)
    }

    /// Retrieves the value of a specified key in a given entry
    ///
    /// This method is used to retrieve the value of a specified key in a specific entry.
    /// It performs a coherency check to ensure that the key has the correct data type.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - A reference to a String representing the name of the entry
    /// * `key_name` - A reference to a String representing the name of the key
    ///
    /// # Returns
    ///
    /// This method returns a Result containing an optional reference to a NaiveDate.
    /// - If the value is found and is of type DbType::Date, it returns Ok(Some(&NaiveDate)).
    /// - If the value is not found or is not of type DbType::Date, it returns Ok(None).
    /// - If an error occurs during the coherency check or value retrieval, it returns an Err(String).
    ///
    pub fn get_entry_value_date(
        &mut self,
        entry_name: &String,
        key_name: &String,
    ) -> Result<Option<&NaiveDate>, String> {
        // Coherency check
        match self
            .find_key(key_name)?
            .1
            .check_type(&DbType::default_from_string(&"Date".to_string())?)
        {
            Ok(_) => (),
            Err(e) => return Err(e),
        }

        if let Some(value) = self.get_entry_value(entry_name, key_name)? {
            if let DbType::Date(d) = value {
                Ok(Some(d))
            } else {
                // Impossible case
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Removes an entry from the collection.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - The name of the entry to remove.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the entry is successfully removed. Otherwise, returns `Err(String)` with an error message.
    pub fn remove_entry(&mut self, entry_name: &String) -> Result<(), String> {
        let index = self.find_entry(entry_name)?.1;
        self.entries.swap_remove(index);

        write_log(
            LogSeverity::Info,
            &format!("DELETE entry {entry_name}"),
            &env!("CARGO_PKG_NAME").to_string(),
        );
        Ok(())
    }

    /// Updates the value of a key in an entry.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - The name of the entry to update.
    /// * `key_name` - The name of the key to update.
    /// * `new_value` - The new value to assign to the key.
    ///
    /// # Errors
    ///
    /// Returns an error if the key is not found in the database or if the type of the key is not compatible with the given type.
    ///
    /// # Remarks
    ///
    /// This method finds the key by name in the database. If the `new_value` parameter is provided, it checks if the type of the key matches the type of the new value. If not, it logs an error message and returns an error. Otherwise, it updates the key with the new value in the entry identified by `entry_name`.
    ///
    /// It also logs a verbose message indicating that an entry has been updated.
    ///
    fn update_entry(
        &mut self,
        entry_name: &String,
        key_name: &String,
        new_value: Option<DbType>,
    ) -> Result<(), String> {
        let key = self.find_key(key_name)?;
        let key_index = key.0;

        if let Some(ref db_val) = new_value {
            if discriminant(key.1) != discriminant(db_val) {
                let msg = format!("Type of key {} is not compatible with given type", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                return Err(msg);
            }
        }

        self.find_entry(entry_name)?.0.update(key_index, new_value);

        write_log(
            LogSeverity::Verbose,
            &format!("UPDATE entry {} key {}", entry_name, key_name),
            &env!("CARGO_PKG_NAME").to_string(),
        );
        Ok(())
    }

    /// Retrieves the value associated with a given key in a specified entry.
    ///
    /// # Arguments
    ///
    /// - `entry_name`: A reference to a String representing the name of the entry.
    /// - `key_name`: A reference to a String representing the name of the key.
    ///
    /// # Returns
    ///
    /// Returns a Result object. If the key is found, it returns an Option containing a reference to the value associated with the key.
    /// If the key or entry does not exist, it returns an error message as a String.
    ///
    fn get_entry_value(
        &mut self,
        entry_name: &String,
        key_name: &String,
    ) -> Result<Option<&DbType>, String> {
        let key_index = self.find_key(key_name)?.0;
        let val = self.find_entry(entry_name)?.0.get(key_index);
        write_log(
            LogSeverity::Verbose,
            &format!("GET entry {} key {}", entry_name, key_name),
            &env!("CARGO_PKG_NAME").to_string(),
        );
        Ok(val)
    }

    /// Returns the number of entries in the collection.
    ///
    /// # Returns
    ///
    /// The number of entries in the collection as a `usize`.
    ///
    pub fn entries_count(&self) -> usize {
        self.entries.len()
    }

    /// Find an entry in the database with the given name.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - The name of the entry to search for.
    ///
    /// # Returns
    ///
    /// Returns a Result containing a mutable reference to the found DbEntry and its index within the entries vector if found.
    /// If the entry is not found, an Err is returned containing an error message.
    ///
    fn find_entry(&mut self, entry_name: &String) -> Result<(&mut DbEntry, usize), String> {
        for (index, entry) in self.entries.iter_mut().enumerate() {
            if entry.name() == entry_name {
                return Ok((entry, index));
            }
        }

        let msg = format!(
            "Entry {} does not exists in table {}",
            entry_name, self.name
        );
        write_log(
            LogSeverity::Error,
            &msg,
            &env!("CARGO_PKG_NAME").to_string(),
        );
        Err(msg)
    }

    /// Checks if an entry with the given name exists in the data structure.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - The name of the entry to look for.
    ///
    /// # Returns
    ///
    /// * `true` - If an entry with the given name exists.
    /// * `false` - If no entry with the given name exists.
    fn entry_exists(&mut self, entry_name: &String) -> bool {
        self.find_entry(entry_name).is_ok()
    }

    /// Finds a key in the database table based on its name.
    ///
    /// # Arguments
    ///
    /// * `key_name` - The name of the key to find.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a tuple `(usize, &DbType)` with the index and a reference to the key,
    /// or a `String` with an error message if the key does not exist.
    ///
    fn find_key(&self, key_name: &String) -> Result<(usize, &DbType), String> {
        for (index, key) in self.keys.iter().enumerate() {
            if &key.0 == key_name {
                return Ok((index, &key.1));
            }
        }

        let msg = format!("Key {} does not exists in table {}", key_name, self.name);
        write_log(
            LogSeverity::Error,
            &msg,
            &env!("CARGO_PKG_NAME").to_string(),
        );
        Err(msg)
    }

    /// Adds a key to the table.
    ///
    /// # Arguments
    ///
    /// * `key_name` - The name of the key.
    /// * `key_type` - The data type of the key.
    ///
    /// # Errors
    ///
    /// Returns an error if `key_type` does not correspond to a known type.
    ///
    pub fn add_key(&mut self, key_name: &String, key_type: &String) -> Result<(), String> {
        self.keys
            .push((key_name.clone(), DbType::default_from_string(key_type)?));

        for entry in self.entries.iter_mut() {
            entry.add_field(None)
        }

        write_log(
            LogSeverity::Info,
            &format!("ADDED key {} to table {}", key_name, self.name),
            &env!("CARGO_PKG_NAME").to_string(),
        );
        Ok(())
    }

    /// Returns the name of the object.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Renames an entry in the system.
    ///
    /// # Arguments
    ///
    /// * `entry_name` - The name of the entry to be renamed.
    /// * `new_name` - The new name for the entry.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the rename operation was successful, otherwise returns an `Err` with an error message.
    pub fn rename_entry(&mut self, entry_name: &String, new_name: &String) -> Result<(), String> {
        self.find_entry(entry_name)?.0.rename(new_name);
        Ok(())
    }

    /// Returns all the entries as a vector of strings.
    ///
    /// If there are no entries, `None` is returned. Otherwise, the names of all the entries are
    /// collected and returned as a vector of strings.
    ///
    /// # Arguments
    /// * `self` - A reference to `self`, an instance of the struct.
    ///
    /// # Returns
    /// * `Option<Vec<String>>` - A vector of strings containing the names of all the entries.
    ///
    pub fn get_all_entries(&self) -> Option<Vec<String>> {
        if self.entries.is_empty() {
            return None;
        }
        Some(
            self.entries
                .iter()
                .map(|entry| entry.name().clone())
                .collect(),
        )
    }

    /// Returns a subset of database entries based on the given entry names.
    ///
    /// # Arguments
    ///
    /// * `entries_subset_names` - An optional vector of entry names to filter the database entries.
    ///
    /// # Returns
    ///
    /// A vector of references to database entries that match the given entry names.
    fn get_entries_subset(&self, entries_subset_names: Option<Vec<&String>>) -> Vec<&DbEntry> {
        self.entries
            .iter()
            .filter(|entry| {
                if let Some(names) = &entries_subset_names {
                    names.contains(&entry.name())
                } else {
                    true
                }
            })
            .collect::<Vec<&DbEntry>>()
    }

    /// Get matching entries based on date criteria.
    ///
    /// This method takes in the following parameters:
    ///
    /// - `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// - `key_name`: A reference to a string value that represents the key used for comparison.
    /// - `criteria`: A `MatchingCriteria` enum value that specifies the type of matching criteria to use.
    /// - `date1`: A `NaiveDate` value representing the first reference date for comparison.
    /// - `date2`: An optional `NaiveDate` value representing the second reference date for comparison. It is required when `criteria` is set to `MatchingCriteria::Between`.
    ///
    /// The method returns a `Result<Option<Vec<String>>, String>`:
    ///
    /// - If the number of entries is zero, it returns `Ok(None)`.
    /// - If a matching error occurs, it returns `Err(String)` with an error message.
    /// - If matching entries are found, it returns `Ok(Some(Vec<String>))` with a vector containing the names of the matching entries.
    ///
    pub fn get_matching_entries_date(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
        criteria: MatchingCriteria,
        date1: NaiveDate,
        date2: Option<NaiveDate>,
    ) -> Result<Option<Vec<String>>, String> {
        if self.entries_count() == 0 {
            return Ok(None);
        }

        // Check input compatibility
        if criteria == MatchingCriteria::Between {
            if date2.is_none() {
                let msg =
                    "Second reference date not defined for Between date comparison".to_string();
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                return Err(msg);
            }
            if let Some(date) = date2 {
                let delta = date - date1;
                if delta.num_days() <= 0 {
                    let msg = "Second reference date is not after first reference date".to_string();
                    write_log(
                        LogSeverity::Error,
                        &msg,
                        &env!("CARGO_PKG_NAME").to_string(),
                    );
                    return Err(msg);
                }
            }
        }

        // Check selected key has a date type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::Date(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::Date(entry_date)) = entry.get(key.0) {
                        let delta = (*entry_date - date1).num_days();
                        match criteria {
                            MatchingCriteria::IsMore => {
                                if delta > 0 {
                                    output.push(entry.name().clone());
                                }
                            }
                            MatchingCriteria::IsLess => {
                                if delta < 0 {
                                    output.push(entry.name().clone());
                                }
                            }
                            MatchingCriteria::Equal => {
                                if delta == 0 {
                                    output.push(entry.name().clone());
                                }
                            }
                            MatchingCriteria::Different => {
                                if delta != 0 {
                                    output.push(entry.name().clone());
                                }
                            }
                            MatchingCriteria::Between => {
                                let delta2 = (*entry_date - date2.unwrap()).num_days();
                                if delta >= 0 && delta2 <= 0 {
                                    output.push(entry.name().clone());
                                }
                            }
                        }
                    }
                }

                if output.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(output))
                }
            }
            _ => {
                let msg = format!("Key {} is not a date", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }

    /// Check condition and log error message.
    ///
    /// # Arguments
    ///
    /// * `condition` - A boolean value representing the condition to be checked.
    /// * `msg` - A static string message to log when the condition is true.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the condition is false.
    /// * `Err(String)` - If the condition is true, containing the formatted error message.
    ///
    fn check_and_log_error(condition: bool, msg: &'static str) -> Result<(), String> {
        if condition {
            let full_msg = format!("Incompatibility between comparison inputs: {}", msg);
            write_log(
                LogSeverity::Error,
                &full_msg,
                &env!("CARGO_PKG_NAME").to_string(),
            );
            return Err(full_msg);
        }
        Ok(())
    }

    /// Checks if the input integers are compatible based on the given matching criteria.
    ///
    /// # Arguments
    ///
    /// * `criteria` - The matching criteria to determine the compatibility.
    /// * `int1` - The first reference integer.
    /// * `int2` - The second reference integer. This argument is an `Option` and can be `None`.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the input integers are compatible. Otherwise, returns `Err` with an error message.
    ///
    /// # Errors
    ///
    /// An error occurs in the following conditions:
    ///
    /// * If `MatchingCriteria::Between` is passed as `criteria`, and `int2` is `None`.
    /// * If `MatchingCriteria::Between` is passed as `criteria`, and `int2` is defined but less than or equal to `int1`.
    ///
    fn check_input_compatibility_int(
        criteria: &MatchingCriteria,
        int1: i32,
        int2: Option<i32>,
    ) -> Result<(), String> {
        if *criteria == MatchingCriteria::Between {
            Self::check_and_log_error(
                int2.is_none(),
                "Second reference integer not defined for Between integer comparison",
            )?;

            if let Some(value) = int2 {
                Self::check_and_log_error(
                    value - int1 <= 0,
                    "Second reference integer is not higher than first reference integer",
                )?;
            }
        }
        Ok(())
    }

    /// Compares an entry value with two integer values using the given matching criteria.
    ///
    /// The function takes an entry value, a matching criteria, an integer value (`int1`), and an optional
    /// second integer value (`int2`). It returns a boolean value indicating whether the comparison
    /// satisfies the matching criteria.
    ///
    /// # Arguments
    ///
    /// * `entry_value` - The value to compare with the integer values.
    /// * `criteria` - The matching criteria to apply.
    /// * `int1` - The first integer value to compare.
    /// * `int2` - An optional second integer value to compare.
    ///
    /// # Returns
    ///
    /// Returns `true` if the comparison satisfies the matching criteria, otherwise `false`.
    ///
    fn integer_comparison(
        entry_value: i32,
        criteria: &MatchingCriteria,
        int1: i32,
        int2: Option<i32>,
    ) -> bool {
        let delta = entry_value - int1;
        match criteria {
            MatchingCriteria::IsMore => delta > 0,
            MatchingCriteria::IsLess => delta < 0,
            MatchingCriteria::Equal => delta == 0,
            MatchingCriteria::Different => delta != 0,
            MatchingCriteria::Between => {
                let delta2 = entry_value - int2.unwrap();
                delta >= 0 && delta2 <= 0
            }
        }
    }

    /// Retrieves entries with a matching integer value based on the specified criteria.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - The name of the key to search for.
    /// * `criteria` - The matching criteria to use.
    /// * `int1` - The first integer value to match.
    /// * `int2` - An optional second integer value to match.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` if no matching entries were found.
    /// * `Ok(Some(output))` with the list of matching entries if any were found.
    /// * `Err(msg)` if an error occurred.
    ///
    /// # Errors
    ///
    /// This function may return an error message if:
    /// * The specified `key_name` is not found in the entries.
    /// * The specified `key_name` does not have an integer type.
    /// * The `criteria` is incompatible with the input.
    ///
    pub fn get_matching_entries_integer(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
        criteria: MatchingCriteria,
        int1: i32,
        int2: Option<i32>,
    ) -> Result<Option<Vec<String>>, String> {
        if self.entries_count() == 0 {
            return Ok(None);
        }
        // Check input compatibility
        Self::check_input_compatibility_int(&criteria, int1, int2)?;

        // Check selected key has an integer type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::Integer(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::Integer(entry_int)) = entry.get(key.0) {
                        if Self::integer_comparison(*entry_int, &criteria, int1, int2) {
                            output.push(entry.name().clone());
                        }
                    }
                }

                if output.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(output))
                }
            }
            _ => {
                let msg = format!("Key {} is not an integer", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }

    /// Retrieves entries with a matching unsigned integer value based on the given criteria and key.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - The name of the key to match against.
    /// * `criteria` - The matching criteria to apply.
    /// * `int1` - The first unsigned integer value to compare against.
    /// * `int2` - An optional second unsigned integer value to compare against.
    ///
    /// # Returns
    ///
    /// * `Result<Option<Vec<String>>, String>` - Returns `Ok(None)` if no matching entries are found. Otherwise, returns `Ok(Some(output))` where `output` is a vector of matching entry names.
    /// * `Result` will return `Err` if the selected key is not an unsigned integer or an error occurs during processing.
    ///
    pub fn get_matching_entries_unsigned_integer(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
        criteria: MatchingCriteria,
        int1: u32,
        int2: Option<u32>,
    ) -> Result<Option<Vec<String>>, String> {
        if self.entries_count() == 0 {
            return Ok(None);
        }
        // Check input compatibility
        Self::check_input_compatibility_int(&criteria, int1 as i32, int2.map(|v| v as i32))?;

        // Check selected key has an unsigned int type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::UnsignedInt(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::UnsignedInt(entry_int)) = entry.get(key.0) {
                        if Self::integer_comparison(
                            *entry_int as i32,
                            &criteria,
                            int1 as i32,
                            int2.map(|v| v as i32),
                        ) {
                            output.push(entry.name().clone());
                        }
                    }
                }

                if output.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(output))
                }
            }
            _ => {
                let msg = format!("Key {} is not an unsigned integer", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }

    /// Get matching entries based on float comparison criteria.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - The name of the key to perform the comparison on.
    /// * `criteria` - The matching criteria to use for comparison.
    /// * `float1` - The first reference float for comparison.
    /// * `float2` - The optional second reference float for comparison (only used for `Between` criteria).
    ///
    /// # Returns
    ///
    /// Returns a `Result` with an optional vector of matching entry names if successful, or an error message if unsuccessful.
    ///
    /// # Errors
    ///
    /// Returns an error message if any of the following conditions are met:
    ///
    /// * The number of entries is zero.
    /// * The second reference float is not defined for `Between` criteria.
    /// * The second reference float is not higher than the first reference float for `Between` criteria.
    /// * The selected key is not of float type.
    ///
    pub fn get_matching_entries_float(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
        criteria: MatchingCriteria,
        float1: f32,
        float2: Option<f32>,
    ) -> Result<Option<Vec<String>>, String> {
        if self.entries_count() == 0 {
            return Ok(None);
        }
        // Check input compatibility
        if criteria == MatchingCriteria::Between {
            if float2.is_none() {
                let msg =
                    "Second reference float not defined for Between integer comparison".to_string();
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                return Err(msg);
            }
            if let Some(value) = float2 {
                if value - float1 <= 0.0 {
                    let msg = "Second reference float is not higher than first reference float"
                        .to_string();
                    write_log(
                        LogSeverity::Error,
                        &msg,
                        &env!("CARGO_PKG_NAME").to_string(),
                    );
                    return Err(msg);
                }
            }
        }

        // Check selected key has a float type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::Float(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::Float(entry_float)) = entry.get(key.0) {
                        let delta = entry_float - float1;
                        match criteria {
                            MatchingCriteria::IsMore => {
                                if delta > 0.0 {
                                    output.push(entry.name().clone());
                                }
                            }
                            MatchingCriteria::IsLess => {
                                if delta < 0.0 {
                                    output.push(entry.name().clone());
                                }
                            }
                            MatchingCriteria::Equal => {
                                if delta == 0.0 {
                                    output.push(entry.name().clone());
                                }
                            }
                            MatchingCriteria::Different => {
                                if delta != 0.0 {
                                    output.push(entry.name().clone());
                                }
                            }
                            MatchingCriteria::Between => {
                                let delta2 = entry_float - float2.unwrap();
                                if delta >= 0.0 && delta2 <= 0.0 {
                                    output.push(entry.name().clone());
                                }
                            }
                        }
                    }
                }

                if output.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(output))
                }
            }
            _ => {
                let msg = format!("Key {} is not a float", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }

    /// Returns matching entries based on the provided key, matching criteria, and reference bool value.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - The name of the key to match.
    /// * `criteria` - The matching criteria (Equal or Different).
    /// * `ref_bool` - The reference bool value for comparison.
    ///
    /// # Returns
    ///
    /// Returns `Ok(None)` if there are no entries in the collection. Otherwise, returns `Ok(Some(output))`
    /// where `output` is a vector of strings containing the names of the matching entries.
    /// If the selected key is not of boolean type, returns `Err(msg)` with the error message.
    ///
    pub fn get_matching_entries_bool(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
        criteria: MatchingCriteria,
        ref_bool: bool,
    ) -> Result<Option<Vec<String>>, String> {
        if self.entries_count() == 0 {
            return Ok(None);
        }
        // Check selected key has a bool type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::Bool(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::Bool(entry_bool)) = entry.get(key.0) {
                        match criteria {
                            MatchingCriteria::Equal => {
                                if ref_bool == *entry_bool {
                                    output.push(entry.name().clone());
                                }
                            }
                            MatchingCriteria::Different => {
                                if ref_bool != *entry_bool {
                                    output.push(entry.name().clone());
                                }
                            }
                            _ => {
                                let msg = "Only Equal and Different matching criteria are allowed for Boolean data".to_string();
                                write_log(
                                    LogSeverity::Error,
                                    &msg,
                                    &env!("CARGO_PKG_NAME").to_string(),
                                );
                                return Err(msg);
                            }
                        }
                    }
                }

                if output.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(output))
                }
            }
            _ => {
                let msg = format!("Key {} is not a boolean", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }

    /// Returns a vector of entry names that match the given criteria for a specific key.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - The name of the key to be matched.
    /// * `criteria` - The matching criteria to be applied.
    /// * `ref_str` - The reference string to compare against.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` - If there are no entries or the selected key is not present in the entries.
    /// * `Ok(Some(output))` - If matching entries are found, returns a vector of their names.
    /// * `Err(msg)` - If there is an error, such as invalid matching criteria or the selected key not being of string type.
    ///
    pub fn get_matching_entries_string(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
        criteria: MatchingCriteria,
        ref_str: &String,
    ) -> Result<Option<Vec<String>>, String> {
        if self.entries_count() == 0 {
            return Ok(None);
        }
        // Check selected key has a String type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::String(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::String(entry_str)) = entry.get(key.0) {
                        match criteria {
                            MatchingCriteria::Equal => {
                                if ref_str == entry_str {
                                    output.push(entry.name().clone());
                                }
                            }
                            MatchingCriteria::Different => {
                                if ref_str != entry_str {
                                    output.push(entry.name().clone());
                                }
                            }
                            _ => {
                                let msg = "Only Equal and Different matching criteria are allowed for String data".to_string();
                                write_log(
                                    LogSeverity::Error,
                                    &msg,
                                    &env!("CARGO_PKG_NAME").to_string(),
                                );
                                return Err(msg);
                            }
                        }
                    }
                }

                if output.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(output))
                }
            }
            _ => {
                let msg = format!("Key {} is not a string", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }

    /// Retrieves entries with no value for a given key name.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - The name of the key to search for.
    ///
    /// # Returns
    ///
    /// * `Result<Option<Vec<String>>, String>` - A result that either contains `None` if there are no entries or `Some(output)` which is a vector of entry names with no value for the given key name.
    ///
    pub fn get_entries_none(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
    ) -> Result<Option<Vec<String>>, String> {
        if self.entries_count() == 0 {
            return Ok(None);
        }
        let key = self.find_key(key_name)?;
        let mut output = Vec::new();

        for entry in self.get_entries_subset(entries_subset) {
            if entry.get(key.0).is_none() {
                output.push(entry.name().clone())
            }
        }

        if output.is_empty() {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }

    /// Retrieves entries that have a non-null value for a given key.
    ///
    /// # Arguments
    ///
    /// - `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// - `key_name`: The name of the key to search for.
    ///
    /// # Returns
    ///
    /// - `Ok(None)`: If the entries collection is empty.
    /// - `Ok(Some(output))`: A `Vec<String>` containing the names of entries where the provided key has a non-null value.
    /// - `Err(err)`: If there was an error while searching for the key.
    ///
    pub fn get_entries_some(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
    ) -> Result<Option<Vec<String>>, String> {
        if self.entries_count() == 0 {
            return Ok(None);
        }
        let key = self.find_key(key_name)?;
        let mut output = Vec::new();

        for entry in self.get_entries_subset(entries_subset) {
            if entry.get(key.0).is_some() {
                output.push(entry.name().clone())
            }
        }

        if output.is_empty() {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }

    /// Returns a vector of unique boolean values for the specified key.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - A reference to the name of the key.
    ///
    /// # Returns
    ///
    /// * If the database is empty, returns `Ok(None)`.
    /// * If the selected key is of boolean type, returns `Ok(Some(Vec<bool>))` with a vector of unique boolean values.
    /// * If the selected key is not of boolean type, logs an error message and returns `Err(String)`.
    ///
    pub fn get_unique_boolean_values_for_key(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
    ) -> Result<Option<Vec<bool>>, String> {
        if self.entries.is_empty() {
            return Ok(None);
        }

        // Check selected key has a bool type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::Bool(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::Bool(val)) = entry.get(key.0) {
                        if !output.contains(val) {
                            output.push(*val);
                        }
                    }
                }

                if !output.is_empty() {
                    Ok(Some(output))
                } else {
                    Ok(None)
                }
            }
            _ => {
                let msg = format!("Key {} is not a bool", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }

    /// Returns a vector of unique integer values for a given key.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - A reference to a string containing the name of the key.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(Vec<i32>)` - If the key is found and is of type `Integer`, returns a vector containing unique integer values for that key.
    /// * `Ok(None)` - If the key is not found or the vector is empty, returns `None`.
    /// * `Err(String)` - If the key is found but is not of type `Integer`, returns an error message.
    pub fn get_unique_integer_values_for_key(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
    ) -> Result<Option<Vec<i32>>, String> {
        if self.entries.is_empty() {
            return Ok(None);
        }

        // Check selected key has a bool type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::Integer(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::Integer(val)) = entry.get(key.0) {
                        if !output.contains(val) {
                            output.push(*val);
                        }
                    }
                }

                if !output.is_empty() {
                    Ok(Some(output))
                } else {
                    Ok(None)
                }
            }
            _ => {
                let msg = format!("Key {} is not an integer", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }

    /// Retrieves unique unsigned integer values for a given key.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - The name of the key to retrieve values for.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(values))` - If the key exists and has unsigned integer values, returns a `Vec<u32>` containing unique values.
    /// * `Ok(None)` - If the key does not exist or has no unsigned integer values.
    /// * `Err(error_message)` - If the key exists but is not of unsigned integer type.
    ///
    pub fn get_unique_unsigned_integer_values_for_key(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
    ) -> Result<Option<Vec<u32>>, String> {
        if self.entries.is_empty() {
            return Ok(None);
        }
        // Check the selected key has an unsigned int type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::UnsignedInt(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::UnsignedInt(val)) = entry.get(key.0) {
                        if !output.contains(val) {
                            output.push(*val);
                        }
                    }
                }
                if !output.is_empty() {
                    Ok(Some(output))
                } else {
                    Ok(None)
                }
            }
            _ => {
                let msg = format!("Key {} is not an unsigned integer", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }

    /// Gets the unique string values associated with a given key.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - The name of the key for which to get the unique string values.
    ///
    /// # Returns
    ///
    /// * If the database entries is empty, the function returns `Ok(None)`.
    /// * If the selected key is not a string, the function returns an `Err` with an error message.
    /// * Otherwise, the function returns `Ok(Some(vec))` where `vec` is a vector containing the unique string values.
    ///
    pub fn get_unique_string_values_for_key(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
    ) -> Result<Option<Vec<String>>, String> {
        if self.entries.is_empty() {
            return Ok(None);
        }
        // Check the selected key has a string type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::String(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::String(val)) = entry.get(key.0) {
                        if !output.contains(val) {
                            output.push(val.clone());
                        }
                    }
                }
                if !output.is_empty() {
                    Ok(Some(output))
                } else {
                    Ok(None)
                }
            }
            _ => {
                let msg = format!("Key {} is not a string", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }

    /// Retrieves unique float values for a given key name.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - The name of the key to search for.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` - If no entries exist in the data structure.
    /// * `Ok(Some(output))` - A vector containing unique float values for the given key.
    /// * `Err(msg)` - If the key is not of float type.
    ///
    pub fn get_unique_float_values_for_key(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
    ) -> Result<Option<Vec<f32>>, String> {
        if self.entries.is_empty() {
            return Ok(None);
        }

        // Check the selected key has a float type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::Float(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::Float(val)) = entry.get(key.0) {
                        if !output.contains(val) {
                            output.push(*val);
                        }
                    }
                }
                if !output.is_empty() {
                    Ok(Some(output))
                } else {
                    Ok(None)
                }
            }
            _ => {
                let msg = format!("Key {} is not a float", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }

    /// Returns unique date values associated with a given key.
    ///
    /// # Arguments
    ///
    /// * `entries_subset`: An optional vector containing references to string values. It specifies a subset of entries to consider. If `None`, all entries are considered.
    /// * `key_name` - The name of the key for which to retrieve unique date values.
    ///
    /// # Returns
    ///
    /// * If the `entries` vector is empty, returns `Ok(None)`.
    /// * If the selected key has a `DbType::Date` type, returns `Ok(Some(output))` where `output`
    ///   is a vector containing the unique date values associated with the key.
    /// * If the selected key is not a `DbType::Date` type, logs an error and returns `Err(msg)`,
    ///   where `msg` is a string indicating that the key is not a date.
    ///
    pub fn get_unique_date_values_for_key(
        &self,
        entries_subset: Option<Vec<&String>>,
        key_name: &String,
    ) -> Result<Option<Vec<NaiveDate>>, String> {
        if self.entries.is_empty() {
            return Ok(None);
        }
        // Check the selected key has a date type
        let key = self.find_key(key_name)?;
        match key.1 {
            DbType::Date(_) => {
                let mut output = Vec::new();
                for entry in self.get_entries_subset(entries_subset) {
                    if let Some(DbType::Date(val)) = entry.get(key.0) {
                        if !output.contains(val) {
                            output.push(*val);
                        }
                    }
                }
                if !output.is_empty() {
                    Ok(Some(output))
                } else {
                    Ok(None)
                }
            }
            _ => {
                let msg = format!("Key {} is not a date", key_name);
                write_log(
                    LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use rusttests::{check_option, check_result, check_struct, check_value, CheckType};

    use super::{DbTable, DbType, MatchingCriteria};

    #[test]
    fn new_table_none() -> Result<(), String> {
        let table = DbTable::new("Table".to_string(), None);

        let expected = DbTable {
            name: "Table".to_string(),
            keys: Vec::new(),
            entries: Vec::new(),
        };

        check_struct((1, 1), &table, &expected, CheckType::Equal)?;
        Ok(())
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

        check_struct((1, 1), &table, &expected, CheckType::Equal)?;
        Ok(())
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

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        check_value((1, 1), &table.entries_count(), &2, CheckType::Equal)?;
        Ok(())
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

        check_result(
            (1, 1),
            table.add_entry(&"entry1".to_string(), new_entry),
            false,
        )?;

        table.add_entry(&"entry2".to_string(), None)?;

        check_value((1, 2), &table.entries_count(), &1, CheckType::Equal)?;
        Ok(())
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

        check_result(
            (1, 1),
            table.add_entry(&"entry1".to_string(), new_entry),
            false,
        )?;

        table.add_entry(&"entry2".to_string(), None)?;

        check_value((1, 2), &table.entries_count(), &1, CheckType::Equal)?;
        Ok(())
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

        table.add_entry(&"entry1".to_string(), new_entry)?;

        check_result((1, 1), table.add_entry(&"entry1".to_string(), None), false)?;
        check_value((1, 2), &table.entries_count(), &1, CheckType::Equal)?;
        Ok(())
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

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        table.update_entry(
            &"entry1".to_string(),
            &"key3".to_string(),
            Some(DbType::Float(5.98)),
        )?;
        table.update_entry(
            &"entry2".to_string(),
            &"key2".to_string(),
            Some(DbType::String("Some value".to_string())),
        )?;

        let val = check_option(
            (1, 1),
            table.get_entry_value(&"entry1".to_string(), &"key3".to_string())?,
            true,
        )?
        .unwrap();
        check_struct((1, 2), val, &DbType::Float(5.98), CheckType::Equal)?;

        let val = check_option(
            (2, 1),
            table.get_entry_value(&"entry2".to_string(), &"key2".to_string())?,
            true,
        )?
        .unwrap();
        check_struct(
            (2, 2),
            val,
            &DbType::String("Some value".to_string()),
            CheckType::Equal,
        )?;
        Ok(())
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

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        table.update_entry(&"entry1".to_string(), &"key1".to_string(), None)?;

        check_option(
            (1, 1),
            table.get_entry_value(&"entry1".to_string(), &"key1".to_string())?,
            false,
        )?;
        Ok(())
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

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        check_result(
            (1, 1),
            table.update_entry(&"entry5".to_string(), &"key2".to_string(), None),
            false,
        )?;
        Ok(())
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

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        check_result(
            (1, 1),
            table.update_entry(&"entry2".to_string(), &"key4".to_string(), None),
            false,
        )?;
        Ok(())
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

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        check_result(
            (1, 1),
            table.update_entry(
                &"entry2".to_string(),
                &"key1".to_string(),
                Some(DbType::Float(0.25)),
            ),
            false,
        )?;
        Ok(())
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

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        table.update_entry_string(
            &"entry1".to_string(),
            &"key2".to_string(),
            Some("New value".to_string()),
        )?;

        let val = check_option(
            (1, 1),
            table.get_entry_value_string(&"entry1".to_string(), &"key2".to_string())?,
            true,
        )?
        .unwrap();
        check_value((1, 2), &val, &"New value".to_string(), CheckType::Equal)?;
        Ok(())
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

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        check_result(
            (1, 1),
            table.update_entry_string(
                &"entry1".to_string(),
                &"key1".to_string(),
                Some("New value".to_string()),
            ),
            false,
        )?;

        let val = check_option(
            (2, 1),
            table.get_entry_value(&"entry1".to_string(), &"key1".to_string())?,
            true,
        )?
        .unwrap();
        check_struct((2, 2), val, &DbType::Integer(1), CheckType::Equal)?;
        Ok(())
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

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        table.update_entry_string(&"entry1".to_string(), &"key2".to_string(), None)?;

        check_option(
            (1, 1),
            table.get_entry_value_string(&"entry1".to_string(), &"key2".to_string())?,
            false,
        )?;
        Ok(())
    }

    #[test]
    fn get_entry_string() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::UnsignedInt(0)),
            ("key3".to_string(), DbType::Float(0.0)),
            ("key4".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("-12".to_string()),
            Some("45".to_string()),
            Some("2.23".to_string()),
            None,
        ];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), None)?;
        table.add_entry(&"entry2".to_string(), new_entry)?;

        let val = check_option(
            (1, 1),
            table.get_entry_value_string(&"entry2".to_string(), &"key1".to_string())?,
            true,
        )?
        .unwrap();
        check_value((1, 2), &val, &"-12".to_string(), CheckType::Equal)?;

        let val = check_option(
            (2, 1),
            table.get_entry_value_string(&"entry2".to_string(), &"key2".to_string())?,
            true,
        )?
        .unwrap();
        check_value((2, 2), &val, &"45".to_string(), CheckType::Equal)?;

        let val = check_option(
            (3, 1),
            table.get_entry_value_string(&"entry2".to_string(), &"key3".to_string())?,
            true,
        )?
        .unwrap();
        check_value((3, 2), &val, &"2.23".to_string(), CheckType::Equal)?;

        check_option(
            (4, 1),
            table.get_entry_value_string(&"entry2".to_string(), &"key4".to_string())?,
            false,
        )?;
        Ok(())
    }

    #[test]
    fn update_entry_integer() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        table.update_entry_integer(&"entry1".to_string(), &"key1".to_string(), Some(-66))?;

        let val = check_option(
            (1, 1),
            table.get_entry_value_integer(&"entry1".to_string(), &"key1".to_string())?,
            true,
        )?
        .unwrap();
        check_value((1, 2), val, &-66, CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn get_entry_integer_wrong_type() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        check_result(
            (1, 1),
            table.get_entry_value_integer(&"entry1".to_string(), &"key2".to_string()),
            false,
        )?;
        Ok(())
    }

    #[test]
    fn update_entry_uinteger() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::UnsignedInt(0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("12".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        table.update_entry_unsigned_integer(
            &"entry1".to_string(),
            &"key3".to_string(),
            Some(66),
        )?;

        let val = check_option(
            (1, 1),
            table.get_entry_value_unsigned_integer(&"entry1".to_string(), &"key3".to_string())?,
            true,
        )?
        .unwrap();
        check_value((1, 2), val, &66, CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn get_entry_uinteger_wrong_type() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::UnsignedInt(0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("14".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        check_result(
            (1, 1),
            table.get_entry_value_unsigned_integer(&"entry1".to_string(), &"key2".to_string()),
            false,
        )?;
        Ok(())
    }

    #[test]
    fn update_entry_float() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("12.56".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        table.update_entry_float(&"entry1".to_string(), &"key3".to_string(), Some(66.99))?;

        let val = check_option(
            (1, 1),
            table.get_entry_value_float(&"entry1".to_string(), &"key3".to_string())?,
            true,
        )?
        .unwrap();
        check_value((1, 2), val, &66.99, CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn get_entry_float_wrong_type() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("14.74".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        check_result(
            (1, 1),
            table.get_entry_value_float(&"entry1".to_string(), &"key2".to_string()),
            false,
        )?;
        Ok(())
    }

    #[test]
    fn update_entry_bool() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Bool(false)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("false".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        table.update_entry_bool(&"entry1".to_string(), &"key3".to_string(), Some(true))?;

        let val = check_option(
            (1, 1),
            table.get_entry_value_bool(&"entry1".to_string(), &"key3".to_string())?,
            true,
        )?
        .unwrap();
        check_value((1, 2), val, &true, CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn get_entry_bool_wrong_type() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Bool(false)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("true".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        check_result(
            (1, 1),
            table.get_entry_value_bool(&"entry1".to_string(), &"key2".to_string()),
            false,
        )?;
        Ok(())
    }

    #[test]
    fn update_entry_date() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            (
                "key3".to_string(),
                DbType::default_from_string(&"Date".to_string())?,
            ),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("15/08/2016".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        table.update_entry_date(
            &"entry1".to_string(),
            &"key3".to_string(),
            Some(NaiveDate::from_ymd_opt(1789, 7, 14).unwrap()),
        )?;

        let val = check_option(
            (1, 1),
            table.get_entry_value_date(&"entry1".to_string(), &"key3".to_string())?,
            true,
        )?
        .unwrap();
        check_value(
            (1, 2),
            val,
            &NaiveDate::from_ymd_opt(1789, 7, 14).unwrap(),
            CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn get_entry_date_wrong_type() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            (
                "key3".to_string(),
                DbType::default_from_string(&"Date".to_string())?,
            ),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("15/08/2016".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        check_result(
            (1, 1),
            table.get_entry_value_date(&"entry1".to_string(), &"key2".to_string()),
            false,
        )?;
        Ok(())
    }

    #[test]
    fn add_key_nominal() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("14.74".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        table.add_key(&"key_new".to_string(), &"UnsignedInt".to_string())?;

        check_option(
            (1, 1),
            table.get_entry_value(&"entry1".to_string(), &"key_new".to_string())?,
            false,
        )?;
        check_option(
            (1, 2),
            table.get_entry_value(&"entry2".to_string(), &"key_new".to_string())?,
            false,
        )?;
        Ok(())
    }

    #[test]
    fn add_key_wrong_name() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("14.74".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        check_result(
            (1, 1),
            table.add_key(&"key_new".to_string(), &"RandomType".to_string()),
            false,
        )?;
        Ok(())
    }

    #[test]
    fn remove_entry() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("14.74".to_string())];
        let new_entry = Some(&mut binding);
        let mut binding2 = vec![Some("3".to_string()), None, Some("32".to_string())];
        let new_entry2 = Some(&mut binding2);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;

        table.remove_entry(&"entry2".to_string())?;

        check_value((1, 1), &table.entries_count(), &2, CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn remove_entry_wrong_name() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("14.74".to_string())];
        let new_entry = Some(&mut binding);
        let mut binding2 = vec![Some("3".to_string()), None, Some("32".to_string())];
        let new_entry2 = Some(&mut binding2);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;

        check_result((1, 1), table.remove_entry(&"entry4".to_string()), false)?;
        check_value((1, 2), &table.entries_count(), &3, CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn rename_entry() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        check_result(
            (1, 1),
            table.get_entry_value_string(&"entry1".to_string(), &"key1".to_string()),
            true,
        )?;

        table.rename_entry(&"entry1".to_string(), &"entry99".to_string())?;
        check_result(
            (1, 2),
            table.get_entry_value_string(&"entry1".to_string(), &"key1".to_string()),
            false,
        )?;
        check_result(
            (1, 3),
            table.get_entry_value_string(&"entry99".to_string(), &"key1".to_string()),
            true,
        )?;

        Ok(())
    }

    #[test]
    fn get_all_entries() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("2".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("3".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry5".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_value(
            (1, 1),
            &table.get_all_entries(),
            &Some(vec![
                "entry1".to_string(),
                "entry2".to_string(),
                "entry5".to_string(),
                "entry4".to_string(),
            ]),
            CheckType::Equal,
        )
    }

    #[test]
    fn get_all_entries_empty() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let table = DbTable::new("Table".to_string(), Some(keys));

        check_value((1, 1), &table.get_all_entries(), &None, CheckType::Equal)
    }

    #[test]
    fn get_entries_matching_date_error() -> Result<(), String> {
        let keys = vec![
            (
                "key1".to_string(),
                DbType::Date(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()),
            ),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("13/03/2014".to_string()),
            None,
            Some("2.23".to_string()),
        ];
        let mut binding2 = vec![
            Some("14/03/2014".to_string()),
            None,
            Some("1.46".to_string()),
        ];
        let mut binding3 = vec![
            Some("13/08/2024".to_string()),
            None,
            Some("-0.27".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_matching_entries_date(
                None,
                &"key2".to_string(),
                MatchingCriteria::Equal,
                NaiveDate::from_ymd_opt(2000, 12, 31).unwrap(),
                None,
            ),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_matching_entries_date(
                None,
                &"key1".to_string(),
                MatchingCriteria::Between,
                NaiveDate::from_ymd_opt(2000, 12, 31).unwrap(),
                None,
            ),
            false,
        )?;
        check_result(
            (3, 1),
            table.get_matching_entries_date(
                None,
                &"key1".to_string(),
                MatchingCriteria::Between,
                NaiveDate::from_ymd_opt(2000, 12, 31).unwrap(),
                Some(NaiveDate::from_ymd_opt(2000, 12, 31).unwrap()),
            ),
            false,
        )?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_date() -> Result<(), String> {
        let keys = vec![
            (
                "key1".to_string(),
                DbType::Date(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()),
            ),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("13/03/2014".to_string()),
            None,
            Some("2.23".to_string()),
        ];
        let mut binding2 = vec![
            Some("14/03/2014".to_string()),
            None,
            Some("1.46".to_string()),
        ];
        let mut binding3 = vec![
            Some("13/08/2024".to_string()),
            None,
            Some("-0.27".to_string()),
        ];
        let mut binding4 = vec![
            Some("13/03/2014".to_string()),
            None,
            Some("-0.27".to_string()),
        ];
        let mut binding5 = vec![
            Some("10/03/2014".to_string()),
            None,
            Some("-0.27".to_string()),
        ];
        let mut binding6 = vec![
            Some("15/03/2014".to_string()),
            None,
            Some("-0.27".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);
        let new_entry5 = Some(&mut binding5);
        let new_entry6 = Some(&mut binding6);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), new_entry2)?;
        table.add_entry(&"entry3".to_string(), new_entry3)?;
        table.add_entry(&"entry4".to_string(), new_entry4)?;
        table.add_entry(&"entry5".to_string(), new_entry5)?;
        table.add_entry(&"entry6".to_string(), new_entry6)?;

        // Equality
        let expected_vec = vec!["entry1".to_string(), "entry4".to_string()];
        let res = check_result(
            (1, 1),
            table.get_matching_entries_date(
                None,
                &"key1".to_string(),
                MatchingCriteria::Equal,
                NaiveDate::from_ymd_opt(2014, 3, 13).unwrap(),
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec, CheckType::Equal)?;

        // No match
        let res = check_result(
            (2, 1),
            table.get_matching_entries_date(
                None,
                &"key1".to_string(),
                MatchingCriteria::Equal,
                NaiveDate::from_ymd_opt(2015, 3, 13).unwrap(),
                None,
            ),
            true,
        )?
        .unwrap();
        check_option((2, 2), res, false)?;

        // Different
        let expected_vec = vec![
            "entry2".to_string(),
            "entry3".to_string(),
            "entry5".to_string(),
            "entry6".to_string(),
        ];
        let res = check_result(
            (3, 1),
            table.get_matching_entries_date(
                None,
                &"key1".to_string(),
                MatchingCriteria::Different,
                NaiveDate::from_ymd_opt(2014, 3, 13).unwrap(),
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((3, 2), res, true)?.unwrap();
        check_value((3, 3), &opt, &expected_vec, CheckType::Equal)?;

        // After
        let expected_vec = vec!["entry3".to_string(), "entry6".to_string()];
        let res = check_result(
            (4, 1),
            table.get_matching_entries_date(
                None,
                &"key1".to_string(),
                MatchingCriteria::IsMore,
                NaiveDate::from_ymd_opt(2014, 3, 14).unwrap(),
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((4, 2), res, true)?.unwrap();
        check_value((4, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Before
        let expected_vec = vec![
            "entry1".to_string(),
            "entry2".to_string(),
            "entry4".to_string(),
            "entry5".to_string(),
        ];
        let res = check_result(
            (5, 1),
            table.get_matching_entries_date(
                None,
                &"key1".to_string(),
                MatchingCriteria::IsLess,
                NaiveDate::from_ymd_opt(2014, 3, 15).unwrap(),
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((5, 2), res, true)?.unwrap();
        check_value((5, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Between
        let expected_vec = vec![
            "entry1".to_string(),
            "entry2".to_string(),
            "entry4".to_string(),
            "entry6".to_string(),
        ];
        let res = check_result(
            (6, 1),
            table.get_matching_entries_date(
                None,
                &"key1".to_string(),
                MatchingCriteria::Between,
                NaiveDate::from_ymd_opt(2014, 3, 13).unwrap(),
                Some(NaiveDate::from_ymd_opt(2014, 3, 15).unwrap()),
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((6, 2), res, true)?.unwrap();
        check_value((6, 3), &opt, &expected_vec, CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn get_entries_none() -> Result<(), String> {
        let keys = vec![
            (
                "key1".to_string(),
                DbType::Date(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()),
            ),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("13/03/2014".to_string()),
            None,
            Some("2.23".to_string()),
        ];
        let mut binding2 = vec![Some("14/03/2014".to_string()), None, None];
        let mut binding3 = vec![
            Some("13/08/2024".to_string()),
            None,
            Some("-0.27".to_string()),
        ];
        let mut binding4 = vec![Some("13/03/2014".to_string()), None, None];
        let mut binding5 = vec![
            Some("10/03/2014".to_string()),
            None,
            Some("-0.27".to_string()),
        ];
        let mut binding6 = vec![
            Some("15/03/2014".to_string()),
            None,
            Some("-0.27".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);
        let new_entry5 = Some(&mut binding5);
        let new_entry6 = Some(&mut binding6);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), new_entry2)?;
        table.add_entry(&"entry3".to_string(), new_entry3)?;
        table.add_entry(&"entry4".to_string(), new_entry4)?;
        table.add_entry(&"entry5".to_string(), new_entry5)?;
        table.add_entry(&"entry6".to_string(), new_entry6)?;

        // None
        let expected_vec = vec!["entry2".to_string(), "entry4".to_string()];
        let res = check_result(
            (1, 1),
            table.get_entries_none(None, &"key3".to_string()),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Some
        let expected_vec = vec![
            "entry1".to_string(),
            "entry3".to_string(),
            "entry5".to_string(),
            "entry6".to_string(),
        ];
        let res = check_result(
            (2, 1),
            table.get_entries_some(None, &"key3".to_string()),
            true,
        )?
        .unwrap();
        let opt = check_option((2, 2), res, true)?.unwrap();
        check_value((2, 3), &opt, &expected_vec, CheckType::Equal)?;

        // No Some
        let res = check_result(
            (3, 1),
            table.get_entries_some(None, &"key2".to_string()),
            true,
        )?
        .unwrap();
        check_option((3, 2), res, false)?;

        // No None
        let res = check_result(
            (4, 1),
            table.get_entries_none(None, &"key1".to_string()),
            true,
        )?
        .unwrap();
        check_option((4, 2), res, false)?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_bool_error() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Bool(false)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("true".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("false".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("true".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_matching_entries_bool(
                None,
                &"key2".to_string(),
                MatchingCriteria::Equal,
                false,
            ),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_matching_entries_bool(
                None,
                &"key1".to_string(),
                MatchingCriteria::Between,
                true,
            ),
            false,
        )?;
        check_result(
            (3, 1),
            table.get_matching_entries_bool(
                None,
                &"key1".to_string(),
                MatchingCriteria::IsLess,
                true,
            ),
            false,
        )?;
        check_result(
            (4, 1),
            table.get_matching_entries_bool(
                None,
                &"key1".to_string(),
                MatchingCriteria::IsMore,
                true,
            ),
            false,
        )?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_bool() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Bool(false)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Bool(false)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("true".to_string()), None, Some("false".to_string())];
        let mut binding2 = vec![Some("true".to_string()), None, Some("false".to_string())];
        let mut binding3 = vec![Some("false".to_string()), None, Some("false".to_string())];
        let mut binding4 = vec![Some("false".to_string()), None, Some("false".to_string())];
        let mut binding5 = vec![Some("true".to_string()), None, Some("false".to_string())];
        let mut binding6 = vec![Some("false".to_string()), None, Some("false".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);
        let new_entry5 = Some(&mut binding5);
        let new_entry6 = Some(&mut binding6);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), new_entry2)?;
        table.add_entry(&"entry3".to_string(), new_entry3)?;
        table.add_entry(&"entry4".to_string(), new_entry4)?;
        table.add_entry(&"entry5".to_string(), new_entry5)?;
        table.add_entry(&"entry6".to_string(), new_entry6)?;

        // Equality True
        let expected_vec = vec![
            "entry1".to_string(),
            "entry2".to_string(),
            "entry5".to_string(),
        ];
        let res = check_result(
            (1, 1),
            table.get_matching_entries_bool(
                None,
                &"key1".to_string(),
                MatchingCriteria::Equal,
                true,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec, CheckType::Equal)?;

        // No match
        let res = check_result(
            (2, 1),
            table.get_matching_entries_bool(
                None,
                &"key3".to_string(),
                MatchingCriteria::Equal,
                true,
            ),
            true,
        )?
        .unwrap();
        check_option((2, 2), res, false)?;

        // Different
        let expected_vec = vec![
            "entry3".to_string(),
            "entry4".to_string(),
            "entry6".to_string(),
        ];
        let res = check_result(
            (3, 1),
            table.get_matching_entries_bool(
                None,
                &"key1".to_string(),
                MatchingCriteria::Different,
                true,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((3, 2), res, true)?.unwrap();
        check_value((3, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Equality False
        let expected_vec = vec![
            "entry3".to_string(),
            "entry4".to_string(),
            "entry6".to_string(),
        ];
        let res = check_result(
            (4, 1),
            table.get_matching_entries_bool(
                None,
                &"key1".to_string(),
                MatchingCriteria::Equal,
                false,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((4, 2), res, true)?.unwrap();
        check_value((4, 3), &opt, &expected_vec, CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_string_error() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Bool(false)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("true".to_string()),
            Some("toto".to_string()),
            Some("2.23".to_string()),
        ];
        let mut binding2 = vec![
            Some("false".to_string()),
            Some("tata".to_string()),
            Some("1.46".to_string()),
        ];
        let mut binding3 = vec![
            Some("true".to_string()),
            Some("titi".to_string()),
            Some("-0.27".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_matching_entries_string(
                None,
                &"key1".to_string(),
                MatchingCriteria::Equal,
                &"toto".to_string(),
            ),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_matching_entries_string(
                None,
                &"key2".to_string(),
                MatchingCriteria::Between,
                &"toto".to_string(),
            ),
            false,
        )?;
        check_result(
            (3, 1),
            table.get_matching_entries_string(
                None,
                &"key2".to_string(),
                MatchingCriteria::IsLess,
                &"toto".to_string(),
            ),
            false,
        )?;
        check_result(
            (4, 1),
            table.get_matching_entries_string(
                None,
                &"key2".to_string(),
                MatchingCriteria::IsMore,
                &"toto".to_string(),
            ),
            false,
        )?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_string() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Bool(false)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Bool(false)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("true".to_string()),
            Some("toto".to_string()),
            Some("false".to_string()),
        ];
        let mut binding2 = vec![
            Some("true".to_string()),
            Some("tata".to_string()),
            Some("false".to_string()),
        ];
        let mut binding3 = vec![
            Some("false".to_string()),
            Some("titi".to_string()),
            Some("false".to_string()),
        ];
        let mut binding4 = vec![
            Some("false".to_string()),
            Some("tutu".to_string()),
            Some("false".to_string()),
        ];
        let mut binding5 = vec![
            Some("true".to_string()),
            Some("tata".to_string()),
            Some("false".to_string()),
        ];
        let mut binding6 = vec![
            Some("false".to_string()),
            Some("tata".to_string()),
            Some("false".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);
        let new_entry5 = Some(&mut binding5);
        let new_entry6 = Some(&mut binding6);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), new_entry2)?;
        table.add_entry(&"entry3".to_string(), new_entry3)?;
        table.add_entry(&"entry4".to_string(), new_entry4)?;
        table.add_entry(&"entry5".to_string(), new_entry5)?;
        table.add_entry(&"entry6".to_string(), new_entry6)?;

        // Equality
        let expected_vec = vec![
            "entry2".to_string(),
            "entry5".to_string(),
            "entry6".to_string(),
        ];
        let res = check_result(
            (1, 1),
            table.get_matching_entries_string(
                None,
                &"key2".to_string(),
                MatchingCriteria::Equal,
                &"tata".to_string(),
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec, CheckType::Equal)?;

        // No match
        let res = check_result(
            (2, 1),
            table.get_matching_entries_string(
                None,
                &"key2".to_string(),
                MatchingCriteria::Equal,
                &"tyty".to_string(),
            ),
            true,
        )?
        .unwrap();
        check_option((2, 2), res, false)?;

        // Different
        let expected_vec = vec![
            "entry1".to_string(),
            "entry3".to_string(),
            "entry4".to_string(),
        ];
        let res = check_result(
            (3, 1),
            table.get_matching_entries_string(
                None,
                &"key2".to_string(),
                MatchingCriteria::Different,
                &"tata".to_string(),
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((3, 2), res, true)?.unwrap();
        check_value((3, 3), &opt, &expected_vec, CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_int_error() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("5".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("12".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("16".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_matching_entries_integer(
                None,
                &"key2".to_string(),
                MatchingCriteria::Equal,
                5,
                None,
            ),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_matching_entries_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Between,
                5,
                None,
            ),
            false,
        )?;
        check_result(
            (3, 1),
            table.get_matching_entries_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Between,
                5,
                Some(4),
            ),
            false,
        )?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_integer() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("5".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("6".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("5".to_string()), None, Some("-0.27".to_string())];
        let mut binding4 = vec![Some("-8".to_string()), None, Some("-0.27".to_string())];
        let mut binding5 = vec![Some("4".to_string()), None, Some("-0.27".to_string())];
        let mut binding6 = vec![Some("2".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);
        let new_entry5 = Some(&mut binding5);
        let new_entry6 = Some(&mut binding6);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), new_entry2)?;
        table.add_entry(&"entry3".to_string(), new_entry3)?;
        table.add_entry(&"entry4".to_string(), new_entry4)?;
        table.add_entry(&"entry5".to_string(), new_entry5)?;
        table.add_entry(&"entry6".to_string(), new_entry6)?;

        // Equality
        let expected_vec = vec!["entry1".to_string(), "entry3".to_string()];
        let res = check_result(
            (1, 1),
            table.get_matching_entries_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Equal,
                5,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec, CheckType::Equal)?;

        // No match
        let res = check_result(
            (2, 1),
            table.get_matching_entries_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Equal,
                7,
                None,
            ),
            true,
        )?
        .unwrap();
        check_option((2, 2), res, false)?;

        // Different
        let expected_vec = vec![
            "entry2".to_string(),
            "entry4".to_string(),
            "entry5".to_string(),
            "entry6".to_string(),
        ];
        let res = check_result(
            (3, 1),
            table.get_matching_entries_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Different,
                5,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((3, 2), res, true)?.unwrap();
        check_value((3, 3), &opt, &expected_vec, CheckType::Equal)?;

        // More
        let expected_vec = vec![
            "entry1".to_string(),
            "entry2".to_string(),
            "entry3".to_string(),
        ];
        let res = check_result(
            (4, 1),
            table.get_matching_entries_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::IsMore,
                4,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((4, 2), res, true)?.unwrap();
        check_value((4, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Less
        let expected_vec = vec!["entry4".to_string(), "entry6".to_string()];
        let res = check_result(
            (5, 1),
            table.get_matching_entries_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::IsLess,
                4,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((5, 2), res, true)?.unwrap();
        check_value((5, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Between
        let expected_vec = vec![
            "entry1".to_string(),
            "entry2".to_string(),
            "entry3".to_string(),
            "entry5".to_string(),
        ];
        let res = check_result(
            (6, 1),
            table.get_matching_entries_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Between,
                4,
                Some(6),
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((6, 2), res, true)?.unwrap();
        check_value((6, 3), &opt, &expected_vec, CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_uint_error() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::UnsignedInt(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("5".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("12".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("16".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_matching_entries_unsigned_integer(
                None,
                &"key2".to_string(),
                MatchingCriteria::Equal,
                5,
                None,
            ),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_matching_entries_unsigned_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Between,
                5,
                None,
            ),
            false,
        )?;
        check_result(
            (3, 1),
            table.get_matching_entries_unsigned_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Between,
                5,
                Some(4),
            ),
            false,
        )?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_uint() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::UnsignedInt(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("5".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("6".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("5".to_string()), None, Some("-0.27".to_string())];
        let mut binding4 = vec![Some("1".to_string()), None, Some("-0.27".to_string())];
        let mut binding5 = vec![Some("4".to_string()), None, Some("-0.27".to_string())];
        let mut binding6 = vec![Some("2".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);
        let new_entry5 = Some(&mut binding5);
        let new_entry6 = Some(&mut binding6);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), new_entry2)?;
        table.add_entry(&"entry3".to_string(), new_entry3)?;
        table.add_entry(&"entry4".to_string(), new_entry4)?;
        table.add_entry(&"entry5".to_string(), new_entry5)?;
        table.add_entry(&"entry6".to_string(), new_entry6)?;

        // Equality
        let expected_vec = vec!["entry1".to_string(), "entry3".to_string()];
        let res = check_result(
            (1, 1),
            table.get_matching_entries_unsigned_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Equal,
                5,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec, CheckType::Equal)?;

        // No match
        let res = check_result(
            (2, 1),
            table.get_matching_entries_unsigned_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Equal,
                7,
                None,
            ),
            true,
        )?
        .unwrap();
        check_option((2, 2), res, false)?;

        // Different
        let expected_vec = vec![
            "entry2".to_string(),
            "entry4".to_string(),
            "entry5".to_string(),
            "entry6".to_string(),
        ];
        let res = check_result(
            (3, 1),
            table.get_matching_entries_unsigned_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Different,
                5,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((3, 2), res, true)?.unwrap();
        check_value((3, 3), &opt, &expected_vec, CheckType::Equal)?;

        // More
        let expected_vec = vec![
            "entry1".to_string(),
            "entry2".to_string(),
            "entry3".to_string(),
        ];
        let res = check_result(
            (4, 1),
            table.get_matching_entries_unsigned_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::IsMore,
                4,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((4, 2), res, true)?.unwrap();
        check_value((4, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Less
        let expected_vec = vec!["entry4".to_string(), "entry6".to_string()];
        let res = check_result(
            (5, 1),
            table.get_matching_entries_unsigned_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::IsLess,
                4,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((5, 2), res, true)?.unwrap();
        check_value((5, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Between
        let expected_vec = vec![
            "entry1".to_string(),
            "entry2".to_string(),
            "entry3".to_string(),
            "entry5".to_string(),
        ];
        let res = check_result(
            (6, 1),
            table.get_matching_entries_unsigned_integer(
                None,
                &"key1".to_string(),
                MatchingCriteria::Between,
                4,
                Some(6),
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((6, 2), res, true)?.unwrap();
        check_value((6, 3), &opt, &expected_vec, CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_float_error() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::UnsignedInt(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("5".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("12".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("16".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_matching_entries_float(
                None,
                &"key2".to_string(),
                MatchingCriteria::Equal,
                5.1,
                None,
            ),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_matching_entries_float(
                None,
                &"key3".to_string(),
                MatchingCriteria::Between,
                5.1,
                None,
            ),
            false,
        )?;
        check_result(
            (3, 1),
            table.get_matching_entries_float(
                None,
                &"key3".to_string(),
                MatchingCriteria::Between,
                5.1,
                Some(5.1),
            ),
            false,
        )?;
        check_result(
            (4, 1),
            table.get_matching_entries_float(
                None,
                &"key3".to_string(),
                MatchingCriteria::Between,
                5.1,
                Some(5.0),
            ),
            false,
        )?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_float() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::UnsignedInt(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("5".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("6".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("5".to_string()), None, Some("-0.27".to_string())];
        let mut binding4 = vec![Some("1".to_string()), None, Some("-0.27".to_string())];
        let mut binding5 = vec![Some("4".to_string()), None, Some("0.45".to_string())];
        let mut binding6 = vec![Some("2".to_string()), None, Some("5.23".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);
        let new_entry5 = Some(&mut binding5);
        let new_entry6 = Some(&mut binding6);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), new_entry2)?;
        table.add_entry(&"entry3".to_string(), new_entry3)?;
        table.add_entry(&"entry4".to_string(), new_entry4)?;
        table.add_entry(&"entry5".to_string(), new_entry5)?;
        table.add_entry(&"entry6".to_string(), new_entry6)?;

        // Equality
        let expected_vec = vec!["entry3".to_string(), "entry4".to_string()];
        let res = check_result(
            (1, 1),
            table.get_matching_entries_float(
                None,
                &"key3".to_string(),
                MatchingCriteria::Equal,
                -0.27,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec, CheckType::Equal)?;

        // No match
        let res = check_result(
            (2, 1),
            table.get_matching_entries_float(
                None,
                &"key3".to_string(),
                MatchingCriteria::Equal,
                7.25,
                None,
            ),
            true,
        )?
        .unwrap();
        check_option((2, 2), res, false)?;

        // Different
        let expected_vec = vec![
            "entry1".to_string(),
            "entry2".to_string(),
            "entry5".to_string(),
            "entry6".to_string(),
        ];
        let res = check_result(
            (3, 1),
            table.get_matching_entries_float(
                None,
                &"key3".to_string(),
                MatchingCriteria::Different,
                -0.27,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((3, 2), res, true)?.unwrap();
        check_value((3, 3), &opt, &expected_vec, CheckType::Equal)?;

        // More
        let expected_vec = vec!["entry1".to_string(), "entry6".to_string()];
        let res = check_result(
            (4, 1),
            table.get_matching_entries_float(
                None,
                &"key3".to_string(),
                MatchingCriteria::IsMore,
                1.46,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((4, 2), res, true)?.unwrap();
        check_value((4, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Less
        let expected_vec = vec![
            "entry3".to_string(),
            "entry4".to_string(),
            "entry5".to_string(),
        ];
        let res = check_result(
            (5, 1),
            table.get_matching_entries_float(
                None,
                &"key3".to_string(),
                MatchingCriteria::IsLess,
                1.46,
                None,
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((5, 2), res, true)?.unwrap();
        check_value((5, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Between
        let expected_vec = vec![
            "entry1".to_string(),
            "entry2".to_string(),
            "entry5".to_string(),
        ];
        let res = check_result(
            (6, 1),
            table.get_matching_entries_float(
                None,
                &"key3".to_string(),
                MatchingCriteria::Between,
                0.45,
                Some(2.23),
            ),
            true,
        )?
        .unwrap();
        let opt = check_option((6, 2), res, true)?.unwrap();
        check_value((6, 3), &opt, &expected_vec, CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn get_key_values_bool_error() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Bool(false)),
            ("key2".to_string(), DbType::Bool(false)),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("true".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("false".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("true".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_unique_boolean_values_for_key(None, &"key3".to_string()),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_unique_boolean_values_for_key(None, &"key8".to_string()),
            false,
        )?;
        let res = check_result(
            (3, 1),
            table.get_unique_boolean_values_for_key(None, &"key2".to_string()),
            true,
        )?
        .unwrap();
        check_option((3, 2), res, false)?;

        Ok(())
    }

    #[test]
    fn get_key_values_bool() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Bool(false)),
            ("key2".to_string(), DbType::Bool(false)),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("false".to_string()),
            Some("true".to_string()),
            Some("2.23".to_string()),
        ];
        let mut binding2 = vec![Some("false".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![
            Some("true".to_string()),
            Some("true".to_string()),
            Some("-0.27".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        let expected_vec_1 = vec![false, true];
        let expected_vec_2 = vec![true];
        let res = check_result(
            (1, 1),
            table.get_unique_boolean_values_for_key(None, &"key1".to_string()),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec_1, CheckType::Equal)?;
        let res = check_result(
            (2, 1),
            table.get_unique_boolean_values_for_key(None, &"key2".to_string()),
            true,
        )?
        .unwrap();
        let opt = check_option((2, 2), res, true)?.unwrap();
        check_value((2, 3), &opt, &expected_vec_2, CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn get_key_values_int_error() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::Integer(0)),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("2".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("3".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_unique_integer_values_for_key(None, &"key3".to_string()),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_unique_integer_values_for_key(None, &"key8".to_string()),
            false,
        )?;
        let res = check_result(
            (3, 1),
            table.get_unique_integer_values_for_key(None, &"key2".to_string()),
            true,
        )?
        .unwrap();
        check_option((3, 2), res, false)?;

        Ok(())
    }

    #[test]
    fn get_key_values_uint_error() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::UnsignedInt(0)),
            ("key2".to_string(), DbType::UnsignedInt(0)),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("2".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("3".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_unique_unsigned_integer_values_for_key(None, &"key3".to_string()),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_unique_unsigned_integer_values_for_key(None, &"key8".to_string()),
            false,
        )?;
        let res = check_result(
            (3, 1),
            table.get_unique_unsigned_integer_values_for_key(None, &"key2".to_string()),
            true,
        )?
        .unwrap();
        check_option((3, 2), res, false)?;

        Ok(())
    }

    #[test]
    fn get_key_values_int() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::UnsignedInt(0)),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("1".to_string()),
            Some("4".to_string()),
            Some("2.23".to_string()),
        ];
        let mut binding2 = vec![
            Some("2".to_string()),
            Some("5".to_string()),
            Some("1.46".to_string()),
        ];
        let mut binding3 = vec![
            Some("3".to_string()),
            Some("6".to_string()),
            Some("-0.27".to_string()),
        ];
        let mut binding4 = vec![
            Some("1".to_string()),
            Some("5".to_string()),
            Some("-0.27".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;
        table.add_entry(&"entry5".to_string(), new_entry4)?;

        let expected_vec_1 = vec![1, 2, 3];
        let expected_vec_2 = vec![4, 5, 6];
        let res = check_result(
            (1, 1),
            table.get_unique_integer_values_for_key(None, &"key1".to_string()),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec_1, CheckType::Equal)?;
        let res = check_result(
            (2, 1),
            table.get_unique_unsigned_integer_values_for_key(None, &"key2".to_string()),
            true,
        )?
        .unwrap();
        let opt = check_option((2, 2), res, true)?.unwrap();
        check_value((2, 3), &opt, &expected_vec_2, CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn get_key_values_string_error() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::String("0".to_string())),
            ("key2".to_string(), DbType::String("0".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("2".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("3".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_unique_string_values_for_key(None, &"key3".to_string()),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_unique_string_values_for_key(None, &"key8".to_string()),
            false,
        )?;
        let res = check_result(
            (3, 1),
            table.get_unique_string_values_for_key(None, &"key2".to_string()),
            true,
        )?
        .unwrap();
        check_option((3, 2), res, false)?;

        Ok(())
    }

    #[test]
    fn get_key_values_string() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::String("".to_string())),
            ("key2".to_string(), DbType::String("".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("1".to_string()),
            Some("4".to_string()),
            Some("2.23".to_string()),
        ];
        let mut binding2 = vec![
            Some("2".to_string()),
            Some("5".to_string()),
            Some("1.46".to_string()),
        ];
        let mut binding3 = vec![
            Some("3".to_string()),
            Some("6".to_string()),
            Some("-0.27".to_string()),
        ];
        let mut binding4 = vec![
            Some("1".to_string()),
            Some("5".to_string()),
            Some("-0.27".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;
        table.add_entry(&"entry5".to_string(), new_entry4)?;

        let expected_vec_1 = vec!["1".to_string(), "2".to_string(), "3".to_string()];
        let expected_vec_2 = vec!["4".to_string(), "5".to_string(), "6".to_string()];
        let res = check_result(
            (1, 1),
            table.get_unique_string_values_for_key(None, &"key1".to_string()),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec_1, CheckType::Equal)?;
        let res = check_result(
            (2, 1),
            table.get_unique_string_values_for_key(None, &"key2".to_string()),
            true,
        )?
        .unwrap();
        let opt = check_option((2, 2), res, true)?.unwrap();
        check_value((2, 3), &opt, &expected_vec_2, CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn get_key_values_float_error() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Float(0.0)),
            ("key2".to_string(), DbType::Float(0.0)),
            ("key3".to_string(), DbType::String("0.0".to_string())),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1.1".to_string()), None, Some("Hello".to_string())];
        let mut binding2 = vec![Some("2.2".to_string()), None, Some("World".to_string())];
        let mut binding3 = vec![Some("3.3".to_string()), None, Some("AI".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_unique_float_values_for_key(None, &"key3".to_string()),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_unique_float_values_for_key(None, &"key8".to_string()),
            false,
        )?;
        let res = check_result(
            (3, 1),
            table.get_unique_float_values_for_key(None, &"key2".to_string()),
            true,
        )?
        .unwrap();
        check_option((3, 2), res, false)?;

        Ok(())
    }

    #[test]
    fn get_key_values_float() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Float(0.0)),
            ("key2".to_string(), DbType::Float(0.0)),
            ("key3".to_string(), DbType::String("0.0".to_string())),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("1.0".to_string()),
            Some("4.1".to_string()),
            Some("Hello".to_string()),
        ];
        let mut binding2 = vec![
            Some("2.2".to_string()),
            Some("5.3".to_string()),
            Some("World".to_string()),
        ];
        let mut binding3 = vec![
            Some("3.3".to_string()),
            Some("6.4".to_string()),
            Some("AI".to_string()),
        ];
        let mut binding4 = vec![
            Some("1.0".to_string()),
            Some("5.5".to_string()),
            Some("Assistant".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;
        table.add_entry(&"entry5".to_string(), new_entry4)?;

        let expected_vec_1 = vec![1.0, 2.2, 3.3];
        let res = check_result(
            (1, 1),
            table.get_unique_float_values_for_key(None, &"key1".to_string()),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec_1, CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn get_key_values_date_error() -> Result<(), String> {
        let keys = vec![
            (
                "key1".to_string(),
                DbType::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            ),
            (
                "key2".to_string(),
                DbType::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            ),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("01/12/2021".to_string()),
            None,
            Some("2.23".to_string()),
        ];
        let mut binding2 = vec![
            Some("02/12/2021".to_string()),
            None,
            Some("1.46".to_string()),
        ];
        let mut binding3 = vec![
            Some("03/12/2021".to_string()),
            None,
            Some("-0.27".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result(
            (1, 1),
            table.get_unique_date_values_for_key(None, &"key3".to_string()),
            false,
        )?;
        check_result(
            (2, 1),
            table.get_unique_date_values_for_key(None, &"key8".to_string()),
            false,
        )?;
        let res = check_result(
            (3, 1),
            table.get_unique_date_values_for_key(None, &"key2".to_string()),
            true,
        )?
        .unwrap();
        check_option((3, 2), res, false)?;

        Ok(())
    }

    #[test]
    fn get_key_values_date() -> Result<(), String> {
        let keys = vec![
            (
                "key1".to_string(),
                DbType::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            ),
            (
                "key2".to_string(),
                DbType::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            ),
            ("key3".to_string(), DbType::String("Test".to_string())),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("01/12/2021".to_string()),
            Some("01/01/2022".to_string()),
            Some("Hello".to_string()),
        ];
        let mut binding2 = vec![
            Some("02/12/2021".to_string()),
            Some("02/01/2022".to_string()),
            Some("World".to_string()),
        ];
        let mut binding3 = vec![
            Some("03/12/2021".to_string()),
            Some("03/01/2022".to_string()),
            Some("AI".to_string()),
        ];
        let mut binding4 = vec![
            Some("02/12/2021".to_string()),
            Some("03/01/2022".to_string()),
            Some("AI".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), new_entry2)?;
        table.add_entry(&"entry3".to_string(), new_entry3)?;
        table.add_entry(&"entry4".to_string(), new_entry4)?;

        let expected_vec_1 = vec![
            NaiveDate::from_ymd_opt(2021, 12, 1).unwrap(),
            NaiveDate::from_ymd_opt(2021, 12, 2).unwrap(),
            NaiveDate::from_ymd_opt(2021, 12, 3).unwrap(),
        ];

        let res = check_result(
            (1, 1),
            table.get_unique_date_values_for_key(None, &"key1".to_string()),
            true,
        )?
        .unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec_1, CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn get_entries_subset() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Float(0.0)),
            ("key2".to_string(), DbType::Float(0.0)),
            ("key3".to_string(), DbType::String("0.0".to_string())),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![
            Some("1.0".to_string()),
            Some("4.1".to_string()),
            Some("Hello".to_string()),
        ];
        let mut binding2 = vec![
            Some("2.2".to_string()),
            Some("5.3".to_string()),
            Some("World".to_string()),
        ];
        let mut binding3 = vec![
            Some("3.3".to_string()),
            Some("6.4".to_string()),
            Some("AI".to_string()),
        ];
        let mut binding4 = vec![
            Some("1.0".to_string()),
            Some("5.5".to_string()),
            Some("Assistant".to_string()),
        ];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);
        let new_entry4 = Some(&mut binding4);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;
        table.add_entry(&"entry5".to_string(), new_entry4)?;

        let entry1 = &"entry1".to_string();
        let entry2 = &"entry2".to_string();
        let entry3 = &"entry3".to_string();
        let entry4 = &"entry4".to_string();
        let entry5 = &"entry5".to_string();

        let expected_vec = vec![entry1, entry2, entry3, entry4, entry5];
        let entries_subset = table
            .get_entries_subset(None)
            .iter()
            .map(|entry| entry.name())
            .collect::<Vec<&String>>();
        check_value((1, 1), &entries_subset, &expected_vec, CheckType::Equal)?;

        let expected_vec = vec![entry1, entry4, entry5];
        let subset = vec![entry1, entry4, entry5];
        let entries_subset = table
            .get_entries_subset(Some(subset))
            .iter()
            .map(|entry| entry.name())
            .collect::<Vec<&String>>();
        check_value((2, 1), &entries_subset, &expected_vec, CheckType::Equal)?;

        let expected_vec = vec![];
        let subset = vec![];
        let entries_subset = table
            .get_entries_subset(Some(subset))
            .iter()
            .map(|entry| entry.name())
            .collect::<Vec<&String>>();
        check_value((3, 1), &entries_subset, &expected_vec, CheckType::Equal)
    }
}
