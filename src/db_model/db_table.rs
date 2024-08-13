//!
//! Database Table definition
//!

use std::cmp::PartialEq;
use std::mem::discriminant;

use chrono::NaiveDate;
use rustlog::{LogSeverity, write_log};
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
    /// Entry name and fields values in String format must be provided, values can be set to `None` globally
    ///
    pub fn add_entry(
        &mut self,
        name: &String,
        values: Option<&mut Vec<Option<String>>>,
    ) -> Result<(), String> {
        let new_entry;

        // Check unicity of entry name
        if self.entry_exists(&name) {
            let msg = format!("Cannot create new entry : name {name} already exists in table");
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

    ///
    /// ## Updates an entry of the table (String format).
    /// Entry name, key to update and field value in string format must be provided, value can be set to `None`.
    ///
    /// If the given String can't be interpreted as the configured key type, `Err` is returned
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
    /// ## Gets an entry value (String format).
    /// Entry name, key to get must be provided.
    ///
    /// If the key is not configured as String, the data is converted into a String
    ///
    pub fn get_entry_value_string(
        &mut self,
        entry_name: &String,
        key_name: &String,
    ) -> Result<Option<String>, String> {
        if let Some(value) = self.get_entry_value(entry_name, key_name)? {
            Ok(Some(value.into_string()))
        } else {
            Ok(None)
        }
    }

    ///
    /// ## Updates an entry of the table (Int format).
    /// Entry name, key to update and field value as integer must be provided, value can be set to `None`
    ///
    /// If the selected key is not configured as Integer, `Err` is returned
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

    ///
    /// ## Gets an entry value (Int format).
    /// Entry name, key to get must be provided
    ///
    /// If the selected key is not configured as Integer, `Err` is returned
    ///
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

    ///
    /// ## Updates an entry of the table (UInt format).
    /// Entry name, key to update and field value as unsigned integer must be provided, value can be set to `None`
    ///
    /// If the selected key is not configured as Unsigned Integer, `Err` is returned
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

    ///
    /// ## Gets an entry value (UInt format).
    /// Entry name, key to get must be provided
    ///
    /// If the selected key is not configured as Unsigned Integer, `Err` is returned
    ///
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

    ///
    /// ## Updates an entry of the table (Float format).
    /// Entry name, key to update and field value as float must be provided, value can be set to `None`
    ///
    /// If the selected key is not configured as Float, `Err` is returned
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

    ///
    /// ## Gets an entry value (Float format).
    /// Entry name, key to get must be provided
    ///
    /// If the selected key is not configured as Float, `Err` is returned
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

    ///
    /// ## Updates an entry of the table (Bool format).
    /// Entry name, key to update and field value as float must be provided, value can be set to `None`
    ///
    /// If the selected key is not configured as Bool, `Err` is returned
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

    ///
    /// ## Gets an entry value (Bool format).
    /// Entry name, key to get must be provided
    ///
    /// If the selected key is not configured as Bool, `Err` is returned
    ///
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

    ///
    /// ## Updates an entry of the table (Date format).
    /// Entry name, key to update and field value as float must be provided, value can be set to `None`
    ///
    /// If the selected key is not configured as Date, `Err` is returned
    ///
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

    ///
    /// ## Gets an entry value (Date format).
    /// Entry name, key to get must be provided
    ///
    /// If the selected key is not configured as Date, `Err` is returned
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
            .check_type(&DbType::default_from_string(&"Date".to_string()).unwrap())
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

    /// Removes the selected entry from the table
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

    /// Updates the selected entry
    ///
    /// Private method called by type-specific public methods
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

    /// Gets key value for selected entry.
    ///
    /// Private method called by type-specific public methods
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

    /// Returns entries count in table
    pub fn entries_count(&self) -> usize {
        self.entries.len()
    }

    /// Search for an entry and returns `Ok` with a reference to it, or `Err` if the entry does not exist
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

        let msg = format!("Key {} does not exists in table {}", key_name, self.name);
        write_log(
            LogSeverity::Error,
            &msg,
            &env!("CARGO_PKG_NAME").to_string(),
        );
        Err(msg)
    }

    /// ## Adds a key to the table
    /// All entries of the table will get the new key with a default `None` value
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

    /// Returns table name
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Rename the selected entry
    pub fn rename_entry(&mut self, entry_name: &String, new_name: &String) -> Result<(), String> {
        self.find_entry(entry_name)?.0.rename(new_name);
        Ok(())
    }

    /// Returns all table's entries name as a vector, or `None` if the table is empty
    pub fn get_all_entries(&self) -> Option<Vec<String>> {
        if self.entries_count() > 0 {
            let mut vect = Vec::new();
            for entry in self.entries.iter() {
                vect.push(entry.name().clone())
            }
            Some(vect)
        } else {
            None
        }
    }

    /// Returns all entries matching the selected date criteria
    ///
    /// ### Inputs
    /// * `key_name`: key to use for comparison
    /// * `criteria`: Comparison criteria
    /// * `date1`: Reference date for comparison
    /// * `date2`: second reference date, used for `Between` comparison only, can be set to `None` for other criteria. `date2` must be after `date1`
    ///
    /// ### Returns
    /// * `Err` if there is any error during processing or wrong parameters are given
    /// * `Ok(None)` if no entry matches the selected criteria
    /// * `Ok(Some(xxx))` in other cases where xxx is a vector containing matching entries names
    pub fn get_matching_entries_date(&self, key_name: &String, criteria: MatchingCriteria, date1: NaiveDate, date2: Option<NaiveDate>) -> Result<Option<Vec<String>>, String> {
        if self.entries_count() > 0 {
            // Check input compatibility
            if criteria == MatchingCriteria::Between {
                if date2.is_none() {
                    let msg = "Second reference date not defined for Between date comparison".to_string();
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
                    for entry in self.entries.iter() {
                        if let Some(entry_date_wrapped) = entry.get(key.0) {
                            if let DbType::Date(entry_date) = entry_date_wrapped {
                                let delta = *entry_date - date1;
                                match criteria {
                                    MatchingCriteria::IsMore => {
                                        if delta.num_days() > 0 {
                                            output.push(entry.name().clone());
                                        }
                                    }
                                    MatchingCriteria::IsLess => {
                                        if delta.num_days() < 0 {
                                            output.push(entry.name().clone());
                                        }
                                    }
                                    MatchingCriteria::Equal => {
                                        if delta.num_days() == 0 {
                                            output.push(entry.name().clone());
                                        }
                                    }
                                    MatchingCriteria::Different => {
                                        if delta.num_days() != 0 {
                                            output.push(entry.name().clone());
                                        }
                                    }
                                    MatchingCriteria::Between => {
                                        let delta2 = *entry_date - date2.unwrap();
                                        if delta.num_days() >= 0 && delta2.num_days() <= 0 {
                                            output.push(entry.name().clone());
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if output.len() == 0 {
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
        } else {
            Ok(None)
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

        check_struct((1, 1), &table, &expected, rusttests::CheckType::Equal)?;
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

        check_struct((1, 1), &table, &expected, rusttests::CheckType::Equal)?;
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

        check_value(
            (1, 1),
            &table.entries_count(),
            &2,
            rusttests::CheckType::Equal,
        )?;
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

        check_value(
            (1, 2),
            &table.entries_count(),
            &1,
            rusttests::CheckType::Equal,
        )?;
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

        check_value(
            (1, 2),
            &table.entries_count(),
            &1,
            rusttests::CheckType::Equal,
        )?;
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
        check_value(
            (1, 2),
            &table.entries_count(),
            &1,
            rusttests::CheckType::Equal,
        )?;
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
        check_struct(
            (1, 2),
            val,
            &DbType::Float(5.98),
            rusttests::CheckType::Equal,
        )?;

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
            rusttests::CheckType::Equal,
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
        check_value(
            (1, 2),
            &val,
            &"New value".to_string(),
            rusttests::CheckType::Equal,
        )?;
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
        check_struct(
            (2, 2),
            val,
            &DbType::Integer(1),
            rusttests::CheckType::Equal,
        )?;
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
        check_value(
            (1, 2),
            &val,
            &"-12".to_string(),
            rusttests::CheckType::Equal,
        )?;

        let val = check_option(
            (2, 1),
            table.get_entry_value_string(&"entry2".to_string(), &"key2".to_string())?,
            true,
        )?
            .unwrap();
        check_value((2, 2), &val, &"45".to_string(), rusttests::CheckType::Equal)?;

        let val = check_option(
            (3, 1),
            table.get_entry_value_string(&"entry2".to_string(), &"key3".to_string())?,
            true,
        )?
            .unwrap();
        check_value(
            (3, 2),
            &val,
            &"2.23".to_string(),
            rusttests::CheckType::Equal,
        )?;

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
        check_value((1, 2), val, &-66, rusttests::CheckType::Equal)?;
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
        check_value((1, 2), val, &66, rusttests::CheckType::Equal)?;
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
        check_value((1, 2), val, &66.99, rusttests::CheckType::Equal)?;
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
        check_value((1, 2), val, &true, rusttests::CheckType::Equal)?;
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
            ("key3".to_string(), DbType::default_from_string(&"Date".to_string()).unwrap()),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("1".to_string()), None, Some("15/08/2016".to_string())];
        let new_entry = Some(&mut binding);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;

        table.update_entry_date(&"entry1".to_string(), &"key3".to_string(), Some(NaiveDate::from_ymd_opt(1789, 7, 14).unwrap()))?;

        let val = check_option(
            (1, 1),
            table.get_entry_value_date(&"entry1".to_string(), &"key3".to_string())?,
            true,
        )?
            .unwrap();
        check_value((1, 2), val, &NaiveDate::from_ymd_opt(1789, 7, 14).unwrap(), rusttests::CheckType::Equal)?;
        Ok(())
    }

    #[test]
    fn get_entry_date_wrong_type() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::default_from_string(&"Date".to_string()).unwrap()),
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

        check_value(
            (1, 1),
            &table.entries_count(),
            &2,
            rusttests::CheckType::Equal,
        )?;
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
        check_value(
            (1, 2),
            &table.entries_count(),
            &3,
            rusttests::CheckType::Equal,
        )?;
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

        check_value((1, 1), &table.get_all_entries(), &Some(vec!["entry1".to_string(), "entry2".to_string(), "entry5".to_string(), "entry4".to_string()]), rusttests::CheckType::Equal)
    }

    #[test]
    fn get_all_entries_empty() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Integer(0)),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let table = DbTable::new("Table".to_string(), Some(keys));

        check_value((1, 1), &table.get_all_entries(), &None, rusttests::CheckType::Equal)
    }

    #[test]
    fn get_entries_matching_date_error() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Date(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap())),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("13/03/2014".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("14/03/2014".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("13/08/2024".to_string()), None, Some("-0.27".to_string())];
        let new_entry = Some(&mut binding);
        let new_entry2 = Some(&mut binding2);
        let new_entry3 = Some(&mut binding3);

        table.add_entry(&"entry1".to_string(), new_entry)?;
        table.add_entry(&"entry2".to_string(), None)?;
        table.add_entry(&"entry3".to_string(), new_entry2)?;
        table.add_entry(&"entry4".to_string(), new_entry3)?;

        check_result((1, 1), table.get_matching_entries_date(&"key2".to_string(), MatchingCriteria::Equal, NaiveDate::from_ymd_opt(2000, 12, 31).unwrap(), None), false)?;
        check_result((2, 1), table.get_matching_entries_date(&"key1".to_string(), MatchingCriteria::Between, NaiveDate::from_ymd_opt(2000, 12, 31).unwrap(), None), false)?;
        check_result((3, 1), table.get_matching_entries_date(&"key1".to_string(), MatchingCriteria::Between, NaiveDate::from_ymd_opt(2000, 12, 31).unwrap(), Some(NaiveDate::from_ymd_opt(2000, 12, 31).unwrap())), false)?;

        Ok(())
    }

    #[test]
    fn get_entries_matching_date() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Date(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap())),
            ("key2".to_string(), DbType::String(" ".to_string())),
            ("key3".to_string(), DbType::Float(0.0)),
        ];
        let mut table = DbTable::new("Table".to_string(), Some(keys));
        let mut binding = vec![Some("13/03/2014".to_string()), None, Some("2.23".to_string())];
        let mut binding2 = vec![Some("14/03/2014".to_string()), None, Some("1.46".to_string())];
        let mut binding3 = vec![Some("13/08/2024".to_string()), None, Some("-0.27".to_string())];
        let mut binding4 = vec![Some("13/03/2014".to_string()), None, Some("-0.27".to_string())];
        let mut binding5 = vec![Some("10/03/2014".to_string()), None, Some("-0.27".to_string())];
        let mut binding6 = vec![Some("15/03/2014".to_string()), None, Some("-0.27".to_string())];
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
        let res = check_result((1, 1), table.get_matching_entries_date(&"key1".to_string(), MatchingCriteria::Equal, NaiveDate::from_ymd_opt(2014, 3, 13).unwrap(), None), true)?.unwrap();
        let opt = check_option((1, 2), res, true)?.unwrap();
        check_value((1, 3), &opt, &expected_vec, CheckType::Equal)?;

        // No match
        let res = check_result((2, 1), table.get_matching_entries_date(&"key1".to_string(), MatchingCriteria::Equal, NaiveDate::from_ymd_opt(2015, 3, 13).unwrap(), None), true)?.unwrap();
        check_option((2, 2), res, false)?;

        // Different
        let expected_vec = vec!["entry2".to_string(), "entry3".to_string(), "entry5".to_string(), "entry6".to_string()];
        let res = check_result((3, 1), table.get_matching_entries_date(&"key1".to_string(), MatchingCriteria::Different, NaiveDate::from_ymd_opt(2014, 3, 13).unwrap(), None), true)?.unwrap();
        let opt = check_option((3, 2), res, true)?.unwrap();
        check_value((3, 3), &opt, &expected_vec, CheckType::Equal)?;

        // After
        let expected_vec = vec!["entry3".to_string(), "entry6".to_string()];
        let res = check_result((4, 1), table.get_matching_entries_date(&"key1".to_string(), MatchingCriteria::IsMore, NaiveDate::from_ymd_opt(2014, 3, 14).unwrap(), None), true)?.unwrap();
        let opt = check_option((4, 2), res, true)?.unwrap();
        check_value((4, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Before
        let expected_vec = vec!["entry1".to_string(), "entry2".to_string(), "entry4".to_string(), "entry5".to_string()];
        let res = check_result((5, 1), table.get_matching_entries_date(&"key1".to_string(), MatchingCriteria::IsLess, NaiveDate::from_ymd_opt(2014, 3, 15).unwrap(), None), true)?.unwrap();
        let opt = check_option((5, 2), res, true)?.unwrap();
        check_value((5, 3), &opt, &expected_vec, CheckType::Equal)?;

        // Between
        let expected_vec = vec!["entry1".to_string(), "entry2".to_string(), "entry4".to_string(), "entry6".to_string()];
        let res = check_result((6, 1), table.get_matching_entries_date(&"key1".to_string(), MatchingCriteria::Between, NaiveDate::from_ymd_opt(2014, 3, 13).unwrap(), Some(NaiveDate::from_ymd_opt(2014, 3, 15).unwrap())), true)?.unwrap();
        let opt = check_option((6, 2), res, true)?.unwrap();
        check_value((6, 3), &opt, &expected_vec, CheckType::Equal)?;

        Ok(())
    }
}
