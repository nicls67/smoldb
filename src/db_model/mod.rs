//!
//! Database Model definition
//!

use rustlog::{write_log, LogSeverity};
use serde_derive::{Deserialize, Serialize};

pub use db_table::DbTable;
pub use db_table::MatchingCriteria;

use self::db_type::DbType;

mod db_entry;
mod db_table;

mod db_type;

/// Database model
#[derive(Deserialize, Serialize, PartialEq, Debug)]
pub struct DbModel {
    name: String,
    version: Vec<u8>,
    tables: Vec<DbTable>,
}

impl DbModel {
    /// Creates a new instance of `DbModel`.
    ///
    /// # Arguments
    ///
    /// * `db_name` - The name of the database.
    ///
    /// # Returns
    ///
    /// A `DbModel` instance.
    ///
    pub(crate) fn new(db_name: String) -> DbModel {
        DbModel {
            name: db_name,
            version: env!("CARGO_PKG_VERSION")
                .split(".")
                .map(|ver| ver.parse::<u8>().unwrap())
                .collect(),
            tables: Vec::new(),
        }
    }

    /// Creates a new table with the given name and optional keys.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the table.
    /// * `keys` - Optional keys (name and type) for the table. Type is among the following :
    /// `Integer`, `UnsignedInt`, `Float`, `Date`, `Bool`, `String`
    ///
    /// # Returns
    ///
    /// * `Ok(())` - if the table is created successfully.
    /// * `Err(String)` - if there is an error during table creation.
    pub fn create_table(
        &mut self,
        name: &str,
        keys: Option<Vec<(String, String)>>,
    ) -> Result<(), String> {
        let mut new_vec = None;
        if let Some(keys_vec) = keys {
            let mut vec_tmp = Vec::new();
            for key in keys_vec.iter() {
                vec_tmp.push((key.0.clone(), DbType::default_from_string(&key.1)?))
            }
            new_vec = Some(vec_tmp);
        }

        self.tables.push(DbTable::new(name.to_string(), new_vec));
        write_log(
            LogSeverity::Info,
            &format!("CREATED table {}", name),
            &env!("CARGO_PKG_NAME").to_string(),
        );

        Ok(())
    }

    /// Returns the reference to the name of the object.
    ///
    /// # Returns
    ///
    /// - `&String`: A reference to the name of the object.
    ///
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Returns the version of the object as a string.
    ///
    /// This function formats the version array into a string using the format "major.minor.patch".
    ///
    /// # Arguments
    ///
    /// * `self` - The current object.
    ///
    /// # Returns
    ///
    /// * A string representation of the version.
    ///
    pub fn version(&self) -> String {
        format!(
            "{}.{}.{}",
            self.version[0], self.version[1], self.version[2]
        )
    }

    /// Retrieves a mutable reference to a database table by its name.
    ///
    /// # Arguments
    ///
    /// * `name` - A reference to the name of the table.
    ///
    /// # Returns
    ///
    /// * `Ok` containing a mutable reference to the `DbTable` if the table exists.
    /// * `Err` containing an error message if the table does not exist.
    pub fn table(&mut self, name: &String) -> Result<&mut DbTable, String> {
        match self.find_table(name) {
            Ok(table) => Ok(table.1),
            Err(s) => Err(s),
        }
    }

    /// Deletes a table from the database.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the table to delete.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the table was successfully deleted, otherwise returns `Err` with an error message.
    pub fn delete_table(&mut self, name: &String) -> Result<(), String> {
        let index = self.find_table(name)?.0;
        self.tables.swap_remove(index);

        write_log(
            LogSeverity::Info,
            &format!("DELETE table {}", name),
            &env!("CARGO_PKG_NAME").to_string(),
        );
        Ok(())
    }

    /// Searches for a table with the given name in the database.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the table to search for, as a reference to a String.
    ///
    /// # Returns
    ///
    /// Returns a Result containing a tuple with the index and a mutable reference to the found table if it exists.
    /// If the table is found, the Result is Ok. Otherwise, an Err is returned with a descriptive error message.
    ///
    fn find_table(&mut self, name: &String) -> Result<(usize, &mut DbTable), String> {
        for table in self.tables.iter_mut().enumerate() {
            if table.1.name() == name {
                return Ok(table);
            }
        }

        let msg = format!("No table named {} in database {}", name, self.name);
        write_log(
            LogSeverity::Error,
            &msg,
            &env!("CARGO_PKG_NAME").to_string(),
        );
        Err(msg)
    }

    /// Returns the number of tables in the current context.
    ///
    /// # Returns
    ///
    /// The number of tables as a `usize`.
    pub fn tables_count(&self) -> usize {
        self.tables.len()
    }
}

#[cfg(test)]
mod tests {
    use rusttests::{check_result, check_value};

    use crate::DbModel;

    #[test]
    fn new_model() -> Result<(), String> {
        let model = DbModel::new("ModelName".to_string());

        check_value(
            (1, 1),
            &model.name,
            &"ModelName".to_string(),
            rusttests::CheckType::Equal,
        )?;
        check_value((1, 2), &model.tables.len(), &0, rusttests::CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn new_table_nominal() -> Result<(), String> {
        let mut model = DbModel::new("ModelName".to_string());

        model.create_table(
            &"NewTable".to_string(),
            Some(vec![
                ("key1".to_string(), "String".to_string()),
                ("key2".to_string(), "Integer".to_string()),
            ]),
        )?;

        check_value((1, 1), &model.tables.len(), &1, rusttests::CheckType::Equal)?;

        model.create_table(&"NewTable".to_string(), None)?;

        check_value((1, 1), &model.tables.len(), &2, rusttests::CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn new_table_key_error() -> Result<(), String> {
        let mut model = DbModel::new("ModelName".to_string());

        check_result(
            (1, 1),
            model.create_table(
                &"NewTable".to_string(),
                Some(vec![
                    ("key1".to_string(), "String".to_string()),
                    ("key2".to_string(), "RandomType".to_string()),
                ]),
            ),
            false,
        )?;
        Ok(())
    }

    #[test]
    fn db_version() -> Result<(), String> {
        let mut model = DbModel::new("ModelName".to_string());

        model.version[0] = 1;
        model.version[1] = 2;
        model.version[2] = 3;

        check_value(
            (1, 1),
            &model.version(),
            &"1.2.3".to_string(),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn get_table_nominal() -> Result<(), String> {
        let mut model = DbModel::new("ModelName".to_string());

        model.create_table(
            &"NewTable".to_string(),
            Some(vec![
                ("key1".to_string(), "String".to_string()),
                ("key2".to_string(), "Integer".to_string()),
            ]),
        )?;
        model.create_table(
            &"OtherTable".to_string(),
            Some(vec![
                ("key3".to_string(), "Float".to_string()),
                ("key4".to_string(), "UnsignedInt".to_string()),
            ]),
        )?;

        let table = model.table(&"NewTable".to_string())?;

        check_value(
            (1, 1),
            table.name(),
            &"NewTable".to_string(),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn get_table_name_error() -> Result<(), String> {
        let mut model = DbModel::new("ModelName".to_string());

        model.create_table(
            &"NewTable".to_string(),
            Some(vec![
                ("key1".to_string(), "String".to_string()),
                ("key2".to_string(), "Integer".to_string()),
            ]),
        )?;
        model.create_table(
            &"OtherTable".to_string(),
            Some(vec![
                ("key3".to_string(), "Float".to_string()),
                ("key4".to_string(), "UnsignedInt".to_string()),
            ]),
        )?;

        check_result((1, 1), model.table(&"Another table".to_string()), false)?;
        Ok(())
    }

    #[test]
    fn delete_table() -> Result<(), String> {
        let mut model = DbModel::new("ModelName".to_string());

        model.create_table(
            &"NewTable".to_string(),
            Some(vec![
                ("key1".to_string(), "String".to_string()),
                ("key2".to_string(), "Integer".to_string()),
            ]),
        )?;
        model.create_table(
            &"OtherTable".to_string(),
            Some(vec![
                ("key3".to_string(), "Float".to_string()),
                ("key4".to_string(), "UnsignedInt".to_string()),
            ]),
        )?;
        model.create_table(
            &"ThirdTable".to_string(),
            Some(vec![
                ("key5".to_string(), "Integer".to_string()),
                ("key6".to_string(), "Float".to_string()),
            ]),
        )?;

        model.delete_table(&"OtherTable".to_string())?;

        check_value(
            (1, 1),
            &model.tables_count(),
            &2,
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn delete_table_wrong_name() -> Result<(), String> {
        let mut model = DbModel::new("ModelName".to_string());

        model.create_table(
            &"NewTable".to_string(),
            Some(vec![
                ("key1".to_string(), "String".to_string()),
                ("key2".to_string(), "Integer".to_string()),
            ]),
        )?;
        model.create_table(
            &"OtherTable".to_string(),
            Some(vec![
                ("key3".to_string(), "Float".to_string()),
                ("key4".to_string(), "UnsignedInt".to_string()),
            ]),
        )?;
        model.create_table(
            &"ThirdTable".to_string(),
            Some(vec![
                ("key5".to_string(), "Integer".to_string()),
                ("key6".to_string(), "Float".to_string()),
            ]),
        )?;

        check_result((1, 1), model.delete_table(&"StupidName".to_string()), false)?;
        check_value(
            (1, 2),
            &model.tables_count(),
            &3,
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }
}
