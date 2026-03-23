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
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
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
    /// * `p_db_name` - The name of the database.
    ///
    /// # Returns
    ///
    /// A `DbModel` instance.
    ///
    pub(crate) fn new(p_db_name: String) -> DbModel {
        DbModel {
            name: p_db_name,
            version: env!("CARGO_PKG_VERSION")
                .split(".")
                .map(|p_ver| p_ver.parse::<u8>().unwrap())
                .collect(),
            tables: Vec::new(),
        }
    }

    /// Creates a new table with the given name and optional keys.
    ///
    /// # Arguments
    ///
    /// * `p_name` - The name of the table.
    /// * `p_keys` - Optional keys (name and type) for the table. Type is among the following :
    ///   `Integer`, `UnsignedInt`, `Float`, `Date`, `Bool`, `String`
    ///
    /// # Returns
    ///
    /// * `Ok(())` - if the table is created successfully.
    /// * `Err(String)` - if there is an error during table creation or the table name already exists.
    pub fn create_table(
        &mut self,
        p_name: &str,
        p_keys: Option<Vec<(String, String)>>,
    ) -> Result<(), String> {
        // Check unicity of table name
        if self.tables.iter().any(|p_t| p_t.name() == p_name) {
            let l_msg = format!(
                "Cannot create table : name {} already exists in database {}",
                p_name, self.name
            );
            write_log(LogSeverity::Error, &l_msg, env!("CARGO_PKG_NAME"));
            return Err(l_msg);
        }

        let mut l_new_vec = None;
        if let Some(l_keys_vec) = p_keys {
            let mut l_vec_tmp = Vec::new();
            for l_key in l_keys_vec.iter() {
                l_vec_tmp.push((l_key.0.clone(), DbType::default_from_string(&l_key.1)?))
            }
            l_new_vec = Some(l_vec_tmp);
        }

        self.tables
            .push(DbTable::new(p_name.to_string(), l_new_vec));
        write_log(
            LogSeverity::Info,
            &format!("CREATED table {}", p_name),
            env!("CARGO_PKG_NAME"),
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
    /// * `p_name` - A reference to the name of the table.
    ///
    /// # Returns
    ///
    /// * `Ok` containing a mutable reference to the `DbTable` if the table exists.
    /// * `Err` containing an error message if the table does not exist.
    pub fn table(&mut self, p_name: &String) -> Result<&mut DbTable, String> {
        match self.find_table(p_name) {
            Ok(l_table) => Ok(l_table.1),
            Err(s) => Err(s),
        }
    }

    /// Deletes a table from the database.
    ///
    /// # Arguments
    ///
    /// * `p_name` - The name of the table to delete.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the table was successfully deleted, otherwise returns `Err` with an error message.
    pub fn delete_table(&mut self, p_name: &String) -> Result<(), String> {
        let l_index = self.find_table(p_name)?.0;
        // Use swap_remove for O(1) removal. Table ordering is not guaranteed.
        self.tables.swap_remove(l_index);

        write_log(
            LogSeverity::Info,
            &format!("DELETE table {}", p_name),
            env!("CARGO_PKG_NAME"),
        );
        Ok(())
    }

    /// Searches for a table with the given name in the database.
    ///
    /// # Arguments
    ///
    /// * `p_name` - The name of the table to search for, as a reference to a String.
    ///
    /// # Returns
    ///
    /// Returns a Result containing a tuple with the index and a mutable reference to the found table if it exists.
    /// If the table is found, the Result is Ok. Otherwise, an Err is returned with a descriptive error message.
    ///
    fn find_table(&mut self, p_name: &String) -> Result<(usize, &mut DbTable), String> {
        for l_table in self.tables.iter_mut().enumerate() {
            if l_table.1.name() == p_name {
                return Ok(l_table);
            }
        }

        let l_msg = format!("No table named {} in database {}", p_name, self.name);
        write_log(LogSeverity::Error, &l_msg, env!("CARGO_PKG_NAME"));
        Err(l_msg)
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
        let l_model = DbModel::new("ModelName".to_string());

        check_value(
            (1, 1),
            &l_model.name,
            &"ModelName".to_string(),
            rusttests::CheckType::Equal,
        )?;
        check_value(
            (1, 2),
            &l_model.tables.len(),
            &0,
            rusttests::CheckType::Equal,
        )?;

        Ok(())
    }

    #[test]
    fn new_model_empty_name() -> Result<(), String> {
        let l_model = DbModel::new("".to_string());

        check_value(
            (1, 1),
            &l_model.name,
            &"".to_string(),
            rusttests::CheckType::Equal,
        )?;
        check_value(
            (1, 2),
            &l_model.tables.len(),
            &0,
            rusttests::CheckType::Equal,
        )?;

        Ok(())
    }

    #[test]
    fn new_model_long_name() -> Result<(), String> {
        let l_long_name = "a".repeat(100_000);
        let l_model = DbModel::new(l_long_name.clone());

        check_value(
            (1, 1),
            &l_model.name,
            &l_long_name,
            rusttests::CheckType::Equal,
        )?;
        check_value(
            (1, 2),
            &l_model.tables.len(),
            &0,
            rusttests::CheckType::Equal,
        )?;

        Ok(())
    }

    #[test]
    fn new_model_strange_inputs() -> Result<(), String> {
        let l_strange_name = "🚀👨‍👩‍👧‍👦\n\t\r\\\"'".to_string();
        let l_model = DbModel::new(l_strange_name.clone());

        check_value(
            (1, 1),
            &l_model.name,
            &l_strange_name,
            rusttests::CheckType::Equal,
        )?;
        check_value(
            (1, 2),
            &l_model.tables.len(),
            &0,
            rusttests::CheckType::Equal,
        )?;

        Ok(())
    }

    #[test]
    fn new_table_nominal() -> Result<(), String> {
        let mut l_model = DbModel::new("ModelName".to_string());

        l_model.create_table(
            "NewTable",
            Some(vec![
                ("key1".to_string(), "String".to_string()),
                ("key2".to_string(), "Integer".to_string()),
            ]),
        )?;

        check_value(
            (1, 1),
            &l_model.tables.len(),
            &1,
            rusttests::CheckType::Equal,
        )?;

        l_model.create_table("OtherTable", None)?;

        check_value(
            (1, 1),
            &l_model.tables.len(),
            &2,
            rusttests::CheckType::Equal,
        )?;

        Ok(())
    }

    #[test]
    fn new_table_key_error() -> Result<(), String> {
        let mut l_model = DbModel::new("ModelName".to_string());

        check_result(
            (1, 1),
            l_model.create_table(
                "NewTable",
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
        let mut l_model = DbModel::new("ModelName".to_string());

        l_model.version[0] = 1;
        l_model.version[1] = 2;
        l_model.version[2] = 3;

        check_value(
            (1, 1),
            &l_model.version(),
            &"1.2.3".to_string(),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn get_table_nominal() -> Result<(), String> {
        let mut l_model = DbModel::new("ModelName".to_string());

        l_model.create_table(
            "NewTable",
            Some(vec![
                ("key1".to_string(), "String".to_string()),
                ("key2".to_string(), "Integer".to_string()),
            ]),
        )?;
        l_model.create_table(
            "OtherTable",
            Some(vec![
                ("key3".to_string(), "Float".to_string()),
                ("key4".to_string(), "UnsignedInt".to_string()),
            ]),
        )?;

        let l_table = l_model.table(&"NewTable".to_string())?;

        check_value(
            (1, 1),
            l_table.name(),
            &"NewTable".to_string(),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn get_table_name_error() -> Result<(), String> {
        let mut l_model = DbModel::new("ModelName".to_string());

        l_model.create_table(
            "NewTable",
            Some(vec![
                ("key1".to_string(), "String".to_string()),
                ("key2".to_string(), "Integer".to_string()),
            ]),
        )?;
        l_model.create_table(
            "OtherTable",
            Some(vec![
                ("key3".to_string(), "Float".to_string()),
                ("key4".to_string(), "UnsignedInt".to_string()),
            ]),
        )?;

        check_result((1, 1), l_model.table(&"Another table".to_string()), false)?;
        Ok(())
    }

    #[test]
    fn delete_table() -> Result<(), String> {
        let mut l_model = DbModel::new("ModelName".to_string());

        l_model.create_table(
            "NewTable",
            Some(vec![
                ("key1".to_string(), "String".to_string()),
                ("key2".to_string(), "Integer".to_string()),
            ]),
        )?;
        l_model.create_table(
            "OtherTable",
            Some(vec![
                ("key3".to_string(), "Float".to_string()),
                ("key4".to_string(), "UnsignedInt".to_string()),
            ]),
        )?;
        l_model.create_table(
            "ThirdTable",
            Some(vec![
                ("key5".to_string(), "Integer".to_string()),
                ("key6".to_string(), "Float".to_string()),
            ]),
        )?;

        l_model.delete_table(&"OtherTable".to_string())?;

        check_value(
            (1, 1),
            &l_model.tables_count(),
            &2,
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn delete_table_wrong_name() -> Result<(), String> {
        let mut l_model = DbModel::new("ModelName".to_string());

        l_model.create_table(
            "NewTable",
            Some(vec![
                ("key1".to_string(), "String".to_string()),
                ("key2".to_string(), "Integer".to_string()),
            ]),
        )?;
        l_model.create_table(
            "OtherTable",
            Some(vec![
                ("key3".to_string(), "Float".to_string()),
                ("key4".to_string(), "UnsignedInt".to_string()),
            ]),
        )?;
        l_model.create_table(
            "ThirdTable",
            Some(vec![
                ("key5".to_string(), "Integer".to_string()),
                ("key6".to_string(), "Float".to_string()),
            ]),
        )?;

        check_result(
            (1, 1),
            l_model.delete_table(&"StupidName".to_string()),
            false,
        )?;
        check_value(
            (1, 2),
            &l_model.tables_count(),
            &3,
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }
}
