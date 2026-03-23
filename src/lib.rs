use package_infos::pkg_infos;
use package_infos::PackageInfos;
use rustlog::{write_log, LogSeverity};
use std::fs;
use std::path::PathBuf;

pub use db_model::MatchingCriteria;
pub use db_model::{DbModel, DbTable};

#[doc = include_str!("../README.md")]
mod db_model;

pkg_infos!(rustlog);

#[derive(PartialEq, Debug, Clone)]
pub struct SmolDb {
    model: DbModel,
    db_file: Option<PathBuf>,
}

impl SmolDb {
    /// Constructs a new SmolDb with the provided database name.
    ///
    /// # Arguments
    ///
    /// * `p_db_name` - A str slice that holds the name of the database
    pub fn init(p_db_name: &str) -> SmolDb {
        SmolDb {
            model: DbModel::new(p_db_name.to_string()),
            db_file: None,
        }
    }

    /// Loads a database file and creates a `SmolDb` instance.
    ///
    /// # Arguments
    ///
    /// * `p_db_file` - The path to the database file to load.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating success or failure. If successful, it returns a `SmolDb`
    /// instance loaded with the data from the database file. If an error occurs, it returns
    /// a `String` containing the error message.
    ///
    pub fn load(p_db_file: PathBuf) -> Result<SmolDb, String> {
        let l_json = fs::read_to_string(&p_db_file).map_err(|p_e| {
            Self::log_and_create_load_err_msg(format!(
                "{}: {}",
                p_db_file.to_str().unwrap_or("Unkown database file"),
                p_e
            ))
        })?;

        let l_model = serde_json::from_str::<DbModel>(&l_json)
            .map_err(|p_e| Self::log_and_create_load_err_msg(format!("{}", p_e)))?;

        Ok(SmolDb {
            model: l_model,
            db_file: Some(p_db_file),
        })
    }

    /// Logs an error message and creates a new error message string.
    ///
    /// This function takes the `p_msg` parameter and formats it as `Error while opening database {p_msg}`.
    /// It then logs this error message with a severity of `Error` using the `write_log` function from the `rustlog` module.
    /// The logged error message includes the name of the package specified in the `CARGO_PKG_NAME` environment variable.
    /// Finally, the function returns the new error message string.
    ///
    /// # Arguments
    ///
    /// * `p_msg` - The error message to include in the new error message string.
    ///
    /// # Returns
    ///
    /// The newly created error message string.
    fn log_and_create_load_err_msg(p_msg: String) -> String {
        let l_new_msg = format!("Error while opening database {}", p_msg);
        write_log(LogSeverity::Error, &l_new_msg, env!("CARGO_PKG_NAME"));
        l_new_msg
    }

    /// Retrieves a mutable reference to the `DbModel` of the current instance.
    ///
    /// # Returns
    ///
    /// A mutable reference to the `DbModel`.
    ///
    pub fn database(&mut self) -> &mut DbModel {
        &mut self.model
    }

    /// Sets the database file for the given object.
    ///
    /// # Arguments
    ///
    /// * `p_file` - A `PathBuf` representing the path to the database file.
    ///
    pub fn set_database_file(&mut self, p_file: PathBuf) {
        self.db_file = Some(p_file)
    }

    /// Writes an error message.
    ///
    /// # Arguments
    ///
    /// * `p_msg` - The error message.
    ///
    /// # Returns
    ///
    /// The error message that was logged.
    ///
    fn write_error_msg_save(p_msg: String) -> String {
        let l_msg = format!("Error while saving database : {}", p_msg);
        write_log(LogSeverity::Error, &l_msg, env!("CARGO_PKG_NAME"));
        l_msg
    }

    /// Saves the database into a JSON file
    ///
    /// This function attempts to save the current state of the database into a JSON file.
    /// When the function is called, it determines if a file for the database has been configured.
    /// If the file is configured, the function serializes the database model into a JSON string
    /// and attempts to write this string into the file.
    ///
    /// On successful operation, it logs an informational message indicating the successful operation.
    /// If an error occurs during processing (either during serialization or file writing),
    /// it logs an error message with the error details.
    ///
    /// If no database file is configured, it logs a warning message and returns an error.
    ///
    /// # Errors
    ///
    /// This function will return an `Err` variant of `Result` in the following situations:
    ///
    /// * If the database file is not configured (`self.db_file` is `None`).
    /// * If there's an error serializing the database model into a JSON string.
    /// * If there's an error writing the serialized string into the file.
    ///
    /// The error message will be a string that explains the error cause.
    ///
    /// # Returns
    ///
    /// * Returns `Ok` variant of `Result` indicating a successful operation.
    /// * Will return `Err` variant of `Result` in case of any error scenarios along with the appropriate error message string.
    ///
    pub fn save(&self) -> Result<(), String> {
        if let Some(l_db_file) = &self.db_file {
            match serde_json::to_string_pretty(&self.model) {
                Ok(l_json) => match fs::write(l_db_file, &l_json) {
                    Ok(_) => {
                        let l_info_msg = format!("Database saved to file {}", l_db_file.display());
                        write_log(LogSeverity::Info, &l_info_msg, env!("CARGO_PKG_NAME"));
                        Ok(())
                    }
                    Err(e) => Err(Self::write_error_msg_save(e.to_string())),
                },
                Err(e) => Err(Self::write_error_msg_save(e.to_string())),
            }
        } else {
            let l_warn_msg = "Cannot save database, no database file configured".to_string();
            write_log(LogSeverity::Warning, &l_warn_msg, env!("CARGO_PKG_NAME"));
            Err(l_warn_msg)
        }
    }

    /// Retrieves the library version and the authors as defined in the package metadata.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - The version of the library as a `&'static str`.
    /// - The authors of the library as a `&'static str`.
    pub fn get_lib_infos() -> (&'static str, &'static str) {
        (env!("CARGO_PKG_VERSION"), env!("CARGO_PKG_AUTHORS"))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;
    use std::path::PathBuf;

    use rusttests::{check_result, check_struct, CheckType};

    use crate::SmolDb;

    #[test]
    fn save_no_file() -> Result<(), String> {
        let l_db = SmolDb::init("db_name");

        check_result((1, 1), l_db.save(), false)?;
        Ok(())
    }

    #[test]
    fn save_and_load() -> Result<(), String> {
        let mut l_db = SmolDb::init("db_name");

        l_db.set_database_file(PathBuf::from("file.json"));

        check_result((1, 1), l_db.save(), true)?;

        let l_new_db = SmolDb::load(PathBuf::from("file.json"))?;

        remove_file("file.json").unwrap_or(());

        check_struct((2, 1), &l_new_db, &l_db, CheckType::Equal)
    }
}
