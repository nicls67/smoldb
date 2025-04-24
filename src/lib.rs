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
    /// * `db_name` - A str slice that holds the name of the database
    pub fn init(db_name: &str) -> SmolDb {
        SmolDb {
            model: DbModel::new(db_name.to_string()),
            db_file: None,
        }
    }

    /// Loads a database file and creates a `SmolDb` instance.
    ///
    /// # Arguments
    ///
    /// * `db_file` - The path to the database file to load.
    ///
    /// # Returns
    ///
    /// Returns a `Res:::lt` indicating su,,ccess or failure. If successful, it returns a `SmolDb`
    /// instance loaded with the data from the database file. If an error occurs, it returns
    /// a `String` containing the error message.
    ///
    pub fn load(db_file: PathBuf) -> Result<SmolDb, String> {
        let json = fs::read_to_string(&db_file).map_err(|e| {
            Self::log_and_create_load_err_msg(format!("{}: {}", db_file.to_str().unwrap(), e))
        })?;

        let model = serde_json::from_str::<DbModel>(&json)
            .map_err(|e| Self::log_and_create_load_err_msg(format!("{}", e)))?;

        Ok(SmolDb {
            model,
            db_file: Some(db_file),
        })
    }

    /// Logs an error message and creates a new error message string.
    ///
    /// This function takes the `msg` parameter and formats it as `Error while opening database {msg}`.
    /// It then logs this error message with a severity of `Error` using the `write_log` function from the `rustlog` module.
    /// The logged error message includes the name of the package specified in the `CARGO_PKG_NAME` environment variable.
    /// Finally, the function returns the new error message string.
    ///
    /// # Arguments
    ///
    /// * `msg` - The error message to include in the new error message string.
    ///
    /// # Returns
    ///
    /// The newly created error message string.
    fn log_and_create_load_err_msg(msg: String) -> String {
        let new_msg = format!("Error while opening database {}", msg);
        write_log(
            LogSeverity::Error,
            &new_msg,
            &env!("CARGO_PKG_NAME").to_string(),
        );
        new_msg
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
    /// * `file` - A `PathBuf` representing the path to the database file.
    ///
    pub fn set_database_file(&mut self, file: PathBuf) {
        self.db_file = Some(file)
    }

    /// Writes an error message.
    ///
    /// # Arguments
    ///
    /// * `msg` - The error message.
    ///
    /// # Returns
    ///
    /// The error message that was logged.
    ///
    fn write_error_msg_save(msg: String) -> String {
        let msg = format!("Error while saving database : {}", msg);
        write_log(
            LogSeverity::Error,
            &msg,
            &env!("CARGO_PKG_NAME").to_string(),
        );
        msg
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
        if let Some(db_file) = &self.db_file {
            match serde_json::to_string(&self.model) {
                Ok(json) => match fs::write(db_file, &json) {
                    Ok(_) => {
                        let info_msg = format!("Database saved to file {}", db_file.display());
                        write_log(
                            LogSeverity::Info,
                            &info_msg,
                            &env!("CARGO_PKG_NAME").to_string(),
                        );
                        Ok(())
                    }
                    Err(e) => Err(Self::write_error_msg_save(e.to_string())),
                },
                Err(e) => Err(Self::write_error_msg_save(e.to_string())),
            }
        } else {
            let warn_msg = "Cannot save database, no database file configured".to_string();
            write_log(
                LogSeverity::Warning,
                &warn_msg,
                &env!("CARGO_PKG_NAME").to_string(),
            );
            Err(warn_msg)
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
        let db = SmolDb::init("db_name");

        check_result((1, 1), db.save(), false)?;
        Ok(())
    }

    #[test]
    fn save_and_load() -> Result<(), String> {
        let mut db = SmolDb::init("db_name");

        db.set_database_file(PathBuf::from("file.json"));

        check_result((1, 1), db.save(), true)?;

        let new_db = SmolDb::load(PathBuf::from("file.json"))?;

        remove_file("file.json").unwrap_or(());

        check_struct((2, 1), &new_db, &db, CheckType::Equal)
    }
}
