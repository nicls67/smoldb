#[doc = include_str!("../README.md")]
mod db_model;

use std::{fs, path::Path};

pub use db_model::{DbModel, DbTable};
use rustlog::write_log;

#[derive(PartialEq)]
pub struct SmolDb {
    model: DbModel,
    db_file: Option<&'static Path>,
}

impl SmolDb {
    /// Database initialization
    pub fn init(db_name: String) -> SmolDb {
        SmolDb {
            model: DbModel::new(db_name),
            db_file: None,
        }
    }

    /// Load databse from file
    pub fn load(db_file: &'static Path) -> Result<SmolDb, String> {
        match fs::read_to_string(db_file) {
            Ok(json) => {
                match serde_json::from_str::<DbModel>(&json) {
                    Ok(model) => {
                        Ok(SmolDb {
                            model,
                            db_file: Some(db_file)
                        })
                    },
                    Err(e) => {
                        let msg = format!("{}",e);
                        write_log(
                            rustlog::LogSeverity::Error,
                            &msg,
                            &env!("CARGO_PKG_NAME").to_string(),
                        );
                        Err(msg)
                    },
                }
            },
            Err(e) => {
                let msg = format!("{}",e);
                write_log(
                    rustlog::LogSeverity::Error,
                    &msg,
                    &env!("CARGO_PKG_NAME").to_string(),
                );
                Err(msg)
            },
        }
    }

    /// Returns reference to database model
    pub fn database(&mut self) -> &mut DbModel {
        &mut self.model
    }

    /// Sets path to database file
    pub fn set_database_file(&mut self, file: &'static Path) {
        self.db_file = Some(file)
    }

    /// Save database to file
    pub fn save(&self) -> Result<(), String> {
        if self.db_file.is_some() {
            match serde_json::to_string(&self.model) {
                Ok(json) => match fs::write(self.db_file.unwrap(), json) {
                    Ok(_) => {
                        write_log(
                            rustlog::LogSeverity::Info,
                            &format!("Database saved to file {}", self.db_file.unwrap().to_str().unwrap()),
                            &env!("CARGO_PKG_NAME").to_string(),
                        );
                        Ok(())
                    },
                    Err(e) => {
                        let msg = format!("{}", e);
                        write_log(
                            rustlog::LogSeverity::Error,
                            &msg,
                            &env!("CARGO_PKG_NAME").to_string(),
                        );
                        Err(msg)
                    },
                },
                Err(e) => {
                    let msg = format!("{}",e);
                    write_log(
                        rustlog::LogSeverity::Error,
                        &msg,
                        &env!("CARGO_PKG_NAME").to_string(),
                    );
                    Err(msg)
                }
            }
        }
        else {
            let msg = format!("Cannot save database, no database file configured");
            write_log(
                rustlog::LogSeverity::Warning,
                &msg,
                &env!("CARGO_PKG_NAME").to_string(),
            );
            Err(msg)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::remove_file, path::Path};

    use crate::SmolDb;

    #[test]
    fn save_no_file() -> Result<(), String> {
        let db = SmolDb::init("db_name".to_string());

        match db.save() {
            Ok(_) => Err(format!("Result should be Err")),
            Err(_) => Ok(()),
        }
    }

    #[test]
    fn save_and_load() -> Result<(), String> {
        let mut db = SmolDb::init("db_name".to_string());

        db.set_database_file(Path::new("test.json"));

        match db.save() {
            Ok(_) => (),
            Err(_) => return Err(format!("Result should be Ok")),
        };

        let new_db = SmolDb::load(Path::new("test.json"))?;

        remove_file("test.json").unwrap_or(());

        if db == new_db {
            Ok(())
        } else {
            Err(format!("Loaded database should be equal to intial database"))
        }

    }
}