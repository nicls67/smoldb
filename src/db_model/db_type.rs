//!
//! Database types definition
//!

use std::fmt;

use chrono::NaiveDate;
use rustlog::{write_log, LogSeverity};
use serde_derive::{Deserialize, Serialize};

/// Field type definition
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum DbType {
    Integer(i32),
    UnsignedInt(u32),
    Float(f32),
    String(String),
    Date(NaiveDate),
    Bool(bool),
}

impl DbType {
    /// Converts a value to a specific database type.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to be converted.
    ///
    /// # Errors
    ///
    /// Returns a `Result` that contains the converted `DbType` if the conversion was successful, or a `String` containing an error message if the conversion failed.
    ///
    pub fn convert(&self, value: &String) -> Result<DbType, String> {
        match &self {
            DbType::Integer(_) => match value.parse::<i32>() {
                Ok(i) => Ok(DbType::Integer(i)),
                Err(_) => Err(format!("{} can't be interpreted as integer", value)),
            },
            DbType::UnsignedInt(_) => match value.parse::<u32>() {
                Ok(u) => Ok(DbType::UnsignedInt(u)),
                Err(_) => Err(format!(
                    "{} can't be interpreted as unsigned integer",
                    value
                )),
            },
            DbType::Float(_) => match value.parse::<f32>() {
                Ok(f) => Ok(DbType::Float(f)),
                Err(_) => Err(format!("{} can't be interpreted as float", value)),
            },
            DbType::String(_) => Ok(DbType::String(value.clone())),
            DbType::Date(_) => match NaiveDate::parse_from_str(value, "%d/%m/%Y") {
                Ok(d) => Ok(DbType::Date(d)),
                Err(_) => Err(format!("{} can't be interpreted as a date", value)),
            },
            DbType::Bool(_) => match value.parse::<bool>() {
                Ok(b) => Ok(DbType::Bool(b)),
                Err(_) => Err(format!("{} can't be interpreted as a boolean", value)),
            },
        }
    }

    /// Checks if the given `new_type` is compatible with the current database type.
    ///
    /// # Arguments
    ///
    /// * `new_type` - The new database type to check.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the new type is compatible.
    /// * `Err(String)` with an error message if the types are incompatible.
    pub fn check_type(&self, new_type: &DbType) -> Result<(), String> {
        let res = match new_type {
            DbType::Integer(_) => matches!(self, DbType::Integer(_)),
            DbType::UnsignedInt(_) => matches!(self, DbType::UnsignedInt(_)),
            DbType::Float(_) => matches!(self, DbType::Float(_)),
            DbType::String(_) => true,
            DbType::Date(_) => matches!(self, DbType::Date(_)),
            DbType::Bool(_) => matches!(self, DbType::Bool(_)),
        };

        if res {
            Ok(())
        } else {
            let msg = "Database type incompatibility".to_string();
            write_log(LogSeverity::Error, &msg, env!("CARGO_PKG_NAME"));
            Err(msg)
        }
    }

    /// Creates a default value of the specified database type from a string.
    ///
    /// # Arguments
    ///
    /// * `type_name` - The name of the database type.
    ///
    /// # Returns
    ///
    /// Returns a default value of the specified database type wrapped in a `Result`,
    /// or an error message if the database type is unknown.
    ///
    pub fn default_from_string(type_name: &String) -> Result<DbType, String> {
        match type_name.as_str() {
            "Integer" => Ok(DbType::Integer(0)),
            "UnsignedInt" => Ok(DbType::UnsignedInt(0)),
            "Float" => Ok(DbType::Float(0.0)),
            "Bool" => Ok(DbType::Bool(false)),
            "String" => Ok(DbType::String(String::new())),
            "Date" => Ok(DbType::Date(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap())),
            _ => {
                let msg = format!("Unknown database type : {}", type_name);
                write_log(LogSeverity::Error, &msg, env!("CARGO_PKG_NAME"));
                Err(msg)
            }
        }
    }
}

/// Display implementation for `DbType`.
///
/// Converts the value of a `DbType` enum variant to a `String` representation.
impl fmt::Display for DbType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbType::Integer(i) => write!(f, "{}", i),
            DbType::UnsignedInt(u) => write!(f, "{}", u),
            DbType::Float(fl) => write!(f, "{}", fl),
            DbType::String(s) => write!(f, "{}", s),
            DbType::Date(d) => write!(f, "{}", d.format("%d/%m/%Y")),
            DbType::Bool(b) => write!(f, "{}", b),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use rusttests::{check_result, check_struct};

    use super::DbType;

    #[test]
    fn check_string() -> Result<(), String> {
        let type_string = DbType::String("a".to_string());

        let val = check_result((1, 1), type_string.convert(&"text".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &val,
            &DbType::String("text".to_string()),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_float_ok() -> Result<(), String> {
        let type_float = DbType::Float(0.0);

        let val = check_result((1, 1), type_float.convert(&"1.23".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &val,
            &DbType::Float(1.23),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_float_ko() -> Result<(), String> {
        let type_float = DbType::Float(0.0);

        check_result((1, 1), type_float.convert(&"text".to_string()), false)?;
        Ok(())
    }

    #[test]
    fn check_int_ok() -> Result<(), String> {
        let type_int = DbType::Integer(0);

        let val = check_result((1, 1), type_int.convert(&"-14".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &val,
            &DbType::Integer(-14),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_int_ko() -> Result<(), String> {
        let type_int = DbType::Integer(0);

        check_result((1, 1), type_int.convert(&"12.5".to_string()), false)?;
        Ok(())
    }

    #[test]
    fn check_uint_ok() -> Result<(), String> {
        let type_uint = DbType::UnsignedInt(0);

        let val = check_result((1, 1), type_uint.convert(&"27".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &val,
            &DbType::UnsignedInt(27),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_uint_ko() -> Result<(), String> {
        let type_uint = DbType::UnsignedInt(0);

        check_result((1, 1), type_uint.convert(&"-4".to_string()), false)?;
        Ok(())
    }

    #[test]
    fn check_date_ok() -> Result<(), String> {
        let type_date = DbType::default_from_string(&"Date".to_string())?;

        let val =
            check_result((1, 1), type_date.convert(&"12/07/1998".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &val,
            &DbType::Date(NaiveDate::from_ymd_opt(1998, 7, 12).unwrap()),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_date_ko() -> Result<(), String> {
        let type_date = DbType::default_from_string(&"Date".to_string())?;

        check_result((1, 1), type_date.convert(&"text".to_string()), false)?;
        Ok(())
    }

    #[test]
    fn check_bool_ok() -> Result<(), String> {
        let type_bool = DbType::default_from_string(&"Bool".to_string())?;

        let val = check_result((1, 1), type_bool.convert(&"true".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &val,
            &DbType::Bool(true),
            rusttests::CheckType::Equal,
        )?;
        let val = check_result((2, 1), type_bool.convert(&"false".to_string()), true)?.unwrap();
        check_struct(
            (2, 2),
            &val,
            &DbType::Bool(false),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_bool_ko() -> Result<(), String> {
        let type_bool = DbType::default_from_string(&"Bool".to_string())?;

        check_result((1, 1), type_bool.convert(&"text".to_string()), false)?;
        Ok(())
    }
}
