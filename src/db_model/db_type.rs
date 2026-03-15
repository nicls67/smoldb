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
    /// * `p_value` - The value to be converted.
    ///
    /// # Errors
    ///
    /// Returns a `Result` that contains the converted `DbType` if the conversion was successful, or a `String` containing an error message if the conversion failed.
    ///
    pub fn convert(&self, p_value: &String) -> Result<DbType, String> {
        match &self {
            DbType::Integer(_) => match p_value.parse::<i32>() {
                Ok(l_i) => Ok(DbType::Integer(l_i)),
                Err(_) => Err(format!("{} can't be interpreted as integer", p_value)),
            },
            DbType::UnsignedInt(_) => match p_value.parse::<u32>() {
                Ok(l_u) => Ok(DbType::UnsignedInt(l_u)),
                Err(_) => Err(format!(
                    "{} can't be interpreted as unsigned integer",
                    p_value
                )),
            },
            DbType::Float(_) => match p_value.parse::<f32>() {
                Ok(l_f) => Ok(DbType::Float(l_f)),
                Err(_) => Err(format!("{} can't be interpreted as float", p_value)),
            },
            DbType::String(_) => Ok(DbType::String(p_value.clone())),
            DbType::Date(_) => match NaiveDate::parse_from_str(p_value, "%d/%m/%Y") {
                Ok(l_d) => Ok(DbType::Date(l_d)),
                Err(_) => Err(format!("{} can't be interpreted as a date", p_value)),
            },
            DbType::Bool(_) => match p_value.parse::<bool>() {
                Ok(l_b) => Ok(DbType::Bool(l_b)),
                Err(_) => Err(format!("{} can't be interpreted as a boolean", p_value)),
            },
        }
    }

    /// Checks if the given `new_type` is compatible with the current database type.
    ///
    /// # Arguments
    ///
    /// * `p_new_type` - The new database type to check.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the new type is compatible.
    /// * `Err(String)` with an error message if the types are incompatible.
    pub fn check_type(&self, p_new_type: &DbType) -> Result<(), String> {
        let l_res = match p_new_type {
            DbType::Integer(_) => matches!(self, DbType::Integer(_)),
            DbType::UnsignedInt(_) => matches!(self, DbType::UnsignedInt(_)),
            DbType::Float(_) => matches!(self, DbType::Float(_)),
            DbType::String(_) => true,
            DbType::Date(_) => matches!(self, DbType::Date(_)),
            DbType::Bool(_) => matches!(self, DbType::Bool(_)),
        };

        if l_res {
            Ok(())
        } else {
            let l_msg = "Database type incompatibility".to_string();
            write_log(LogSeverity::Error, &l_msg, env!("CARGO_PKG_NAME"));
            Err(l_msg)
        }
    }

    /// Creates a default value of the specified database type from a string.
    ///
    /// # Arguments
    ///
    /// * `p_type_name` - The name of the database type.
    ///
    /// # Returns
    ///
    /// Returns a default value of the specified database type wrapped in a `Result`,
    /// or an error message if the database type is unknown.
    ///
    pub fn default_from_string(p_type_name: &String) -> Result<DbType, String> {
        match p_type_name.to_lowercase().as_str() {
            "integer" => Ok(DbType::Integer(0)),
            "unsignedinteger" | "unsignedint" => Ok(DbType::UnsignedInt(0)),
            "float" => Ok(DbType::Float(0.0)),
            "boolean" | "bool" => Ok(DbType::Bool(false)),
            "string" => Ok(DbType::String(String::new())),
            "date" => Ok(DbType::Date(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap())),
            _ => {
                let l_msg = format!("Unknown database type : {}", p_type_name);
                write_log(LogSeverity::Error, &l_msg, env!("CARGO_PKG_NAME"));
                Err(l_msg)
            }
        }
    }
}

/// Display implementation for `DbType`.
///
/// Converts the value of a `DbType` enum variant to a `String` representation.
impl fmt::Display for DbType {
    fn fmt(&self, p_f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbType::Integer(l_i) => write!(p_f, "{}", l_i),
            DbType::UnsignedInt(l_u) => write!(p_f, "{}", l_u),
            DbType::Float(l_fl) => write!(p_f, "{}", l_fl),
            DbType::String(l_s) => write!(p_f, "{}", l_s),
            DbType::Date(l_d) => write!(p_f, "{}", l_d.format("%d/%m/%Y")),
            DbType::Bool(l_b) => write!(p_f, "{}", l_b),
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
        let l_type_string = DbType::String("a".to_string());

        let l_val =
            check_result((1, 1), l_type_string.convert(&"text".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &l_val,
            &DbType::String("text".to_string()),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_float_ok() -> Result<(), String> {
        let l_type_float = DbType::Float(0.0);

        let l_val =
            check_result((1, 1), l_type_float.convert(&"1.23".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &l_val,
            &DbType::Float(1.23),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_float_ko() -> Result<(), String> {
        let l_type_float = DbType::Float(0.0);

        check_result((1, 1), l_type_float.convert(&"text".to_string()), false)?;
        Ok(())
    }

    #[test]
    fn check_int_ok() -> Result<(), String> {
        let l_type_int = DbType::Integer(0);

        let l_val = check_result((1, 1), l_type_int.convert(&"-14".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &l_val,
            &DbType::Integer(-14),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_int_ko() -> Result<(), String> {
        let l_type_int = DbType::Integer(0);

        check_result((1, 1), l_type_int.convert(&"12.5".to_string()), false)?;
        Ok(())
    }

    #[test]
    fn check_uint_ok() -> Result<(), String> {
        let l_type_uint = DbType::UnsignedInt(0);

        let l_val =
            check_result((1, 1), l_type_uint.convert(&"27".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &l_val,
            &DbType::UnsignedInt(27),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_uint_ko() -> Result<(), String> {
        let l_type_uint = DbType::UnsignedInt(0);

        check_result((1, 1), l_type_uint.convert(&"-4".to_string()), false)?;
        Ok(())
    }

    #[test]
    fn check_date_ok() -> Result<(), String> {
        let l_type_date = DbType::default_from_string(&"Date".to_string())?;

        let l_val =
            check_result((1, 1), l_type_date.convert(&"12/07/1998".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &l_val,
            &DbType::Date(NaiveDate::from_ymd_opt(1998, 7, 12).unwrap()),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_date_ko() -> Result<(), String> {
        let l_type_date = DbType::default_from_string(&"Date".to_string())?;

        check_result((1, 1), l_type_date.convert(&"text".to_string()), false)?;
        Ok(())
    }

    #[test]
    fn check_bool_ok() -> Result<(), String> {
        let l_type_bool = DbType::default_from_string(&"Bool".to_string())?;

        let l_val =
            check_result((1, 1), l_type_bool.convert(&"true".to_string()), true)?.unwrap();
        check_struct(
            (1, 2),
            &l_val,
            &DbType::Bool(true),
            rusttests::CheckType::Equal,
        )?;
        let l_val =
            check_result((2, 1), l_type_bool.convert(&"false".to_string()), true)?.unwrap();
        check_struct(
            (2, 2),
            &l_val,
            &DbType::Bool(false),
            rusttests::CheckType::Equal,
        )?;
        Ok(())
    }

    #[test]
    fn check_bool_ko() -> Result<(), String> {
        let l_type_bool = DbType::default_from_string(&"Bool".to_string())?;

        check_result((1, 1), l_type_bool.convert(&"text".to_string()), false)?;
        Ok(())
    }

    #[test]
    fn check_default_from_string_ok() -> Result<(), String> {
        let l_val = check_result((1, 1), DbType::default_from_string(&"integer".to_string()), true)?.unwrap();
        check_struct((1, 2), &l_val, &DbType::Integer(0), rusttests::CheckType::Equal)?;

        let l_val = check_result((1, 3), DbType::default_from_string(&"Integer".to_string()), true)?.unwrap();
        check_struct((1, 4), &l_val, &DbType::Integer(0), rusttests::CheckType::Equal)?;

        let l_val = check_result((2, 1), DbType::default_from_string(&"unsignedinteger".to_string()), true)?.unwrap();
        check_struct((2, 2), &l_val, &DbType::UnsignedInt(0), rusttests::CheckType::Equal)?;

        let l_val = check_result((2, 3), DbType::default_from_string(&"UnsignedInt".to_string()), true)?.unwrap();
        check_struct((2, 4), &l_val, &DbType::UnsignedInt(0), rusttests::CheckType::Equal)?;

        let l_val = check_result((3, 1), DbType::default_from_string(&"float".to_string()), true)?.unwrap();
        check_struct((3, 2), &l_val, &DbType::Float(0.0), rusttests::CheckType::Equal)?;

        let l_val = check_result((4, 1), DbType::default_from_string(&"boolean".to_string()), true)?.unwrap();
        check_struct((4, 2), &l_val, &DbType::Bool(false), rusttests::CheckType::Equal)?;

        let l_val = check_result((4, 3), DbType::default_from_string(&"Bool".to_string()), true)?.unwrap();
        check_struct((4, 4), &l_val, &DbType::Bool(false), rusttests::CheckType::Equal)?;

        let l_val = check_result((5, 1), DbType::default_from_string(&"string".to_string()), true)?.unwrap();
        check_struct((5, 2), &l_val, &DbType::String("".to_string()), rusttests::CheckType::Equal)?;

        let l_val = check_result((6, 1), DbType::default_from_string(&"date".to_string()), true)?.unwrap();
        check_struct((6, 2), &l_val, &DbType::Date(NaiveDate::from_ymd_opt(1990, 1, 1).unwrap()), rusttests::CheckType::Equal)?;

        Ok(())
    }

    #[test]
    fn check_default_from_string_ko() -> Result<(), String> {
        check_result((1, 1), DbType::default_from_string(&"Unknown".to_string()), false)?;
        Ok(())
    }
}
