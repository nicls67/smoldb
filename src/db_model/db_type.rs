//!
//! Database types definition
//!

/// Field type definition
#[derive(PartialEq, Clone, Debug)]
pub enum DbType {
    Integer(i32),
    UnsignedInt(u32),
    Float(f32),
    String(String),
}

impl DbType {
    /// Converts a String into the variant contained by `self`, returns `Err` if the string doesn't match the correct type
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
        }
    }

    /// Converts the variant conatained by `self` into a String
    pub fn into_string(&self) -> String {
        match &self {
            DbType::Integer(i) => i.to_string(),
            DbType::UnsignedInt(u) => u.to_string(),
            DbType::Float(f) => f.to_string(),
            DbType::String(s) => s.clone(),
        }
    }

    /// Checks coherency between variant in `self` and the given type
    ///
    /// If `new_type` is String, it is assumed to be coherent with all types
    pub fn check_type(&self, new_type: &DbType) -> Result<(), String> {
        match new_type {
            DbType::Integer(_) => match &self {
                DbType::Integer(_) => Ok(()),
                _ => Err(format!("Database type incompatibility")),
            },
            DbType::UnsignedInt(_) => match &self {
                DbType::UnsignedInt(_) => Ok(()),
                _ => Err(format!("Database type incompatibility")),
            },
            DbType::Float(_) => match &self {
                DbType::Float(_) => Ok(()),
                _ => Err(format!("Database type incompatibility")),
            },
            DbType::String(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::DbType;

    #[test]
    fn check_string() -> Result<(), String> {
        let type_string = DbType::String("a".to_string());
        match type_string.convert(&"text".to_string()) {
            Ok(t) => {
                if t == DbType::String("text".to_string()) {
                    Ok(())
                } else {
                    Err(format!("Wrong string returned"))
                }
            }
            Err(_) => Err("String input should return Ok".to_string()),
        }
    }

    #[test]
    fn check_float_ok() -> Result<(), String> {
        let type_float = DbType::Float(0.0);
        match type_float.convert(&"1.23".to_string()) {
            Ok(t) => {
                if t == DbType::Float(1.23) {
                    Ok(())
                } else {
                    Err(format!("Wrong float returned"))
                }
            }
            Err(_) => Err("Float input should return Ok".to_string()),
        }
    }

    #[test]
    fn check_float_ko() -> Result<(), String> {
        let type_float = DbType::Float(0.0);
        match type_float.convert(&"text".to_string()) {
            Ok(_) => Err("Not float input should return Err".to_string()),
            Err(_) => Ok(()),
        }
    }

    #[test]
    fn check_int_ok() -> Result<(), String> {
        let type_int = DbType::Integer(0);
        match type_int.convert(&"-14".to_string()) {
            Ok(t) => {
                if t == DbType::Integer(-14) {
                    Ok(())
                } else {
                    Err(format!("Wrong integer returned"))
                }
            }
            Err(_) => Err("Integer input should return Ok".to_string()),
        }
    }

    #[test]
    fn check_int_ko() -> Result<(), String> {
        let type_int = DbType::Integer(0);
        match type_int.convert(&"12.5".to_string()) {
            Ok(_) => Err("Not integer input should return Err".to_string()),
            Err(_) => Ok(()),
        }
    }

    #[test]
    fn check_uint_ok() -> Result<(), String> {
        let type_uint = DbType::UnsignedInt(0);
        match type_uint.convert(&"27".to_string()) {
            Ok(t) => {
                if t == DbType::UnsignedInt(27) {
                    Ok(())
                } else {
                    Err(format!("Wrong unsigned integer returned"))
                }
            }
            Err(_) => Err("Unsigned integer input should return Ok".to_string()),
        }
    }

    #[test]
    fn check_uint_ko() -> Result<(), String> {
        let type_uint = DbType::UnsignedInt(0);
        match type_uint.convert(&"-4".to_string()) {
            Ok(_) => Err("Not unsigned integer input should return Err".to_string()),
            Err(_) => Ok(()),
        }
    }
}
