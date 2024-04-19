//!
//! Database types definition
//!

/// Field type definition
#[derive(PartialEq)]
pub enum DbType {
    Integer,
    UnsignedInt,
    Float,
    String,
}

impl DbType {
    /// Checks if the String received can be interpreted as the correct database type
    pub fn check(&self, value: &String) -> Result<(), String> {
        match &self {
            DbType::Integer => match value.parse::<i32>() {
                Ok(_) => Ok(()),
                Err(_) => Err(format!("{} can't be interpreted as integer", value)),
            },
            DbType::UnsignedInt => match value.parse::<u32>() {
                Ok(_) => Ok(()),
                Err(_) => Err(format!(
                    "{} can't be interpreted as unsigned integer",
                    value
                )),
            },
            DbType::Float => match value.parse::<f32>() {
                Ok(_) => Ok(()),
                Err(_) => Err(format!("{} can't be interpreted as float", value)),
            },
            DbType::String => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::DbType;

    #[test]
    fn check_string() -> Result<(), String> {
        let type_string = DbType::String;
        match type_string.check(&"text".to_string()) {
            Ok(_) => Ok(()),
            Err(_) => Err("String input should return Ok".to_string()),
        }
    }

    #[test]
    fn check_float_ok() -> Result<(), String> {
        let type_float = DbType::Float;
        match type_float.check(&"1.23".to_string()) {
            Ok(_) => Ok(()),
            Err(_) => Err("Float input should return Ok".to_string()),
        }
    }

    #[test]
    fn check_float_ko() -> Result<(), String> {
        let type_float = DbType::Float;
        match type_float.check(&"text".to_string()) {
            Ok(_) => Err("Not float input should return Err".to_string()),
            Err(_) => Ok(()),
        }
    }

    #[test]
    fn check_int_ok() -> Result<(), String> {
        let type_int = DbType::Integer;
        match type_int.check(&"-14".to_string()) {
            Ok(_) => Ok(()),
            Err(_) => Err("Integer input should return Ok".to_string()),
        }
    }

    #[test]
    fn check_int_ko() -> Result<(), String> {
        let type_int = DbType::Integer;
        match type_int.check(&"12.5".to_string()) {
            Ok(_) => Err("Not integer input should return Err".to_string()),
            Err(_) => Ok(()),
        }
    }

    #[test]
    fn check_uint_ok() -> Result<(), String> {
        let type_uint = DbType::UnsignedInt;
        match type_uint.check(&"27".to_string()) {
            Ok(_) => Ok(()),
            Err(_) => Err("Unsigned integer input should return Ok".to_string()),
        }
    }

    #[test]
    fn check_uint_ko() -> Result<(), String> {
        let type_uint = DbType::UnsignedInt;
        match type_uint.check(&"-4".to_string()) {
            Ok(_) => Err("Not unsigned integer input should return Err".to_string()),
            Err(_) => Ok(()),
        }
    }
}
