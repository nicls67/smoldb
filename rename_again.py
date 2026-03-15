with open("src/db_model/db_table.rs", "r") as f:
    content = f.read()

# Let's rename the test to just get_unique_boolean_values_for_key
# as the reviewer pointed out it tests both empty and non-empty.
# Wait, let's keep it clean. Let's make get_unique_boolean_values_for_key_empty
# strictly test the empty table case. And we can also add test for get_unique_boolean_values_for_key
# or get_unique_boolean_values_for_key_subset if they don't exist.

# Wait, `get_unique_boolean_values_for_key_empty` is the test I just added. Let's modify it to ONLY test empty case.
new_test_empty = """
    #[test]
    fn get_unique_boolean_values_for_key_empty() -> Result<(), String> {
        let keys = vec![
            ("key1".to_string(), DbType::Bool(false)),
            ("key2".to_string(), DbType::Bool(false)),
            ("key3".to_string(), DbType::String("0.0".to_string())),
        ];
        let table = DbTable::new("Table".to_string(), Some(keys));

        let res = check_result(
            (1, 1),
            table.get_unique_boolean_values_for_key(None, &"key1".to_string()),
            true,
        )?.unwrap();
        check_option((1, 2), res, false)?;

        Ok(())
    }
"""

import re
content = re.sub(r'#\[test\]\n    fn get_unique_boolean_values_for_key_empty\(\).*?Ok\(\(\)\)\n    }', new_test_empty.strip(), content, flags=re.DOTALL)

with open("src/db_model/db_table.rs", "w") as f:
    f.write(content)
