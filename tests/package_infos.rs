//!
//! test for 'package_infos' macro
//!

use package_infos::PackageInfos;
use rusttests::{check_value, CheckType};
use smoldb::get_package_infos;

#[test]
fn test_macro() -> Result<(), String> {
    let l_infos = get_package_infos();

    let l_expected = PackageInfos {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        authors: env!("CARGO_PKG_AUTHORS"),
        description: env!("CARGO_PKG_DESCRIPTION"),
        dependencies: vec![rustlog::get_package_infos()],
    };

    check_value((1, 1), &l_infos, &l_expected, CheckType::Equal)?;

    // Uncomment for debugging package infos
    //println!("{}", l_infos);

    Ok(())
}
