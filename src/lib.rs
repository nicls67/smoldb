
#[doc = include_str!("../README.md")]

mod db_init;
mod db_model;

pub use db_model::{DbModel, DbTable};
