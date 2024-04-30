
#[doc = include_str!("../README.md")]

mod db_init;
mod db_model;

pub use db_model::{DbModel, DbTable};

pub struct SmolDb {
    model: DbModel
}

impl SmolDb {
    /// Database initialization
    pub fn init(db_name: String) -> SmolDb {
        SmolDb { model: DbModel::new(db_name) }
    }

    /// Returns reference to database model
    pub fn get_database(&mut self) -> &mut DbModel {
        &mut self.model
    }
}