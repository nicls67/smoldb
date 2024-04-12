# SmolDB

A small database library written in Rust

## Principle

This library is meant to be used inside a Rust binary application. Database is stored inside a `.smoldb` file (or any other extension) which is opened or created during library loading.

**SmolDB** only requires a few accesses to disk as database file is loaded once during initialization, and then all requests are handled from RAM (read and write accesses). The database file on disk is updated when `save` method is called.

A database contains one or many tables, each tables containing one or many database fields. Tables and fields are defined during initialization process.
A database entry is linked to a table and has its fields filled with a value. It is possible to leave a field empty.
