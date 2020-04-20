#![feature(generic_associated_types)]

#[macro_use] extern crate lazy_static;

#[macro_use] extern crate log;

pub mod cache;
pub use cache::{
    Read,
    Write,
    ReadWrite,
    WriteError,
    Validate,
    ContentUpdate,
    Validation,
    ValidateMap,
    Content,
    Versioned,
    Constant
};

#[cfg(feature = "redis-backend")]
pub mod redis;

pub mod prelude;
