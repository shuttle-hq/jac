#![feature(generic_associated_types)]

#[macro_use] extern crate lazy_static;

pub mod cache;
pub use cache::{
    Read,
    Write,
    Validate,
    ContentUpdate,
    Validation,
    Content,
    Cached
};

#[cfg(feature = "redis-backend")]
pub mod redis;

pub mod prelude;
