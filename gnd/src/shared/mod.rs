//! Shared utilities for gnd code generation.
//!
//! This module contains common utilities used across multiple codegen
//! and scaffold modules to reduce duplication.

pub mod sanitize;

pub use sanitize::{RESERVED_WORDS, capitalize, handle_reserved_word};
