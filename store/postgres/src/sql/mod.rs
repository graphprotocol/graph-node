mod constants;
mod formatter;
mod parser;
mod validation;

use std::collections::{HashMap, HashSet};

pub(self) type Schema = HashMap<String, HashSet<String>>; // HashMap<Table, HashSet<Column>>

pub use parser::Parser;
