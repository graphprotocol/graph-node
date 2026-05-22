//! Shared sanitization utilities for code generation.
//!
//! Contains reserved word handling and name sanitization used by both
//! schema codegen and ABI codegen.

/// Reserved words in AssemblyScript that need to be escaped.
///
/// This is a superset of reserved words from ECMAScript plus AssemblyScript-specific
/// additions. When a field or parameter name matches one of these, it gets
/// an underscore appended.
pub const RESERVED_WORDS: &[&str] = &[
    "await",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "debugger",
    "default",
    "delete",
    "do",
    "else",
    "enum",
    "export",
    "extends",
    "false",
    "finally",
    "for",
    "function",
    "if",
    "implements",
    "import",
    "in",
    "instanceof",
    "interface",
    "let",
    "new",
    "null",
    "package",
    "private",
    "protected",
    "public",
    "return",
    "static",
    "super",
    "switch",
    "this",
    "throw",
    "true",
    "try",
    "typeof",
    "var",
    "void",
    "while",
    "with",
    "yield",
];

/// Handle reserved words by appending an underscore.
///
/// If the given name is a reserved word in AssemblyScript, returns the name
/// with an underscore appended. Otherwise returns the name unchanged.
pub fn handle_reserved_word(name: &str) -> String {
    if RESERVED_WORDS.contains(&name) {
        format!("{}_", name)
    } else {
        name.to_string()
    }
}

/// Capitalize the first letter of a string.
///
/// Returns an empty string if the input is empty, otherwise returns the
/// string with the first character uppercased.
pub fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_reserved_word_reserved() {
        assert_eq!(handle_reserved_word("class"), "class_");
        assert_eq!(handle_reserved_word("return"), "return_");
        assert_eq!(handle_reserved_word("await"), "await_");
        assert_eq!(handle_reserved_word("default"), "default_");
    }

    #[test]
    fn test_handle_reserved_word_not_reserved() {
        assert_eq!(handle_reserved_word("myField"), "myField");
        assert_eq!(handle_reserved_word("amount"), "amount");
        assert_eq!(handle_reserved_word("value"), "value");
    }

    #[test]
    fn test_capitalize() {
        assert_eq!(capitalize("hello"), "Hello");
        assert_eq!(capitalize("world"), "World");
        assert_eq!(capitalize(""), "");
        assert_eq!(capitalize("a"), "A");
        assert_eq!(capitalize("ALREADY"), "ALREADY");
    }
}
