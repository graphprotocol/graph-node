//! Shared name sanitization for scaffold code generation.
//!
//! A single source of truth for turning ABI parameter names into valid GraphQL
//! field / AssemblyScript identifiers, used by both the schema and mapping
//! generators so the two never disagree.

/// Sanitize a parameter name into a valid GraphQL/AssemblyScript field identifier.
pub(crate) fn sanitize_field_name(name: &str) -> String {
    if name.is_empty() {
        return "value".to_string();
    }

    // Identifiers must start with a letter or underscore; replace anything else.
    let mut result = String::new();
    for (i, c) in name.chars().enumerate() {
        if i == 0 && c.is_ascii_digit() {
            result.push('_');
        }
        if c.is_alphanumeric() || c == '_' {
            result.push(c);
        } else {
            result.push('_');
        }
    }

    // Convert a leading uppercase to camelCase.
    if result
        .chars()
        .next()
        .map(|c| c.is_uppercase())
        .unwrap_or(false)
    {
        let mut chars = result.chars();
        if let Some(first) = chars.next() {
            result = first.to_lowercase().collect::<String>() + chars.as_str();
        }
    }

    // Avoid clashing with the entity `id` field and the GraphQL `type` keyword.
    let result = match result.as_str() {
        "id" => "eventId".to_string(),
        "type" => "eventType".to_string(),
        _ => result,
    };

    // Suffix reserved words so the generated AssemblyScript stays valid.
    if RESERVED_WORDS.contains(&result.as_str()) {
        format!("{result}_")
    } else {
        result
    }
}

/// Words reserved in JS / AssemblyScript that cannot be used as identifiers as-is.
const RESERVED_WORDS: &[&str] = &[
    "await",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "debugger",
    "delete",
    "do",
    "else",
    "enum",
    "export",
    "extends",
    "false",
    "finally",
    "function",
    "if",
    "implements",
    "import",
    "in",
    "interface",
    "let",
    "new",
    "package",
    "private",
    "protected",
    "public",
    "return",
    "super",
    "switch",
    "static",
    "this",
    "throw",
    "true",
    "try",
    "typeof",
    "var",
    "while",
    "with",
    "yield",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_field_name() {
        // Normal names pass through.
        assert_eq!(sanitize_field_name("owner"), "owner");
        assert_eq!(sanitize_field_name("from"), "from");
        // Empty -> placeholder.
        assert_eq!(sanitize_field_name(""), "value");
        // Leading uppercase -> camelCase.
        assert_eq!(sanitize_field_name("Owner"), "owner");
        assert_eq!(sanitize_field_name("TokenId"), "tokenId");
        // Names that clash with the entity id / GraphQL keyword.
        assert_eq!(sanitize_field_name("id"), "eventId");
        assert_eq!(sanitize_field_name("type"), "eventType");
        // Reserved words are suffixed so the generated code compiles.
        assert_eq!(sanitize_field_name("new"), "new_");
        assert_eq!(sanitize_field_name("class"), "class_");
        assert_eq!(sanitize_field_name("return"), "return_");
        // Leading uppercase reserved word still resolves after camelCasing.
        assert_eq!(sanitize_field_name("New"), "new_");
        // Leading digit -> underscore prefix.
        assert_eq!(sanitize_field_name("0value"), "_0value");
        // Non-alphanumeric -> underscore.
        assert_eq!(sanitize_field_name("a-b"), "a_b");
    }
}
