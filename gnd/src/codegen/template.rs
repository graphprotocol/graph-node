//! Data source template code generation.
//!
//! Generates AssemblyScript classes for subgraph templates that allow
//! dynamic data source creation at runtime.

use super::typescript::{self as ts, Class, ModuleImports, Param, StaticMethod};

/// The kind of a data source template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateKind {
    /// Ethereum contract template
    Ethereum,
    /// IPFS file template
    FileIpfs,
    /// Arweave file template
    FileArweave,
}

impl TemplateKind {
    /// Parse a template kind from a string (e.g., "ethereum/contract", "file/ipfs").
    pub fn from_str_kind(kind: &str) -> Option<Self> {
        match kind {
            "ethereum/contract" | "ethereum" => Some(TemplateKind::Ethereum),
            "file/ipfs" => Some(TemplateKind::FileIpfs),
            "file/arweave" => Some(TemplateKind::FileArweave),
            _ => None,
        }
    }
}

/// A data source template from the subgraph manifest.
pub struct Template {
    /// The name of the template.
    pub name: String,
    /// The kind of template.
    pub kind: TemplateKind,
}

impl Template {
    /// Create a new template.
    pub fn new(name: impl Into<String>, kind: TemplateKind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }
}

const GRAPH_TS_MODULE: &str = "@graphprotocol/graph-ts";

/// Template code generator.
pub struct TemplateCodeGenerator {
    templates: Vec<Template>,
}

impl TemplateCodeGenerator {
    /// Create a new template code generator.
    pub fn new(templates: Vec<Template>) -> Self {
        Self { templates }
    }

    /// Generate module imports for templates.
    pub fn generate_module_imports(&self) -> Vec<ModuleImports> {
        if self.templates.is_empty() {
            return vec![];
        }

        let mut imports = vec![
            "DataSourceTemplate".to_string(),
            "DataSourceContext".to_string(),
        ];

        // Add Address if any Ethereum templates exist
        if self
            .templates
            .iter()
            .any(|t| t.kind == TemplateKind::Ethereum)
        {
            imports.push("Address".to_string());
        }

        vec![ModuleImports::new(imports, GRAPH_TS_MODULE)]
    }

    /// Generate all template classes.
    pub fn generate_types(&self) -> Vec<Class> {
        self.templates
            .iter()
            .map(|t| self.generate_type(t))
            .collect()
    }

    /// Generate a single template class.
    fn generate_type(&self, template: &Template) -> Class {
        let mut klass = ts::klass(&template.name)
            .exported()
            .extends("DataSourceTemplate");

        klass.add_static_method(self.generate_create_method(template));
        klass.add_static_method(self.generate_create_with_context_method(template));

        klass
    }

    /// Generate the static `create` method.
    fn generate_create_method(&self, template: &Template) -> StaticMethod {
        match template.kind {
            TemplateKind::Ethereum => StaticMethod::new(
                "create",
                vec![Param::new("address", ts::NamedType::new("Address"))],
                ts::NamedType::new("void"),
                format!(
                    "\n      DataSourceTemplate.create('{}', [address.toHex()])\n      ",
                    template.name
                ),
            ),
            TemplateKind::FileIpfs | TemplateKind::FileArweave => StaticMethod::new(
                "create",
                vec![Param::new("cid", ts::NamedType::new("string"))],
                ts::NamedType::new("void"),
                format!(
                    "\n      DataSourceTemplate.create('{}', [cid])\n      ",
                    template.name
                ),
            ),
        }
    }

    /// Generate the static `createWithContext` method.
    fn generate_create_with_context_method(&self, template: &Template) -> StaticMethod {
        match template.kind {
            TemplateKind::Ethereum => StaticMethod::new(
                "createWithContext",
                vec![
                    Param::new("address", ts::NamedType::new("Address")),
                    Param::new("context", ts::NamedType::new("DataSourceContext")),
                ],
                ts::NamedType::new("void"),
                format!(
                    "\n      DataSourceTemplate.createWithContext('{}', [address.toHex()], context)\n      ",
                    template.name
                ),
            ),
            TemplateKind::FileIpfs | TemplateKind::FileArweave => StaticMethod::new(
                "createWithContext",
                vec![
                    Param::new("cid", ts::NamedType::new("string")),
                    Param::new("context", ts::NamedType::new("DataSourceContext")),
                ],
                ts::NamedType::new("void"),
                format!(
                    "\n      DataSourceTemplate.createWithContext('{}', [cid], context)\n      ",
                    template.name
                ),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_template_kind_parsing() {
        assert_eq!(
            TemplateKind::from_str_kind("ethereum/contract"),
            Some(TemplateKind::Ethereum)
        );
        assert_eq!(
            TemplateKind::from_str_kind("ethereum"),
            Some(TemplateKind::Ethereum)
        );
        assert_eq!(
            TemplateKind::from_str_kind("file/ipfs"),
            Some(TemplateKind::FileIpfs)
        );
        assert_eq!(
            TemplateKind::from_str_kind("file/arweave"),
            Some(TemplateKind::FileArweave)
        );
        assert_eq!(TemplateKind::from_str_kind("unknown"), None);
    }

    #[test]
    fn test_ethereum_template() {
        let template = Template::new("DynamicToken", TemplateKind::Ethereum);
        let gen = TemplateCodeGenerator::new(vec![template]);

        let imports = gen.generate_module_imports();
        assert_eq!(imports.len(), 1);
        let import_names: Vec<_> = imports[0].names.iter().collect();
        assert!(import_names.contains(&&"DataSourceTemplate".to_string()));
        assert!(import_names.contains(&&"DataSourceContext".to_string()));
        assert!(import_names.contains(&&"Address".to_string()));

        let types = gen.generate_types();
        assert_eq!(types.len(), 1);

        let klass = &types[0];
        assert_eq!(klass.name, "DynamicToken");
        assert!(klass.export);
        assert_eq!(klass.extends, Some("DataSourceTemplate".to_string()));
        assert_eq!(klass.static_methods.len(), 2);

        // Check create method
        let create = &klass.static_methods[0];
        assert_eq!(create.name, "create");
        assert_eq!(create.params.len(), 1);
        assert_eq!(create.params[0].name, "address");

        // Check createWithContext method
        let create_ctx = &klass.static_methods[1];
        assert_eq!(create_ctx.name, "createWithContext");
        assert_eq!(create_ctx.params.len(), 2);
    }

    #[test]
    fn test_file_ipfs_template() {
        let template = Template::new("TokenMetadata", TemplateKind::FileIpfs);
        let gen = TemplateCodeGenerator::new(vec![template]);

        let imports = gen.generate_module_imports();
        assert_eq!(imports.len(), 1);
        let import_names: Vec<_> = imports[0].names.iter().collect();
        assert!(import_names.contains(&&"DataSourceTemplate".to_string()));
        assert!(import_names.contains(&&"DataSourceContext".to_string()));
        // No Address for file templates
        assert!(!import_names.contains(&&"Address".to_string()));

        let types = gen.generate_types();
        assert_eq!(types.len(), 1);

        let klass = &types[0];
        assert_eq!(klass.name, "TokenMetadata");

        // Check create method uses cid parameter
        let create = &klass.static_methods[0];
        assert_eq!(create.params[0].name, "cid");
        assert!(create.body.contains("[cid]"));
    }

    #[test]
    fn test_mixed_templates() {
        let templates = vec![
            Template::new("DynamicToken", TemplateKind::Ethereum),
            Template::new("TokenMetadata", TemplateKind::FileIpfs),
        ];
        let gen = TemplateCodeGenerator::new(templates);

        let imports = gen.generate_module_imports();
        // Should have Address because of Ethereum template
        let import_names: Vec<_> = imports[0].names.iter().collect();
        assert!(import_names.contains(&&"Address".to_string()));

        let types = gen.generate_types();
        assert_eq!(types.len(), 2);
    }

    #[test]
    fn test_empty_templates() {
        let gen = TemplateCodeGenerator::new(vec![]);
        assert!(gen.generate_module_imports().is_empty());
        assert!(gen.generate_types().is_empty());
    }

    #[test]
    fn test_generated_output() {
        let template = Template::new("DynamicToken", TemplateKind::Ethereum);
        let gen = TemplateCodeGenerator::new(vec![template]);

        let types = gen.generate_types();
        let output = types[0].to_string();

        assert!(output.contains("export class DynamicToken extends DataSourceTemplate"));
        assert!(output.contains("static create(address: Address): void"));
        assert!(output.contains("DataSourceTemplate.create('DynamicToken', [address.toHex()])"));
        assert!(output.contains(
            "static createWithContext(address: Address, context: DataSourceContext): void"
        ));
        assert!(output.contains(
            "DataSourceTemplate.createWithContext('DynamicToken', [address.toHex()], context)"
        ));
    }
}
