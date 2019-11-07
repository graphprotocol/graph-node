use graphql_parser::schema as s;

pub trait ObjectTypeExt {
    fn field(&self, name: &s::Name) -> Option<&s::Field>;
}

impl ObjectTypeExt for s::ObjectType {
    fn field(&self, name: &s::Name) -> Option<&s::Field> {
        self.fields.iter().find(|field| &field.name == name)
    }
}

impl ObjectTypeExt for s::InterfaceType {
    fn field(&self, name: &s::Name) -> Option<&s::Field> {
        self.fields.iter().find(|field| &field.name == name)
    }
}
