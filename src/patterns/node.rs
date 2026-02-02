use std::fmt::Display;

use error_stack::ResultExt;

use crate::ctx;
use crate::errors::ImplicaResult;
use crate::patterns::term_schema::TermSchema;
use crate::patterns::type_schema::TypeSchema;
use crate::properties::PropertyMap;
use crate::utils::validate_variable_name;

#[derive(Debug)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub type_schema: Option<TypeSchema>,
    pub term_schema: Option<TermSchema>,
    pub properties: Option<PropertyMap>,
}

impl Clone for NodePattern {
    fn clone(&self) -> Self {
        NodePattern {
            variable: self.variable.clone(),
            type_schema: self.type_schema.clone(),
            term_schema: self.term_schema.clone(),
            properties: self.properties.clone(),
        }
    }
}

impl Display for NodePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut content = Vec::new();

        if let Some(ref var) = self.variable {
            content.push(format!("variable='{}'", var));
        }

        if let Some(ref type_schema) = self.type_schema {
            content.push(format!("type_schema={}", type_schema))
        }

        if let Some(ref term_schema) = self.term_schema {
            content.push(format!("term_schema={}", term_schema));
        }

        write!(f, "NodePattern({})", content.join(", "))
    }
}

impl NodePattern {
    pub fn new(
        variable: Option<String>,
        type_schema: Option<TypeSchema>,
        term_schema: Option<TermSchema>,
        properties: Option<PropertyMap>,
    ) -> ImplicaResult<Self> {
        if let Some(ref var) = variable {
            validate_variable_name(var).attach(ctx!("node pattern - new"))?;
        }

        Ok(NodePattern {
            variable,
            type_schema,
            term_schema,
            properties,
        })
    }
}
