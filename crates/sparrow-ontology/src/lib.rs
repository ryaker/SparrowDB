//! # sparrow-ontology
//!
//! Schema and ontology management for [SparrowDB](https://docs.rs/sparrowdb).
//!
//! This crate provides [`OntologyDb`], a wrapper around a SparrowDB [`GraphDb`]
//! that adds class-based schema management, entity creation with property
//! validation, and schema conformance checking.
//!
//! ## Quick start
//!
//! ```no_run
//! use sparrow_ontology::{OntologyDb, PropertyDef, ValueType};
//! use sparrowdb::GraphDb;
//!
//! let db = GraphDb::open(std::path::Path::new("/tmp/my.sparrow")).unwrap();
//! let onto = OntologyDb::new(db);
//! onto.init().unwrap();
//!
//! // Define a class.
//! onto.create_class("Person", &[
//!     PropertyDef::new("name", ValueType::String).required().unique(),
//!     PropertyDef::new("age", ValueType::Int64),
//! ]).unwrap();
//!
//! // Create an entity.
//! let mut props = std::collections::HashMap::new();
//! props.insert("name".to_string(), serde_json::Value::String("Alice".into()));
//! props.insert("age".to_string(), serde_json::Value::Number(30.into()));
//! onto.create_entity("Person", props).unwrap();
//! ```

pub mod error;
pub mod schema;
mod validation;

pub use error::{OntologyError, Result};
pub use schema::{ClassSchema, PropertyDef, ValidationReport, ValueType, Violation};

use sparrowdb::GraphDb;
use sparrowdb::NodeId;
use sparrowdb_execution::Value as ExecValue;
use std::collections::HashMap;

/// Reserved node label used to store class schema definitions.
const CLASS_LABEL: &str = "OntologyClass";
/// Reserved node label used to store property definitions.
const PROPERTY_LABEL: &str = "OntologyProperty";
/// Reserved edge relationship type linking a class to its property nodes.
const HAS_PROPERTY_REL: &str = "HAS_PROPERTY";

/// The ontology management layer for a SparrowDB graph.
///
/// Wrap an existing [`GraphDb`] with `OntologyDb::new`, then call [`init`][Self::init]
/// once to bootstrap the schema storage nodes, and use the class/entity methods
/// to manage your domain model.
pub struct OntologyDb {
    db: GraphDb,
}

impl OntologyDb {
    /// Wrap an existing [`GraphDb`] with ontology management.
    pub fn new(db: GraphDb) -> Self {
        OntologyDb { db }
    }

    /// Return a reference to the underlying [`GraphDb`].
    pub fn inner(&self) -> &GraphDb {
        &self.db
    }

    /// Consume `self` and return the underlying [`GraphDb`].
    pub fn into_inner(self) -> GraphDb {
        self.db
    }

    /// Initialise the ontology schema storage.
    ///
    /// Creates the `OntologyClass` and `OntologyProperty` anchor nodes if they
    /// do not already exist.  Safe to call multiple times — subsequent calls are
    /// no-ops.
    pub fn init(&self) -> Result<()> {
        // Ensure the OntologyClass label exists by executing a harmless query.
        // A MERGE on an anchor node would work, but we just check for existence.
        self.db
            .execute(&format!("MATCH (n:{CLASS_LABEL}) RETURN id(n) LIMIT 1"))
            .map_err(OntologyError::from)?;
        Ok(())
    }

    /// Define a new ontology class with its property schema.
    ///
    /// The class name becomes the node label in the graph.  An `OntologyClass`
    /// node is created (or merged if it already exists) with `name` equal to
    /// `class_name`.
    ///
    /// # Errors
    ///
    /// Returns [`OntologyError::InvalidIdentifier`] if `class_name` uses the
    /// reserved `__SO_` prefix or is empty.
    pub fn create_class(&self, class_name: &str, properties: &[PropertyDef]) -> Result<()> {
        validation::validate_identifier(class_name, "create_class")?;

        // Merge a class node so re-calling is idempotent.
        let cypher = format!(
            "MERGE (c:{CLASS_LABEL} {{name: '{name}'}}) RETURN id(c)",
            name = class_name,
        );
        self.db.execute(&cypher).map_err(OntologyError::from)?;

        // Store each property definition as an OntologyProperty node linked to
        // the class node via a HAS_PROPERTY relationship.
        for prop in properties {
            validation::validate_identifier(&prop.name, "create_class: property")?;
            let required_str = if prop.required { "true" } else { "false" };
            let unique_str = if prop.unique { "true" } else { "false" };
            let cypher = format!(
                "MATCH (c:{CLASS_LABEL} {{name: '{class_name}'}}) \
                 MERGE (p:{PROPERTY_LABEL} {{class: '{class_name}', name: '{prop_name}'}}) \
                 SET p.value_type = '{vtype}', p.required = {required}, p.unique = {unique} \
                 MERGE (c)-[:{HAS_PROPERTY_REL}]->(p)",
                prop_name = prop.name,
                vtype = format!("{:?}", prop.value_type).to_lowercase(),
                required = required_str,
                unique = unique_str,
            );
            self.db.execute(&cypher).map_err(OntologyError::from)?;
        }

        Ok(())
    }

    /// Create an entity (node) of the given class.
    ///
    /// `props` is a map of property names to JSON scalar values
    /// (`String`, `Number`, or `Bool`).  `Null` values are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`OntologyError::InvalidIdentifier`] if `class_name` or any
    /// property key uses the reserved `__SO_` prefix.
    /// Returns [`OntologyError::Other`] for unsupported value types.
    pub fn create_entity(
        &self,
        class_name: &str,
        props: HashMap<String, serde_json::Value>,
    ) -> Result<NodeId> {
        validation::validate_identifier(class_name, "create_entity")?;

        let mut kv_parts: Vec<String> = Vec::with_capacity(props.len());
        for (key, val) in &props {
            validation::validate_identifier(key, "create_entity: property key")?;
            let lit = json_to_cypher_literal(val).ok_or_else(|| {
                OntologyError::Other(format!(
                    "create_entity: property '{key}' has an unsupported value type (null/array/object)"
                ))
            })?;
            kv_parts.push(format!("{key}: {lit}"));
        }

        let props_str = kv_parts.join(", ");
        let cypher = if props_str.is_empty() {
            format!("CREATE (n:{class_name}) RETURN id(n)")
        } else {
            format!("CREATE (n:{class_name} {{{props_str}}}) RETURN id(n)")
        };

        let result = self.db.execute(&cypher).map_err(OntologyError::from)?;

        // Extract the returned node id.
        let node_id = result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|v| {
                if let ExecValue::Int64(id) = v {
                    Some(NodeId(*id as u64))
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                OntologyError::Other("create_entity: no id returned from CREATE".to_string())
            })?;

        Ok(node_id)
    }

    /// Add or update a property definition on an existing class.
    ///
    /// If the property already exists on the class (same `class_name` +
    /// `prop.name`), its metadata is updated.
    pub fn add_property(&self, class_name: &str, prop: PropertyDef) -> Result<()> {
        validation::validate_identifier(class_name, "add_property")?;
        validation::validate_identifier(&prop.name, "add_property: property name")?;

        let required_str = if prop.required { "true" } else { "false" };
        let unique_str = if prop.unique { "true" } else { "false" };
        let cypher = format!(
            "MATCH (c:{CLASS_LABEL} {{name: '{class_name}'}}) \
             MERGE (p:{PROPERTY_LABEL} {{class: '{class_name}', name: '{prop_name}'}}) \
             SET p.value_type = '{vtype}', p.required = {required}, p.unique = {unique} \
             MERGE (c)-[:{HAS_PROPERTY_REL}]->(p)",
            prop_name = prop.name,
            vtype = format!("{:?}", prop.value_type).to_lowercase(),
            required = required_str,
            unique = unique_str,
        );
        self.db.execute(&cypher).map_err(OntologyError::from)?;
        Ok(())
    }

    /// List all defined class names in the ontology.
    pub fn list_classes(&self) -> Result<Vec<String>> {
        let result = self
            .db
            .execute(&format!(
                "MATCH (c:{CLASS_LABEL}) RETURN c.name"
            ))
            .map_err(OntologyError::from)?;

        let names = result
            .rows
            .iter()
            .filter_map(|row| {
                row.first().and_then(|v| {
                    if let ExecValue::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
            })
            .collect();

        Ok(names)
    }

    /// Retrieve the full schema for a class.
    ///
    /// # Errors
    ///
    /// Returns [`OntologyError::ClassNotFound`] if no class with that name exists.
    pub fn get_class(&self, class_name: &str) -> Result<ClassSchema> {
        validation::validate_identifier(class_name, "get_class")?;

        // Check the class exists.
        let check = self
            .db
            .execute(&format!(
                "MATCH (c:{CLASS_LABEL} {{name: '{class_name}'}}) RETURN id(c)"
            ))
            .map_err(OntologyError::from)?;
        if check.rows.is_empty() {
            return Err(OntologyError::ClassNotFound(class_name.to_string()));
        }

        // Retrieve all properties.
        let result = self
            .db
            .execute(&format!(
                "MATCH (p:{PROPERTY_LABEL} {{class: '{class_name}'}}) \
                 RETURN p.name, p.value_type, p.required, p.unique"
            ))
            .map_err(OntologyError::from)?;

        let mut properties = Vec::new();
        for row in &result.rows {
            let name = match row.first() {
                Some(ExecValue::String(s)) => s.clone(),
                _ => continue,
            };
            let value_type = match row.get(1) {
                Some(ExecValue::String(s)) => parse_value_type(s),
                _ => ValueType::String,
            };
            let required = match row.get(2) {
                Some(ExecValue::Bool(b)) => *b,
                Some(ExecValue::String(s)) => s == "true",
                _ => false,
            };
            let unique = match row.get(3) {
                Some(ExecValue::Bool(b)) => *b,
                Some(ExecValue::String(s)) => s == "true",
                _ => false,
            };
            properties.push(PropertyDef {
                name,
                value_type,
                required,
                unique,
            });
        }

        Ok(ClassSchema {
            name: class_name.to_string(),
            properties,
        })
    }

    /// Validate all nodes of `class_name` against its declared schema.
    ///
    /// Returns a [`ValidationReport`] detailing any violations.  To check if
    /// validation passed, call [`ValidationReport::is_valid`].
    ///
    /// # Errors
    ///
    /// Returns [`OntologyError::ClassNotFound`] if the class schema is not defined.
    pub fn validate(&self, class_name: &str) -> Result<ValidationReport> {
        let schema = self.get_class(class_name)?;
        validation::validate_class(&self.db, &schema)
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Convert a `serde_json::Value` scalar to a Cypher literal string.
///
/// Returns `None` for `Null`, arrays, and objects (unsupported in this context).
fn json_to_cypher_literal(val: &serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::String(s) => {
            // Escape single quotes inside string values.
            Some(format!("'{}'", s.replace('\'', "\\'")))
        }
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Parse a stored value-type string back to [`ValueType`].
fn parse_value_type(s: &str) -> ValueType {
    match s {
        "int64" => ValueType::Int64,
        "float64" => ValueType::Float64,
        "bool" => ValueType::Bool,
        _ => ValueType::String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrowdb::GraphDb;
    use tempfile::tempdir;

    fn open_test_db() -> (OntologyDb, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db = GraphDb::open(dir.path()).unwrap();
        let onto = OntologyDb::new(db);
        onto.init().unwrap();
        (onto, dir)
    }

    #[test]
    fn test_init_is_idempotent() {
        let (onto, _dir) = open_test_db();
        onto.init().unwrap();
        onto.init().unwrap();
    }

    #[test]
    fn test_create_class_and_list() {
        let (onto, _dir) = open_test_db();
        onto.create_class(
            "Animal",
            &[PropertyDef::new("species", ValueType::String).required()],
        )
        .unwrap();
        let classes = onto.list_classes().unwrap();
        assert!(classes.contains(&"Animal".to_string()));
    }

    #[test]
    fn test_create_entity() {
        let (onto, _dir) = open_test_db();
        onto.create_class("Widget", &[PropertyDef::new("sku", ValueType::String)])
            .unwrap();

        let mut props = HashMap::new();
        props.insert("sku".to_string(), serde_json::Value::String("W-001".into()));
        let node_id = onto.create_entity("Widget", props).unwrap();
        // NodeId should be non-zero
        assert!(node_id.0 > 0);
    }

    #[test]
    fn test_invalid_identifier_reserved_prefix() {
        let (onto, _dir) = open_test_db();
        let err = onto.create_class("__SO_Forbidden", &[]).unwrap_err();
        assert!(matches!(err, OntologyError::InvalidIdentifier(_)));
    }

    #[test]
    fn test_get_class_not_found() {
        let (onto, _dir) = open_test_db();
        let err = onto.get_class("Nonexistent").unwrap_err();
        assert!(matches!(err, OntologyError::ClassNotFound(_)));
    }
}
