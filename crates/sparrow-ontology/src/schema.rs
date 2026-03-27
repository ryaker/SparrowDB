use serde::{Deserialize, Serialize};

/// Scalar value types that a property may hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValueType {
    String,
    Int64,
    Float64,
    Bool,
}

impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueType::String => write!(f, "String"),
            ValueType::Int64 => write!(f, "Int64"),
            ValueType::Float64 => write!(f, "Float64"),
            ValueType::Bool => write!(f, "Bool"),
        }
    }
}

/// Definition of a single property on an ontology class.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyDef {
    /// Property name (must be a valid Cypher identifier).
    pub name: String,
    /// Declared value type.
    pub value_type: ValueType,
    /// Whether this property must be present on every entity of this class.
    pub required: bool,
    /// Whether values must be unique across all entities of this class.
    pub unique: bool,
}

impl PropertyDef {
    /// Create a new optional, non-unique property definition.
    pub fn new(name: impl Into<String>, value_type: ValueType) -> Self {
        PropertyDef {
            name: name.into(),
            value_type,
            required: false,
            unique: false,
        }
    }

    /// Mark property as required.
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Mark property as unique.
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// Full schema for a class: its name and all declared properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassSchema {
    /// Class name (the node label in the graph).
    pub name: String,
    /// Declared properties.
    pub properties: Vec<PropertyDef>,
}

/// A summary report produced by [`crate::OntologyDb::validate`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationReport {
    /// Name of the class that was validated.
    pub class_name: String,
    /// Total number of entities checked.
    pub total: usize,
    /// Number of entities that passed all checks.
    pub passed: usize,
    /// Validation failures.
    pub violations: Vec<Violation>,
}

impl ValidationReport {
    /// Returns `true` if no violations were found.
    pub fn is_valid(&self) -> bool {
        self.violations.is_empty()
    }
}

/// A single validation violation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Violation {
    /// NodeId of the offending entity.
    pub node_id: u64,
    /// Human-readable description of the problem.
    pub message: String,
}
