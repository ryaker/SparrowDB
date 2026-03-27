use std::fmt;

/// Errors returned by the SparrowOntology layer.
#[derive(Debug)]
pub enum OntologyError {
    /// Underlying SparrowDB storage error.
    Storage(sparrowdb::Error),
    /// A class with the given name was not found.
    ClassNotFound(String),
    /// A class with the given name already exists.
    ClassAlreadyExists(String),
    /// A required property was missing from an entity.
    MissingRequiredProperty(String),
    /// A property value did not match its declared type.
    TypeMismatch { property: String, expected: String, got: String },
    /// An identifier is invalid (e.g. empty string, reserved prefix).
    InvalidIdentifier(String),
    /// The ontology schema is not yet initialised; call `init()` first.
    NotInitialised,
    /// Other error.
    Other(String),
}

impl fmt::Display for OntologyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OntologyError::Storage(e) => write!(f, "storage error: {e}"),
            OntologyError::ClassNotFound(n) => write!(f, "ontology class not found: {n}"),
            OntologyError::ClassAlreadyExists(n) => {
                write!(f, "ontology class already exists: {n}")
            }
            OntologyError::MissingRequiredProperty(p) => {
                write!(f, "missing required property: {p}")
            }
            OntologyError::TypeMismatch { property, expected, got } => {
                write!(f, "type mismatch for property '{property}': expected {expected}, got {got}")
            }
            OntologyError::InvalidIdentifier(s) => write!(f, "invalid identifier: {s}"),
            OntologyError::NotInitialised => {
                write!(f, "ontology not initialised — call OntologyDb::init() first")
            }
            OntologyError::Other(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for OntologyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OntologyError::Storage(e) => Some(e),
            _ => None,
        }
    }
}

impl From<sparrowdb::Error> for OntologyError {
    fn from(e: sparrowdb::Error) -> Self {
        OntologyError::Storage(e)
    }
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, OntologyError>;
