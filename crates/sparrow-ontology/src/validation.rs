use crate::error::{OntologyError, Result};
use crate::schema::{ClassSchema, ValidationReport, ValueType, Violation};
use sparrowdb::GraphDb;
use sparrowdb_execution::Value;

/// Validate all nodes of `class_name` against `schema`.
///
/// Queries the live graph and checks that:
/// - every required property is present and non-null, and
/// - every present property value matches its declared type.
///
/// Returns a [`ValidationReport`] rather than an error on failure so callers
/// can inspect individual violations.
pub(crate) fn validate_class(
    db: &GraphDb,
    schema: &ClassSchema,
) -> Result<ValidationReport> {
    // Build the RETURN clause: id(n) + each declared property.
    let prop_names: Vec<&str> = schema.properties.iter().map(|p| p.name.as_str()).collect();

    let return_clause = if prop_names.is_empty() {
        "id(n)".to_string()
    } else {
        let props = prop_names
            .iter()
            .map(|p| format!("n.{p}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("id(n), {props}")
    };

    let cypher = format!(
        "MATCH (n:{label}) RETURN {return_clause}",
        label = schema.name,
        return_clause = return_clause,
    );

    let result = db.execute(&cypher)?;

    let mut report = ValidationReport {
        class_name: schema.name.clone(),
        total: result.rows.len(),
        passed: 0,
        violations: vec![],
    };

    for row in &result.rows {
        let node_id = match row.first() {
            Some(Value::Int64(id)) => *id as u64,
            _ => {
                report.violations.push(Violation {
                    node_id: 0,
                    message: "unexpected id() type in result row".to_string(),
                });
                continue;
            }
        };

        let mut node_ok = true;

        for (i, prop_def) in schema.properties.iter().enumerate() {
            // Column 0 = id(n); property columns start at 1.
            let val = row.get(i + 1).unwrap_or(&Value::Null);

            // Required-property check.
            if prop_def.required && matches!(val, Value::Null) {
                report.violations.push(Violation {
                    node_id,
                    message: format!(
                        "required property '{}' is null or missing",
                        prop_def.name
                    ),
                });
                node_ok = false;
                continue;
            }

            // Type check (only for non-null values).
            if !matches!(val, Value::Null) {
                let type_ok = match (&prop_def.value_type, val) {
                    (ValueType::String, Value::String(_)) => true,
                    (ValueType::Int64, Value::Int64(_)) => true,
                    (ValueType::Float64, Value::Float64(_)) => true,
                    (ValueType::Bool, Value::Bool(_)) => true,
                    _ => false,
                };

                if !type_ok {
                    let got = match val {
                        Value::String(_) => "String",
                        Value::Int64(_) => "Int64",
                        Value::Float64(_) => "Float64",
                        Value::Bool(_) => "Bool",
                        Value::Null => "Null",
                        _ => "Unknown",
                    };
                    report.violations.push(Violation {
                        node_id,
                        message: format!(
                            "property '{}': expected {}, got {}",
                            prop_def.name, prop_def.value_type, got
                        ),
                    });
                    node_ok = false;
                }
            }
        }

        if node_ok {
            report.passed += 1;
        }
    }

    Ok(report)
}

/// Check that `identifier` is non-empty and does not start with `__SO_`.
pub(crate) fn validate_identifier(identifier: &str, context: &str) -> Result<()> {
    if identifier.is_empty() {
        return Err(OntologyError::InvalidIdentifier(format!(
            "{context}: identifier must not be empty"
        )));
    }
    if identifier.starts_with("__SO_") {
        return Err(OntologyError::InvalidIdentifier(format!(
            "{context}: '{identifier}' uses the reserved __SO_ prefix"
        )));
    }
    Ok(())
}
