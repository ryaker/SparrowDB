# sparrow-ontology

Schema and ontology management layer for [SparrowDB](https://github.com/ryaker/SparrowDB).

## Overview

`sparrow-ontology` wraps a SparrowDB `GraphDb` with class-based schema
management.  It lets you:

- **Define classes** — named node labels with typed property schemas.
- **Create entities** — nodes validated against their class schema.
- **Add properties** — extend an existing class schema.
- **Validate** — check that all nodes of a class conform to their schema.
- **Introspect** — list defined classes and retrieve full schemas.

## Install

```toml
[dependencies]
sparrow-ontology = "0.1.12"
sparrowdb = "0.1.12"
```

## Quick Start

```rust
use sparrow_ontology::{OntologyDb, PropertyDef, ValueType};
use sparrowdb::GraphDb;

let db = GraphDb::open(std::path::Path::new("/tmp/my.sparrow")).unwrap();
let onto = OntologyDb::new(db);

// Bootstrap schema storage (idempotent).
onto.init().unwrap();

// Define a class.
onto.create_class("Person", &[
    PropertyDef::new("name", ValueType::String).required().unique(),
    PropertyDef::new("age",  ValueType::Int64),
]).unwrap();

// Create an entity.
let mut props = std::collections::HashMap::new();
props.insert("name".to_string(), serde_json::Value::String("Alice".into()));
props.insert("age".to_string(),  serde_json::Value::Number(30.into()));
onto.create_entity("Person", props).unwrap();

// Validate all Person nodes against the schema.
let report = onto.validate("Person").unwrap();
assert!(report.is_valid());
```

## Schema Storage

Class definitions are stored as `OntologyClass` nodes; property definitions are
stored as `OntologyProperty` nodes linked via `HAS_PROPERTY` edges.  The
`__SO_` label prefix is reserved for SparrowDB internals — ontology labels do
not use it.

## License

MIT — see [LICENSE](../../LICENSE).
