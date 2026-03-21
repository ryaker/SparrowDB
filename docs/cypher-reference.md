# Cypher Reference (SparrowDB v0.1)

SparrowDB implements a subset of openCypher sufficient for real knowledge graph and social graph workloads. This document covers every supported feature with examples.

## CREATE

### Create a node

```cypher
CREATE (n:Label {prop: value, prop2: value2})
```

```cypher
CREATE (p:Person {name: "Alice", age: 30})
CREATE (k:Knowledge {content: "Neural networks learn from data", confidence: 0.95})
```

Property values: string (quoted), integer, float, boolean.

### Create an edge

First MATCH the nodes, then CREATE the relationship:

```cypher
MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
CREATE (a)-[:KNOWS]->(b)
```

```cypher
MATCH (k:Knowledge {content: "Neural networks"}), (t:Technology {name: "AI"})
CREATE (k)-[:ABOUT]->(t)
```

---

## MATCH … RETURN

### Node scan

```cypher
MATCH (n:Label) RETURN n.property
MATCH (n:Person) RETURN n.name
```

Returns all nodes with the given label.

### Node scan with property filter

```cypher
MATCH (n:Label {prop: value}) RETURN n.prop2
MATCH (p:Person {name: "Alice"}) RETURN p.age
```

Multiple property predicates are ANDed:

```cypher
MATCH (p:Person {name: "Alice", age: 30}) RETURN p.name
```

### WHERE clause

```cypher
MATCH (n:Label) WHERE n.prop = value RETURN n
MATCH (k:Knowledge) WHERE k.confidence > 0.8 RETURN k.content
MATCH (k:Knowledge) WHERE k.content CONTAINS "neural" RETURN k.content
```

Supported operators: `=`, `<>`, `<`, `<=`, `>`, `>=`, `CONTAINS`

### 1-hop traversal

```cypher
MATCH (a:Person {name: "Alice"})-[:KNOWS]->(f:Person)
RETURN f.name
```

Directed edges only in v0.1. Direction matters.

### 1-hop with relationship variable

```cypher
MATCH (a:Person)-[r:KNOWS]->(b:Person)
RETURN a.name, b.name
```

### 2-hop traversal

```cypher
MATCH (a:Person {name: "Alice"})-[:KNOWS]->()-[:KNOWS]->(fof:Person)
RETURN DISTINCT fof.name
```

The anonymous middle node `()` is not bound to a variable. 2-hop queries use binary ASP-Join internally — no Cartesian blowup.

### DISTINCT

```cypher
MATCH (a:Person)-[:KNOWS]->()-[:KNOWS]->(fof:Person)
RETURN DISTINCT fof.name
```

Deduplicates returned rows at the projection boundary.

### Mutual friends

```cypher
MATCH (a:Person {name: "Alice"})-[:KNOWS]->(m:Person)<-[:KNOWS]-(b:Person {name: "Bob"})
RETURN m.name
```

### ORDER BY

```cypher
MATCH (k:Knowledge)
WHERE k.confidence > 0.7
RETURN k.content, k.confidence
ORDER BY k.confidence DESC
```

`ASC` (default) or `DESC`.

### LIMIT

```cypher
MATCH (k:Knowledge)
RETURN k.content
ORDER BY k.confidence DESC
LIMIT 20
```

### Multiple RETURN columns with alias

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
RETURN p.name AS person, f.name AS friend
```

---

## Aggregates

```cypher
MATCH (p:Person) RETURN COUNT(p)
MATCH (p:Person) RETURN COUNT(p.name)

MATCH (p:Person) RETURN AVG(p.age), MIN(p.age), MAX(p.age), SUM(p.age)
```

---

## DELETE

```cypher
MATCH (n:Person {name: "Alice"})
DELETE n
```

> Note: DELETE on nodes with edges is not validated in v0.1. Delete edges first.

---

## SET

```cypher
MATCH (n:Person {name: "Alice"})
SET n.age = 31
```

---

## Parameters

Use `$param` to pass values without string concatenation:

```cypher
MATCH (p:Person {name: $name}) RETURN p.age
```

In Rust:

```rust
let mut params = HashMap::new();
params.insert("name".to_string(), Value::String("Alice".to_string()));
engine.execute_with_params("MATCH (p:Person {name: $name}) RETURN p.age", params)?;
```

---

## Maintenance

```cypher
CHECKPOINT   -- fold delta log into CSR base files
OPTIMIZE     -- rewrite base files for read performance (stub in v0.1)
```

---

## Complete UC-1 Example (Social Graph)

```cypher
-- Build graph
CREATE (:Person {name: "Alice"})
CREATE (:Person {name: "Bob"})
CREATE (:Person {name: "Carol"})
CREATE (:Person {name: "Dave"})

MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})   CREATE (a)-[:KNOWS]->(b)
MATCH (a:Person {name: "Bob"}),   (c:Person {name: "Carol"}) CREATE (a)-[:KNOWS]->(c)
MATCH (a:Person {name: "Bob"}),   (d:Person {name: "Dave"})  CREATE (a)-[:KNOWS]->(d)

-- Who does Alice know?
MATCH (a:Person {name: "Alice"})-[:KNOWS]->(f:Person) RETURN f.name

-- Friends of friends (excluding direct friends)
MATCH (a:Person {name: "Alice"})-[:KNOWS]->()-[:KNOWS]->(fof:Person)
WHERE NOT (a)-[:KNOWS]->(fof)
RETURN DISTINCT fof.name
```

---

## Complete UC-3 Example (KMS Workload)

```cypher
-- Store a knowledge node
CREATE (k:Knowledge {
  content: "SparrowDB replaces Neo4j for local KMS",
  source: "architecture-decision",
  confidence: 0.95
})

-- Search by content
MATCH (k:Knowledge)
WHERE k.content CONTAINS "SparrowDB"
RETURN k.content, k.confidence
ORDER BY k.confidence DESC
LIMIT 20

-- Entity traversal
MATCH (k:Knowledge)-[:ABOUT]->(t:Technology {name: "Rust"})
RETURN k.content, k.confidence
ORDER BY k.confidence DESC

-- 2-hop: what knowledge relates to this project via technology?
MATCH (p:Project {name: "KMS"})<-[:ABOUT]-(k:Knowledge)-[:MENTIONS]->(t:Technology)
RETURN k.content, t.name
```

---

## Not Supported in v0.1

| Feature | Status |
|---------|--------|
| `OPTIONAL MATCH` | ❌ Returns `Err(Unimplemented)` |
| Variable-length paths `[:R*1..3]` | ❌ Returns `Err(Unimplemented)` |
| `UNION` / `UNION ALL` | ❌ |
| `UNWIND` | ❌ |
| Subqueries `CALL { ... }` | ❌ |
| `WITH` clause | ❌ |
| Full-text search `CALL db.index.fulltext.queryNodes` | ❌ (use `CONTAINS` instead) |
| Multi-label nodes `(n:A:B)` | ❌ |

Unsupported syntax returns `Err(Error::Unimplemented)` — never panics.
