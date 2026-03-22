# Cypher Reference

SparrowDB supports a substantial subset of openCypher. This page is your authoritative reference — every feature listed here is tested end-to-end against a real on-disk database.

**Quick jump:** [Clauses](#clauses) · [Patterns](#patterns) · [Expressions](#expressions) · [Functions](#functions) · [Known gaps](#known-gaps)

---

## Clauses

### CREATE

Create a node:

```cypher
CREATE (n:Person {name: 'Alice', age: 30})
CREATE (n:Article {title: 'Hello', published: true, score: 4.5})
```

Create an edge (match the endpoints first):

```cypher
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS]->(b)

-- With relationship properties
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2020, weight: 0.9}]->(b)
```

---

### MATCH

Scan all nodes with a label:

```cypher
MATCH (n:Person) RETURN n.name
```

Scan all nodes (no label filter):

```cypher
MATCH (n) RETURN labels(n), id(n)
```

Inline property filter (ANDs multiple predicates):

```cypher
MATCH (n:Person {name: 'Alice'}) RETURN n.age
MATCH (n:Person {name: 'Alice', age: 30}) RETURN n.name
```

With a WHERE clause:

```cypher
MATCH (n:Person) WHERE n.age > 25 RETURN n.name
MATCH (n:Person) WHERE n.name CONTAINS 'li' RETURN n.name
MATCH (n:Person) WHERE n.age IN [25, 30, 35] RETURN n.name
MATCH (n:Person) WHERE n.nickname IS NULL RETURN n.name
MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n.name
```

Supported WHERE operators: `=`, `<>`, `<`, `<=`, `>`, `>=`, `CONTAINS`, `IN`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`.

---

### OPTIONAL MATCH

Returns `null` values for the optional part when no match is found, instead of dropping the row:

```cypher
MATCH (a:Person {name: 'Alice'})
OPTIONAL MATCH (a)-[:KNOWS]->(f:Person)
RETURN a.name, f.name
-- f.name is null if Alice has no KNOWS relationships
```

---

### MERGE

Upsert a node — create it if it doesn't exist, match it if it does:

```cypher
MERGE (n:Person {name: 'Alice'})
RETURN n.name
```

---

### SET

Update a property on a matched node:

```cypher
MATCH (n:Person {name: 'Alice'})
SET n.age = 31

-- Set multiple properties
MATCH (n:Person {name: 'Alice'})
SET n.age = 31, n.city = 'Portland'
```

---

### DELETE

Delete a node. Delete its edges first — deleting a node with live edges produces undefined behaviour in v0.1.

```cypher
-- Safe pattern: delete edges and node together
MATCH (n:Person {name: 'Alice'})-[r]-()
DELETE r, n
```

---

### WITH

Materialise an intermediate result, optionally filter, then continue the pipeline:

```cypher
-- Rename columns
MATCH (n:Person) WITH n.name AS name RETURN name

-- Filter mid-query (WHERE runs on the WITH output)
MATCH (n:Person)
WITH n.name AS name, n.age AS age
WHERE age > 28
RETURN name, age ORDER BY age DESC

-- Chain aggregation
MATCH (p:Person)-[:KNOWS]->(f:Person)
WITH p.name AS person, COUNT(f) AS friends
WHERE friends > 2
RETURN person, friends ORDER BY friends DESC
```

---

### RETURN

Return columns, with optional alias:

```cypher
MATCH (n:Person) RETURN n.name, n.age
MATCH (n:Person) RETURN n.name AS name, n.age AS years
MATCH (n:Person) RETURN DISTINCT n.age
```

---

### RETURN DISTINCT

Deduplicate the result set on all returned columns:

```cypher
MATCH (a:Person)-[:KNOWS]->()-[:KNOWS]->(fof:Person)
RETURN DISTINCT fof.name
```

---

### ORDER BY

```cypher
MATCH (n:Person) RETURN n.name ORDER BY n.name         -- ASC (default)
MATCH (n:Person) RETURN n.age  ORDER BY n.age DESC
MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC, n.name ASC
```

---

### LIMIT and SKIP

```cypher
MATCH (n:Person) RETURN n.name LIMIT 10
MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 20 LIMIT 10
```

---

### UNWIND

Expand a list into rows:

```cypher
UNWIND [1, 2, 3] AS x RETURN x

-- With range()
UNWIND range(1, 10) AS n RETURN n * n AS squared

-- Expand collected lists
MATCH (p:Person) WITH collect(p.name) AS names
UNWIND names AS name RETURN name
```

---

### UNION / UNION ALL

```cypher
-- UNION deduplicates; UNION ALL keeps duplicates
MATCH (n:Person) RETURN n.name AS name
UNION
MATCH (n:Company) RETURN n.name AS name

MATCH (n:Person) RETURN n.name AS name
UNION ALL
MATCH (n:Bot) RETURN n.name AS name
```

Column counts and names must match across branches.

---

## Patterns

### Directed traversal

```cypher
-- 1-hop
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name

-- 2-hop (anonymous intermediate)
MATCH (a:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(fof:Person)
RETURN DISTINCT fof.name

-- Explicit relationship variable
MATCH (a:Person)-[r:KNOWS]->(b:Person)
RETURN a.name, type(r), b.name
```

### Undirected traversal

```cypher
MATCH (a:Person {name: 'Alice'})-[:KNOWS]-(b:Person)
RETURN b.name
```

### Variable-length paths

```cypher
-- Exactly 2 hops
MATCH (a:Person)-[:KNOWS*2]->(b:Person) RETURN b.name

-- 1 to 3 hops
MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b:Person)
RETURN DISTINCT b.name

-- Unbounded (use carefully on large graphs)
MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(b:Person)
RETURN DISTINCT b.name
```

### Multiple patterns in one MATCH

```cypher
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS]->(b)

-- Mutual friends
MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(m:Person)<-[:KNOWS]-(b:Person {name: 'Bob'})
RETURN m.name
```

---

## Expressions

### CASE WHEN

```cypher
MATCH (n:Person)
RETURN n.name,
       CASE WHEN n.age >= 30 THEN 'senior'
            WHEN n.age >= 21 THEN 'adult'
            ELSE 'young'
       END AS tier

-- Simple CASE (match on value)
RETURN CASE n.status
         WHEN 'active' THEN 1
         WHEN 'pending' THEN 0
         ELSE -1
       END AS code
```

### EXISTS { }

Pattern-based existence check in a WHERE predicate:

```cypher
-- Nodes that have at least one outgoing KNOWS edge
MATCH (n:Person)
WHERE EXISTS { (n)-[:KNOWS]->(:Person) }
RETURN n.name

-- Nodes that have NO outgoing edges
MATCH (n:Person)
WHERE NOT EXISTS { (n)-[:KNOWS]->() }
RETURN n.name
```

### shortestPath

Returns the path length (as an integer) between two nodes:

```cypher
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Carol'})
RETURN shortestPath((a)-[:KNOWS*]->(b))
```

Returns `null` if no path exists.

### List predicates

```cypher
-- ANY: at least one element satisfies the predicate
MATCH (n:Person)
WHERE ANY(x IN n.scores WHERE x > 90)
RETURN n.name

-- ALL: every element satisfies the predicate
WHERE ALL(x IN n.scores WHERE x >= 50)

-- NONE: no element satisfies the predicate
WHERE NONE(x IN n.tags WHERE x = 'spam')

-- SINGLE: exactly one element satisfies the predicate
WHERE SINGLE(x IN n.roles WHERE x = 'admin')
```

### IN operator

```cypher
MATCH (n:Person) WHERE n.age IN [25, 30, 35] RETURN n.name
MATCH (n:Person) WHERE NOT n.status IN ['banned', 'suspended'] RETURN n.name
```

### IS NULL / IS NOT NULL

```cypher
MATCH (n:Person) WHERE n.email IS NULL RETURN n.name
MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n.name
```

### Arithmetic

```cypher
RETURN 2 + 3, 10 - 4, 6 * 7, 22 / 7, 10 % 3
MATCH (n:Person) RETURN n.age * 2 AS doubled
```

### String concatenation

```cypher
RETURN 'Hello' + ', ' + 'World'
MATCH (n:Person) RETURN n.name + ' (age ' + toString(n.age) + ')'
```

---

## Functions

### Aggregate functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Number of matched rows |
| `COUNT(expr)` | Number of non-null values |
| `COUNT(DISTINCT expr)` | Number of distinct non-null values |
| `SUM(expr)` | Sum of numeric values |
| `AVG(expr)` | Average of numeric values |
| `MIN(expr)` | Minimum value |
| `MAX(expr)` | Maximum value |
| `collect(expr)` | Collect all values into a list |

```cypher
MATCH (p:Person) RETURN COUNT(*), AVG(p.age), MIN(p.age), MAX(p.age)
MATCH (p:Person) RETURN COUNT(DISTINCT p.age) AS unique_ages
MATCH (p:Person) RETURN collect(p.name) AS all_names
```

Implicit grouping — non-aggregate columns become GROUP BY keys:

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
RETURN p.name, COUNT(f) AS friend_count
ORDER BY friend_count DESC
```

### Graph functions

| Function | Returns | Example |
|----------|---------|---------|
| `id(n)` | Node ID (integer) | `RETURN id(n)` |
| `id(r)` | Edge ID (integer) | `RETURN id(r)` |
| `labels(n)` | List of label strings | `RETURN labels(n)` |
| `type(r)` | Relationship type string | `RETURN type(r)` |

### String functions

| Function | Description | Example |
|----------|-------------|---------|
| `toString(val)` | Convert value to string | `toString(42)` → `"42"` |
| `toUpper(str)` | Uppercase | `toUpper('hello')` → `"HELLO"` |
| `toLower(str)` | Lowercase | `toLower('WORLD')` → `"world"` |
| `trim(str)` | Strip leading/trailing whitespace | `trim('  hi  ')` → `"hi"` |
| `replace(str, find, repl)` | Replace all occurrences | `replace('hello world', 'world', 'db')` |
| `substring(str, start, len)` | Slice a string | `substring('hello', 1, 3)` → `"ell"` |
| `size(str)` | String length | `size('hello')` → `5` |

### Math functions

| Function | Description | Example |
|----------|-------------|---------|
| `abs(n)` | Absolute value | `abs(-5)` → `5` |
| `ceil(n)` | Round up | `ceil(1.2)` → `2.0` |
| `floor(n)` | Round down | `floor(3.9)` → `3.0` |
| `sqrt(n)` | Square root | `sqrt(9.0)` → `3.0` |
| `sign(n)` | Sign (-1, 0, 1) | `sign(-5)` → `-1` |

### List / range functions

| Function | Description | Example |
|----------|-------------|---------|
| `range(start, end)` | Inclusive integer list | `range(1, 5)` → `[1,2,3,4,5]` |
| `range(start, end, step)` | With step | `range(0, 10, 2)` → `[0,2,4,6,8,10]` |
| `size(list)` | List length | `size([1,2,3])` → `3` |

### Type conversion

| Function | Description |
|----------|-------------|
| `toInteger(val)` | Parse/truncate to integer |
| `toString(val)` | Convert to string |
| `toFloat(val)` | Parse/convert to float |

### Schema inspection

```cypher
-- All labels, relationship types, and property keys
CALL db.schema()

-- Full-text search (requires indexed data)
CALL db.index.fulltext.queryNodes('indexName', 'search terms')
YIELD node, score
RETURN node.name, score
```

### Parameters

```cypher
-- Use $param to safely pass values
MATCH (n:Person {name: $name}) RETURN n.age
CREATE (n:Person {name: $name, age: $age})
```

Pass parameters via the language bindings — see [bindings.md](bindings.md) or the [API reference](api-reference.md).

---

## Maintenance statements

```cypher
CHECKPOINT   -- fold WAL delta log into CSR base files
OPTIMIZE     -- checkpoint + sort adjacency lists (heavier; use in maintenance windows)
```

These are also available as CLI commands and as `GraphDb` methods.

---

## Known gaps

These features are not yet implemented. They return `Err(Error::Unimplemented)` — never a panic.

| Feature | Status | Issue |
|---------|--------|-------|
| `coalesce(a, b, ...)` | Not implemented | — |
| Multi-label nodes `(n:A:B)` | Not implemented | — |
| Subqueries `CALL { … }` | Partial | — |
| `UNION` across complex multi-clause queries | Partial | — |
| `WriteTx.execute()` transactional mutations (Node.js / Python) | Planned | — |
| `ReadTx.execute()` snapshot-pinned queries | Planned | — |
| Full-text index creation (`CREATE FULLTEXT INDEX`) | Not implemented | — |

If you hit an unsupported feature, please [open an issue](https://github.com/ryaker/SparrowDB/issues).
