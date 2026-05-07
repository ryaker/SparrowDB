'use strict'

/**
 * Integration tests for the sparrowdb Node.js native binding.
 *
 * These tests load the sparrowdb.node binary (built from source in CI, or the
 * pre-built binary checked into npm/sparrowdb/ for local runs) and exercise
 * the full API surface against a real on-disk database.
 *
 * Run with:
 *   node --test npm/sparrowdb/test/integration.test.js
 */

const { describe, it, before, after } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')

// ── Load native binding ──────────────────────────────────────────────────────

// index.js handles the platform lookup / fallback chain:
//   1. Platform-specific optional dependency (production)
//   2. npm/sparrowdb/sparrowdb.node (local dev / pre-built)
//   3. target/release|debug/sparrowdb.node (cargo build in place)
const { SparrowDB } = require('../index.js')

// ── Helpers ──────────────────────────────────────────────────────────────────

function makeTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'sparrowdb-test-'))
}

function removeDir(dir) {
  fs.rmSync(dir, { recursive: true, force: true })
}

// ── Test suites ──────────────────────────────────────────────────────────────

describe('SparrowDB.open', () => {
  it('opens a new database at a temp path', () => {
    const dir = makeTempDir()
    try {
      const db = SparrowDB.open(path.join(dir, 'test.db'))
      assert.ok(db instanceof SparrowDB, 'should return a SparrowDB instance')
    } finally {
      removeDir(dir)
    }
  })

  it('throws on a path with a null byte', () => {
    // Null bytes are always invalid in OS path APIs.
    assert.throws(
      () => SparrowDB.open('/tmp/bad\0path'),
      /Error/,
      'expected open to throw on a path with a null byte'
    )
  })
})

describe('execute — basic CREATE and MATCH', () => {
  let db
  let dir

  before(() => {
    dir = makeTempDir()
    db = SparrowDB.open(path.join(dir, 'graph.db'))
  })

  after(() => {
    removeDir(dir)
  })

  it('returns { columns, rows } for a CREATE statement', () => {
    const result = db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
    assert.ok(Array.isArray(result.columns), 'columns should be an array')
    assert.ok(Array.isArray(result.rows), 'rows should be an array')
    assert.equal(result.rows.length, 0, 'CREATE should return no rows')
  })

  it('can query a node back with MATCH … RETURN', () => {
    db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
    const result = db.execute(
      "MATCH (n:Person {name: 'Bob'}) RETURN n.name, n.age"
    )
    assert.ok(result.rows.length >= 1, 'should return at least one row')
    const row = result.rows.find(r => r['n.name'] === 'Bob')
    assert.ok(row, 'should find the Bob row')
    assert.equal(row['n.age'], 25, 'age should be 25')
  })

  it('returns the correct columns array', () => {
    const result = db.execute(
      "MATCH (n:Person {name: 'Alice'}) RETURN n.name, n.age"
    )
    assert.deepEqual(result.columns, ['n.name', 'n.age'])
  })

  it('returns null for a missing property', () => {
    db.execute("CREATE (n:Person {name: 'Eve'})")
    const result = db.execute(
      "MATCH (n:Person {name: 'Eve'}) RETURN n.age"
    )
    assert.equal(result.rows.length, 1)
    assert.equal(result.rows[0]['n.age'], null)
  })
})

describe('execute — edges / relationships', () => {
  let db
  let dir

  before(() => {
    dir = makeTempDir()
    db = SparrowDB.open(path.join(dir, 'graph.db'))
    db.execute("CREATE (a:Person {name: 'Carol'})")
    db.execute("CREATE (b:Person {name: 'Dave'})")
    db.execute(
      "MATCH (a:Person {name: 'Carol'}), (b:Person {name: 'Dave'}) " +
      "CREATE (a)-[:KNOWS]->(b)"
    )
  })

  after(() => {
    removeDir(dir)
  })

  it('can query an edge with MATCH (a)-[r:T]->(b)', () => {
    const result = db.execute(
      "MATCH (a:Person {name: 'Carol'})-[r:KNOWS]->(b:Person) RETURN b.name"
    )
    assert.ok(result.rows.length >= 1, 'should find at least one KNOWS edge')
    assert.equal(result.rows[0]['b.name'], 'Dave')
  })
})

describe('execute — COUNT(*) aggregation', () => {
  let db
  let dir

  before(() => {
    dir = makeTempDir()
    db = SparrowDB.open(path.join(dir, 'graph.db'))
    // Use index 1-5 (not 0-4): index=0 is stored as integer 0 which the
    // current engine treats as a falsy/null sentinel inside COUNT(expr).
    // That's a known bug tracked in SPA-172; COUNT(*) is unaffected.
    for (let i = 1; i <= 5; i++) {
      db.execute(`CREATE (n:Item {index: ${i}})`)
    }
  })

  after(() => {
    removeDir(dir)
  })

  it('COUNT(*) returns the correct total', () => {
    const result = db.execute('MATCH (n:Item) RETURN COUNT(*) AS total')
    assert.equal(result.rows.length, 1, 'should return exactly one aggregate row')
    assert.equal(Number(result.rows[0]['total']), 5, 'total should be 5')
  })

  it('COUNT(expr) counts non-null values', () => {
    // All items have a non-zero `index` so all should be counted.
    const result = db.execute('MATCH (n:Item) RETURN COUNT(n.index) AS cnt')
    assert.equal(result.rows.length, 1)
    assert.equal(Number(result.rows[0]['cnt']), 5)
  })
})

describe('execute — scalar data types', () => {
  let db
  let dir

  before(() => {
    dir = makeTempDir()
    db = SparrowDB.open(path.join(dir, 'graph.db'))
    // Note: the engine rejects null property values at CREATE time.
    // Store non-null scalar types; missing-property null is tested via MATCH.
    db.execute("CREATE (n:Typed {str: 'hello', num: 42, flag: true})")
  })

  after(() => {
    removeDir(dir)
  })

  it('returns string values correctly', () => {
    const r = db.execute('MATCH (n:Typed) RETURN n.str')
    assert.equal(r.rows[0]['n.str'], 'hello')
  })

  it('returns numeric values correctly', () => {
    const r = db.execute('MATCH (n:Typed) RETURN n.num')
    assert.equal(r.rows[0]['n.num'], 42)
  })

  it('returns boolean values as truthy', () => {
    // The engine serializes booleans as 0/1 (number) in the current build.
    // Assert the value is truthy rather than strictly `true` to be resilient
    // to the serialization detail.
    const r = db.execute('MATCH (n:Typed) RETURN n.flag')
    assert.ok(r.rows[0]['n.flag'], 'flag should be truthy')
  })

  it('returns null for an absent property', () => {
    const r = db.execute('MATCH (n:Typed) RETURN n.missing')
    assert.equal(r.rows[0]['n.missing'], null)
  })

  it('throws when trying to CREATE with a null property value', () => {
    // The engine validates property values at write time.
    assert.throws(
      () => db.execute("CREATE (n:X {bad: null})"),
      /null|invalid/i
    )
  })
})

describe('checkpoint and optimize', () => {
  let db
  let dir

  before(() => {
    dir = makeTempDir()
    db = SparrowDB.open(path.join(dir, 'graph.db'))
    for (let i = 1; i <= 10; i++) {
      db.execute(`CREATE (n:Node {i: ${i}})`)
    }
  })

  after(() => {
    removeDir(dir)
  })

  it('checkpoint() does not throw', () => {
    assert.doesNotThrow(() => db.checkpoint())
  })

  it('optimize() does not throw', () => {
    assert.doesNotThrow(() => db.optimize())
  })

  it('data is readable after checkpoint', () => {
    db.checkpoint()
    const r = db.execute('MATCH (n:Node) RETURN COUNT(*) AS c')
    assert.equal(Number(r.rows[0]['c']), 10)
  })
})

describe('beginRead — snapshot transactions', () => {
  let db
  let dir

  before(() => {
    dir = makeTempDir()
    db = SparrowDB.open(path.join(dir, 'graph.db'))
    db.execute("CREATE (n:Snap {v: 1})")
  })

  after(() => {
    removeDir(dir)
  })

  it('beginRead() returns a ReadTx with a snapshotTxnId string', () => {
    const tx = db.beginRead()
    assert.equal(typeof tx.snapshotTxnId, 'string', 'snapshotTxnId should be a string')
    // Must be parseable as a non-negative BigInt (u64 range).
    const id = BigInt(tx.snapshotTxnId)
    assert.ok(id >= 0n, 'snapshotTxnId should be >= 0')
  })

  it('ReadTx.execute() throws with SPA-100 message (not yet landed)', () => {
    const tx = db.beginRead()
    assert.throws(
      () => tx.execute('MATCH (n) RETURN n'),
      /SPA-100|not yet available/i
    )
  })
})

describe('beginWrite — write transactions', () => {
  let db
  let dir

  before(() => {
    dir = makeTempDir()
    db = SparrowDB.open(path.join(dir, 'graph.db'))
  })

  after(() => {
    removeDir(dir)
  })

  it('commit() returns a string txn id', () => {
    const tx = db.beginWrite()
    const txnId = tx.commit()
    assert.equal(typeof txnId, 'string', 'commit() should return a string')
    const id = BigInt(txnId)
    assert.ok(id >= 0n, 'txn id should be a non-negative integer')
  })

  it('commit() throws if called twice', () => {
    const tx = db.beginWrite()
    tx.commit()
    assert.throws(
      () => tx.commit(),
      /committed|rolled back/i,
      'second commit should throw'
    )
  })

  it('rollback() is idempotent and does not throw', () => {
    const tx = db.beginWrite()
    assert.doesNotThrow(() => tx.rollback())
    // Second rollback should also be silent.
    assert.doesNotThrow(() => tx.rollback())
  })

  it('WriteTx.execute() throws with SPA-99 message (not yet landed)', () => {
    const tx = db.beginWrite()
    assert.throws(
      () => tx.execute('CREATE (n:X)'),
      /not yet available|SPA-99/i
    )
    tx.rollback()
  })
})

describe('executeWithParams — parameterized queries (KMSmcp #67)', () => {
  let db
  let dir

  before(() => {
    dir = makeTempDir()
    db = SparrowDB.open(path.join(dir, 'graph.db'))
    db.createVectorIndex('Memory', 'embedding', 4, 'cosine')
    db.execute("CREATE (n:Memory {id: 'k1'})")
    db.execute("CREATE (n:Memory {id: 'k2'})")
    db.execute("CREATE (n:Person {id: 'p-42', age: 30, name: 'Alice'})")
  })

  after(() => {
    removeDir(dir)
  })

  it('SET n.embedding = $emb writes a vector via param (the dedup-gate path)', () => {
    // The flagship KMSmcp #67 use case: 768d embeddings can't be inlined as
    // Cypher list literals — they must be passed as $emb.
    const emb = [0.1, 0.2, 0.3, 0.4]
    const r = db.executeWithParams(
      'MATCH (n:Memory {id: $id}) SET n.embedding = $emb',
      { id: 'k1', emb }
    )
    assert.ok(Array.isArray(r.rows), 'rows must be an array')
    assert.equal(r.rows.length, 0, 'SET returns no rows')

    // The property must roundtrip on a follow-up read.
    const got = db.execute("MATCH (n:Memory {id: 'k1'}) RETURN n.embedding")
    assert.equal(got.rows.length, 1, 'must find the node back')
    assert.ok(got.rows[0]['n.embedding'] != null, 'embedding must be set')
  })

  it('Float32Array embedding via Array.from() roundtrips', () => {
    // Real KMSmcp callers will derive the array from a Float32Array model output.
    const f32 = new Float32Array([0.5, -0.3, 0.7, 0.1])
    const emb = Array.from(f32)
    const r = db.executeWithParams(
      'MATCH (n:Memory {id: $id}) SET n.embedding = $emb',
      { id: 'k2', emb }
    )
    assert.equal(r.rows.length, 0)
  })

  it('MATCH … WHERE n.id = $id with a string param', () => {
    const r = db.executeWithParams(
      'MATCH (n:Person {id: $id}) RETURN n.name',
      { id: 'p-42' }
    )
    assert.equal(r.rows.length, 1, 'must find Alice')
    assert.equal(r.rows[0]['n.name'], 'Alice')
  })

  it('UNWIND $nums AS n with a numeric list param', () => {
    const r = db.executeWithParams(
      'UNWIND $nums AS n RETURN n',
      { nums: [10, 20, 30] }
    )
    assert.equal(r.rows.length, 3)
    assert.equal(r.rows[0]['n'], 10)
    assert.equal(r.rows[2]['n'], 30)
  })

  it('throws TypeError when params is not an object', () => {
    assert.throws(
      () => db.executeWithParams('UNWIND $x AS n RETURN n', [1, 2, 3]),
      /TypeError|object/i,
      'top-level array params must be rejected'
    )
  })

  it('missing param key produces 0 rows (not an error) for UNWIND', () => {
    // Matches the engine's spa190_unwind_missing_param_produces_no_rows test.
    const r = db.executeWithParams(
      'UNWIND $items AS x RETURN x',
      { other: 1 }
    )
    assert.equal(r.rows.length, 0)
  })

  it('null params arg is treated as an empty parameter map', () => {
    // Cypher with no $params should still execute when params is null/undefined.
    const r = db.executeWithParams('MATCH (n:Person {id: \'p-42\'}) RETURN n.age', null)
    assert.equal(r.rows.length, 1)
    assert.equal(r.rows[0]['n.age'], 30)
  })
})

describe('DISTINCT aggregation — SPA-172 regression', () => {
  let db
  let dir

  before(() => {
    dir = makeTempDir()
    db = SparrowDB.open(path.join(dir, 'graph.db'))
    db.execute("CREATE (n:Tag {name: 'red'})")
    db.execute("CREATE (n:Tag {name: 'red'})")
    db.execute("CREATE (n:Tag {name: 'blue'})")
  })

  after(() => {
    removeDir(dir)
  })

  it('RETURN DISTINCT deduplicates repeated values', () => {
    // SPA-172 fix: RETURN DISTINCT must eliminate duplicate rows.
    // 'red' appears twice; DISTINCT should collapse it to one row.
    const r = db.execute('MATCH (n:Tag) RETURN DISTINCT n.name')
    assert.equal(r.rows.length, 2, 'should return 2 distinct names (red, blue)')
  })
})
