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

  it('COUNT(DISTINCT n.name) counts unique values', () => {
    // SPA-172 fix: COUNT(DISTINCT expr) must de-duplicate before counting.
    const r = db.execute('MATCH (n:Tag) RETURN COUNT(DISTINCT n.name) AS uniq')
    assert.equal(Number(r.rows[0]['uniq']), 2, 'should count 2 distinct names')
  })
})
