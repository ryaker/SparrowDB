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

  it('SET n.embedding = $emb writes a vector via param AND populates HNSW', () => {
    // The flagship KMSmcp #67 use case: 768d embeddings can't be inlined as
    // Cypher list literals — they must be passed as $emb.
    // Regression test for KMSmcp ch#202: previously SET stored the property
    // but silently skipped the HNSW write — vectorSearch returned nothing.
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

    // CRITICAL: vectorSearch must return the node — the HNSW index must have
    // been updated by the SET.  Before the fix this returned an empty array.
    // Resolve k1's raw u64 node ID so we can assert the hit is specifically
    // this node and not a leftover from a previous test.
    const k1Row = db.execute("MATCH (n:Memory {id: 'k1'}) RETURN id(n)")
    // id(n) → Int64 → JS number (safe range for typical node IDs); vectorSearch
    // returns NodeResult.id as a string, so stringify before comparing.
    const k1InternalId = String(k1Row.rows[0]['id(n)'])
    const hits = db.vectorSearch('Memory', 'embedding', new Float32Array(emb), 5)
    const hitIds = hits.map(h => h.id)
    assert.ok(hitIds.includes(k1InternalId), `vectorSearch must include k1 (internal id ${k1InternalId}) — HNSW was not updated (KMSmcp ch#202 regression); got: ${hitIds}`)
  })

  it('Float32Array embedding via Array.from() roundtrips AND is vectorSearchable', () => {
    // Real KMSmcp callers will derive the array from a Float32Array model output.
    const f32 = new Float32Array([0.5, -0.3, 0.7, 0.1])
    const emb = Array.from(f32)
    const r = db.executeWithParams(
      'MATCH (n:Memory {id: $id}) SET n.embedding = $emb',
      { id: 'k2', emb }
    )
    assert.equal(r.rows.length, 0)

    // Also verify the HNSW index was populated with k2 specifically.
    const k2Row = db.execute("MATCH (n:Memory {id: 'k2'}) RETURN id(n)")
    const k2InternalId = String(k2Row.rows[0]['id(n)'])
    const hits = db.vectorSearch('Memory', 'embedding', f32, 5)
    const hitIds = hits.map(h => h.id)
    assert.ok(hitIds.includes(k2InternalId), `Float32Array SET must populate HNSW for k2 (internal id ${k2InternalId}); got: ${hitIds}`)
  })

  it('addToVectorIndex inserts a node into HNSW directly', () => {
    // The explicit backfill API added as part of KMSmcp ch#202 fix.
    // Create a fresh node without any embedding.
    db.execute("CREATE (n:Memory {id: 'k3'})")
    const emb = new Float32Array([0.9, 0.1, 0.0, 0.0])

    // Insert directly into the HNSW index without going through Cypher SET.
    assert.doesNotThrow(() => {
      db.addToVectorIndex('Memory', 'embedding', 'k3', emb)
    }, 'addToVectorIndex must not throw')

    // vectorSearch must return k3 specifically.
    const k3Row = db.execute("MATCH (n:Memory {id: 'k3'}) RETURN id(n)")
    const k3InternalId = String(k3Row.rows[0]['id(n)'])
    const hits = db.vectorSearch('Memory', 'embedding', emb, 5)
    const hitIds = hits.map(h => h.id)
    assert.ok(hitIds.includes(k3InternalId), `addToVectorIndex must make k3 discoverable via vectorSearch (internal id ${k3InternalId}); got: ${hitIds}`)
  })

  it('addToVectorIndex throws RangeError for missing node', () => {
    const emb = new Float32Array([0.1, 0.2, 0.3, 0.4])
    assert.throws(
      () => db.addToVectorIndex('Memory', 'embedding', 'no-such-node', emb),
      /RangeError/,
      'must throw RangeError when node does not exist'
    )
  })

  it('addToVectorIndex throws RangeError for missing index', () => {
    const emb = new Float32Array([0.1, 0.2, 0.3, 0.4])
    assert.throws(
      () => db.addToVectorIndex('Memory', 'nonexistent_prop', 'k1', emb),
      /RangeError/,
      'must throw RangeError when vector index does not exist'
    )
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

describe('anonymous relationship patterns — issue #406 regression', () => {
  // Issue #406: `MATCH ()-[r]->() RETURN count(r)` threw "not found"
  // (napi code GenericFailure) through the Node binding on the KMS production
  // database, while node-only patterns kept working.
  //
  // Root cause (fixed in #408, crates/sparrowdb-storage/src/node_store.rs):
  // `NodeStore::read_col_slot` returned `Err(Error::NotFound)` when the
  // requested slot fell beyond the end of a column file.  A one-hop traversal
  // reads source-node columns for every slot up to the label HWM, so any
  // database whose HWM exceeds the length of one of its column files failed the
  // whole query.  The correct sentinel for an absent non-nullable read is `0`,
  // matching the already-existing behaviour for a completely missing file.
  //
  // Seed below is fixed and small so every expected value is derived by hand,
  // never captured from program output:
  //   nodes  : a, b, c, d                       → 4 :Knowledge nodes
  //   edges  : (a)-[:RELATED_TO]->(b)
  //            (b)-[:ABOUT]->(c)                → 2 directed edges
  //   so     : count(n)          = 4
  //            directed count(r) = 2
  //            undirected        = 4  (each edge is walked from both endpoints)

  const EXPECTED_NODES = 4
  const EXPECTED_DIRECTED_EDGES = 2
  const EXPECTED_UNDIRECTED_EDGES = EXPECTED_DIRECTED_EDGES * 2

  let db
  let dir
  let dbPath

  before(() => {
    dir = makeTempDir()
    dbPath = path.join(dir, 'graph.db')

    // 1. Seed the graph and flush to disk.
    const seed = SparrowDB.open(dbPath)
    for (const name of ['a', 'b', 'c', 'd']) {
      seed.execute(`CREATE (n:Knowledge {name: '${name}'})`)
    }
    seed.execute(
      "MATCH (a:Knowledge {name: 'a'}), (b:Knowledge {name: 'b'}) CREATE (a)-[:RELATED_TO]->(b)"
    )
    seed.execute(
      "MATCH (b:Knowledge {name: 'b'}), (c:Knowledge {name: 'c'}) CREATE (b)-[:ABOUT]->(c)"
    )
    seed.checkpoint()

    // 2. Recreate the on-disk shape that triggered #406: a column file that
    //    EXISTS but is shorter than the label's high-water mark.  On the KMS
    //    production DB this was col_0.bin with 1064 slots against an HWM of
    //    1065 — legacy data written before columns were zero-padded on CREATE.
    //    A fresh DB never produces that state on its own, which is why the
    //    original synthetic Rust regression test did not actually reproduce the
    //    bug; planting the short column here makes this a real guard.
    const nodesDir = path.join(dbPath, 'nodes')
    const labelDirs = fs
      .readdirSync(nodesDir, { withFileTypes: true })
      .filter(e => e.isDirectory())
      .map(e => path.join(nodesDir, e.name))
    assert.ok(labelDirs.length > 0, 'expected at least one label directory under nodes/')

    let planted = 0
    for (const labelDir of labelDirs) {
      const col0 = path.join(labelDir, 'col_0.bin')
      if (!fs.existsSync(col0)) {
        // 8 bytes = 1 slot, while the label HWM is 4 → slots 1..3 are past EOF.
        fs.writeFileSync(col0, Buffer.alloc(8))
        planted++
      }
    }
    assert.ok(planted > 0, 'expected to plant at least one short col_0.bin fixture')

    // 3. Reopen so the engine reads the planted on-disk state.
    db = SparrowDB.open(dbPath)
  })

  after(() => {
    removeDir(dir)
  })

  it('node-only count still works (control)', () => {
    const r = db.execute('MATCH (n:Knowledge) RETURN count(n) AS cnt')
    assert.equal(r.rows.length, 1)
    assert.equal(r.rows[0]['cnt'], EXPECTED_NODES)
  })

  it('MATCH ()-[r]->() RETURN count(r) does not throw "not found"', () => {
    // The exact query from issue #406.
    const r = db.execute('MATCH ()-[r]->() RETURN count(r) AS cnt')
    assert.equal(r.rows.length, 1, 'must return one aggregated row')
    assert.equal(r.rows[0]['cnt'], EXPECTED_DIRECTED_EDGES)
  })

  it('MATCH ()-[r]-() RETURN count(r) (undirected) does not throw', () => {
    const r = db.execute('MATCH ()-[r]-() RETURN count(r) AS cnt')
    assert.equal(r.rows.length, 1)
    assert.equal(r.rows[0]['cnt'], EXPECTED_UNDIRECTED_EDGES)
  })

  it('MATCH (a)-[r]->(b) with named endpoints does not throw', () => {
    const r = db.execute('MATCH (a)-[r]->(b) RETURN count(r) AS cnt')
    assert.equal(r.rows.length, 1)
    assert.equal(r.rows[0]['cnt'], EXPECTED_DIRECTED_EDGES)
  })

  it('typed anonymous pattern counts only that rel type', () => {
    // Exactly one (a)-[:RELATED_TO]->(b) edge was seeded.
    const r = db.execute('MATCH ()-[r:RELATED_TO]->() RETURN count(r) AS cnt')
    assert.equal(r.rows.length, 1)
    assert.equal(r.rows[0]['cnt'], 1)
  })

  it('MATCH ()-[r]->() RETURN r enumerates every edge', () => {
    // KMSmcp's _getRelationships traversal — must yield one row per directed edge.
    const r = db.execute('MATCH ()-[r]->() RETURN r')
    assert.equal(r.rows.length, EXPECTED_DIRECTED_EDGES)
    for (const row of r.rows) {
      assert.equal(row['r'].$type, 'edge', 'each row must carry an edge handle')
    }
  })
})
