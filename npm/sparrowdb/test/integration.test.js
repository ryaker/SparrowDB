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
//   1. Bundled platform-specific binary, e.g. sparrowdb.darwin-arm64.node (production)
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

function escapeRegExp(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
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

    // `n.embedding` reads back as null via plain RETURN — this is correct,
    // not a gap. The storage layer's property columns have no vector type
    // (Int64 / Bytes / Float only), so a vector SET on an indexed property
    // is written *exclusively* to the HNSW index below; the column is left
    // absent rather than holding a value that isn't the real embedding.
    //
    // Before SPA-475/#473's fix, this assertion read `!= null` and passed —
    // but only because the property WAS being written, as a silently
    // coerced `Int64(0)` (the exact bug those issues fixed: a null/non-
    // representable value written as the literal integer 0, indistinguishable
    // from a real stored zero). `0 != null` in JS, so the assertion was
    // satisfied by the coercion artifact, never by a real embedding value —
    // it was enshrining the bug it looked like it was guarding against. See
    // the discussion on PR #505.
    const got = db.execute("MATCH (n:Memory {id: 'k1'}) RETURN n.embedding")
    assert.equal(got.rows.length, 1, 'must find the node back')
    assert.equal(
      got.rows[0]['n.embedding'],
      null,
      'a vector property has no property-column representation; it must read back absent (null), not a coercion artifact — the real value lives only in the HNSW index, verified below'
    )

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

    //    Plant unconditionally. A fresh DB names columns by FNV hash of the
    //    property (col_2369371622, …), so col_0.bin does not normally exist and
    //    a conditional write happens to fire — but making the fixture depend on
    //    that is fragile: if col_0.bin ever did exist, a conditional write would
    //    skip it and this guard would silently stop guarding.
    let planted = 0
    for (const labelDir of labelDirs) {
      const col0 = path.join(labelDir, 'col_0.bin')
      // 8 bytes = 1 slot, while the label HWM is 4 → slots 1..3 are past EOF.
      fs.writeFileSync(col0, Buffer.alloc(8))
      planted++
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

describe('SparrowDB vector index damage reporting — issues #446 / #451 / #455 / #456', () => {
  // #446 made a damaged HNSW index fail `open()` loudly instead of silently
  // degrading to "no index configured", and added
  // `GraphDb::vector_index_load_failures(path)` as the call an operator makes
  // to find out WHICH files are the problem.  #454 gave it a napi binding.
  //
  // #451 is why this is the only usable health signal: once a damaged file has
  // been quarantined to `<name>.bin.corrupt.<millis>`, the FIRST `open()` has
  // already thrown and every subsequent `open()` succeeds with the index
  // silently absent.  Vector writes for that (label, prop) are dropped and
  // every search returns nothing, and nothing else on the API surface
  // distinguishes that store from a healthy one.
  //
  // #456 changed the shape, because the bare array said three different things
  // with the same value:
  //   - `[]` for a healthy store AND for a directory that could not be read;
  //   - a quarantine artifact stayed reported forever, even after the pair was
  //     rebuilt, so the check could go green -> red and never back;
  //   - probing a live entry QUARANTINED it, so the reported `path` had already
  //     been renamed away by the time the caller saw it (#455).
  // The report is now { unscannable?, active, historical, healthy }.
  //
  // ── Hand-derived fixture facts (from the on-disk format, not from output) ──
  //
  // File name: `VectorIndex::index_path` formats `hnsw_{label}_{prop}.bin`
  //   under `<db_root>/vector_indexes/`.  With label `Memory` and prop
  //   `embedding` that is
  //   `<dbPath>/vector_indexes/hnsw_Memory_embedding.bin`.
  //
  // Damage: the file is truncated to 4 bytes.  A v2 file carries a 36-byte
  //   header (8 magic + 4 version + 4 reserved + 8 payload_len + 8 generation
  //   + 4 crc32c), so a 4-byte file is too short to be read as v2 and falls to
  //   the legacy bincode path.  bincode encodes the struct's first field,
  //   `nodes: Vec<HnswNode>`, as an 8-byte little-endian length prefix — 4
  //   bytes cannot even supply that prefix, so decoding hits end-of-input.
  //   There is no 4-byte string that decodes to a VectorIndex, so this does
  //   not depend on which 4 bytes survive or on any field values.
  //
  // Count: exactly one index is created, for exactly one (label, prop), so
  //   exactly ONE entry must be reported — 1, not 0 and not 2.  It reaches 1
  //   by two different routes (live-unloadable before anything probes it,
  //   quarantine artifact afterwards); asserting the count and the pair rather
  //   than which route produced it is what makes this hold across #442.

  const LABEL = 'Memory'
  const PROP = 'embedding'
  const DIMENSIONS = 3
  const EXPECTED_FAILURES = 1
  const TRUNCATE_TO_BYTES = 4

  // Build a database whose one persisted vector index has been truncated to 4
  // bytes.  Nothing has probed the damaged file yet, so it is still LIVE.
  function makeDamagedDb() {
    const dir = makeTempDir()
    const dbPath = path.join(dir, 'graph.db')

    const seed = SparrowDB.open(dbPath)
    seed.createVectorIndex(LABEL, PROP, DIMENSIONS, 'cosine')

    const indexFile = path.join(
      dbPath, 'vector_indexes', `hnsw_${LABEL}_${PROP}.bin`
    )
    assert.ok(
      fs.existsSync(indexFile),
      `fixture precondition: createVectorIndex must persist ${indexFile}`
    )
    const size = fs.statSync(indexFile).size
    assert.ok(
      size > TRUNCATE_TO_BYTES,
      `fixture precondition: a persisted index must be longer than the ` +
      `${TRUNCATE_TO_BYTES}-byte truncation point, got ${size} bytes`
    )
    fs.truncateSync(indexFile, TRUNCATE_TO_BYTES)
    assert.equal(
      fs.statSync(indexFile).size,
      TRUNCATE_TO_BYTES,
      'fixture precondition: truncation must have taken effect'
    )

    return { dir, dbPath, indexFile }
  }

  function artifactsFor(dbPath, label = LABEL, prop = PROP) {
    const vecDir = path.join(dbPath, 'vector_indexes')
    if (!fs.existsSync(vecDir)) return []
    return fs.readdirSync(vecDir)
      .filter(n => n.startsWith(`hnsw_${label}_${prop}.bin.corrupt.`))
  }

  // Every report, from either tier, must carry the same three signals.  A
  // consumer that cannot see all three is back to the defect: it cannot tell
  // green from blind, and it cannot tell current damage from an old scar.
  function assertReportShape(report) {
    assert.equal(typeof report, 'object', 'report must be an object')
    assert.ok(!Array.isArray(report), 'report must not be a bare array (#456)')
    assert.ok(Array.isArray(report.active), 'active must be an array')
    assert.ok(Array.isArray(report.historical), 'historical must be an array')
    assert.equal(typeof report.healthy, 'boolean', 'healthy must be a boolean')
    assert.ok(
      report.unscannable === undefined || typeof report.unscannable === 'string',
      `unscannable must be absent or a string, got ${typeof report.unscannable}`
    )
    // `healthy` is computed natively; assert the composition it claims so a JS
    // caller can rely on the single field instead of re-deriving it.
    assert.equal(
      report.healthy,
      !report.unscannable && report.active.length === 0,
      'healthy must equal (!unscannable && active.length === 0)'
    )
  }

  function assertFailureShape(entry, indexFile) {
    // Named fields, not tuple positions — a consumer building an alert needs
    // to read `entry.path`, not `entry[2]`.
    assert.deepEqual(
      Object.keys(entry).sort(),
      ['label', 'path', 'prop', 'reason'],
      'entry must expose exactly { label, prop, path, reason }'
    )
    for (const field of ['label', 'prop', 'path', 'reason']) {
      assert.equal(
        typeof entry[field], 'string',
        `${field} must be a string, got ${typeof entry[field]}`
      )
      assert.ok(entry[field].length > 0, `${field} must not be empty`)
    }
    assert.equal(entry.label, LABEL, 'must name the damaged label')
    assert.equal(entry.prop, PROP, 'must name the damaged property')
    // The path must belong to the index we damaged: either the live .bin or
    // its quarantine artifact.
    assert.ok(
      entry.path === indexFile || entry.path.startsWith(`${indexFile}.corrupt.`),
      `path must name the damaged index (${indexFile} or its .corrupt.* ` +
      `artifact); got ${entry.path}`
    )
  }

  it('both tiers are static methods on the class, not instance methods', () => {
    // Non-negotiable: the database the deep tier diagnoses is the one that will
    // not open, so an instance method would be unreachable exactly when it is
    // needed.  `SparrowDB.prototype` must NOT carry either.
    for (const name of ['vectorIndexLoadFailures', 'vectorIndexDamageScan']) {
      assert.equal(
        typeof SparrowDB[name], 'function',
        `${name} must exist as a static on SparrowDB`
      )
      assert.equal(
        typeof SparrowDB.prototype[name], 'undefined',
        `${name} must NOT be an instance method`
      )
    }
    // And the static damage scan must not be confused with the INSTANCE
    // `vectorIndexHealth(label, prop)`, which counts reachable vectors inside
    // one healthy index.  Different question, different receiver.
    assert.equal(
      typeof SparrowDB.prototype.vectorIndexHealth, 'function',
      'the per-index reachability method stays an instance method'
    )
  })

  it('never throws, and distinguishes "nothing wrong" from "could not look" (#456)', () => {
    // The diagnostic must not itself become a failure path — but it must also
    // stop answering the same thing for two different situations.
    //
    // Hand-derived, from `scan_vector_index_dir`:
    //   - a path whose `vector_indexes/` simply is not there gives read_dir
    //     ENOENT and symlink_metadata ENOENT, which is the ordinary "no vector
    //     index was ever created" case -> scanned successfully, nothing found.
    //   - a path containing a NUL byte cannot be handed to the OS at all;
    //     read_dir fails with InvalidInput, which is NOT NotFound -> we did not
    //     observe the directory, so the report must say so.
    const dir = makeTempDir()
    try {
      for (const tier of ['vectorIndexLoadFailures', 'vectorIndexDamageScan']) {
        for (const p of [
          path.join(dir, 'does-not-exist'),   // absent directory
          dir,                                // a directory that is not a db
          '',                                 // empty string
        ]) {
          let out
          assert.doesNotThrow(
            () => { out = SparrowDB[tier](p) },
            `${tier}(${JSON.stringify(p)}) must not throw`
          )
          assertReportShape(out)
          assert.equal(
            out.unscannable, undefined,
            `${tier}(${JSON.stringify(p)}): an absent directory is scanned ` +
            `successfully, not unscannable`
          )
          assert.equal(out.active.length, 0, 'nothing on disk can be damaged')
          assert.equal(out.historical.length, 0, 'and nothing is historical')
          assert.equal(out.healthy, true, 'so the report is healthy')
        }

        // The blind case.  Same call, same absence of findings — and it must
        // NOT read as healthy.
        const bad = '/tmp/sparrowdb-\0-bad-path'
        let blind
        assert.doesNotThrow(
          () => { blind = SparrowDB[tier](bad) },
          `${tier} must not throw on an unusable path`
        )
        assertReportShape(blind)
        assert.equal(
          typeof blind.unscannable, 'string',
          `${tier}: a path the OS cannot accept must report unscannable`
        )
        assert.equal(
          blind.healthy, false,
          'not knowing is not the same as being fine'
        )
      }
    } finally {
      removeDir(dir)
    }
  })

  it('a chmod-000 index directory is not reported as healthy (#456)', () => {
    // The demonstrated case: a misconfigured service account or a bad mount
    // during a poll used to produce output byte-identical to a healthy store.
    const dir = makeTempDir()
    const dbPath = path.join(dir, 'graph.db')
    const vecDir = path.join(dbPath, 'vector_indexes')
    try {
      const db = SparrowDB.open(dbPath)
      db.createVectorIndex(LABEL, PROP, DIMENSIONS, 'cosine')

      // Control: hand-derived — one live index, intact, no artifacts, so a
      // healthy report with both lists empty.
      const control = SparrowDB.vectorIndexLoadFailures(dbPath)
      assertReportShape(control)
      assert.equal(control.healthy, true, 'fixture precondition: healthy control')

      fs.chmodSync(vecDir, 0o000)
      // Running as root defeats the permission bits, so the property under test
      // cannot be exercised; skip rather than assert something untrue.
      let blocked = true
      try { fs.readdirSync(vecDir); blocked = false } catch { /* expected */ }
      if (!blocked) return

      for (const tier of ['vectorIndexLoadFailures', 'vectorIndexDamageScan']) {
        const blind = SparrowDB[tier](dbPath)
        assertReportShape(blind)
        assert.equal(
          typeof blind.unscannable, 'string',
          `${tier}: an unlistable directory must say so`
        )
        assert.equal(
          blind.active.length, 0,
          'nothing was observed, so nothing may be claimed'
        )
        assert.equal(blind.healthy, false, 'blind must not read as healthy')
        assert.notDeepEqual(
          blind, control,
          'blind must not be observationally identical to green'
        )
      }

      // And `open()` must refuse: a directory we cannot list may hold indexes,
      // and starting anyway means "no index configured" for every one of them.
      assert.throws(
        () => SparrowDB.open(dbPath),
        'open() must not treat an unlistable vector_indexes/ as empty'
      )
    } finally {
      try { fs.chmodSync(vecDir, 0o755) } catch { /* best effort */ }
      removeDir(dir)
    }
  })

  it('a healthy database reports healthy, with both lists empty', () => {
    // Control.  One index is created and left intact, so the hand-derived
    // expectation is 0 active and 0 historical — an always-non-empty report
    // would be useless as a health check.
    const dir = makeTempDir()
    try {
      const dbPath = path.join(dir, 'graph.db')
      const db = SparrowDB.open(dbPath)
      db.createVectorIndex(LABEL, PROP, DIMENSIONS, 'cosine')
      db.execute(`CREATE (n:${LABEL} {id: 'ok-1'})`)
      db.addToVectorIndex(
        LABEL, PROP, 'ok-1', new Float32Array([0.1, 0.2, 0.3])
      )

      for (const tier of ['vectorIndexLoadFailures', 'vectorIndexDamageScan']) {
        const report = SparrowDB[tier](dbPath)
        assertReportShape(report)
        assert.equal(
          report.active.length, 0,
          `${tier}: a healthy index must report no active damage, got ` +
          JSON.stringify(report.active)
        )
        assert.equal(report.historical.length, 0, `${tier}: nothing historical`)
        assert.equal(report.healthy, true, `${tier}: must read healthy`)
      }
    } finally {
      removeDir(dir)
    }
  })

  it('reports live damage WITHOUT quarantining it — the report is read-only (#455 / #456)', () => {
    // First observer of the damage.  The deep tier reads the file, rejects it,
    // and MUST leave it exactly where it found it.
    //
    // Before #456 this same call renamed the .bin to `.bin.corrupt.<millis>`
    // as a side effect of reporting on it, so `fs.existsSync(entry.path)` —
    // the obvious sanity check, and the one a health check would use to
    // confirm the damage is real — was false on the very first report, and a
    // second call answered differently from the first.
    const { dir, indexFile, dbPath } = makeDamagedDb()
    try {
      const before = fs.readFileSync(indexFile)
      assert.equal(
        before.length, TRUNCATE_TO_BYTES,
        'fixture precondition: the damaged file is 4 bytes'
      )

      const report = SparrowDB.vectorIndexLoadFailures(dbPath)
      assertReportShape(report)
      assert.equal(
        report.active.length, EXPECTED_FAILURES,
        `expected exactly ${EXPECTED_FAILURES} damaged index, got ` +
        JSON.stringify(report.active)
      )
      assert.equal(report.historical.length, 0, 'nothing has been rebuilt')
      assert.equal(report.healthy, false, 'a damaged live index is not healthy')

      assertFailureShape(report.active[0], indexFile)
      assert.equal(
        report.active[0].path, indexFile,
        'the live arm reports the .bin path it scanned'
      )
      assert.ok(
        /corrupt/i.test(report.active[0].reason),
        `reason must describe the damage, got: ${report.active[0].reason}`
      )

      // The three properties #455 and #456 are about.
      assert.ok(
        fs.existsSync(report.active[0].path),
        `reported path ${report.active[0].path} must still resolve — a report ` +
        `naming a file that is not there is not actionable (#455)`
      )
      assert.deepEqual(
        fs.readFileSync(indexFile), before,
        'the diagnostic must not alter the bytes it is diagnosing'
      )
      assert.deepEqual(
        artifactsFor(dbPath), [],
        'reading a report must not quarantine anything (#456)'
      )

      // Idempotent by construction, now that it does not mutate.
      const second = SparrowDB.vectorIndexLoadFailures(dbPath)
      assert.deepEqual(
        second, report,
        'a health check that runs twice must not get a different answer than ' +
        'one that runs once'
      )
    } finally {
      removeDir(dir)
    }
  })

  it('a rebuilt pair moves from active to historical, so the check can go green again (#456)', () => {
    // Quarantine artifacts are never cleaned up automatically.  Reporting one
    // forever means a store that has been REPAIRED can never go back to green,
    // and an alert that will not clear after the problem is fixed gets muted —
    // worse than no alert, because the attention has been spent for nothing.
    //
    // The artifact is not suppressed either: #442 records no reason at
    // quarantine time, so the file is the only surviving evidence of the
    // incident.  It moves to `historical`, which a monitor ignores and an
    // operator can still read.
    const { dir, dbPath, indexFile } = makeDamagedDb()
    try {
      // Phase 1 — the damaged index makes open() fail, and the failed open
      // quarantines it.  Hand-derived: 1 artifact, 0 live .bin.
      assert.throws(() => SparrowDB.open(dbPath), 'a live damaged index must fail open()')
      assert.equal(
        artifactsFor(dbPath).length, 1,
        'the open path quarantines what it rejects'
      )
      assert.equal(fs.existsSync(indexFile), false, 'the live .bin was moved aside')

      const damaged = SparrowDB.vectorIndexLoadFailures(dbPath)
      assertReportShape(damaged)
      assert.equal(
        damaged.active.length, EXPECTED_FAILURES,
        'nothing serves the pair, so the artifact is ACTIVE damage'
      )
      assert.equal(damaged.historical.length, 0, 'and nothing is historical yet')
      assert.equal(damaged.healthy, false)

      // Phase 2 — the operator rebuilds the index for the same (label, prop).
      // Hand-derived: the artifact is untouched (still exactly 1), a fresh live
      // .bin now exists, and the pair is served again.  So the SAME artifact
      // must move to `historical` and the store must read healthy.
      const db = SparrowDB.open(dbPath)
      db.createVectorIndex(LABEL, PROP, DIMENSIONS, 'cosine')
      assert.ok(fs.existsSync(indexFile), 'the rebuilt index is persisted')
      assert.equal(
        artifactsFor(dbPath).length, 1,
        'rebuilding must not touch the artifact — it is the only evidence left'
      )

      for (const tier of ['vectorIndexLoadFailures', 'vectorIndexDamageScan']) {
        const after = SparrowDB[tier](dbPath)
        assertReportShape(after)
        assert.equal(
          after.active.length, 0,
          `${tier}: a repaired pair must not stay in active, got ` +
          JSON.stringify(after.active)
        )
        assert.equal(
          after.historical.length, 1,
          `${tier}: the wreckage must remain visible as history, got ` +
          JSON.stringify(after.historical)
        )
        assert.equal(after.historical[0].label, LABEL)
        assert.equal(after.historical[0].prop, PROP)
        assert.ok(
          after.historical[0].path.startsWith(`${indexFile}.corrupt.`),
          `history must name the artifact, got ${after.historical[0].path}`
        )
        assert.ok(
          fs.existsSync(after.historical[0].path),
          'and that path must resolve, so an operator can recover from it'
        )
        assert.equal(
          after.healthy, true,
          `${tier}: with the pair served again the store must read healthy — ` +
          `this is the green-again property the check needs to be usable`
        )
      }
    } finally {
      removeDir(dir)
    }
  })

  it('is callable on a database that refuses to open, and keeps reporting after it starts opening "clean" (#451)', () => {
    const { dir, dbPath, indexFile } = makeDamagedDb()
    try {
      // ── Phase 1: the state #446 created. `open()` refuses. ───────────────
      assert.throws(
        () => SparrowDB.open(dbPath),
        err => {
          assert.ok(
            new RegExp(`${LABEL}`).test(err.message) &&
            new RegExp(`${PROP}`).test(err.message),
            `open() must name the (label, prop) pair, got: ${err.message}`
          )
          return true
        },
        'a live damaged index must make open() fail'
      )

      // The whole reason this is static: there is no instance to hang it off.
      const duringOutage = SparrowDB.vectorIndexLoadFailures(dbPath)
      assertReportShape(duringOutage)
      assert.equal(
        duringOutage.active.length, EXPECTED_FAILURES,
        `expected exactly ${EXPECTED_FAILURES} damaged index while open() is ` +
        `failing, got ${JSON.stringify(duringOutage.active)}`
      )
      assertFailureShape(duringOutage.active[0], indexFile)

      // The failed open() quarantined the bytes, so the reported path is now
      // the artifact: `<indexFile>.corrupt.<unix_millis>`.  The stem is
      // hand-derived; only the millisecond stamp is matched loosely because it
      // is a wall-clock value and cannot be derived.
      const artifactPattern = new RegExp(
        `^${escapeRegExp(indexFile)}\\.corrupt\\.\\d+$`
      )
      assert.match(
        duringOutage.active[0].path, artifactPattern,
        'the reported path must be the quarantine artifact'
      )
      // A report naming a file that is not on disk is not actionable.
      assert.ok(
        fs.existsSync(duringOutage.active[0].path),
        `reported path ${duringOutage.active[0].path} must exist on disk`
      )
      assert.ok(
        fs.statSync(duringOutage.active[0].path).isFile(),
        'the reported path must be a regular file'
      )
      assert.ok(
        /quarantine/i.test(duringOutage.active[0].reason),
        `reason must identify this as a quarantine artifact, got: ` +
        duringOutage.active[0].reason
      )

      // ── Phase 2: #451. The .bin is gone, so this is now the "absent" case
      //    and open() succeeds — with the index silently missing. ───────────
      const db = SparrowDB.open(dbPath)
      assert.ok(
        db instanceof SparrowDB,
        'after quarantine the second open() succeeds — that is #451'
      )
      // Confirm the index really is absent from the reopened database: the
      // vectors are gone and nothing on the instance API says so.
      assert.throws(
        () => db.vectorIndexHealth(LABEL, PROP),
        /RangeError/,
        'the quarantined index must be absent from the reopened database'
      )

      // ── Phase 3: the diagnostic is the ONLY remaining signal. ────────────
      const afterReopen = SparrowDB.vectorIndexLoadFailures(dbPath)
      assertReportShape(afterReopen)
      assert.equal(
        afterReopen.active.length, EXPECTED_FAILURES,
        `the quarantine must remain visible after the database reopens ` +
        `"clean"; got ${JSON.stringify(afterReopen.active)}`
      )
      assert.equal(
        afterReopen.healthy, false,
        'a store silently dropping vector writes is not healthy'
      )
      assertFailureShape(afterReopen.active[0], indexFile)
      assert.equal(
        afterReopen.active[0].path, duringOutage.active[0].path,
        'the artifact path must be stable across calls'
      )
      assert.ok(
        fs.existsSync(afterReopen.active[0].path),
        `reported path ${afterReopen.active[0].path} must exist on disk`
      )
    } finally {
      removeDir(dir)
    }
  })
})
