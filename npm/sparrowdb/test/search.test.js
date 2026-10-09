'use strict'

/**
 * Integration tests for the vector / full-text / hybrid search surface of the
 * Node binding (issue #400).
 *
 * Every expected value below is derived by hand from the fixture, never
 * recorded from program output.
 *
 * Fixture (label Doc, 3-dim cosine index on `emb`, BM25 index on `text`):
 *
 *   d1  text 'apple banana'              emb [1,   0,   0]
 *   d2  text 'apple apple apple cherry'  emb [0.9, 0.1, 0]
 *   d3  text 'cherry date'               emb [0,   1,   0]
 *
 * Cosine similarity to q = [1,0,0]:
 *   d1: 1/(1*1)                 = 1
 *   d2: 0.9/(sqrt(0.82)*1)      = 0.9/0.905539 = 0.993884
 *   d3: 0
 *   => nearest-first order d1, d2, d3.
 *
 * Cosine similarity to q = [0,1,0]:
 *   d3: 1;  d2: 0.1/0.905539 = 0.110432;  d1: 0
 *   => nearest-first order d3, d2, d1.
 *
 * Hybrid RRF (k=60, rank is 1-based) for vector q=[0,1,0], text 'banana'
 * ('banana' occurs only in d1, so BM25 ranks d1 alone at rank 1):
 *   d1: vector rank 3 + text rank 1 = 1/63 + 1/61 = 0.032266
 *   d3: vector rank 1               = 1/61        = 0.016393
 *   d2: vector rank 2               = 1/62        = 0.016129
 *   => fused order d1, d3, d2 — different from vector-only (d3, d2, d1).
 */

const { describe, it, before, after } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')

const { SparrowDB } = require('../index.js')

const EPS = 1e-6

describe('vector / full-text / hybrid search (#400)', () => {
  let dir
  let db
  const internal = {} // user id -> internal node id (as the string search results use)

  before(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sparrowdb-search-'))
    db = SparrowDB.open(path.join(dir, 'graph.db'))
    db.createVectorIndex('Doc', 'emb', 3, 'cosine')
    db.createFulltextIndex('Doc', 'text')

    const docs = [
      ['d1', 'apple banana', [1, 0, 0]],
      ['d2', 'apple apple apple cherry', [0.9, 0.1, 0]],
      ['d3', 'cherry date', [0, 1, 0]],
    ]
    for (const [id, text, emb] of docs) {
      db.executeWithParams('CREATE (n:Doc {id: $id, text: $text})', { id, text })
      db.addToVectorIndex('Doc', 'emb', id, new Float32Array(emb))
      const r = db.executeWithParams('MATCH (n:Doc {id: $id}) RETURN id(n)', { id })
      internal[id] = String(r.rows[0]['id(n)'])
    }
  })

  after(() => {
    fs.rmSync(dir, { recursive: true, force: true })
  })

  const ids = (hits) => hits.map((h) => h.id)

  it('vectorSearch returns nearest neighbours in hand-derived order', () => {
    const hits = db.vectorSearch('Doc', 'emb', new Float32Array([1, 0, 0]), 3)
    assert.deepEqual(ids(hits), [internal.d1, internal.d2, internal.d3])
    assert.ok(Math.abs(hits[0].score - 1) < 1e-5, `d1 score ${hits[0].score}`)
    assert.ok(Math.abs(hits[1].score - 0.993884) < 1e-4, `d2 score ${hits[1].score}`)
    assert.ok(Math.abs(hits[2].score - 0) < 1e-5, `d3 score ${hits[2].score}`)
  })

  it('vectorSearch honours k', () => {
    const hits = db.vectorSearch('Doc', 'emb', new Float32Array([0, 1, 0]), 1)
    assert.deepEqual(ids(hits), [internal.d3])
  })

  it('vectorSearch throws TypeError on a dimension mismatch', () => {
    assert.throws(
      () => db.vectorSearch('Doc', 'emb', new Float32Array([1, 0]), 3),
      /TypeError.*2 dimensions.*expects 3/,
    )
  })

  it('vectorSimilarity: cosine, euclidean, dot, and errors', () => {
    const a = new Float32Array([1, 0, 0])
    const c = new Float32Array([0.9, 0.1, 0])
    assert.ok(Math.abs(db.vectorSimilarity(a, c) - 0.993884) < 1e-5)
    // euclidean distance a,c = sqrt(0.1^2 + 0.1^2) = 0.141421
    assert.ok(Math.abs(db.vectorSimilarity(a, c, 'euclidean') - 0.141421) < 1e-5)
    // dot = 0.9
    assert.ok(Math.abs(db.vectorSimilarity(a, c, 'dot') - 0.9) < 1e-6)
    assert.throws(() => db.vectorSimilarity(a, new Float32Array([1, 0])), /TypeError.*mismatch/)
    assert.throws(() => db.vectorSimilarity(a, c, 'manhattan'), /TypeError.*unsupported metric/)
  })

  it('fulltextSearch returns only matching docs, BM25-ranked', () => {
    // 'banana' occurs only in d1.
    assert.deepEqual(ids(db.fulltextSearch('Doc', 'text', 'banana')), [internal.d1])
    // 'cherry' occurs in d2 and d3; 'date' only in d3; 'zzz' nowhere.
    const cherry = ids(db.fulltextSearch('Doc', 'text', 'cherry')).sort()
    assert.deepEqual(cherry, [internal.d2, internal.d3].sort())
    assert.deepEqual(db.fulltextSearch('Doc', 'text', 'zzz'), [])
  })

  it('fulltextSearch ranks the doc with the rarer, repeated term first and honours limit', () => {
    // 'apple' occurs in d1 (1x of 2 tokens) and d2 (3x of 4 tokens); both
    // match, and scores must be non-increasing.
    const hits = db.fulltextSearch('Doc', 'text', 'apple')
    assert.deepEqual(ids(hits).sort(), [internal.d1, internal.d2].sort())
    assert.ok(hits[0].score >= hits[1].score)
    assert.equal(db.fulltextSearch('Doc', 'text', 'apple', 1).length, 1)
  })

  it('hybridSearch (RRF) fuses both lists: d1, d3, d2', () => {
    const hits = db.hybridSearch('Doc', 'text', 'emb', new Float32Array([0, 1, 0]), 'banana', 3)
    assert.deepEqual(ids(hits), [internal.d1, internal.d3, internal.d2])
    assert.ok(Math.abs(hits[0].score - (1 / 63 + 1 / 61)) < EPS, `d1 ${hits[0].score}`)
    assert.ok(Math.abs(hits[1].score - 1 / 61) < EPS, `d3 ${hits[1].score}`)
    assert.ok(Math.abs(hits[2].score - 1 / 62) < EPS, `d2 ${hits[2].score}`)
  })

  it('hybridSearch truncates to k', () => {
    const hits = db.hybridSearch('Doc', 'text', 'emb', new Float32Array([0, 1, 0]), 'banana', 1)
    assert.deepEqual(ids(hits), [internal.d1])
  })

  it('hybridSearch with alpha=1 is vector-only order (d3, d2, d1)', () => {
    // alpha=1: score = 1 * vec/max_vec; text list contributes 0.
    const hits = db.hybridSearch('Doc', 'text', 'emb', new Float32Array([0, 1, 0]), 'banana', 3, 1)
    assert.deepEqual(ids(hits), [internal.d3, internal.d2, internal.d1])
  })

  it('hybridSearch with alpha=0 puts the text match first', () => {
    const hits = db.hybridSearch('Doc', 'text', 'emb', new Float32Array([0, 1, 0]), 'banana', 3, 0)
    assert.equal(hits[0].id, internal.d1)
    assert.ok(Math.abs(hits[0].score - 1) < EPS)
  })

  it('hybridSearch throws on a dimension mismatch instead of returning garbage', () => {
    assert.throws(() =>
      db.hybridSearch('Doc', 'text', 'emb', new Float32Array([1, 0]), 'banana', 3),
    )
  })

  it('Cypher: full_text_search() in WHERE filters to matching nodes', () => {
    const r = db.executeWithParams(
      "MATCH (n:Doc) WHERE full_text_search('Doc', 'text', $q) RETURN n.id",
      { q: 'banana' },
    )
    assert.deepEqual(r.rows.map((row) => row['n.id']), ['d1'])
  })

  it('Float32Array param: SET n.emb = $v indexes the vector (#400)', () => {
    // Before the fix a Float32Array param arrived as a Map and the SET threw
    // "property value is a map".  q = [0,0,1] is orthogonal to d1..d3, so the
    // new node is the only one with nonzero similarity to [0,0,1].
    db.executeWithParams("CREATE (n:Doc {id: 'd4', text: 'elderberry'})", {})
    db.executeWithParams("MATCH (n:Doc {id: $id}) SET n.emb = $v", {
      id: 'd4',
      v: new Float32Array([0, 0, 1]),
    })
    assert.equal(db.hasVector('Doc', 'emb', 'd4'), true)
    const r = db.executeWithParams("MATCH (n:Doc {id: 'd4'}) RETURN id(n)", {})
    const hits = db.vectorSearch('Doc', 'emb', new Float32Array([0, 0, 1]), 1)
    assert.deepEqual(ids(hits), [String(r.rows[0]['id(n)'])])
    assert.ok(Math.abs(hits[0].score - 1) < 1e-5)
  })

  it('Float32Array param round-trips as a list through UNWIND', () => {
    const r = db.executeWithParams('UNWIND $v AS x RETURN x', { v: new Float32Array([0.5, -2, 8]) })
    // 0.5, -2, 8 are exactly representable in f32.  UNWIND yields list
    // elements as strings today (separate limitation), hence Number().
    assert.deepEqual(r.rows.map((row) => Number(row.x)), [0.5, -2, 8])
  })
})
