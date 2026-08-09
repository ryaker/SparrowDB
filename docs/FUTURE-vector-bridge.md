# FUTURE: Hybrid vector bridge (WS5 — deferred)

**Status:** deferred, deliberately. This page is the whole deliverable of WS5 in the Sparrow
integrations directive. No vector code is to be written against it.

**Why deferred, in the directive's own terms:** WS1–WS4 each make Sparrow fit into someone
else's stack. A vector feature makes Sparrow _bigger_, which is the opposite bet, and it
competes on an axis with brutal incumbents. Revisit after WS2 (`sparrow-vault`) and WS3
(`langchain-sparrowdb`) have users.

Written 2026-08-01 against `main` @ `74c1219`. Every claim below cites a file or an issue so
a later reader can check whether it is still true.

---

## 1. The premise is mostly already shipped

WS5 was written as "pair SparrowDB's typed graph with an embedding index." That pairing
largely exists in the tree today:

| Piece                                                                                                   | Where                                                               | Issue / PR  |
| ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- | ----------- |
| HNSW index (pure Rust, no deps; cosine / euclidean / dot)                                               | `crates/sparrowdb-storage/src/vector_index.rs`                      | #394 / #398 |
| BM25 inverted full-text index                                                                           | `crates/sparrowdb-storage/src/fts_index.rs`                         | #395 / #399 |
| RRF + weighted fusion, `hybrid_search()`                                                                | `crates/sparrowdb-execution/src/functions.rs`, `engine/expr.rs:168` | #396 / #403 |
| `create_vector_index` / `get_vector_index` / `drop_vector_index`                                        | `crates/sparrowdb/src/db.rs:2113`–`2183`                            | #394        |
| `SET n.embedding = $vec` also writes HNSW                                                               | `crates/sparrowdb/src/db.rs:1550`–`1595`                            | #410        |
| Node binding: `createVectorIndex`, `addToVectorIndex`, `vectorSearch`, `fulltextSearch`, `hybridSearch` | `crates/sparrowdb-node/src/lib.rs:373`–`752`                        | #400 (open) |

So the question is not "should we add vectors." They are here. The question is whether to
build the remaining _bridge_ — a query surface and a lifecycle contract — and that is a
smaller, more specific decision than the workstream title implies.

## 2. The feature existed but was unreachable through Cypher

`CREATE VECTOR INDEX FOR (n:Memory) ON (n.embedding) OPTIONS {…}` failed to parse with
`InvalidArgument("expected LParen, got For")` until PR #420 (issue #417) landed on
2026-08-01, commit `25f3559`. The lexer emits `Token::For`; `parse_create_vector_index`
matched only the `Ident("FOR")` form and never consumed it. `parse_create_fulltext_index`
had it right, so the vector parser copied the shape but not the token.

That means every published release through v0.1.21 — and 0.1.22 as of the fix — shipped an
HNSW index whose only DDL entry point could not be invoked from Cypher. It was reachable
solely through the Rust and Node APIs.

Relatedly, there is no user-facing documentation for the feature at all — only this
design note, which documents its absence rather than its use:

```console
$ grep -rl 'HNSW\|VECTOR INDEX\|hybrid_search' docs/ README.md \
    --exclude=FUTURE-vector-bridge.md
$   # no matches
```

A feature that could not be invoked and was never documented has not been validated by
users. Treat all demand signal for "more vectors" accordingly.

## 3. What is actually missing

These are lifecycle and identity problems, not algorithm problems. They are the real WS5
scope, and they are the reason a `similar()` function alone would not be enough.

1. **No Cypher-level vector search.** `hybrid_search()` is the only engine-dispatched
   retrieval function (`engine/expr.rs:354`). Pure vector search exists only on the Rust
   (`VectorIndex::search`) and Node (`vectorSearch`) surfaces. There is no `similar()`.
2. **The graph join is undemonstrated.** `hybrid_search` returns
   `List<Map{node_id, score, rank}>` and is only ever exercised as `RETURN hybrid_search(…)`
   (`crates/sparrowdb/tests/hybrid_search.rs`). No test joins those ids back to nodes. The
   ingredients exist — `id(n)` returns the full packed `NodeId` as `Int64`
   (`functions.rs:646`) and `WHERE id(n) = <int>` filters correctly
   (`tests/regression_372.rs:132`, `tests/spa_196_id_function.rs`) — but `UNWIND` over a
   list of maps with `hit.node_id` field access is unverified.
3. **Re-embedding is a silent no-op.** `VectorIndex::insert` returns early if the `node_id`
   is already present (`vector_index.rs:374`, comment: "tombstone / update support is a
   future enhancement"). Update a node's text, recompute its embedding, write it back — the
   index keeps the old vector and reports success.
4. **No delete.** There is no `VectorIndex::delete`. A deleted node leaves a live HNSW entry
   pointing at a `NodeId` that no longer resolves.
5. **Not transactional, not atomic.** `save()` bincode-serializes the entire index and
   `fs::write`s it (`vector_index.rs:178`) — no temp-file-plus-rename, not in the WAL, not
   covered by the SWMR snapshot. A crash between commit and save leaves graph and index
   disagreeing in either direction, and a crash _during_ save can truncate the file.
6. **`hybrid_search` ignores the in-memory index.** It calls `VectorIndex::load` from disk on
   every invocation (`engine/expr.rs:217`) rather than using the `Arc<RwLock<VectorIndex>>`
   that `create_vector_index` populates. So it cannot see unsaved writes, and it pays a full
   deserialize of every vector per query.

## 4. API sketch

```text
similar(label, prop, query_vector, k [, ef]) -> List<Map{node_id, score}>
```

Joinable form, which is the entire point of putting it in Cypher:

```cypher
UNWIND similar('Memory', 'embedding', $q, 10) AS hit
MATCH (n:Memory) WHERE id(n) = hit.node_id
RETURN n.title, hit.score
ORDER BY hit.score DESC
```

`ORDER BY … DESC` above assumes higher-is-better, which is **not** true for every metric the
existing binding already supports. `crates/sparrowdb-node/src/lib.rs:1123-1181` offers cosine,
Euclidean, and dot product; Euclidean yields a *distance*, where lower is more similar. Sorting
it descending returns the least similar rows first. Resolving this is a prerequisite for the
signature above, not a detail to settle during implementation — see §7.

**No `similar(text, k)` overload.** Embedding text requires a model, which means a network
or model dependency, which violates the integration directive's ground rule §2.6 ("everything must keep
working offline and embedded"). Callers embed; Sparrow indexes. The text-in convenience
belongs in the LangChain layer (WS3), where a model is already configured.

### The trap: `node_id` must be a full `NodeId`, never a bare slot

`NodeId` packs `(label_id << 32) | slot` — see `crates/sparrowdb/src/write_tx.rs:81`,
`src/export.rs:157`, `tests/spa_186_csr_nodeid.rs`. Treating a slot as identity has caused
four separate silent-correctness bugs in this codebase: #415 (multi-label MATCH returns no
rows), #427 (`shortestPath` matches destination by slot without label), #429 (`execute_n_hop`
unions CSRs so nodes inherit another label's edges), #431 (`get_node_neighbors_labeled` falls
back to the source node's label). All four produce _wrong rows_, not errors.

The wired path already gets this right: the `SET` → HNSW hook groups by
`(node_id.0 >> 32)` and inserts `node_id.0` (`db.rs:1571`–`1578`). But `VectorIndex::insert`
takes a bare `u64`, and the tests pass literals like `42` and `1`
(`tests/vector_index.rs:80` inserts `1`, `:209` asserts on `42`), so nothing stops a caller
from feeding it a slot. If `similar()` is built, put a newtype at the index boundary —
`VectorKey(NodeId)` — so slot-vs-NodeId confusion is a compile error rather than the fifth
instance of this bug.

One more landmine for whoever implements this: the doc comment on `NodeId`
(`crates/sparrowdb-common/src/lib.rs:13`) says "upper 16 bits = label_id, lower 48 bits =
slot_id", and its unit test at `:178` packs with `<< 48`. Every production site uses
`<< 32`. The comment is stale, and an implementer trusting the type's own documentation
would pack ids wrong.

## 5. Open question: do embeddings belong in the storage layer?

De facto this is already half-answered — vectors persist under `<db>/vector_indexes/`. The
live question is whether the index belongs in the _transactional_ layer (WAL, snapshot,
checkpoint) or stays a sidecar.

**Judgement: sidecar, but a rebuildable one — not WAL-backed.**

The reasoning is that the vector itself is a node property. `SET n.embedding = $vec` stores
the property _and_ updates HNSW. So the HNSW graph is entirely derived state: it can be
reconstructed by scanning that property column. That places it in the same category as the
existing property and text indexes, which are explicitly rebuildable from column files —
`property_index.rs:561` states that a failed index load "is not fatal; the next `Engine` will
simply rebuild from column files," and both expose `build_for(store, label_id, col_id)`.
Putting HNSW graph mutations in the WAL would make every embedding write a durable log
record for information the WAL already contains, and would couple a research-grade ANN
structure to the format that defines on-disk compatibility. That is a bad trade for a
feature this unvalidated.

What is needed instead is cheap and specific: stamp each index file with the LSN it was
built through, verify on open, and rebuild in the background when it is stale or torn. That
converts problems (3), (4), and (5) in §3 from silent-wrong-answer bugs into a
detectable-and-recoverable condition.

One caveat that must be fixed for "rebuildable" to be true: `addToVectorIndex`
(`sparrowdb-node/src/lib.rs:406`) inserts into HNSW _without_ writing a node property. Any
index built that way cannot be rebuilt. Either that path writes the property too, or it is
documented as explicitly unrecoverable.

## 6. In-process HNSW vs. delegating to an external store

Reading the existing code changed my view here, so I will state it rather than list options.

**Delegation is now clearly the worse choice for the primary path.** The in-process index is
already written, dependency-free, and durable enough for read-mostly workloads. Removing it
would not shrink the codebase — BM25 and the fusion functions stay regardless — it would only
add a required network dependency, which the directive's ground rule §2.6 forbids outright. Worse,
delegation cannot do the one thing that makes graph+vector interesting here: fuse a vector
ranking with a BM25 ranking computed in the same process over the same `NodeId` space.
Cross-process fusion means shipping ids and scores over a wire and reconciling two id
spaces — the §4 identity trap, now with a serialization boundary in the middle.

**Delegation is right for exactly one case, and it needs no SparrowDB code.** A user who
already runs pgvector or Pinecone should keep it: the external store returns ids, SparrowDB
expands the graph around them, and the two meet in the LangChain layer (WS3). That is a page
in `langchain-sparrowdb`'s docs, not a Rust abstraction. Do not build a pluggable vector
backend trait inside SparrowDB to serve it.

## 7. Open questions that must be answered before `similar()` is specified

This note deliberately records open questions rather than inventing answers for a deferred
workstream. These three are load-bearing: each one changes the API's shape, so none can be
deferred to implementation time.

1. **Score direction across metrics.** Does `similar()` expose the index's native metric, or
   normalise every result to higher-is-better? Exposing it makes `ORDER BY` metric-dependent
   and quietly wrong when the index is rebuilt with a different metric; normalising loses the
   raw distance some callers want. Whichever is chosen needs coverage per supported metric —
   cosine, Euclidean, and dot product all reach this path today.

2. **Query behaviour during a rebuild, and rejecting incompatible sidecars.** An LSN stamp
   (§5) establishes *staleness* but says nothing about *compatibility*: vector dimension,
   metric, and index format can all differ while the LSN looks current. Two decisions are
   needed — what a query does while a background rebuild is in flight (block, fail, or serve
   the old index), and what metadata is stored so an incompatible index is rejected rather
   than silently queried. Claiming stale-or-torn indexes are handled is not true until both
   exist.

3. **The external-ID mapping contract.** §6's pgvector/Pinecone path assumes the external
   store's ids can be joined back with `id(n) = external_id`. That is only safe if the
   external store holds packed `NodeId` values, or WS3 maintains a validated mapping. Neither
   is specified. The contract must also define behaviour for ids whose node has been deleted
   or never existed — silently dropping them and returning fewer than `k` rows is a different
   failure from raising.

Note that (3) compounds the identity hazard in §3: a bare slot and a full `NodeId` are both
`u64`, so a wrong mapping is a silent wrong-answer bug, not a type error. See #474 — the
`NodeId` doc comment and its unit test currently describe a 48-bit layout while production
uses 32, which must be corrected before any external mapping is defined against it.

## 8. Trigger to revisit, and what to do first

Revisit when a named user hits a retrieval-quality ceiling that external-store-plus-graph-
expansion at the LangChain layer cannot fix. KMSmcp, the consumer named in #400, is the
plausible candidate — it is already the reason the vector, BM25, and fusion work was
prioritized.

Note that "deferred" here does not mean "no vector code" — the substrate already shipped. It
means no new vector _surface area_. The highest-value use of the deferral is to make what
exists correct rather than to extend it. In order:

1. **Correct the authoritative `NodeId` encoding first (#474)** — this gates everything below
   that crosses the index boundary. Three contracts currently disagree: production packs
   `label_id << 32`, while the `NodeId` doc comment and `node_id_packing_roundtrip` both
   describe `<< 48`, and `VectorIndex::insert` takes a bare `u64` that accepts either. Adding
   a `VectorKey(NodeId)` newtype only at the `similar()` boundary would leave every existing
   caller free to pass a raw slot. Fix the comment, rewrite the test to assert against the
   production packing helper rather than re-implementing the shift, then tighten the
   `VectorIndex` boundary and migrate its callers. A slot and a full `NodeId` are both `u64`,
   so getting this wrong is a silent wrong-answer bug rather than a type error — the same
   root cause as #427, #415, and the `execute_n_hop` cross-label edge bug.
2. Close out #400 and document HNSW / `hybrid_search` in `docs/` — today there is zero
   user-facing coverage of a shipped feature.
3. Upsert and delete semantics in `VectorIndex` (§3 items 3–4). Silent stale results are worse
   than a missing feature.
4. Atomic `save()` (temp file + rename) and the LSN stamp from §5.
5. Make `hybrid_search` use the in-memory index instead of reloading from disk (§3 item 6).
6. Only then `similar()` in Cypher — landing with an integration test that actually joins its
   output back to nodes, which no test does today, and with §7's three questions answered.
