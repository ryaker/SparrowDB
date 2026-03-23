# SparrowDB: Build vs. Think — Design Decisions Requiring Rigor

## Just Build It (ticket → delegate → done)

These are implementation tasks with no significant design ambiguity.

| Ticket | Item | Why it's obvious |
|--------|------|-----------------|
| SPA-272 | Degree cache (Q7: 1.27s → ~80ms) | Pre-compute degree at edge write time. Store as u32 in a HashMap<(label_id, slot), u32> in NodeStore. Update on EdgeCreate/EdgeDelete WAL replay. Sort descending for top-k. Clear algorithm, no concurrency issues. |
| SPA-253 | WAL CRC32C upgrade | Drop-in replacement for CRC32. Algorithm is defined, just swap the hash function and update the version byte in the WAL header. Backward compat: read old CRC32, write new CRC32C. |
| SPA-171 | Storage sizing / db.stats() | Count bytes per label from column files. Expose via `GraphDb::stats()`. Pure read operation. |
| SPA-235 | CREATE INDEX syntax | Parser already has property index infrastructure. Just wire the Cypher `CREATE INDEX ON :Label(prop)` syntax to call `prop_index.build_for()`. |
| SPA-234 | UNIQUE constraints | Add a uniqueness check in `WriteTx::create_node` — lookup the property value in the index, fail if already present. Enforcement, not new infrastructure. |
| SPA-111 | LDBC SNB benchmark suite | Data loader + 14 IC queries. Mechanical work, well-specified by LDBC. High value for credibility. |
| — | Architecture doc | The design is already built. This is documentation work, not design work. |

---

## Needs Deep Thinking — Multi-LLM Extended Reasoning

These have non-obvious tradeoffs, correctness risks, or design decisions that will lock in for years. Worth passing through Claude Opus + o3 with extended thinking before committing.

---

### 1. Parallel BFS / Rayon Traversal

**Why it's not obvious:** The current execution model uses `&self` with interior mutability (`RefCell`). Rayon requires `Send + Sync`. The delta log is read per-query but the CSR is memory-mapped and shared. The SWMR transaction model guarantees no concurrent writers, but the read path hasn't been audited for thread safety at the data structure level.

**Questions to resolve before building:**
- Which data structures in `Engine` are safe to share across Rayon threads, and which need per-thread copies?
- Does `RefCell<PropertyIndex>` and `RefCell<TextIndex>` need to become `Arc<Mutex<>>` or `Arc<RwLock<>>`? What's the lock granularity?
- BFS frontier expansion: parallelize per-frontier-node (fine-grained, lots of sync overhead) or per-source-node (coarser, better for dense graphs)?
- How does parallel BFS interact with the `visited` HashSet? Does each thread need its own visited set, or is a concurrent HashSet (DashMap) needed?
- What's the expected speedup on SNAP Facebook (4M edges) with 8 cores? If it's 2x due to synchronization overhead, is it worth the complexity?

**Prompt for extended reasoning:**
> "Given SparrowDB's SWMR transaction model, interior-mutability execution engine (RefCell-based), and CSR+delta-log storage, design the minimal-invasive parallelization strategy for BFS frontier expansion using Rayon. What must be cloned per-thread vs shared? What are the correctness invariants that must hold? Show the expected speedup model for a scale-free graph with mean degree 22 and diameter 7."

---

### 2. Cache-Friendly CSR Layout

**Why it's not obvious:** This is a storage format change. Once committed, migrating existing databases requires a format version bump and either an in-place migration tool or a "rebuild from WAL" path. Getting it wrong means data loss or silent corruption. Also: the bottleneck may not be where you think — needs profiling before designing.

**Questions to resolve:**
- Current layout: `col_{id}.bin` files per label, row-per-slot. Is the bottleneck the column file layout, or random I/O into the CSR adjacency arrays?
- What does Neo4j's CSR actually do? (Compressed adjacency lists, relationship type grouping, sorted by rel type for binary search.)
- Options: (a) sort adjacency by rel_type for binary search, (b) pack per-node adjacency contiguously (DSR format), (c) mmap with readahead hints, (d) columnar storage for node properties + separate adjacency store. Wildly different implementation costs and compatibility implications.
- Does the bottleneck show up in `csr_neighbors_all` profiling, or in delta log iteration? (Needs flamegraph before designing anything.)

**Prompt for extended reasoning:**
> "For an embedded single-writer graph database with a CSR + delta-log layered storage model, what cache-friendly adjacency layout changes deliver the most traversal improvement with the least format migration risk? Show the tradeoffs between sorted adjacency lists, DSR format, and mmap readahead. What profiling signals would distinguish these bottlenecks?"

---

### 3. Cost-Based Query Planner

**Why it's not obvious:** The current planner is rule-based (inline in `execute_scan`). A cost-based planner requires: (a) cardinality statistics per (label, property), (b) selectivity estimation for predicates, (c) a join ordering algorithm for multi-hop patterns, (d) a plan representation separate from execution. Each is a multi-week project.

**Questions to resolve:**
- What statistics are needed, and how are they collected without blocking writes? Histogram? Count-min sketch? Simple row counts?
- When are statistics stale? Does SparrowDB need auto-analyze like Postgres?
- For the common case (`MATCH (n:Person {age: 30})`), the rule-based planner already works. What query patterns actually need cost-based decisions?
- Is the right next step a full planner, or just adding cardinality hints to `choose_scan_strategy`?

**Prompt for extended reasoning:**
> "What is the minimal statistics infrastructure needed to make a rule-based property index planner in an embedded graph database cost-aware for the 5 most common query patterns? Compare a histogram approach vs simple row-count statistics. What are the maintenance cost tradeoffs? When does the rule-based approach fail catastrophically vs gracefully?"

---

### 4. Variable-Length Path Engine

**Why it's not obvious:** The current BFS is general but slow. Alternatives (bidirectional BFS, DFS with pruning, Dijkstra, A*) each have correctness constraints under Cypher's path semantics. Cypher's `*M..N` means simple paths (no repeated nodes) — which BFS naturally enforces but parallel DFS does not without extra bookkeeping. Getting this wrong produces incorrect query results silently.

**Questions to resolve:**
- Does SparrowDB's BFS correctly implement simple-path semantics for all `*M..N` patterns across all graph topologies (cycles, self-loops, dense subgraphs)? The SPA-268 fix changed the visited key type — needs verification.
- Bidirectional BFS is O(b^(d/2)) vs O(b^d). For SNAP Facebook (mean degree ~22, diameter ~7), this is potentially 1000x for long paths. But it requires knowing the target set upfront — does unbounded `*` allow this?
- For `MATCH (a)-[:R*]->(b) WHERE b.prop = val`, can the planner push the `b.prop` filter into BFS termination to prune early? How does this interact with the lazy index?

**Prompt for extended reasoning:**
> "For Cypher `*M..N` variable-length path semantics (simple paths, no repeated nodes), compare BFS, bidirectional BFS, and DFS+pruning in terms of: (1) correctness guarantees for all graph topologies including cycles, (2) implementation complexity in a single-threaded Rust execution engine, (3) expected performance on scale-free social graphs with mean degree 22 and diameter 7. What is the correct algorithm choice for an embedded single-threaded graph database?"

---

## Full Backlog (non-performance)

### Data Integrity
- **SPA-253** — WAL CRC32C (High, just build)

### Cypher Gaps
- **SPA-238** — `n.type IN list` compound predicates (Low)
- **SPA-236** — `ANY(label IN labels(n) WHERE ...)` (Low)
- **SPA-235** — `CREATE INDEX` syntax (Low, just build)
- **SPA-234** — `CREATE CONSTRAINT UNIQUE` (Low, just build)
- **SPA-144** — openCypher TCK compliance harness (Medium)

### Benchmarks / Credibility
- **SPA-111** — LDBC SNB full suite (Urgent — this is what gets cited in papers and blog posts)
- **SPA-145** — LDBC data loader (CSV → SparrowDB)
- **SPA-146** — LDBC IC1-IC14 read workload queries

### Ecosystem
- **SPA-226** — SparrowOntology crates.io publish (unblocked as of today)
- **SPA-230** — Template-driven data import overlay (Low)
- **SPA-171** — Storage sizing / db.stats() (Medium, just build)

### Documentation
- Architecture doc (highest-trust technical artifact, equivalent to SQLite's architecture page)
- On-disk format spec (byte-level, enables third-party readers)

### Language Bindings
- Python ✅
- Node.js ✅
- Ruby (in progress)
- Go (not started)
- WASM (not started)

---

## Priority Call

**Highest-leverage "think" item:** Parallel BFS. Highest correctness risk. Worth 2-3 hours of extended LLM reasoning before writing a line of code.

**Highest-leverage credibility item:** LDBC SNB. Once SparrowDB passes LDBC, it's citeable. That matters more for adoption than any single performance number.

**Highest-leverage near-term build:** SPA-272 (degree cache). Q7 from 1.27s to ~80ms. Low effort, high visibility.
