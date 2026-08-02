//! Reachability guards for the HNSW vector index (issue #443).
//!
//! # The defect
//!
//! `insert` wired `new -> neighbours` unconditionally, but the reciprocal step
//! `neighbours -> new` only landed when the new node beat an existing occupant
//! of that neighbour's adjacency list on distance. A node inserted into a
//! saturated neighbourhood could lose all 32 of those contests and end up a
//! pure sink: full outgoing degree, present in `id_to_slot`, nothing pointing at
//! it. Greedy descent only follows *outgoing* edges from the entry point, so
//! such a node is invisible to `search` at any `k` and any `ef`.
//!
//! On the live store that surfaced this: 1347 vectors stored, 1307 reachable,
//! 40 stranded — 33 of them with zero in-degree and 7 forming a closed island
//! that pointed at each other with no path back to the main component. Nothing
//! repaired it and nothing reported it; re-inserting a stranded node takes the
//! "already present" path, so even a full backfill could not heal it.
//!
//! # What these tests assert
//!
//! Reachability, not in-degree. An in-degree check calls the 7 island members
//! healthy. Only a traversal from the entry point finds them.
//!
//! Every expected value below is derived by hand from the fixture the test
//! itself builds. Nothing here is a recording of what the code returns — see
//! `regression_406.rs` for what that mistake costs: a guard that passes against
//! the unfixed code guards nothing.

use sparrowdb_storage::vector_index::{InsertOutcome, Metric, VectorIndex};

// ── Deterministic fixture ─────────────────────────────────────────────────────

/// xorshift64 + Box–Muller. Dependency-free and identical on every platform, so
/// these fixtures reproduce exactly.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn normal(&mut self) -> f64 {
        let u1 = self.unit().max(1e-12);
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }
}

fn l2_normalise(v: &mut [f32]) {
    let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n > 0.0 {
        for x in v.iter_mut() {
            *x /= n;
        }
    }
}

fn random_direction(rng: &mut Rng, dims: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dims).map(|_| rng.normal() as f32).collect();
    l2_normalise(&mut v);
    v
}

/// A corpus with the geometry that actually triggers #443.
///
/// Each vector is `aniso * mu + centre + noise`, L2-normalised, and vectors
/// arrive cluster by cluster rather than shuffled — topical batches over time,
/// which is how a memory store fills up.
///
/// The shared `mu` term matters. Real sentence embeddings do not spread over the
/// sphere; they occupy a narrow cone, so pairwise distances are compressed and a
/// new node has to squeeze into neighbour lists whose occupants are all at
/// similar range. Isotropic random vectors — the shape most HNSW tests use —
/// never reproduce this, which is a large part of why the defect survived so
/// long. Neither do near-duplicates of an already-indexed vector, which land
/// beside a node the graph already reaches and are trivially findable.
fn cone_corpus(n: usize, dims: usize, clusters: usize, aniso: f32, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng::new(seed);
    let mu = random_direction(&mut rng, dims);
    let centres: Vec<Vec<f32>> = (0..clusters)
        .map(|_| random_direction(&mut rng, dims))
        .collect();
    (0..n)
        .map(|i| {
            let c = &centres[(i * clusters) / n];
            let mut v: Vec<f32> = (0..dims)
                .map(|d| aniso * mu[d] + c[d] + (rng.normal() * 0.35) as f32)
                .collect();
            l2_normalise(&mut v);
            v
        })
        .collect()
}

fn build(
    n: usize,
    dims: usize,
    clusters: usize,
    aniso: f32,
    seed: u64,
) -> (VectorIndex, Vec<Vec<f32>>) {
    let vectors = cone_corpus(n, dims, clusters, aniso, seed);
    let mut idx = VectorIndex::new(dims, Metric::Cosine);
    for (i, v) in vectors.iter().enumerate() {
        assert_eq!(
            idx.insert(i as u64, v),
            InsertOutcome::Inserted,
            "id {i} is distinct, so it must take the insert path"
        );
    }
    (idx, vectors)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// No stored vector may be unreachable.
///
/// Hand-derived expectations:
/// - 700 inserts of the distinct ids `0..700` each take the "new id" path (the
///   `InsertOutcome::Inserted` assertion in `build` proves it), so `len() == 700`.
/// - `reachable_count()` walks layer-0 edges from `entry_point`. It can never
///   exceed `len()`. A correct index has them equal, because a node no path
///   leads to cannot be returned by `search` for any `k` or `ef`.
/// - So `unreachable_ids()` must be *empty*, not merely short.
#[test]
fn spa443_no_stored_vector_is_unreachable() {
    const N: usize = 700;
    let (idx, _) = build(N, 48, 9, 2.0, 0x5EED);

    assert_eq!(idx.len(), N, "700 distinct ids were inserted");
    let stranded = idx.unreachable_ids();
    assert!(
        stranded.is_empty(),
        "{} of {N} stored vectors are unreachable from the entry point and can never \
         be returned by search: {:?}",
        stranded.len(),
        &stranded[..stranded.len().min(20)]
    );
    assert_eq!(idx.reachable_count(), N);
}

/// The user-visible form of the same invariant: every inserted vector is
/// returned by a search for *itself*, once the search budget is large enough to
/// stop being the limiting factor.
///
/// Hand-derived expectations:
/// - `cosine_similarity(v, v) = (v·v) / (|v| · |v|) = 1.0` for any non-zero `v`,
///   and `search` reports cosine similarity directly as the score. So a query
///   with a stored vector must return that vector's own id with a score of
///   exactly 1.0; `> 0.999` allows only for f32 rounding in the accumulation.
///   No other id can score higher, so it must occupy the top of the window
///   whenever the traversal visits it at all.
/// - `ef = N` makes the candidate window as large as the index, so the window
///   never evicts and best-first search degenerates into a full traversal of the
///   reachable component. Under that budget "returned by search" and "reachable
///   from the entry point" are the same predicate.
/// - Therefore the expected number of vectors that fail to retrieve themselves
///   is exactly 0 — and, crucially, no budget whatsoever rescues a node with no
///   path in, which is what makes this the user-facing statement of #443 rather
///   than a statement about search tuning.
#[test]
fn spa443_every_vector_retrieves_itself_given_full_budget() {
    const N: usize = 700;
    let (idx, vectors) = build(N, 48, 9, 2.0, 0x5EED);

    let mut missing: Vec<usize> = Vec::new();
    for (i, v) in vectors.iter().enumerate() {
        let hits = idx.search(v, 10, N);
        if !hits
            .iter()
            .any(|&(id, score)| id == i as u64 && score > 0.999)
        {
            missing.push(i);
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {N} vectors are not returned by a search for their own embedding even \
         with the candidate window opened to the size of the whole index — they have \
         no path from the entry point: {:?}",
        missing.len(),
        &missing[..missing.len().min(20)]
    );
}

/// Self-retrieval at the *default* search budget, on a corpus without extreme
/// outliers.
///
/// This one is a forward guard, not a reproduction: it passes against the
/// pre-fix code too, and is recorded as such rather than dressed up as
/// evidence. It exists to catch a future change that degrades ordinary
/// retrieval.
///
/// # A real limitation this test is deliberately scoped around
///
/// Reachability is necessary for retrieval but not sufficient. `select_neighbours`
/// takes the nearest `m_max` candidates with no diversity heuristic (HNSW's
/// Algorithm 4), so the graph carries few long-range links. A strong outlier can
/// be reachable through a single inbound edge and still be missed at `ef = 50`,
/// because greedy descent never brings the node holding that edge into the
/// candidate window. Measured on the harsher `aniso = 2.0` corpus at N = 700:
/// 2 nodes, both with in-degree 1, retrievable at `ef = 800` but not at `ef` up
/// to 200. That is a navigability limit in neighbour *selection*, present before
/// and after this fix and unchanged by it — a separate problem from #443, which
/// is about nodes with no path at all. Fixing it means redesigning neighbour
/// selection and re-validating recall, which does not belong in this change.
///
/// Hand-derived: same 1.0-self-similarity argument as above; the expected number
/// of vectors that fail to retrieve themselves is 0.
#[test]
fn spa443_every_vector_retrieves_itself_at_default_ef() {
    const N: usize = 700;
    // aniso = 1.0: a realistic embedding cone, without the pathological
    // outliers that the 2.0 corpus generates to stress connectivity.
    let (idx, vectors) = build(N, 48, 9, 1.0, 0x5EED);

    let mut missing: Vec<usize> = Vec::new();
    for (i, v) in vectors.iter().enumerate() {
        let hits = idx.search(v, 10, 50);
        if !hits
            .iter()
            .any(|&(id, score)| id == i as u64 && score > 0.999)
        {
            missing.push(i);
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {N} vectors are not returned by a search for their own embedding at the \
         default ef: {:?}",
        missing.len(),
        &missing[..missing.len().min(20)]
    );
}

/// A whole class of defect in this codebase only appears after the index has
/// been through a save/load cycle, so the invariant is re-checked on the far
/// side of one — and inserts that arrive *after* the reload must preserve it
/// too, since that is exactly the state a long-running daemon is in.
///
/// Hand-derived expectations:
/// - 500 vectors are inserted, saved and reloaded. `save`/`load` round-trip the
///   node array and `id_to_slot` verbatim, so the reloaded index has
///   `len() == 500` and `reachable_count() == 500`.
/// - 200 further vectors are then inserted into the reloaded handle, using ids
///   `500..700` which the reloaded index has never seen, so each is `Inserted`
///   and `len() == 700`.
/// - The reachability invariant is not a property of a freshly built graph, it
///   is a property of the graph at all times, so `reachable_count() == 700`.
#[test]
fn spa443_reachability_survives_save_and_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let vectors = cone_corpus(700, 48, 9, 2.0, 0xC0FFEE);

    let mut idx = VectorIndex::new(48, Metric::Cosine);
    for (i, v) in vectors.iter().enumerate().take(500) {
        idx.insert(i as u64, v);
    }
    idx.save(dir.path(), "Memory", "embedding")
        .expect("save must succeed");

    let mut reloaded = VectorIndex::load(dir.path(), "Memory", "embedding")
        .expect("load must succeed")
        .expect("the index file was just written, so it exists");
    assert_eq!(reloaded.len(), 500, "500 vectors were saved");
    assert_eq!(
        reloaded.reachable_count(),
        500,
        "reload must not strand anything"
    );

    for (i, v) in vectors.iter().enumerate().skip(500) {
        assert_eq!(
            reloaded.insert(i as u64, v),
            InsertOutcome::Inserted,
            "id {i} is new to the reloaded index"
        );
    }
    assert_eq!(reloaded.len(), 700);
    let stranded = reloaded.unreachable_ids();
    assert!(
        stranded.is_empty(),
        "{} vectors inserted after a reload are unreachable: {:?}",
        stranded.len(),
        &stranded[..stranded.len().min(20)]
    );

    // And the invariant must still hold once *that* state is persisted too.
    reloaded
        .save(dir.path(), "Memory", "embedding")
        .expect("second save must succeed");
    let twice = VectorIndex::load(dir.path(), "Memory", "embedding")
        .expect("load must succeed")
        .expect("file exists");
    assert_eq!(twice.len(), 700);
    assert_eq!(twice.reachable_count(), 700);
}

/// `has_vector` answers the question `search` cannot.
///
/// "Not returned by `vectorSearch`" and "absent from the index" are different
/// facts, and with only `vectorSearch` to probe with there was no way to tell
/// them apart — which is how an investigation twice concluded that writes were
/// failing when the vectors were stored and merely unreachable.
///
/// Hand-derived: ids `0..700` are inserted and nothing else is, so `has_vector`
/// is true for exactly those 700 ids and false for every id at or above 700.
#[test]
fn spa443_has_vector_reports_storage_independently_of_search() {
    const N: usize = 700;
    let (idx, _) = build(N, 48, 9, 2.0, 0x5EED);

    for i in 0..N as u64 {
        assert!(idx.has_vector(i), "id {i} was inserted");
    }
    for i in N as u64..N as u64 + 25 {
        assert!(!idx.has_vector(i), "id {i} was never inserted");
    }
    assert!(!idx.has_vector(u64::MAX));

    // The two counts a monitoring caller needs, and their relationship.
    assert_eq!(idx.len(), N);
    assert_eq!(idx.reachable_count(), N);
    assert!(
        idx.reachable_count() <= idx.len(),
        "reachable can never exceed stored"
    );
}

/// `repair()` must be safe to call unconditionally: a no-op on a healthy index,
/// reporting 0 and rewiring nothing.
///
/// Hand-derived: the index built above satisfies the invariant, so the first
/// traversal finds no unreachable slots, and the function returns before
/// touching an edge. Search results must therefore be bit-identical either side
/// of the call.
#[test]
fn spa443_repair_is_safe_to_call_on_a_healthy_index() {
    const N: usize = 400;
    let (mut idx, vectors) = build(N, 48, 8, 2.0, 0xD00D);

    let before: Vec<Vec<(u64, f32)>> = vectors.iter().map(|v| idx.search(v, 5, 50)).collect();
    assert_eq!(idx.repair(), 0, "a healthy index needs no repair");
    let after: Vec<Vec<(u64, f32)>> = vectors.iter().map(|v| idx.search(v, 5, 50)).collect();

    assert_eq!(
        before, after,
        "repair must not change what a healthy index returns"
    );
    assert_eq!(idx.reachable_count(), N);
}

/// Replacing a vector for an existing id must leave the index fully reachable.
///
/// The update path rewires the node's outgoing links wholesale, which is another
/// opportunity to strand whatever those links were the last route to.
///
/// Hand-derived expectations:
/// - 400 ids are inserted, then ids `0..100` are re-inserted with fresh vectors.
///   Each of those 100 must report `InsertOutcome::Updated`, because the id is
///   already mapped, and `len()` must stay at 400 — an update takes no new slot.
/// - Reachability is an always-invariant, so `reachable_count() == 400` after.
#[test]
fn spa443_updates_do_not_strand_vectors() {
    const N: usize = 400;
    let (mut idx, _) = build(N, 48, 8, 2.0, 0xABBA);

    let replacements = cone_corpus(100, 48, 4, 2.0, 0x1234);
    for (i, v) in replacements.iter().enumerate() {
        assert_eq!(
            idx.insert(i as u64, v),
            InsertOutcome::Updated,
            "id {i} already has a vector, so this is a replacement"
        );
    }
    assert_eq!(idx.len(), N, "an update must not allocate a new slot");

    let stranded = idx.unreachable_ids();
    assert!(
        stranded.is_empty(),
        "{} vectors were stranded by in-place updates: {:?}",
        stranded.len(),
        &stranded[..stranded.len().min(20)]
    );
}
