//! Auto-generated submodule — see engine/mod.rs for context.
use super::*;

/// Signature shared by every scalar function that can only be evaluated with
/// engine (graph) context — `eval_full_text_search`, `eval_bm25_score`, and
/// `eval_hybrid_search` all have this exact shape.
type GraphOnlyFn = fn(&Engine, &[Expr], &HashMap<String, Value>) -> Value;

/// The single source of truth for which scalar functions require
/// `Engine::eval_expr_graph` rather than the free, non-engine `eval_expr`,
/// and how to dispatch each one.
///
/// # #459: one table instead of two name lists
///
/// Before this table existed, `eval_expr_graph`'s `FnCall` arm and
/// `expr_needs_graph` (`engine/mod.rs`) each kept their own copy of the same
/// three names, with nothing keeping them in sync. When they agreed the
/// system worked; the day someone added a fourth graph-only function to one
/// list and not the other, `expr_needs_graph` would keep saying "no" for it,
/// `aggregate_rows_graph` would keep delegating to the non-engine
/// `aggregate_rows` → `eval_expr`, and the new function would silently
/// return `Value::Null` against a perfectly healthy index — this issue,
/// verbatim, for the next function. This project has hit the same shape
/// twice already for a match/dispatch pair that can drift (`is_mutation` vs
/// its dispatch match: #478 missing-from-classifier, #502
/// missing-from-dispatch).
///
/// A table is used instead of a plain name list so the dispatch itself reads
/// from the same data `is_graph_only_fn` checks against — adding a function
/// here is the only step; both the "does this need graph context" check and
/// the actual call site update together. See `eval_expr_graph`'s `FnCall`
/// arm below and `regression_459_hybrid_search_return.rs` /
/// `graph_only_fn_table_covers_every_dispatched_name` (this file's tests).
///
/// To add a new graph-only function: add its `eval_*` method to this `impl
/// Engine` block, then add one row here. Nothing else needs to change.
const GRAPH_ONLY_FNS: &[(&str, GraphOnlyFn)] = &[
    ("full_text_search", Engine::eval_full_text_search),
    ("bm25_score", Engine::eval_bm25_score),
    ("hybrid_search", Engine::eval_hybrid_search),
];

/// Returns `true` when `name` (case-insensitive) names one of
/// [`GRAPH_ONLY_FNS`] — a scalar function that can only be evaluated via
/// `Engine::eval_expr_graph`. Used by `expr_needs_graph` (`engine/mod.rs`)
/// to route `aggregate_rows_graph` correctly; see the module doc on
/// `GRAPH_ONLY_FNS` for why this must not be a second, independent list.
pub(crate) fn is_graph_only_fn(name: &str) -> bool {
    let name_lc = name.to_ascii_lowercase();
    GRAPH_ONLY_FNS.iter().any(|(n, _)| *n == name_lc)
}

/// Key of the process-global `hybrid_search` index cache: which database
/// directory, which `(label, prop)` pair.
type HybridCacheKey = (std::path::PathBuf, String, String);

/// A cached verdict about one on-disk HNSW index, together with the fingerprint
/// it was derived from.
type HybridCacheEntry = ((u64, u64), CachedIndex);

/// What the last look at an index file concluded — valid for exactly as long as
/// that file's fingerprint is unchanged.
enum CachedIndex {
    /// The file loaded; this is the shared deserialised copy.
    Ready(std::sync::Arc<sparrowdb_storage::vector_index::VectorIndex>),
    /// The file is present and cannot be served, for this reason.
    ///
    /// # Why the negative is cached too (#456)
    ///
    /// `hybrid_search` is a scalar function: it is evaluated once per candidate
    /// row.  Caching only the success meant a damaged index was re-read and
    /// re-decoded on *every* row — for KMSmcp's 8 MB `Knowledge.embedding`
    /// index that is N x 8 MB of I/O for an N-row scan, and a file that decodes
    /// cleanly but then fails `validate_invariants` paid a full `bincode`
    /// decode each time.  It also emitted one identical warning per row.
    ///
    /// Both arrive at the moment the store is already degraded and someone is
    /// reading the logs to find out why, which is the worst possible time to
    /// add an I/O storm and bury the signal.  A fix whose purpose is "behave
    /// well when the index is damaged" must not amplify the incident.
    Damaged(String),
}

/// A damaged-index verdict handed back to the caller.
struct HybridIndexDamage {
    /// The loader's own error text.
    reason: String,
    /// True only on the evaluation that first observed this damage for the
    /// current fingerprint.  Every later row reuses the cached verdict and sets
    /// this to `false`, so one damaged index produces one log line rather than
    /// one per row.
    first_observation: bool,
}

/// Insert into the `hybrid_search` index cache, keeping it bounded.
///
/// The cache is process-global and keyed by `(dir, label, prop)`, so without a
/// bound a long-running process — an MCP server that opens many databases over
/// its lifetime is the realistic case — would retain one entry for every pair it
/// had ever queried, and each `Ready` entry pins a fully deserialised index that
/// can be megabytes.  Eight is chosen to comfortably cover the pairs one query
/// touches while keeping the retained set small; the eviction is a wholesale
/// clear rather than an LRU because there is no access-recency information worth
/// tracking here and a cleared cache costs one reload, not a wrong answer.
fn hybrid_cache_insert(
    map: &mut HashMap<HybridCacheKey, HybridCacheEntry>,
    key: HybridCacheKey,
    entry: HybridCacheEntry,
) {
    const MAX_CACHED_INDEXES: usize = 8;
    if map.len() >= MAX_CACHED_INDEXES && !map.contains_key(&key) {
        map.clear();
    }
    map.insert(key, entry);
}

impl Engine {
    // ── FTS scalar functions ──────────────────────────────────────────────────

    /// Evaluate `full_text_search(...)`, in either of two forms:
    ///
    ///  - `full_text_search(label, property, query)` — all-literal form. The
    ///    current node is resolved by scanning `vals` for any `__node_id__`
    ///    entry whose label matches the given `label` literal.
    ///  - `full_text_search(var.property, query)` — the natural form used from
    ///    a `WHERE` clause once a variable is already bound by `MATCH`, mirroring
    ///    `bm25_score(var.property, query)`. The label and node are inferred
    ///    directly from `var`'s bound `NodeRef`, exactly as `eval_bm25_score` does.
    ///
    /// Returns `Value::Bool(true)` if the current node's ID appears in the BM25
    /// results for the resolved `(label, property, query)`.
    ///
    /// Returns `Value::Bool(false)` when the node/label cannot be resolved or no
    /// FTS index is configured for the pair, and `Value::Null` when a configured
    /// index exists on disk but could not be opened (issue #462: corruption must
    /// not read the same as "no match").
    fn eval_full_text_search(&self, args: &[Expr], vals: &HashMap<String, Value>) -> Value {
        let (label, property, query, node_id) = match args.len() {
            3 => {
                let label_val = eval_expr(&args[0], vals);
                let prop_val = eval_expr(&args[1], vals);
                let query_val = eval_expr(&args[2], vals);

                let (Value::String(label), Value::String(property), Value::String(query)) =
                    (label_val, prop_val, query_val)
                else {
                    return Value::Bool(false);
                };

                // Locate the current node's ID for the given label.
                //
                // During WHERE evaluation (`execute_scan`) the NodeRef is stored
                // under the plain variable name (e.g. "n"). During aggregation /
                // eval path it is also stored under "{var}.__node_id__". We
                // accept both.
                let expected_lid: Option<u32> = self
                    .snapshot
                    .catalog
                    .get_label(&label)
                    .ok()
                    .flatten()
                    .map(|id| id as u32);

                let node_id: u64 = {
                    let mut found = None;

                    // Pass 1: prefer explicit __node_id__ keys.
                    for (k, v) in vals.iter() {
                        if k.ends_with(".__node_id__") {
                            if let Value::NodeRef(nid) = v {
                                let label_id_from_node = (nid.0 >> 32) as u32;
                                if expected_lid.is_none_or(|eid| label_id_from_node == eid) {
                                    found = Some(nid.0);
                                    break;
                                }
                            }
                        }
                    }

                    // Pass 2: fall back to any plain NodeRef entry matching the label.
                    if found.is_none() {
                        for v in vals.values() {
                            if let Value::NodeRef(nid) = v {
                                let label_id_from_node = (nid.0 >> 32) as u32;
                                if expected_lid.is_none_or(|eid| label_id_from_node == eid) {
                                    found = Some(nid.0);
                                    break;
                                }
                            }
                        }
                    }

                    match found {
                        Some(id) => id,
                        None => return Value::Bool(false),
                    }
                };

                (label, property, query, node_id)
            }
            2 => {
                // `full_text_search(var.property, query)` — the label and node
                // are known precisely from `var`'s binding, so unlike the 3-arg
                // form there is no need to scan `vals` for a matching NodeRef.
                let query_val = eval_expr(&args[1], vals);
                let Value::String(query) = query_val else {
                    return Value::Bool(false);
                };

                let (var_name, prop_name) = match &args[0] {
                    Expr::PropAccess { var, prop } => (var.clone(), prop.clone()),
                    _ => return Value::Bool(false),
                };

                let node_id_key = format!("{var_name}.__node_id__");
                let node_id: u64 = match vals.get(&node_id_key) {
                    Some(Value::NodeRef(nid)) => nid.0,
                    _ => match vals.get(var_name.as_str()) {
                        Some(Value::NodeRef(nid)) => nid.0,
                        _ => return Value::Bool(false),
                    },
                };

                // Infer the label from the node_id (high 32 bits = label_id),
                // exactly as `eval_bm25_score` does for the same call shape.
                let label_id = (node_id >> 32) as u32;
                let label = match self.snapshot.catalog.list_labels() {
                    Ok(labels) => match labels.into_iter().find(|(id, _)| *id as u32 == label_id) {
                        Some((_, name)) => name,
                        None => return Value::Bool(false),
                    },
                    Err(_) => return Value::Bool(false),
                };

                (label, prop_name, query, node_id)
            }
            _ => return Value::Bool(false),
        };

        // Use the per-query FTS cache so the index is loaded from disk at most
        // once per (label, property) pair regardless of how many rows are scanned.
        match self.snapshot.fts_index(&label, &property) {
            Some(cache) => {
                let key = (label, property);
                let idx = cache.get(&key).expect("key was just inserted");
                // Use matches_query for a fast O(|terms|*avg_postings) membership
                // check instead of computing and sorting all BM25 scores.
                Value::Bool(idx.matches_query(node_id, &query))
            }
            // `fts_index()` only returns `None` when a registered index exists
            // on disk but could not be opened (an absent/unconfigured index is
            // `Some(empty index)`, and correctly yields `false` above) — so this
            // is corruption, not "no match". `Null` reports that distinctly
            // rather than reusing `false`, which a caller cannot tell apart
            // from a genuine non-match (issue #462).
            None => Value::Null,
        }
    }

    /// Evaluate `bm25_score(prop_expr, query)`.
    ///
    /// When `prop_expr` is a `PropAccess { var, prop }`, the BM25 score is
    /// looked up from the persisted FTS index for that `(inferred_label, prop)`
    /// pair.  Otherwise (bare string), returns 0.0.
    fn eval_bm25_score(&self, args: &[Expr], vals: &HashMap<String, Value>) -> Value {
        if args.len() != 2 {
            return Value::Float64(0.0);
        }

        let query_val = eval_expr(&args[1], vals);
        let Value::String(query) = query_val else {
            return Value::Float64(0.0);
        };

        // Extract (var, prop) from the first argument.
        let (var_name, prop_name) = match &args[0] {
            Expr::PropAccess { var, prop } => (var.clone(), prop.clone()),
            _ => return Value::Float64(0.0),
        };

        // Resolve the node_id and label for this variable.
        let node_id_key = format!("{var_name}.__node_id__");
        let node_id: u64 = match vals.get(&node_id_key) {
            Some(Value::NodeRef(nid)) => nid.0,
            _ => {
                // Fallback: look for a var entry that is a NodeRef.
                match vals.get(var_name.as_str()) {
                    Some(Value::NodeRef(nid)) => nid.0,
                    _ => return Value::Float64(0.0),
                }
            }
        };

        // Infer the label from the node_id (high 32 bits = label_id).
        let label_id = (node_id >> 32) as u32;
        let label = match self.snapshot.catalog.list_labels() {
            Ok(labels) => match labels.into_iter().find(|(id, _)| *id as u32 == label_id) {
                Some((_, name)) => name,
                None => return Value::Float64(0.0),
            },
            Err(_) => return Value::Float64(0.0),
        };

        // Use the per-query FTS cache so the index is loaded from disk at most
        // once per (label, property) pair across all rows in the result set.
        match self.snapshot.fts_index(&label, &prop_name) {
            Some(cache) => {
                let key = (label, prop_name);
                let idx = cache.get(&key).expect("key was just inserted");
                let score = idx.score(node_id, &query);
                Value::Float64(score as f64)
            }
            // See the matching comment in `eval_full_text_search`: `None` here
            // means the index is present but broken, never "unconfigured", so
            // it must not read the same as a genuine score of 0.0 (issue #462).
            None => Value::Null,
        }
    }

    // ── Hybrid search (issue #396) ────────────────────────────────────────────

    /// Return the HNSW index for `(label, prop)` under `vec_dir`, reusing an
    /// already-deserialised copy whenever the file on disk has not changed.
    ///
    /// `hybrid_search` is a scalar function: it is evaluated once per candidate
    /// row.  The previous implementation called `VectorIndex::load()` on every
    /// evaluation, so a query over N rows performed N full reads and `bincode`
    /// decodes of the index file — for KMSmcp's 8 MB `Knowledge.embedding`
    /// index that is 8 MB of I/O and a complete graph rebuild *per row*.
    ///
    /// Staleness is handled by validating the cheap on-disk fingerprint
    /// (generation + CRC32C, read from the file's 36-byte header — no payload
    /// I/O) before each reuse.  Because every writer persists the index in the
    /// same write lock that mutates it, a matching fingerprint means the cached
    /// copy is byte-identical to what the current writer would hand us.
    ///
    /// # Absent is not damaged (#456)
    ///
    /// `Ok(None)` means no index is configured for the pair — a legitimate
    /// state, and the caller degrades to full-text-only results exactly as
    /// before.  `Err(..)` means a file *is* there and cannot be served.
    ///
    /// Collapsing those two into `None` is what made this a data-loss path: the
    /// loader used to be `VectorIndex::load(..).ok().flatten()?`, and since #442
    /// had moved the quarantine rename inside `load`, a plain read query
    /// renamed the last live copy of the index aside, then reported success
    /// with zero vector hits.  This function now calls the non-destructive
    /// `load` and keeps the two outcomes apart.
    ///
    /// # Both verdicts are cached, under the same token
    ///
    /// A damaged file is remembered as [`CachedIndex::Damaged`] against the same
    /// fingerprint the successful load is cached against, so an N-row scan reads
    /// and decodes it once rather than N times, and warns once rather than N
    /// times.
    ///
    /// The invalidation is therefore identical for both: rebuild the index and
    /// its fingerprint changes, the cached entry stops matching, and the next
    /// row re-probes and sees the repair.  A negative verdict that outlived its
    /// repair would recreate, one layer down, exactly the "fixed but still
    /// reports broken" failure this PR removes from the diagnostic.
    ///
    /// A verdict is only cached when a fingerprint exists, because the
    /// fingerprint *is* the invalidation token and there is nothing safe to do
    /// without one.  That costs nothing in practice: `fingerprint` fails only
    /// when the file cannot be opened at all (a dangling symlink, a permission
    /// block), and those re-probe for one failed `open` plus one `lstat` with no
    /// payload read — never the full decode this cache exists to avoid.
    fn hybrid_vector_index(
        vec_dir: &std::path::Path,
        label: &str,
        prop: &str,
    ) -> std::result::Result<
        Option<std::sync::Arc<sparrowdb_storage::vector_index::VectorIndex>>,
        HybridIndexDamage,
    > {
        static CACHE: std::sync::OnceLock<
            std::sync::Mutex<HashMap<HybridCacheKey, HybridCacheEntry>>,
        > = std::sync::OnceLock::new();

        // The fingerprint is a cache-validity token, not an existence test.
        // `fingerprint` opens the path, so a dangling symlink reports NotFound
        // and a damaged header reports an error — neither of which means "no
        // index here".  When it yields nothing we fall through to `load`, which
        // is the one place that distinguishes absent from unreadable.  That
        // costs two extra syscalls per row in the no-index-configured case,
        // against the full `FtsIndex::open` the same row already performs.
        let fingerprint =
            sparrowdb_storage::vector_index::VectorIndex::fingerprint(vec_dir, label, prop)
                .unwrap_or_default();

        let key: HybridCacheKey = (vec_dir.to_path_buf(), label.to_owned(), prop.to_owned());
        let cache = CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
        let mut guard = match cache.lock() {
            Ok(g) => g,
            // A poisoned cache mutex is not evidence about the index file.
            // Skip the cache rather than claim the index is missing; with no
            // cache to consult, every evaluation is a first observation.
            Err(_) => {
                return match sparrowdb_storage::vector_index::VectorIndex::load(
                    vec_dir, label, prop,
                ) {
                    Ok(idx) => Ok(idx.map(std::sync::Arc::new)),
                    Err(e) => Err(HybridIndexDamage {
                        reason: e.to_string(),
                        first_observation: true,
                    }),
                }
            }
        };

        if let Some(fp) = fingerprint {
            if let Some((cached_fp, verdict)) = guard.get(&key) {
                if *cached_fp == fp {
                    return match verdict {
                        CachedIndex::Ready(idx) => Ok(Some(std::sync::Arc::clone(idx))),
                        CachedIndex::Damaged(reason) => Err(HybridIndexDamage {
                            reason: reason.clone(),
                            first_observation: false,
                        }),
                    };
                }
            }
        }

        let loaded = match sparrowdb_storage::vector_index::VectorIndex::load(vec_dir, label, prop)
        {
            Ok(Some(idx)) => idx,
            // The pair no longer has a usable index.  Drop any cached copy
            // rather than keeping an 8 MB deserialised graph — or a stale
            // damage verdict — alive for a key that is now simply absent.
            Ok(None) => {
                guard.remove(&key);
                return Ok(None);
            }
            Err(e) => {
                let reason = e.to_string();
                match fingerprint {
                    Some(fp) => hybrid_cache_insert(
                        &mut guard,
                        key,
                        (fp, CachedIndex::Damaged(reason.clone())),
                    ),
                    // No token to invalidate against, so nothing may be
                    // remembered.  See the note on this function.
                    None => {
                        guard.remove(&key);
                    }
                }
                return Err(HybridIndexDamage {
                    reason,
                    first_observation: true,
                });
            }
        };
        let idx = std::sync::Arc::new(loaded);

        if let Some(fp) = fingerprint {
            hybrid_cache_insert(
                &mut guard,
                key,
                (fp, CachedIndex::Ready(std::sync::Arc::clone(&idx))),
            );
        }
        Ok(Some(idx))
    }

    /// Evaluate `hybrid_search(label, emb_prop, text_prop, query_vec, query_text, k[, alpha])`.
    ///
    /// Runs vector search (HNSW) + BM25 full-text search independently, then
    /// fuses the result lists using Reciprocal Rank Fusion (default) or a
    /// weighted combination when `alpha` is supplied.
    ///
    /// Arguments:
    /// - `label`      — node label (String)
    /// - `emb_prop`   — property name holding the embedding (String)
    /// - `text_prop`  — property name holding the text (String)
    /// - `query_vec`  — query embedding (Value::Vector or List<Float>)
    /// - `query_text` — query string (String)
    /// - `k`          — number of results to return (Int64)
    /// - `alpha`      — optional; if provided, uses weighted_fusion with this weight;
    ///   if omitted, uses RRF (k=60)
    ///
    /// Returns `Value::List<Value::Map({node_id, score, rank}>>` sorted by
    /// descending fused score, or `Value::Null` on any error.
    pub(crate) fn eval_hybrid_search(&self, args: &[Expr], vals: &HashMap<String, Value>) -> Value {
        if args.len() < 6 || args.len() > 7 {
            return Value::Null;
        }

        // Evaluate all arguments.
        let label_val = eval_expr(&args[0], vals);
        let emb_prop_val = eval_expr(&args[1], vals);
        let text_prop_val = eval_expr(&args[2], vals);
        let query_vec_val = eval_expr(&args[3], vals);
        let query_text_val = eval_expr(&args[4], vals);
        let k_val = eval_expr(&args[5], vals);

        let (Value::String(label), Value::String(emb_prop), Value::String(text_prop)) =
            (label_val, emb_prop_val, text_prop_val)
        else {
            return Value::Null;
        };

        let query_vec = match query_vec_val.as_vector() {
            Some(v) => v,
            None => return Value::Null,
        };

        let Value::String(query_text) = query_text_val else {
            return Value::Null;
        };

        let k: usize = match k_val {
            Value::Int64(n) if n > 0 => n as usize,
            Value::Float64(f) if f > 0.0 => f as usize,
            _ => return Value::Null,
        };

        // Optional alpha for weighted fusion.
        let alpha: Option<f64> = if args.len() == 7 {
            let av = eval_expr(&args[6], vals);
            match av {
                Value::Float64(f) => Some(f),
                Value::Int64(n) => Some(n as f64),
                _ => return Value::Null,
            }
        } else {
            None
        };

        // ── 1. Vector search ────────────────────────────────────────────────
        //
        // A damaged index does NOT degrade to full-text-only here (#456).
        //
        // The three candidate behaviours were: return an error, quietly fall
        // back to FTS, or refuse to answer.  Returning an error is not
        // reachable from a scalar function — `eval_expr_graph` and every
        // function it dispatches are infallible by signature, and making them
        // fallible is an engine-wide change well outside this fix.  That leaves
        // fall-back or refusal, and fall-back is the wrong one: it produces the
        // *same* observable as a legitimately absent index, which is precisely
        // the absent-vs-damaged confusion #445 exists to eliminate.  A caller
        // who asked for a fusion of vector and text results, and silently got
        // text only, has been told the store is healthy by a store that is not.
        //
        // So: absent → FTS-only, unchanged.  Damaged → `Value::Null`, which is
        // already this function's failure signal (bad arity, bad argument
        // types, fusion failure), plus a warning naming the file and the
        // decode reason.  Null is a shape the caller cannot mistake for an
        // empty result list.
        let vec_dir = self.snapshot.db_root.join("vector_indexes");
        let vec_results: Vec<(u64, f32)> = match Self::hybrid_vector_index(
            &vec_dir, &label, &emb_prop,
        ) {
            // ef = max(k*2, 50) gives a reasonable exploration budget.
            Ok(Some(idx)) => idx.search(&query_vec, k * 2, (k * 2).max(50)),
            // No index configured for this pair: full-text-only is the
            // honest answer and always has been.
            Ok(None) => vec![],
            Err(damage) => {
                // Once per damaged index, not once per row.  An N-row scan
                // against a damaged index used to emit N identical lines,
                // burying the signal at the exact moment someone is reading
                // the logs to find out what broke.
                if damage.first_observation {
                    tracing::warn!(
                        label = %label,
                        property = %emb_prop,
                        reason = %damage.reason,
                        "hybrid_search: the vector index for this pair is present but unusable; \
                         returning NULL rather than silently degrading to full-text-only results"
                    );
                }
                return Value::Null;
            }
        };

        // ── 2. Full-text (BM25) search ───────────────────────────────────────
        //
        // Same absent-vs-damaged distinction as the vector arm above:
        // `fts_index()` only returns `None` when a registered index is present
        // but broken — an absent/unconfigured index comes back as
        // `Some(empty index)` and correctly yields no results — so silently
        // treating `None` as "no matches" would reproduce the exact
        // vector-only-degrade bug #456 fixed on the vector arm, just for FTS
        // (issue #462). Routed through the shared per-query cache so the
        // open and its corruption warning happen at most once per pair
        // regardless of how many rows call this function.
        let fts_results: Vec<(u64, f32)> = match self.snapshot.fts_index(&label, &text_prop) {
            Some(cache) => {
                let key = (label.clone(), text_prop.clone());
                let idx = cache.get(&key).expect("key was just inserted");
                idx.search(&query_text, k * 2)
            }
            None => return Value::Null,
        };

        // ── 3. Build Value::List inputs for fusion functions ─────────────────
        let make_list = |results: Vec<(u64, f32)>| -> Value {
            Value::List(
                results
                    .into_iter()
                    .map(|(nid, score)| {
                        Value::Map(vec![
                            ("node_id".to_owned(), Value::Int64(nid as i64)),
                            ("score".to_owned(), Value::Float64(score as f64)),
                        ])
                    })
                    .collect(),
            )
        };

        let list1 = make_list(vec_results);
        let list2 = make_list(fts_results);

        // ── 4. Fuse ──────────────────────────────────────────────────────────
        let fused = if let Some(a) = alpha {
            crate::functions::dispatch_function(
                "weighted_fusion",
                vec![list1, list2, Value::Float64(a)],
            )
        } else {
            crate::functions::dispatch_function("rrf_fusion", vec![list1, list2])
        };

        match fused {
            Ok(Value::List(mut items)) => {
                items.truncate(k);
                Value::List(items)
            }
            _ => Value::Null,
        }
    }

    // ── Property filter helpers ───────────────────────────────────────────────

    pub(crate) fn matches_prop_filter(
        &self,
        props: &[(u32, u64)],
        filters: &[sparrowdb_cypher::ast::PropEntry],
    ) -> bool {
        matches_prop_filter_static(props, filters, &self.dollar_params(), &self.snapshot.store)
    }

    /// Build a map of runtime parameters keyed with a `$` prefix,
    /// suitable for passing to `eval_expr` / `eval_where`.
    ///
    /// For example, `params["name"] = Value::String("Alice")` becomes
    /// `{"$name": Value::String("Alice")}` in the returned map.
    pub(crate) fn dollar_params(&self) -> HashMap<String, Value> {
        self.params
            .iter()
            .map(|(k, v)| (format!("${k}"), v.clone()))
            .collect()
    }

    // ── Graph-aware expression evaluation (SPA-136, SPA-137, SPA-138) ────────

    /// Evaluate an expression that may require graph access (EXISTS, ShortestPath).
    pub(crate) fn eval_expr_graph(&self, expr: &Expr, vals: &HashMap<String, Value>) -> Value {
        match expr {
            Expr::ExistsSubquery(ep) => Value::Bool(self.eval_exists_subquery(ep, vals)),
            Expr::ShortestPath(sp) => self.eval_shortest_path_expr(sp, vals),
            Expr::CaseWhen {
                branches,
                else_expr,
            } => {
                for (cond, then_val) in branches {
                    if let Value::Bool(true) = self.eval_expr_graph(cond, vals) {
                        return self.eval_expr_graph(then_val, vals);
                    }
                }
                else_expr
                    .as_ref()
                    .map(|e| self.eval_expr_graph(e, vals))
                    .unwrap_or(Value::Null)
            }
            Expr::And(l, r) => {
                match (self.eval_expr_graph(l, vals), self.eval_expr_graph(r, vals)) {
                    (Value::Bool(a), Value::Bool(b)) => Value::Bool(a && b),
                    _ => Value::Null,
                }
            }
            Expr::Or(l, r) => {
                match (self.eval_expr_graph(l, vals), self.eval_expr_graph(r, vals)) {
                    (Value::Bool(a), Value::Bool(b)) => Value::Bool(a || b),
                    _ => Value::Null,
                }
            }
            Expr::Not(inner) => match self.eval_expr_graph(inner, vals) {
                Value::Bool(b) => Value::Bool(!b),
                _ => Value::Null,
            },
            // SPA-134: PropAccess where the variable resolves to a NodeRef (e.g. `WITH n AS person
            // RETURN person.name`).  Fetch the property from the node store directly.
            Expr::PropAccess { var, prop } => {
                // Try normal key first (col_N or direct "var.prop" entry).
                let normal = eval_expr(expr, vals);
                if !matches!(normal, Value::Null) {
                    return normal;
                }
                // Fallback: if the variable is a NodeRef, read the property from the store.
                if let Some(Value::NodeRef(node_id)) = vals
                    .get(var.as_str())
                    .or_else(|| vals.get(&format!("{var}.__node_id__")))
                {
                    let col_id = prop_name_to_col_id(prop);
                    if let Ok(props) = self.snapshot.store.get_node_raw(*node_id, &[col_id]) {
                        if let Some(&(_, raw)) = props.iter().find(|(c, _)| *c == col_id) {
                            return decode_raw_val(raw, &self.snapshot.store);
                        }
                    }
                }
                Value::Null
            }
            // Dispatch through the shared GRAPH_ONLY_FNS table (#459) rather
            // than a hand-written match, so this call site and
            // `is_graph_only_fn` can never disagree about which names route
            // here.
            Expr::FnCall { name, args } => {
                let name_lc = name.to_ascii_lowercase();
                match GRAPH_ONLY_FNS.iter().find(|(n, _)| *n == name_lc) {
                    Some((_, f)) => f(self, args, vals),
                    None => eval_expr(expr, vals),
                }
            }
            // #477: a bare `full_text_search(...)`/`bm25_score(...)` call is
            // routed above, but a threshold comparison like
            // `bm25_score(n.text, 'q') > 0.5` wraps the call in a `BinOp` —
            // and without an arm here, the whole expression fell through to
            // `_ => eval_expr(expr, vals)` below, which recurses on `left`/
            // `right` with the *generic* `eval_expr` rather than
            // `eval_expr_graph`. That generic recursion evaluates the nested
            // `FnCall` via `dispatch_function`, which does not know these
            // three functions, so the comparison always saw `Value::Null` on
            // the FTS side and the row was rejected regardless of score.
            // Mirrors `eval_expr`'s `BinOp` arm in `engine/mod.rs` exactly,
            // just recursing through `eval_expr_graph` so a nested
            // `full_text_search`/`bm25_score`/`hybrid_search` call resolves.
            Expr::BinOp { left, op, right } => {
                let lv = self.eval_expr_graph(left, vals);
                let rv = self.eval_expr_graph(right, vals);
                match op {
                    BinOpKind::Eq => Value::Bool(values_equal(&lv, &rv)),
                    BinOpKind::Neq => Value::Bool(!values_equal(&lv, &rv)),
                    BinOpKind::Lt => match (&lv, &rv) {
                        (Value::Int64(a), Value::Int64(b)) => Value::Bool(a < b),
                        (Value::Float64(a), Value::Float64(b)) => Value::Bool(a < b),
                        (Value::Int64(a), Value::Float64(b)) => {
                            cmp_i64_f64(*a, *b).map_or(Value::Null, |o| Value::Bool(o.is_lt()))
                        }
                        (Value::Float64(a), Value::Int64(b)) => {
                            cmp_i64_f64(*b, *a).map_or(Value::Null, |o| Value::Bool(o.is_gt()))
                        }
                        _ => Value::Null,
                    },
                    BinOpKind::Le => match (&lv, &rv) {
                        (Value::Int64(a), Value::Int64(b)) => Value::Bool(a <= b),
                        (Value::Float64(a), Value::Float64(b)) => Value::Bool(a <= b),
                        (Value::Int64(a), Value::Float64(b)) => {
                            cmp_i64_f64(*a, *b).map_or(Value::Null, |o| Value::Bool(o.is_le()))
                        }
                        (Value::Float64(a), Value::Int64(b)) => {
                            cmp_i64_f64(*b, *a).map_or(Value::Null, |o| Value::Bool(o.is_ge()))
                        }
                        _ => Value::Null,
                    },
                    BinOpKind::Gt => match (&lv, &rv) {
                        (Value::Int64(a), Value::Int64(b)) => Value::Bool(a > b),
                        (Value::Float64(a), Value::Float64(b)) => Value::Bool(a > b),
                        (Value::Int64(a), Value::Float64(b)) => {
                            cmp_i64_f64(*a, *b).map_or(Value::Null, |o| Value::Bool(o.is_gt()))
                        }
                        (Value::Float64(a), Value::Int64(b)) => {
                            cmp_i64_f64(*b, *a).map_or(Value::Null, |o| Value::Bool(o.is_lt()))
                        }
                        _ => Value::Null,
                    },
                    BinOpKind::Ge => match (&lv, &rv) {
                        (Value::Int64(a), Value::Int64(b)) => Value::Bool(a >= b),
                        (Value::Float64(a), Value::Float64(b)) => Value::Bool(a >= b),
                        (Value::Int64(a), Value::Float64(b)) => {
                            cmp_i64_f64(*a, *b).map_or(Value::Null, |o| Value::Bool(o.is_ge()))
                        }
                        (Value::Float64(a), Value::Int64(b)) => {
                            cmp_i64_f64(*b, *a).map_or(Value::Null, |o| Value::Bool(o.is_le()))
                        }
                        _ => Value::Null,
                    },
                    BinOpKind::Contains => match (&lv, &rv) {
                        (Value::String(l), Value::String(r)) => Value::Bool(l.contains(r.as_str())),
                        _ => Value::Null,
                    },
                    BinOpKind::StartsWith => match (&lv, &rv) {
                        (Value::String(l), Value::String(r)) => {
                            Value::Bool(l.starts_with(r.as_str()))
                        }
                        _ => Value::Null,
                    },
                    BinOpKind::EndsWith => match (&lv, &rv) {
                        (Value::String(l), Value::String(r)) => {
                            Value::Bool(l.ends_with(r.as_str()))
                        }
                        _ => Value::Null,
                    },
                    BinOpKind::And => match (&lv, &rv) {
                        (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a && *b),
                        _ => Value::Null,
                    },
                    BinOpKind::Or => match (&lv, &rv) {
                        (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a || *b),
                        _ => Value::Null,
                    },
                    BinOpKind::Add => match (&lv, &rv) {
                        (Value::Int64(a), Value::Int64(b)) => Value::Int64(a + b),
                        (Value::Float64(a), Value::Float64(b)) => Value::Float64(a + b),
                        (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 + b),
                        (Value::Float64(a), Value::Int64(b)) => Value::Float64(a + *b as f64),
                        (Value::String(a), Value::String(b)) => Value::String(format!("{a}{b}")),
                        _ => Value::Null,
                    },
                    BinOpKind::Sub => match (&lv, &rv) {
                        (Value::Int64(a), Value::Int64(b)) => Value::Int64(a - b),
                        (Value::Float64(a), Value::Float64(b)) => Value::Float64(a - b),
                        (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 - b),
                        (Value::Float64(a), Value::Int64(b)) => Value::Float64(a - *b as f64),
                        _ => Value::Null,
                    },
                    BinOpKind::Mul => match (&lv, &rv) {
                        (Value::Int64(a), Value::Int64(b)) => Value::Int64(a * b),
                        (Value::Float64(a), Value::Float64(b)) => Value::Float64(a * b),
                        (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 * b),
                        (Value::Float64(a), Value::Int64(b)) => Value::Float64(a * *b as f64),
                        _ => Value::Null,
                    },
                    BinOpKind::Div => match (&lv, &rv) {
                        (Value::Int64(a), Value::Int64(b)) => {
                            if *b == 0 {
                                Value::Null
                            } else {
                                Value::Int64(a / b)
                            }
                        }
                        (Value::Float64(a), Value::Float64(b)) => Value::Float64(a / b),
                        (Value::Int64(a), Value::Float64(b)) => Value::Float64(*a as f64 / b),
                        (Value::Float64(a), Value::Int64(b)) => Value::Float64(a / *b as f64),
                        _ => Value::Null,
                    },
                    BinOpKind::Mod => match (&lv, &rv) {
                        (Value::Int64(a), Value::Int64(b)) => {
                            if *b == 0 {
                                Value::Null
                            } else {
                                Value::Int64(a % b)
                            }
                        }
                        _ => Value::Null,
                    },
                }
            }
            _ => eval_expr(expr, vals),
        }
    }

    /// Graph-aware WHERE evaluation — falls back to eval_where for pure expressions.
    pub(crate) fn eval_where_graph(&self, expr: &Expr, vals: &HashMap<String, Value>) -> bool {
        match self.eval_expr_graph(expr, vals) {
            Value::Bool(b) => b,
            _ => eval_where(expr, vals),
        }
    }

    /// Evaluate `EXISTS { (n)-[:REL]->(:DstLabel) }` — SPA-137.
    pub(crate) fn eval_exists_subquery(
        &self,
        ep: &sparrowdb_cypher::ast::ExistsPattern,
        vals: &HashMap<String, Value>,
    ) -> bool {
        let path = &ep.path;
        if path.nodes.len() < 2 || path.rels.is_empty() {
            return false;
        }
        let src_pat = &path.nodes[0];
        let dst_pat = &path.nodes[1];
        let rel_pat = &path.rels[0];

        let src_node_id = match self.resolve_node_id_from_var(&src_pat.var, vals) {
            Some(id) => id,
            None => return false,
        };
        let src_slot = src_node_id.0 & 0xFFFF_FFFF;
        let src_label_id = (src_node_id.0 >> 32) as u32;

        let dst_label = dst_pat.labels.first().map(String::as_str).unwrap_or("");
        let dst_label_id_opt: Option<u32> = if dst_label.is_empty() {
            None
        } else {
            self.snapshot
                .catalog
                .get_label(dst_label)
                .ok()
                .flatten()
                .map(|id| id as u32)
        };

        let rel_lookup = if let Some(dst_lid) = dst_label_id_opt {
            self.resolve_rel_table_id(src_label_id, dst_lid, &rel_pat.rel_type)
        } else {
            RelTableLookup::All
        };

        let csr_nb: Vec<u64> = match rel_lookup {
            RelTableLookup::Found(rtid) => self.csr_neighbors(rtid, src_slot),
            RelTableLookup::NotFound => return false,
            // Untyped edge: every table whose source label is this node's, and
            // whose destination label is the one the pattern asks for.  A bare
            // slot would otherwise let a neighbour of any label answer for one
            // of `dst_label_id_opt`.
            RelTableLookup::All => {
                self.csr_neighbor_slots_to_label(src_slot, src_label_id, dst_label_id_opt, &[])
            }
        };
        let delta_nb: Vec<u64> = self
            .read_delta_all()
            .into_iter()
            .filter(|r| {
                let r_src_label = (r.src.0 >> 32) as u32;
                let r_src_slot = r.src.0 & 0xFFFF_FFFF;
                if r_src_label != src_label_id || r_src_slot != src_slot {
                    return false;
                }
                // When a destination label is known, only keep edges that point
                // to nodes of that label — slots are label-relative so mixing
                // labels causes false positive matches.
                if let Some(dst_lid) = dst_label_id_opt {
                    let r_dst_label = (r.dst.0 >> 32) as u32;
                    r_dst_label == dst_lid
                } else {
                    true
                }
            })
            .map(|r| r.dst.0 & 0xFFFF_FFFF)
            .collect();

        let all_nb: std::collections::HashSet<u64> = csr_nb.into_iter().chain(delta_nb).collect();

        for dst_slot in all_nb {
            if let Some(did) = dst_label_id_opt {
                let probe_id = NodeId(((did as u64) << 32) | dst_slot);
                if self.snapshot.store.get_node_raw(probe_id, &[]).is_err() {
                    continue;
                }
                if !dst_pat.props.is_empty() {
                    let col_ids: Vec<u32> = dst_pat
                        .props
                        .iter()
                        .map(|p| prop_name_to_col_id(&p.key))
                        .collect();
                    // #479: nullable accessor + drop-absent, matching every
                    // read-path prop-filter site — see node_matches_prop_filter.
                    match self
                        .snapshot
                        .store
                        .get_node_raw_nullable(probe_id, &col_ids)
                    {
                        Ok(raw_props) => {
                            let params = self.dollar_params();
                            let props: Vec<(u32, u64)> = raw_props
                                .into_iter()
                                .filter_map(|(c, opt)| opt.map(|v| (c, v)))
                                .collect();
                            if !matches_prop_filter_static(
                                &props,
                                &dst_pat.props,
                                &params,
                                &self.snapshot.store,
                            ) {
                                continue;
                            }
                        }
                        Err(_) => continue,
                    }
                }
            }
            return true;
        }
        false
    }

    /// Resolve a NodeId from `vals` for a variable name.
    pub(crate) fn resolve_node_id_from_var(
        &self,
        var: &str,
        vals: &HashMap<String, Value>,
    ) -> Option<NodeId> {
        let id_key = format!("{var}.__node_id__");
        if let Some(Value::NodeRef(nid)) = vals.get(&id_key) {
            return Some(*nid);
        }
        if let Some(Value::NodeRef(nid)) = vals.get(var) {
            return Some(*nid);
        }
        None
    }

    /// Evaluate `shortestPath((src)-[:REL*]->(dst))` — SPA-136.
    pub(crate) fn eval_shortest_path_expr(
        &self,
        sp: &sparrowdb_cypher::ast::ShortestPathExpr,
        vals: &HashMap<String, Value>,
    ) -> Value {
        // Resolve src: if the variable is already bound as a NodeRef, extract
        // label_id and slot from the NodeId directly (high 32 bits = label_id,
        // low 32 bits = slot). This handles the case where shortestPath((a)-...)
        // refers to a variable bound in the outer MATCH without repeating its label.
        let (src_label_id, src_slot) =
            if let Some(nid) = self.resolve_node_id_from_var(&sp.src_var, vals) {
                let label_id = (nid.0 >> 32) as u32;
                let slot = nid.0 & 0xFFFF_FFFF;
                (label_id, slot)
            } else {
                // Fall back to label lookup + property scan.
                let label_id = match self.snapshot.catalog.get_label(&sp.src_label) {
                    Ok(Some(id)) => id as u32,
                    _ => return Value::Null,
                };
                match self.find_node_by_props(label_id, &sp.src_props) {
                    Some(slot) => (label_id, slot),
                    None => return Value::Null,
                }
            };

        // #427: the destination's label is part of its identity — a `NodeId` is
        // `(label_id << 32) | slot`, so slot 0 exists once per label. Carrying
        // only the slot let a `City` stand in for a `Person`.
        let (dst_label_id, dst_slot) =
            if let Some(nid) = self.resolve_node_id_from_var(&sp.dst_var, vals) {
                (((nid.0 >> 32) as u32), nid.0 & 0xFFFF_FFFF)
            } else {
                let dst_label_id = match self.snapshot.catalog.get_label(&sp.dst_label) {
                    Ok(Some(id)) => id as u32,
                    _ => return Value::Null,
                };
                match self.find_node_by_props(dst_label_id, &sp.dst_props) {
                    Some(slot) => (dst_label_id, slot),
                    None => return Value::Null,
                }
            };

        // #427: honour the pattern's relationship type. An empty `rel_ids` means
        // "no type constraint" to the neighbour lookups, so a named type that is
        // absent from the catalog must short-circuit rather than fall through to
        // an unfiltered traversal of every edge type in the graph.
        let rel_ids = self.resolve_rel_ids_for_type(&sp.rel_type);
        if !sp.rel_type.is_empty() && rel_ids.is_empty() {
            // No relationship table of that type exists, so no edge of that type
            // can exist. The only reachable node is the source itself.
            return if (src_label_id, src_slot) == (dst_label_id, dst_slot) {
                Value::Int64(0)
            } else {
                Value::Null
            };
        }

        match self.bfs_shortest_path(src_slot, src_label_id, dst_slot, dst_label_id, &rel_ids, 10) {
            Some(hops) => Value::Int64(hops as i64),
            None => Value::Null,
        }
    }

    /// Scan a label for the first node matching all property filters.
    pub(crate) fn find_node_by_props(
        &self,
        label_id: u32,
        props: &[sparrowdb_cypher::ast::PropEntry],
    ) -> Option<u64> {
        if props.is_empty() {
            return None;
        }
        let hwm = self.snapshot.store.hwm_for_label(label_id).ok()?;
        let col_ids: Vec<u32> = props.iter().map(|p| prop_name_to_col_id(&p.key)).collect();
        let params = self.dollar_params();
        for slot in 0..hwm {
            let node_id = NodeId(((label_id as u64) << 32) | slot);
            // #479: route through the nullable accessor (as the read-path scan
            // does) and drop absent columns, so a genuinely null-bound filter
            // can match the node whose property is actually absent instead of
            // silently never matching via `get_node_raw`'s zero-sentinel.
            if let Ok(raw_props) = self.snapshot.store.get_node_raw_nullable(node_id, &col_ids) {
                let stored: Vec<(u32, u64)> = raw_props
                    .into_iter()
                    .filter_map(|(c, opt)| opt.map(|v| (c, v)))
                    .collect();
                if matches_prop_filter_static(&stored, props, &params, &self.snapshot.store) {
                    return Some(slot);
                }
            }
        }
        None
    }

    /// BFS from `(src_slot, src_label_id)` to `(dst_slot, dst_label_id)`,
    /// returning the hop count or `None` when no path exists.
    ///
    /// Every node is identified by the **pair** `(slot, label_id)`, never by the
    /// slot alone.  A `NodeId` is `(label_id << 32) | slot`, so slots are only
    /// unique within a label: `Person` slot 0, `City` slot 0 and `Post` slot 0
    /// are three different nodes.  Issue #427 was caused by comparing bare slots
    /// in the zero-hop shortcut, in the destination test and in `visited`, which
    /// both invented paths that did not exist and pruned real ones.
    ///
    /// `rel_ids` restricts the traversal to those relationship-table IDs; an
    /// empty slice means "any type".  Callers that parsed an explicit type must
    /// resolve it (see [`Engine::resolve_rel_ids_for_type`]) and pass the result
    /// — issue #427 also covered the BFS passing `&[]` unconditionally, which
    /// made `[:KNOWS*]` walk every edge type in the graph.
    pub(crate) fn bfs_shortest_path(
        &self,
        src_slot: u64,
        src_label_id: u32,
        dst_slot: u64,
        dst_label_id: u32,
        rel_ids: &[u32],
        max_hops: u32,
    ) -> Option<u32> {
        if (src_slot, src_label_id) == (dst_slot, dst_label_id) {
            return Some(0);
        }
        // Hoist delta read out of the BFS loop to avoid repeated I/O.
        let delta_all = self.read_delta_all();
        // SPA-283: build HashMap index for O(1) per-node delta lookups.
        let delta_idx = build_delta_index(&delta_all);

        // Frontier carries (slot, label_id) so each hop uses the correct label
        // when looking up neighbours.
        let mut visited: std::collections::HashSet<(u64, u32)> = std::collections::HashSet::new();
        visited.insert((src_slot, src_label_id));
        let mut frontier: Vec<(u64, u32)> = vec![(src_slot, src_label_id)];
        let mut neighbors: std::collections::HashSet<(u64, u32)> = std::collections::HashSet::new();

        for depth in 1..=max_hops {
            let mut next_frontier: Vec<(u64, u32)> = Vec::new();
            for &(node_slot, node_label_id) in &frontier {
                // #427/#431: both edge sources report the neighbour's label
                // rather than leaving it to be guessed — the catalog's
                // `dst_label_id` for checkpointed edges, the stored `NodeId` for
                // delta edges.  This loop used to inline that logic; it now
                // shares the one implementation with variable-length traversal.
                self.get_node_neighbors_labeled(
                    node_slot,
                    node_label_id,
                    &delta_idx,
                    &mut neighbors,
                    rel_ids,
                );

                for &(nb_slot, nb_label) in neighbors.iter() {
                    if (nb_slot, nb_label) == (dst_slot, dst_label_id) {
                        return Some(depth);
                    }
                    if visited.insert((nb_slot, nb_label)) {
                        next_frontier.push((nb_slot, nb_label));
                    }
                }
            }
            if next_frontier.is_empty() {
                break;
            }
            frontier = next_frontier;
        }
        None
    }

    /// Engine-aware aggregate_rows: evaluates graph-dependent RETURN expressions
    /// (ShortestPath, EXISTS) via self before delegating to the standalone helper.
    pub(crate) fn aggregate_rows_graph(
        &self,
        rows: &[HashMap<String, Value>],
        return_items: &[ReturnItem],
    ) -> Vec<Vec<Value>> {
        // Check if any return item needs graph access.
        let needs_graph = return_items.iter().any(|item| expr_needs_graph(&item.expr));
        if !needs_graph {
            return aggregate_rows(self, rows, return_items);
        }
        // For graph-dependent items, project each row using eval_expr_graph.
        rows.iter()
            .map(|row_vals| {
                return_items
                    .iter()
                    .map(|item| self.eval_expr_graph(&item.expr, row_vals))
                    .collect()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #459 pin: `expr_needs_graph` (`engine/mod.rs`) must return `true` for
    /// every name `eval_expr_graph` actually dispatches through
    /// `GRAPH_ONLY_FNS` — read directly off that table, not a fourth copy of
    /// the name list, so this test cannot silently drift the same way the
    /// two independent name lists it replaces did before this fix. If a
    /// future graph-only function is added to `GRAPH_ONLY_FNS` without a
    /// matching change anywhere, this test still passes (there is nothing
    /// left to keep in sync) — that is the point.
    #[test]
    fn expr_needs_graph_covers_every_name_in_the_dispatch_table() {
        for (name, _) in GRAPH_ONLY_FNS {
            let expr = Expr::FnCall {
                name: (*name).to_string(),
                args: vec![],
            };
            assert!(
                expr_needs_graph(&expr),
                "expr_needs_graph() must return true for {name:?} — it is in \
                 GRAPH_ONLY_FNS (eval_expr_graph's real dispatch table), so \
                 aggregate_rows_graph must route it through eval_expr_graph \
                 rather than the non-engine eval_expr fallback that produced \
                 issue #459's Null-on-a-healthy-index bug"
            );
            assert!(
                is_graph_only_fn(name),
                "is_graph_only_fn({name:?}) must agree with its own table"
            );
        }
    }

    /// Sanity companion: an ordinary function name outside the table must
    /// NOT be flagged as graph-only — otherwise the pin above would pass
    /// trivially by treating every function as graph-only.
    #[test]
    fn expr_needs_graph_does_not_flag_an_ordinary_function() {
        let expr = Expr::FnCall {
            name: "toUpper".to_string(),
            args: vec![],
        };
        assert!(!expr_needs_graph(&expr));
        assert!(!is_graph_only_fn("toUpper"));
    }

    /// Case-insensitivity parity: `eval_expr_graph`'s dispatch lowercases the
    /// name before matching, so the routing check must too.
    #[test]
    fn expr_needs_graph_is_case_insensitive() {
        let expr = Expr::FnCall {
            name: "HYBRID_SEARCH".to_string(),
            args: vec![],
        };
        assert!(expr_needs_graph(&expr));
    }
}
