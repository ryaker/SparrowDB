//! Auto-generated submodule — see engine/mod.rs for context.
use super::*;

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

    /// Evaluate `full_text_search(label, property, query)`.
    ///
    /// Returns `Value::Bool(true)` if the current node's ID appears in the BM25
    /// results for `(label, property, query)`.  The current node is resolved by
    /// scanning `vals` for any `__node_id__` entry that matches the given label.
    ///
    /// Returns `Value::Bool(false)` when no matching entry is found or the FTS
    /// index does not exist for the pair.
    fn eval_full_text_search(&self, args: &[Expr], vals: &HashMap<String, Value>) -> Value {
        if args.len() != 3 {
            return Value::Bool(false);
        }
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
        // During WHERE evaluation (`execute_scan`) the NodeRef is stored under
        // the plain variable name (e.g. "n").  During aggregation / eval path
        // it is also stored under "{var}.__node_id__".  We accept both.
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
            None => Value::Bool(false),
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
            None => Value::Float64(0.0),
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
        let fts_results: Vec<(u64, f32)> = match sparrowdb_storage::fts_index::FtsIndex::open(
            &self.snapshot.db_root,
            &label,
            &text_prop,
        ) {
            Ok(idx) => idx.search(&query_text, k * 2),
            Err(_) => vec![],
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
            Expr::FnCall { name, args } => match name.to_ascii_lowercase().as_str() {
                "full_text_search" => self.eval_full_text_search(args, vals),
                "bm25_score" => self.eval_bm25_score(args, vals),
                "hybrid_search" => self.eval_hybrid_search(args, vals),
                _ => eval_expr(expr, vals),
            },
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
            RelTableLookup::All => self.csr_neighbors_all(src_slot),
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
                    match self.snapshot.store.get_node_raw(probe_id, &col_ids) {
                        Ok(props) => {
                            let params = self.dollar_params();
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

        let dst_slot = if let Some(nid) = self.resolve_node_id_from_var(&sp.dst_var, vals) {
            nid.0 & 0xFFFF_FFFF
        } else {
            let dst_label_id = match self.snapshot.catalog.get_label(&sp.dst_label) {
                Ok(Some(id)) => id as u32,
                _ => return Value::Null,
            };
            match self.find_node_by_props(dst_label_id, &sp.dst_props) {
                Some(slot) => slot,
                None => return Value::Null,
            }
        };

        match self.bfs_shortest_path(src_slot, src_label_id, dst_slot, 10) {
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
            if let Ok(raw_props) = self.snapshot.store.get_node_raw(node_id, &col_ids) {
                if matches_prop_filter_static(&raw_props, props, &params, &self.snapshot.store) {
                    return Some(slot);
                }
            }
        }
        None
    }

    /// BFS from `src_slot` to `dst_slot`, returning the hop count or None.
    ///
    /// Each frontier node carries its own `label_id` so that delta-log edge
    /// lookups use the correct `(label_id, slot)` key at every hop.  Without
    /// this, BFS through heterogeneous graphs would use the source label for
    /// all intermediate nodes, missing WAL edges on label-boundary crossings.
    pub(crate) fn bfs_shortest_path(
        &self,
        src_slot: u64,
        src_label_id: u32,
        dst_slot: u64,
        max_hops: u32,
    ) -> Option<u32> {
        if src_slot == dst_slot {
            return Some(0);
        }
        // Hoist delta read out of the BFS loop to avoid repeated I/O.
        let delta_all = self.read_delta_all();
        // SPA-283: build HashMap index for O(1) per-node delta lookups.
        let delta_idx = build_delta_index(&delta_all);
        // Frontier carries (slot, label_id) so each hop uses the correct label
        // when probing the delta index.
        let mut visited: std::collections::HashSet<u64> = std::collections::HashSet::new();
        visited.insert(src_slot);
        let mut frontier: Vec<(u64, u32)> = vec![(src_slot, src_label_id)];

        for depth in 1..=max_hops {
            let mut next_frontier: Vec<(u64, u32)> = Vec::new();
            for &(node_slot, node_label_id) in &frontier {
                let neighbors =
                    self.get_node_neighbors_by_slot(node_slot, node_label_id, &delta_idx, &[]);
                for nb_slot in neighbors {
                    if nb_slot == dst_slot {
                        return Some(depth);
                    }
                    if visited.insert(nb_slot) {
                        // Recover the neighbor's label from the delta index; fall
                        // back to node_label_id for CSR-only nodes in homogeneous
                        // graphs (the same conservative default used elsewhere).
                        let nb_label = delta_neighbors_labeled_from_index(
                            &delta_idx,
                            node_label_id,
                            node_slot,
                        )
                        .find(|&(s, _)| s == nb_slot)
                        .map(|(_, l)| l)
                        .unwrap_or(node_label_id);
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
            return aggregate_rows(rows, return_items);
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
