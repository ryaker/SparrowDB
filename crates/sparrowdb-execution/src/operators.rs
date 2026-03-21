//! Execution operators: LabelScan, Filter, Project, Expand.
//!
//! Each operator implements a `next_chunk()` iterator pattern returning
//! `Option<FactorizedChunk>`.

use std::collections::HashMap;

use sparrowdb_common::{NodeId, Result};
use sparrowdb_storage::node_store::NodeStore;

use crate::types::{FactorizedChunk, TypedVector, Value, VectorGroup};

// ── Operator trait ────────────────────────────────────────────────────────────

/// Operator trait: produces `FactorizedChunk`s lazily.
pub trait Operator {
    fn next_chunk(&mut self) -> Result<Option<FactorizedChunk>>;

    /// Drain all chunks and materialize as a Vec<FactorizedChunk>.
    fn collect_all(&mut self) -> Result<Vec<FactorizedChunk>> {
        let mut result = Vec::new();
        while let Some(chunk) = self.next_chunk()? {
            result.push(chunk);
        }
        Ok(result)
    }
}

// ── LabelScan ────────────────────────────────────────────────────────────────

/// Scans all slots for a given label_id, reading specified columns.
///
/// Each call to `next_chunk()` returns one chunk containing one VectorGroup
/// with all rows for the label (for simplicity in Phase 4; chunked in Phase 5+).
pub struct LabelScan<'a> {
    store: &'a NodeStore,
    label_id: u32,
    col_ids: Vec<u32>,
    done: bool,
}

impl<'a> LabelScan<'a> {
    pub fn new(store: &'a NodeStore, label_id: u32, col_ids: &[u32]) -> Self {
        LabelScan {
            store,
            label_id,
            col_ids: col_ids.to_vec(),
            done: false,
        }
    }
}

impl<'a> Operator for LabelScan<'a> {
    fn next_chunk(&mut self) -> Result<Option<FactorizedChunk>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // Read the HWM to know how many slots exist.
        let hwm = self.store.hwm_for_label(self.label_id)?;
        if hwm == 0 {
            return Ok(Some(FactorizedChunk::new()));
        }

        let n = hwm as usize;
        let mut col_vecs: HashMap<String, Vec<i64>> = HashMap::new();
        let mut node_ids: Vec<NodeId> = Vec::with_capacity(n);

        // Read each column for all slots.
        for &col_id in &self.col_ids {
            let mut vals = Vec::with_capacity(n);
            for slot in 0..hwm as u32 {
                let node_id = NodeId(((self.label_id as u64) << 32) | (slot as u64));
                let raw = self.store.get_node_raw(node_id, &[col_id])?;
                vals.push(if raw.is_empty() { 0 } else { raw[0].1 as i64 });
            }
            col_vecs.insert(format!("col_{col_id}"), vals);
        }

        // Build node_id vector.
        for slot in 0..hwm as u32 {
            let node_id = NodeId(((self.label_id as u64) << 32) | (slot as u64));
            node_ids.push(node_id);
        }

        let mut group = VectorGroup::new(1);
        group.add_column("__node_id__".into(), TypedVector::NodeRef(node_ids));
        for (name, vals) in col_vecs {
            group.add_column(name, TypedVector::Int64(vals));
        }

        let mut chunk = FactorizedChunk::new();
        chunk.push_group(group);
        Ok(Some(chunk))
    }
}

// ── Filter ────────────────────────────────────────────────────────────────────

/// Filters rows in each chunk where `column_name == predicate_value`.
pub struct Filter<'a, O: Operator + 'a> {
    inner: &'a mut O,
    column: String,
    predicate: FilterPredicate,
}

/// Filter predicate types.
pub enum FilterPredicate {
    /// column == value
    Eq(Value),
    /// column CONTAINS string
    Contains(String),
    /// column > value (int)
    Gt(i64),
    /// column >= value
    Ge(i64),
    /// column < value
    Lt(i64),
}

impl<'a, O: Operator> Filter<'a, O> {
    /// Construct a filter: `column_name == value`.
    pub fn new(inner: &'a mut O, column_name: &str, value: Value) -> Self {
        Filter {
            inner,
            column: column_name.to_string(),
            predicate: FilterPredicate::Eq(value),
        }
    }

    /// Construct a CONTAINS filter.
    pub fn contains(inner: &'a mut O, column_name: &str, substr: &str) -> Self {
        Filter {
            inner,
            column: column_name.to_string(),
            predicate: FilterPredicate::Contains(substr.to_string()),
        }
    }

    /// Construct a greater-than filter (int64).
    pub fn gt(inner: &'a mut O, column_name: &str, val: i64) -> Self {
        Filter {
            inner,
            column: column_name.to_string(),
            predicate: FilterPredicate::Gt(val),
        }
    }

    fn matches(&self, v: &Value) -> bool {
        match &self.predicate {
            FilterPredicate::Eq(expected) => v == expected,
            FilterPredicate::Contains(substr) => match v {
                Value::String(s) => s.contains(substr.as_str()),
                _ => false,
            },
            FilterPredicate::Gt(thresh) => match v {
                Value::Int64(n) => *n > *thresh,
                _ => false,
            },
            FilterPredicate::Ge(thresh) => match v {
                Value::Int64(n) => *n >= *thresh,
                _ => false,
            },
            FilterPredicate::Lt(thresh) => match v {
                Value::Int64(n) => *n < *thresh,
                _ => false,
            },
        }
    }

    fn filter_group(&self, group: VectorGroup) -> Option<VectorGroup> {
        let col = group.columns.get(&self.column)?;
        let n = col.len();

        // Compute keep mask.
        let keep: Vec<bool> = (0..n).map(|i| self.matches(&col.get(i))).collect();

        if keep.iter().all(|&k| !k) {
            return None;
        }

        let mut new_group = VectorGroup::new(group.multiplicity);
        for (col_name, col_vec) in &group.columns {
            let filtered = filter_typed_vector(col_vec, &keep);
            new_group.add_column(col_name.clone(), filtered);
        }
        if new_group.is_empty() {
            None
        } else {
            Some(new_group)
        }
    }
}

impl<'a, O: Operator> Operator for Filter<'a, O> {
    fn next_chunk(&mut self) -> Result<Option<FactorizedChunk>> {
        loop {
            match self.inner.next_chunk()? {
                None => return Ok(None),
                Some(chunk) => {
                    let mut out = FactorizedChunk::new();
                    for group in chunk.groups {
                        if let Some(filtered) = self.filter_group(group) {
                            out.push_group(filtered);
                        }
                    }
                    if !out.is_empty() {
                        return Ok(Some(out));
                    }
                    // If all groups were filtered out, ask for the next chunk.
                }
            }
        }
    }
}

fn filter_typed_vector(vec: &TypedVector, keep: &[bool]) -> TypedVector {
    match vec {
        TypedVector::Int64(v) => TypedVector::Int64(
            v.iter()
                .zip(keep)
                .filter_map(|(x, &k)| if k { Some(*x) } else { None })
                .collect(),
        ),
        TypedVector::Float64(v) => TypedVector::Float64(
            v.iter()
                .zip(keep)
                .filter_map(|(x, &k)| if k { Some(*x) } else { None })
                .collect(),
        ),
        TypedVector::Bool(v) => TypedVector::Bool(
            v.iter()
                .zip(keep)
                .filter_map(|(x, &k)| if k { Some(*x) } else { None })
                .collect(),
        ),
        TypedVector::String(v) => TypedVector::String(
            v.iter()
                .zip(keep)
                .filter_map(|(x, &k)| if k { Some(x.clone()) } else { None })
                .collect(),
        ),
        TypedVector::NodeRef(v) => TypedVector::NodeRef(
            v.iter()
                .zip(keep)
                .filter_map(|(x, &k)| if k { Some(*x) } else { None })
                .collect(),
        ),
        TypedVector::EdgeRef(v) => TypedVector::EdgeRef(
            v.iter()
                .zip(keep)
                .filter_map(|(x, &k)| if k { Some(*x) } else { None })
                .collect(),
        ),
    }
}

// ── Project ───────────────────────────────────────────────────────────────────

/// Projects (selects) specific named columns from each chunk.
pub struct Project<'a, O: Operator + 'a> {
    inner: &'a mut O,
    columns: Vec<String>,
}

impl<'a, O: Operator> Project<'a, O> {
    pub fn new(inner: &'a mut O, columns: Vec<String>) -> Self {
        Project { inner, columns }
    }
}

impl<'a, O: Operator> Operator for Project<'a, O> {
    fn next_chunk(&mut self) -> Result<Option<FactorizedChunk>> {
        match self.inner.next_chunk()? {
            None => Ok(None),
            Some(chunk) => {
                let mut out = FactorizedChunk::new();
                for group in chunk.groups {
                    let mut new_group = VectorGroup::new(group.multiplicity);
                    for col_name in &self.columns {
                        if let Some(col) = group.columns.get(col_name) {
                            new_group.add_column(col_name.clone(), col.clone());
                        }
                    }
                    out.push_group(new_group);
                }
                Ok(Some(out))
            }
        }
    }
}

// ── Expand ────────────────────────────────────────────────────────────────────

/// Expands a NodeRef column by looking up neighbors in a CSR.
///
/// For each node in `src_col`, produces neighbor node IDs in `dst_col`.
/// Preserves group multiplicity.
pub struct Expand<'a, O: Operator + 'a> {
    inner: &'a mut O,
    src_col: String,
    dst_col: String,
    csr: &'a sparrowdb_storage::csr::CsrForward,
    // Buffered output chunks from expansion.
    buffer: Vec<FactorizedChunk>,
    done: bool,
}

impl<'a, O: Operator> Expand<'a, O> {
    pub fn new(
        inner: &'a mut O,
        src_col: &str,
        dst_col: &str,
        csr: &'a sparrowdb_storage::csr::CsrForward,
    ) -> Self {
        Expand {
            inner,
            src_col: src_col.to_string(),
            dst_col: dst_col.to_string(),
            csr,
            buffer: Vec::new(),
            done: false,
        }
    }
}

impl<'a, O: Operator> Operator for Expand<'a, O> {
    fn next_chunk(&mut self) -> Result<Option<FactorizedChunk>> {
        if !self.buffer.is_empty() {
            return Ok(Some(self.buffer.remove(0)));
        }
        if self.done {
            return Ok(None);
        }

        match self.inner.next_chunk()? {
            None => {
                self.done = true;
                Ok(None)
            }
            Some(chunk) => {
                let mut out = FactorizedChunk::new();
                for group in chunk.groups {
                    let node_col = match group.columns.get(&self.src_col) {
                        Some(TypedVector::NodeRef(v)) => v.clone(),
                        _ => continue,
                    };

                    // For each source node, expand to neighbors.
                    for src_node in &node_col {
                        let slot = src_node.0 & 0xFFFF_FFFF;
                        let neighbors = self.csr.neighbors(slot);
                        if neighbors.is_empty() {
                            continue;
                        }

                        // Build a new group for each src with all its neighbors.
                        let label_id = src_node.0 >> 32;
                        let dst_nodes: Vec<NodeId> = neighbors
                            .iter()
                            .map(|&nb_slot| NodeId((label_id << 32) | nb_slot))
                            .collect();

                        let n = dst_nodes.len();
                        let mut new_group = VectorGroup::new(group.multiplicity);
                        // Repeat the source node N times to match the destination vector length,
                        // preserving the VectorGroup invariant that all columns have equal length.
                        new_group.add_column(
                            self.src_col.clone(),
                            TypedVector::NodeRef(vec![*src_node; n]),
                        );
                        new_group.add_column(self.dst_col.clone(), TypedVector::NodeRef(dst_nodes));
                        out.push_group(new_group);
                    }
                }
                if out.is_empty() {
                    // Recurse to get next chunk with data.
                    self.next_chunk()
                } else {
                    Ok(Some(out))
                }
            }
        }
    }
}

// ── UnwindOperator ────────────────────────────────────────────────────────────

/// Iterates a list of scalar `Value`s, emitting one row per element.
///
/// Each row has a single column named after `alias`.
/// Empty lists produce zero rows.
pub struct UnwindOperator {
    /// Pre-evaluated list of values to iterate.
    values: Vec<crate::types::Value>,
    /// Column name bound to each element.
    alias: String,
    /// Index of the next value to emit.
    idx: usize,
    done: bool,
}

impl UnwindOperator {
    /// Create an UNWIND operator that emits each element of `values` in turn.
    pub fn new(alias: String, values: Vec<crate::types::Value>) -> Self {
        let done = values.is_empty();
        UnwindOperator {
            values,
            alias,
            idx: 0,
            done,
        }
    }
}

impl Operator for UnwindOperator {
    fn next_chunk(&mut self) -> Result<Option<FactorizedChunk>> {
        if self.done {
            return Ok(None);
        }

        // Emit all elements in a single chunk as typed vectors.
        // We detect the type from the first element and coerce the rest;
        // mixed-type lists produce Int64 / String / Float64 chunks respectively.
        let remaining = &self.values[self.idx..];
        if remaining.is_empty() {
            self.done = true;
            return Ok(None);
        }

        // Build a TypedVector matching the dominant type.
        let typed = build_typed_vector(remaining);
        self.idx = self.values.len();
        self.done = true;

        let mut group = VectorGroup::new(1);
        group.add_column(self.alias.clone(), typed);
        let mut chunk = FactorizedChunk::new();
        chunk.push_group(group);
        Ok(Some(chunk))
    }
}

/// Convert a slice of `Value`s into a `TypedVector`.
///
/// If all values are the same primitive type, uses that type's vector;
/// otherwise falls back to `String` (via `Display`).
fn build_typed_vector(values: &[crate::types::Value]) -> TypedVector {
    use crate::types::Value;

    // Check if all values are Int64.
    if values.iter().all(|v| matches!(v, Value::Int64(_))) {
        return TypedVector::Int64(
            values
                .iter()
                .map(|v| match v {
                    Value::Int64(n) => *n,
                    _ => unreachable!(),
                })
                .collect(),
        );
    }

    // Check if all values are Float64.
    if values.iter().all(|v| matches!(v, Value::Float64(_))) {
        return TypedVector::Float64(
            values
                .iter()
                .map(|v| match v {
                    Value::Float64(f) => *f,
                    _ => unreachable!(),
                })
                .collect(),
        );
    }

    // Check if all values are Bool.
    if values.iter().all(|v| matches!(v, Value::Bool(_))) {
        return TypedVector::Bool(
            values
                .iter()
                .map(|v| match v {
                    Value::Bool(b) => *b,
                    _ => unreachable!(),
                })
                .collect(),
        );
    }

    // Fall back to String.
    TypedVector::String(values.iter().map(|v| v.to_string()).collect())
}
