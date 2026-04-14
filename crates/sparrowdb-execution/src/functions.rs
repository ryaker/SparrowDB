//! openCypher built-in function library.
//!
//! Covers SPA-140 (string), SPA-141 (math), SPA-142 (list), and SPA-143
//! (type conversion & predicate) functions.
//!
//! Each function takes evaluated `Vec<Value>` arguments and returns
//! `Result<Value>`.  The dispatcher is `dispatch_function`.

use sparrowdb_common::{Error, Result};

use crate::types::Value;

// ── Public dispatcher ─────────────────────────────────────────────────────────

/// Dispatch a built-in function call by name.
///
/// `name` is compared case-insensitively.
/// Returns `Err(InvalidArgument)` for unknown function names or arity errors.
pub fn dispatch_function(name: &str, args: Vec<Value>) -> Result<Value> {
    match name.to_lowercase().as_str() {
        // ── SPA-140: String functions ─────────────────────────────────────────
        "toupper" => fn_to_upper(args),
        "tolower" => fn_to_lower(args),
        "trim" => fn_trim(args),
        "ltrim" => fn_ltrim(args),
        "rtrim" => fn_rtrim(args),
        "split" => fn_split(args),
        "substring" => fn_substring(args),
        "size" => fn_size(args),
        "startswith" => fn_starts_with(args),
        "endswith" => fn_ends_with(args),
        "contains" => fn_contains(args),
        "replace" => fn_replace(args),

        // ── SPA-141: Math functions ───────────────────────────────────────────
        "abs" => fn_abs(args),
        "ceil" => fn_ceil(args),
        "floor" => fn_floor(args),
        "round" => fn_round(args),
        "sqrt" => fn_sqrt(args),
        "log" => fn_log(args),
        "log10" => fn_log10(args),
        "exp" => fn_exp(args),
        "sign" => fn_sign(args),
        "rand" => fn_rand(args),

        // ── SPA-142: List functions ───────────────────────────────────────────
        "range" => fn_range(args),
        "head" => fn_head(args),
        "tail" => fn_tail(args),
        "last" => fn_last(args),
        "reverse" => fn_reverse(args),
        "sort" => fn_sort(args),
        "distinct" => fn_distinct(args),
        "reduce" => Err(Error::InvalidArgument(
            "reduce() must be handled by the evaluator (it requires a lambda)".into(),
        )),

        // ── SPA-143: Type conversion & predicate functions ────────────────────
        "tostring" => fn_to_string(args),
        "tointeger" => fn_to_integer(args),
        "tofloat" => fn_to_float(args),
        "toboolean" => fn_to_boolean(args),
        "type" => fn_type(args),
        "labels" => fn_labels(args),
        "keys" => fn_keys(args),
        "properties" => fn_properties(args),
        "id" => fn_id(args),
        "coalesce" => fn_coalesce(args),
        "isnull" => fn_is_null(args),
        "isnotnull" => fn_is_not_null(args),

        // Aggregate functions — handled by the engine's aggregate_rows(), not as scalar functions.
        "collect" | "count" | "sum" | "avg" | "min" | "max" => Err(Error::InvalidArgument(
            format!("{name}() is an aggregate function and cannot be used as a scalar expression"),
        )),

        // ── Temporal functions ────────────────────────────────────────────────
        "datetime" => fn_datetime(args),
        "timestamp" => fn_datetime(args), // alias for datetime()
        "date" => fn_date(args),
        "duration" => fn_duration(args),

        // ── Vector functions (issue #394) ─────────────────────────────────────
        // These are brute-force scalar functions; the planner / engine can use
        // HNSW when an index exists, but these always work regardless of index.
        "vector_similarity" | "vector_cosine" => fn_vector_similarity(args),
        "vector_distance" | "vector_euclidean" => fn_vector_distance(args),
        "vector_dot" | "vector_dot_product" => fn_vector_dot(args),
        "tofloatvector" | "vector" => fn_to_float_vector(args),

        // ── Hybrid search fusion functions (issue #396) ───────────────────────
        // Pure list-processing functions; hybrid_search() is engine-dispatched
        // (requires DB access) and is handled separately in engine/expr.rs.
        "rrf_fusion" => fn_rrf_fusion(args),
        "weighted_fusion" => fn_weighted_fusion(args),

        other => Err(Error::InvalidArgument(format!("unknown function: {other}"))),
    }
}

// ── Arity helpers ─────────────────────────────────────────────────────────────

fn expect_arity(name: &str, args: &[Value], expected: usize) -> Result<()> {
    if args.len() != expected {
        Err(Error::InvalidArgument(format!(
            "{name}() expects {expected} argument(s), got {}",
            args.len()
        )))
    } else {
        Ok(())
    }
}

fn expect_min_arity(name: &str, args: &[Value], min: usize) -> Result<()> {
    if args.len() < min {
        Err(Error::InvalidArgument(format!(
            "{name}() expects at least {min} argument(s), got {}",
            args.len()
        )))
    } else {
        Ok(())
    }
}

fn as_string<'a>(name: &str, v: &'a Value) -> Result<&'a str> {
    match v {
        Value::String(s) => Ok(s.as_str()),
        Value::Null => Err(Error::InvalidArgument(format!(
            "{name}(): argument is null"
        ))),
        other => Err(Error::InvalidArgument(format!(
            "{name}(): expected string, got {other}"
        ))),
    }
}

fn as_int(name: &str, v: &Value) -> Result<i64> {
    match v {
        Value::Int64(n) => Ok(*n),
        Value::Float64(f) => Ok(*f as i64),
        Value::Null => Err(Error::InvalidArgument(format!(
            "{name}(): argument is null"
        ))),
        other => Err(Error::InvalidArgument(format!(
            "{name}(): expected integer, got {other}"
        ))),
    }
}

fn as_float(name: &str, v: &Value) -> Result<f64> {
    match v {
        Value::Float64(f) => Ok(*f),
        Value::Int64(n) => Ok(*n as f64),
        Value::Null => Err(Error::InvalidArgument(format!(
            "{name}(): argument is null"
        ))),
        other => Err(Error::InvalidArgument(format!(
            "{name}(): expected numeric, got {other}"
        ))),
    }
}

// ── SPA-140: String functions ─────────────────────────────────────────────────

fn fn_to_upper(args: Vec<Value>) -> Result<Value> {
    expect_arity("toUpper", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("toUpper", &args[0])?;
    Ok(Value::String(s.to_uppercase()))
}

fn fn_to_lower(args: Vec<Value>) -> Result<Value> {
    expect_arity("toLower", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("toLower", &args[0])?;
    Ok(Value::String(s.to_lowercase()))
}

fn fn_trim(args: Vec<Value>) -> Result<Value> {
    expect_arity("trim", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("trim", &args[0])?;
    Ok(Value::String(s.trim().to_string()))
}

fn fn_ltrim(args: Vec<Value>) -> Result<Value> {
    expect_arity("ltrim", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("ltrim", &args[0])?;
    Ok(Value::String(s.trim_start().to_string()))
}

fn fn_rtrim(args: Vec<Value>) -> Result<Value> {
    expect_arity("rtrim", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("rtrim", &args[0])?;
    Ok(Value::String(s.trim_end().to_string()))
}

/// `split(string, delimiter)` — returns the first part for now.
///
/// openCypher `split()` returns a list; since SparrowDB's `Value` type has no
/// `List` variant yet, we return the number of parts as `Int64`.  This is a
/// pragmatic stub: calling code that needs individual parts should use UNWIND.
fn fn_split(args: Vec<Value>) -> Result<Value> {
    expect_arity("split", &args, 2)?;
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("split", &args[0])?;
    let delim = as_string("split", &args[1])?;
    // Return the count of parts (openCypher returns a list; we return its size
    // since `Value::List` doesn't exist yet).
    let count = s.split(delim).count() as i64;
    Ok(Value::Int64(count))
}

/// `substring(string, start[, length])`.
fn fn_substring(args: Vec<Value>) -> Result<Value> {
    expect_min_arity("substring", &args, 2)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("substring", &args[0])?;
    let start = as_int("substring", &args[1])?;
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;

    let start = start.max(0).min(len) as usize;

    let result: String = if args.len() >= 3 {
        let take = as_int("substring", &args[2])?.max(0) as usize;
        chars[start..].iter().take(take).collect()
    } else {
        chars[start..].iter().collect()
    };

    Ok(Value::String(result))
}

/// `size(string)` — character length; also handles `null` → `null`.
fn fn_size(args: Vec<Value>) -> Result<Value> {
    expect_arity("size", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::Int64(s.chars().count() as i64)),
        other => Err(Error::InvalidArgument(format!(
            "size(): expected string or null, got {other}"
        ))),
    }
}

fn fn_starts_with(args: Vec<Value>) -> Result<Value> {
    expect_arity("startsWith", &args, 2)?;
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("startsWith", &args[0])?;
    let prefix = as_string("startsWith", &args[1])?;
    Ok(Value::Bool(s.starts_with(prefix)))
}

fn fn_ends_with(args: Vec<Value>) -> Result<Value> {
    expect_arity("endsWith", &args, 2)?;
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("endsWith", &args[0])?;
    let suffix = as_string("endsWith", &args[1])?;
    Ok(Value::Bool(s.ends_with(suffix)))
}

fn fn_contains(args: Vec<Value>) -> Result<Value> {
    expect_arity("contains", &args, 2)?;
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("contains", &args[0])?;
    let needle = as_string("contains", &args[1])?;
    Ok(Value::Bool(s.contains(needle)))
}

fn fn_replace(args: Vec<Value>) -> Result<Value> {
    expect_arity("replace", &args, 3)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("replace", &args[0])?;
    let from = as_string("replace", &args[1])?;
    let to = as_string("replace", &args[2])?;
    Ok(Value::String(s.replace(from, to)))
}

// ── SPA-141: Math functions ───────────────────────────────────────────────────

fn fn_abs(args: Vec<Value>) -> Result<Value> {
    expect_arity("abs", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Int64(n) => Ok(Value::Int64(n.abs())),
        Value::Float64(f) => Ok(Value::Float64(f.abs())),
        other => Err(Error::InvalidArgument(format!(
            "abs(): expected numeric, got {other}"
        ))),
    }
}

fn fn_ceil(args: Vec<Value>) -> Result<Value> {
    expect_arity("ceil", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let f = as_float("ceil", &args[0])?;
    Ok(Value::Float64(f.ceil()))
}

fn fn_floor(args: Vec<Value>) -> Result<Value> {
    expect_arity("floor", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let f = as_float("floor", &args[0])?;
    Ok(Value::Float64(f.floor()))
}

fn fn_round(args: Vec<Value>) -> Result<Value> {
    expect_arity("round", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let f = as_float("round", &args[0])?;
    Ok(Value::Float64(f.round()))
}

fn fn_sqrt(args: Vec<Value>) -> Result<Value> {
    expect_arity("sqrt", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let f = as_float("sqrt", &args[0])?;
    Ok(Value::Float64(f.sqrt()))
}

/// `log(n)` — natural logarithm.
fn fn_log(args: Vec<Value>) -> Result<Value> {
    expect_arity("log", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let f = as_float("log", &args[0])?;
    Ok(Value::Float64(f.ln()))
}

fn fn_log10(args: Vec<Value>) -> Result<Value> {
    expect_arity("log10", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let f = as_float("log10", &args[0])?;
    Ok(Value::Float64(f.log10()))
}

fn fn_exp(args: Vec<Value>) -> Result<Value> {
    expect_arity("exp", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let f = as_float("exp", &args[0])?;
    Ok(Value::Float64(f.exp()))
}

fn fn_sign(args: Vec<Value>) -> Result<Value> {
    expect_arity("sign", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Int64(n) => Ok(Value::Int64(n.signum())),
        Value::Float64(f) => {
            let s = if *f > 0.0 {
                1i64
            } else if *f < 0.0 {
                -1
            } else {
                0
            };
            Ok(Value::Int64(s))
        }
        other => Err(Error::InvalidArgument(format!(
            "sign(): expected numeric, got {other}"
        ))),
    }
}

/// `rand()` — uniform random float in [0, 1).
fn fn_rand(args: Vec<Value>) -> Result<Value> {
    expect_arity("rand", &args, 0)?;
    // Use a simple LCG seeded from current time to avoid pulling in rand crate.
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(42);
    // LCG: same constants as glibc.
    let v = (seed as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let f = (v >> 11) as f64 / (1u64 << 53) as f64;
    Ok(Value::Float64(f))
}

// ── SPA-142: List functions ───────────────────────────────────────────────────

/// `range(start, end[, step])` — returns a list of integers.
///
/// Matches openCypher semantics: `range(0, 5)` returns `[0,1,2,3,4,5]`.
fn fn_range(args: Vec<Value>) -> Result<Value> {
    expect_min_arity("range", &args, 2)?;
    let start = as_int("range", &args[0])?;
    let end = as_int("range", &args[1])?;
    let step: i64 = if args.len() >= 3 {
        as_int("range", &args[2])?
    } else {
        1
    };
    if step == 0 {
        return Err(Error::InvalidArgument(
            "range(): step must not be zero".into(),
        ));
    }
    let mut values = Vec::new();
    if step > 0 {
        let mut i = start;
        while i <= end {
            values.push(Value::Int64(i));
            i += step;
        }
    } else {
        let mut i = start;
        while i >= end {
            values.push(Value::Int64(i));
            i += step;
        }
    }
    Ok(Value::List(values))
}

/// `head(list)` — first element of a list-like value.
///
/// Since `Value` has no `List` variant, this is a no-op that returns `Null`
/// unless the argument is already a scalar (in which case we return it).
/// When UNWIND is used the caller gets per-element rows; `head` is rarely
/// needed in that pattern.
fn fn_head(args: Vec<Value>) -> Result<Value> {
    expect_arity("head", &args, 1)?;
    // Without a List variant we cannot iterate — return Null.
    match &args[0] {
        Value::Null => Ok(Value::Null),
        // If someone passes a scalar, treat it as a single-element list.
        v => Ok(v.clone()),
    }
}

fn fn_tail(args: Vec<Value>) -> Result<Value> {
    expect_arity("tail", &args, 1)?;
    // Without a List variant there is no "rest" to return.
    Ok(Value::Null)
}

fn fn_last(args: Vec<Value>) -> Result<Value> {
    expect_arity("last", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        v => Ok(v.clone()),
    }
}

fn fn_reverse(args: Vec<Value>) -> Result<Value> {
    expect_arity("reverse", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::String(s.chars().rev().collect())),
        // For non-string scalars we cannot reverse in-place without a List type.
        v => Ok(v.clone()),
    }
}

fn fn_sort(args: Vec<Value>) -> Result<Value> {
    expect_arity("sort", &args, 1)?;
    // Without a List variant, sorting is a no-op.
    Ok(args.into_iter().next().unwrap_or(Value::Null))
}

fn fn_distinct(args: Vec<Value>) -> Result<Value> {
    expect_arity("distinct", &args, 1)?;
    Ok(args.into_iter().next().unwrap_or(Value::Null))
}

// ── SPA-143: Type conversion & predicate functions ────────────────────────────

fn fn_to_string(args: Vec<Value>) -> Result<Value> {
    expect_arity("toString", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::String(s.clone())),
        Value::Int64(n) => Ok(Value::String(n.to_string())),
        Value::Float64(f) => Ok(Value::String(f.to_string())),
        Value::Bool(b) => Ok(Value::String(b.to_string())),
        Value::NodeRef(id) => Ok(Value::String(format!("node({})", id.0))),
        Value::EdgeRef(id) => Ok(Value::String(format!("edge({})", id.0))),
        Value::List(items) => Ok(Value::String(format!(
            "{}",
            crate::types::Value::List(items.clone())
        ))),
        Value::Map(entries) => Ok(Value::String(format!(
            "{}",
            crate::types::Value::Map(entries.clone())
        ))),
        Value::Vector(v) => Ok(Value::String(format!("{:?}", v))),
    }
}

fn fn_to_integer(args: Vec<Value>) -> Result<Value> {
    expect_arity("toInteger", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Int64(n) => Ok(Value::Int64(*n)),
        Value::Float64(f) => Ok(Value::Int64(*f as i64)),
        Value::Bool(b) => Ok(Value::Int64(if *b { 1 } else { 0 })),
        Value::String(s) => {
            // Try integer parse first, then float.
            if let Ok(n) = s.trim().parse::<i64>() {
                Ok(Value::Int64(n))
            } else if let Ok(f) = s.trim().parse::<f64>() {
                Ok(Value::Int64(f as i64))
            } else {
                Ok(Value::Null) // openCypher: return null for non-parseable
            }
        }
        _ => Ok(Value::Null),
    }
}

fn fn_to_float(args: Vec<Value>) -> Result<Value> {
    expect_arity("toFloat", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Float64(f) => Ok(Value::Float64(*f)),
        Value::Int64(n) => Ok(Value::Float64(*n as f64)),
        Value::Bool(b) => Ok(Value::Float64(if *b { 1.0 } else { 0.0 })),
        Value::String(s) => {
            if let Ok(f) = s.trim().parse::<f64>() {
                Ok(Value::Float64(f))
            } else {
                Ok(Value::Null)
            }
        }
        _ => Ok(Value::Null),
    }
}

fn fn_to_boolean(args: Vec<Value>) -> Result<Value> {
    expect_arity("toBoolean", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Bool(b) => Ok(Value::Bool(*b)),
        Value::Int64(n) => Ok(Value::Bool(*n != 0)),
        Value::String(s) => match s.to_lowercase().as_str() {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => Ok(Value::Null),
        },
        _ => Ok(Value::Null),
    }
}

/// `type(rel)` — relationship type name.
///
/// Without a type-metadata lookup in Value, we return a placeholder string.
/// A real implementation would look up the rel table name from the catalog.
fn fn_type(args: Vec<Value>) -> Result<Value> {
    expect_arity("type", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::EdgeRef(_) => {
            // Catalog lookup would be needed for the actual type name.
            // Return a sentinel; callers should use the rel variable's type
            // from the query pattern directly.
            Ok(Value::String("UNKNOWN".into()))
        }
        other => Err(Error::InvalidArgument(format!(
            "type(): expected relationship, got {other}"
        ))),
    }
}

/// `labels(node)` — node labels.  Returns a stub string.
fn fn_labels(args: Vec<Value>) -> Result<Value> {
    expect_arity("labels", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::NodeRef(_) => {
            // Without schema lookup in Value, we cannot enumerate labels here.
            Ok(Value::String("[]".into()))
        }
        other => Err(Error::InvalidArgument(format!(
            "labels(): expected node, got {other}"
        ))),
    }
}

/// `keys(node|rel)` — property key names.  Returns a stub string.
fn fn_keys(args: Vec<Value>) -> Result<Value> {
    expect_arity("keys", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::NodeRef(_) | Value::EdgeRef(_) => Ok(Value::String("[]".into())),
        other => Err(Error::InvalidArgument(format!(
            "keys(): expected node or relationship, got {other}"
        ))),
    }
}

/// `properties(node|rel)` — returns a stub string.
fn fn_properties(args: Vec<Value>) -> Result<Value> {
    expect_arity("properties", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::NodeRef(_) | Value::EdgeRef(_) => Ok(Value::String("{}".into())),
        other => Err(Error::InvalidArgument(format!(
            "properties(): expected node or relationship, got {other}"
        ))),
    }
}

/// `id(node)` — node internal ID.
fn fn_id(args: Vec<Value>) -> Result<Value> {
    expect_arity("id", &args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::NodeRef(id) => Ok(Value::Int64(id.0 as i64)),
        Value::EdgeRef(id) => Ok(Value::Int64(id.0 as i64)),
        other => Err(Error::InvalidArgument(format!(
            "id(): expected node or relationship, got {other}"
        ))),
    }
}

/// `coalesce(expr, …)` — first non-null value.
fn fn_coalesce(args: Vec<Value>) -> Result<Value> {
    for v in args {
        if !matches!(v, Value::Null) {
            return Ok(v);
        }
    }
    Ok(Value::Null)
}

fn fn_is_null(args: Vec<Value>) -> Result<Value> {
    expect_arity("isNull", &args, 1)?;
    Ok(Value::Bool(matches!(args[0], Value::Null)))
}

fn fn_is_not_null(args: Vec<Value>) -> Result<Value> {
    expect_arity("isNotNull", &args, 1)?;
    Ok(Value::Bool(!matches!(args[0], Value::Null)))
}

// ── Temporal functions ─────────────────────────────────────────────────────────

/// `datetime()` / `timestamp()` — current UTC time as epoch milliseconds.
///
/// Returns `Value::Int64` with the number of milliseconds since the Unix epoch
/// (1970-01-01T00:00:00Z).  No arguments are accepted.
fn fn_datetime(args: Vec<Value>) -> Result<Value> {
    expect_arity("datetime", &args, 0)?;
    use std::time::{SystemTime, UNIX_EPOCH};
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    Ok(Value::Int64(millis))
}

/// `date()` — today as days since the Unix epoch.
///
/// Returns `Value::Int64`.  Computed directly from whole seconds to avoid
/// floating-point rounding errors.
fn fn_date(args: Vec<Value>) -> Result<Value> {
    expect_arity("date", &args, 0)?;
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    Ok(Value::Int64(secs / 86_400))
}

/// `duration(iso_string)` — minimal ISO-8601 duration stub.
///
/// Parses a small subset of ISO-8601 period strings and returns the
/// equivalent number of **milliseconds** as `Value::Int64`.
///
/// Supported tokens: `P`, `nY`, `nM`, `nW`, `nD`, `T`, `nH`, `nM`, `nS`.
/// Calendar approximations: 1 year ≈ 365 days, 1 month ≈ 30 days.
///
/// Unrecognised strings return `Err(InvalidArgument)`.
fn fn_duration(args: Vec<Value>) -> Result<Value> {
    expect_arity("duration", &args, 1)?;
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let s = as_string("duration", &args[0])?;
    let millis = parse_iso_duration(s).ok_or_else(|| {
        Error::InvalidArgument(format!("duration(): cannot parse ISO-8601 duration: {s}"))
    })?;
    Ok(Value::Int64(millis))
}

/// Parse a tiny subset of ISO-8601 duration strings → milliseconds.
fn parse_iso_duration(s: &str) -> Option<i64> {
    let s = s.trim();
    // Must start with 'P' or 'p'.
    let s = if s.starts_with(['P', 'p']) {
        &s[1..]
    } else {
        return None;
    };

    const MS_PER_SEC: i64 = 1_000;
    const MS_PER_MIN: i64 = 60 * MS_PER_SEC;
    const MS_PER_HOUR: i64 = 60 * MS_PER_MIN;
    const MS_PER_DAY: i64 = 24 * MS_PER_HOUR;
    const MS_PER_WEEK: i64 = 7 * MS_PER_DAY;
    const MS_PER_MONTH: i64 = 30 * MS_PER_DAY;
    const MS_PER_YEAR: i64 = 365 * MS_PER_DAY;

    let mut total: i64 = 0;
    let mut in_time = false;
    let mut buf = String::new();

    for ch in s.chars() {
        match ch {
            'T' | 't' => {
                in_time = true;
                buf.clear();
            }
            '0'..='9' | '.' => buf.push(ch),
            'Y' | 'y' if !in_time => {
                let n: i64 = buf.parse().ok()?;
                total += n * MS_PER_YEAR;
                buf.clear();
            }
            'M' | 'm' if !in_time => {
                let n: i64 = buf.parse().ok()?;
                total += n * MS_PER_MONTH;
                buf.clear();
            }
            'W' | 'w' if !in_time => {
                let n: i64 = buf.parse().ok()?;
                total += n * MS_PER_WEEK;
                buf.clear();
            }
            'D' | 'd' if !in_time => {
                let n: i64 = buf.parse().ok()?;
                total += n * MS_PER_DAY;
                buf.clear();
            }
            'H' | 'h' if in_time => {
                let n: i64 = buf.parse().ok()?;
                total += n * MS_PER_HOUR;
                buf.clear();
            }
            'M' | 'm' if in_time => {
                let n: i64 = buf.parse().ok()?;
                total += n * MS_PER_MIN;
                buf.clear();
            }
            'S' | 's' if in_time => {
                // Seconds may have a fractional part.
                let f: f64 = buf.parse().ok()?;
                total += (f * MS_PER_SEC as f64) as i64;
                buf.clear();
            }
            _ => return None,
        }
    }

    Some(total)
}

// ── Vector helper functions (issue #394) ─────────────────────────────────────

/// Extract and validate two equal-length vector arguments from `args`.
///
/// Returns `Err(InvalidArgument)` if either argument is not a vector/float-list,
/// or if the two vectors have different dimensions.
fn get_two_vectors(fn_name: &str, args: Vec<Value>) -> Result<(Vec<f32>, Vec<f32>)> {
    expect_arity(fn_name, &args, 2)?;
    let a = args[0].as_vector().ok_or_else(|| {
        Error::InvalidArgument(format!(
            "{fn_name}: first argument must be a vector or float list"
        ))
    })?;
    let b = args[1].as_vector().ok_or_else(|| {
        Error::InvalidArgument(format!(
            "{fn_name}: second argument must be a vector or float list"
        ))
    })?;
    if a.len() != b.len() {
        return Err(Error::InvalidArgument(format!(
            "{fn_name}: dimension mismatch ({} vs {})",
            a.len(),
            b.len()
        )));
    }
    Ok((a, b))
}

/// `vector_similarity(vec_a, vec_b)` → Float64 (cosine similarity).
fn fn_vector_similarity(args: Vec<Value>) -> Result<Value> {
    let (a, b) = get_two_vectors("vector_similarity", args)?;
    let sim = sparrowdb_storage::vector_index::cosine_similarity(&a, &b);
    Ok(Value::Float64(sim as f64))
}

/// `vector_distance(vec_a, vec_b)` → Float64 (Euclidean / L2 distance).
fn fn_vector_distance(args: Vec<Value>) -> Result<Value> {
    let (a, b) = get_two_vectors("vector_distance", args)?;
    let dist = sparrowdb_storage::vector_index::euclidean_distance(&a, &b);
    Ok(Value::Float64(dist as f64))
}

/// `vector_dot(vec_a, vec_b)` → Float64 (dot product).
fn fn_vector_dot(args: Vec<Value>) -> Result<Value> {
    let (a, b) = get_two_vectors("vector_dot", args)?;
    let dp = sparrowdb_storage::vector_index::dot_product(&a, &b);
    Ok(Value::Float64(dp as f64))
}

/// `vector([f, f, ...])` or `tofloatvector([f, f, ...])` — convert a list of numbers to a Vector value.
fn fn_to_float_vector(args: Vec<Value>) -> Result<Value> {
    expect_arity("vector", &args, 1)?;
    match args[0].as_vector() {
        Some(v) => Ok(Value::Vector(v)),
        None => Err(Error::InvalidArgument(
            "vector(): argument must be a list of numbers".into(),
        )),
    }
}

// ── Hybrid search fusion functions (issue #396) ───────────────────────────────

/// Extract a `Vec<(node_id: u64, score: f64)>` from a `Value::List` of `Value::Map` entries.
///
/// Each map must contain a `"node_id"` key (Int64) and a `"score"` key (Float64 or Int64).
/// Entries that are malformed are silently skipped so partial lists still work.
fn extract_scored_list(fn_name: &str, v: &Value) -> Result<Vec<(u64, f64)>> {
    let items = match v {
        Value::List(items) => items,
        _ => {
            return Err(Error::InvalidArgument(format!(
                "{fn_name}: expected a List of Maps, got {v}"
            )))
        }
    };

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        if let Value::Map(entries) = item {
            let node_id = entries
                .iter()
                .find(|(k, _)| k == "node_id")
                .and_then(|(_, v)| match v {
                    Value::Int64(n) => Some(*n as u64),
                    _ => None,
                });
            let score = entries
                .iter()
                .find(|(k, _)| k == "score")
                .and_then(|(_, v)| match v {
                    Value::Float64(f) => Some(*f),
                    Value::Int64(n) => Some(*n as f64),
                    _ => None,
                });
            if let (Some(nid), Some(sc)) = (node_id, score) {
                out.push((nid, sc));
            }
        }
    }
    Ok(out)
}

/// Build a `Value::List` of `Value::Map({node_id, score, rank})` from a scored list.
///
/// The list is already sorted by descending score before this is called.
fn build_result_list(scored: &[(u64, f64)]) -> Value {
    let items: Vec<Value> = scored
        .iter()
        .enumerate()
        .map(|(rank, &(node_id, score))| {
            Value::Map(vec![
                ("node_id".to_owned(), Value::Int64(node_id as i64)),
                ("score".to_owned(), Value::Float64(score)),
                ("rank".to_owned(), Value::Int64((rank + 1) as i64)),
            ])
        })
        .collect();
    Value::List(items)
}

/// `rrf_fusion(list1, list2[, k])` — Reciprocal Rank Fusion.
///
/// Computes `score(d) = 1/(k + rank1(d)) + 1/(k + rank2(d))` for each document
/// that appears in either list.  Documents absent from a list are assigned an
/// implicit rank equal to `list.len() + 1` (worst rank + 1).
///
/// Arguments:
/// - `list1` — `Value::List` of `Value::Map({node_id: Int64, score: Float64})`
/// - `list2` — same format
/// - `k`     — (optional, default 60) smoothing constant (Int64 or Float64)
///
/// Returns a `Value::List` of `Value::Map({node_id, score, rank})` sorted by
/// descending RRF score.
fn fn_rrf_fusion(args: Vec<Value>) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(Error::InvalidArgument(
            "rrf_fusion() expects 2 or 3 arguments: (list1, list2[, k])".into(),
        ));
    }

    let list1 = extract_scored_list("rrf_fusion", &args[0])?;
    let list2 = extract_scored_list("rrf_fusion", &args[1])?;
    let k: f64 = if args.len() == 3 {
        as_float("rrf_fusion", &args[2])?
    } else {
        60.0
    };

    if k <= 0.0 {
        return Err(Error::InvalidArgument(
            "rrf_fusion(): k must be positive".into(),
        ));
    }

    // Build rank maps: node_id → 1-based rank in each list.
    use std::collections::HashMap;
    let rank1: HashMap<u64, usize> = list1
        .iter()
        .enumerate()
        .map(|(i, &(nid, _))| (nid, i + 1))
        .collect();
    let rank2: HashMap<u64, usize> = list2
        .iter()
        .enumerate()
        .map(|(i, &(nid, _))| (nid, i + 1))
        .collect();

    // Collect all unique node IDs from both lists.
    let mut all_ids: Vec<u64> = rank1.keys().copied().collect();
    for &(nid, _) in &list2 {
        if !rank1.contains_key(&nid) {
            all_ids.push(nid);
        }
    }

    // Worst-rank sentinels (absent from one list = rank len + 1).
    let worst1 = list1.len() + 1;
    let worst2 = list2.len() + 1;

    // Compute RRF scores.
    let mut scored: Vec<(u64, f64)> = all_ids
        .into_iter()
        .map(|nid| {
            let r1 = *rank1.get(&nid).unwrap_or(&worst1) as f64;
            let r2 = *rank2.get(&nid).unwrap_or(&worst2) as f64;
            let score = 1.0 / (k + r1) + 1.0 / (k + r2);
            (nid, score)
        })
        .collect();

    // Sort: descending score, then ascending node_id for determinism.
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });

    Ok(build_result_list(&scored))
}

/// `weighted_fusion(list1, list2, alpha)` — alpha-weighted score fusion.
///
/// Normalises each list's scores to [0, 1] (min-max), then combines:
/// `score(d) = alpha * norm_score1(d) + (1 - alpha) * norm_score2(d)`.
///
/// Arguments:
/// - `list1`  — vector results: `Value::List<Value::Map({node_id, score})>`
/// - `list2`  — FTS results in the same format
/// - `alpha`  — weight for list1 in [0, 1]; `1 - alpha` is the weight for list2
///
/// Returns a `Value::List` of `Value::Map({node_id, score, rank})` sorted by
/// descending weighted score.
fn fn_weighted_fusion(args: Vec<Value>) -> Result<Value> {
    expect_arity("weighted_fusion", &args, 3)?;

    let list1 = extract_scored_list("weighted_fusion", &args[0])?;
    let list2 = extract_scored_list("weighted_fusion", &args[1])?;
    let alpha = as_float("weighted_fusion", &args[2])?;

    if !(0.0..=1.0).contains(&alpha) {
        return Err(Error::InvalidArgument(
            "weighted_fusion(): alpha must be in [0, 1]".into(),
        ));
    }

    // Min-max normalise a scored list to [0, 1].
    let normalise = |list: &[(u64, f64)]| -> Vec<(u64, f64)> {
        if list.is_empty() {
            return vec![];
        }
        let min = list.iter().map(|(_, s)| *s).fold(f64::INFINITY, f64::min);
        let max = list
            .iter()
            .map(|(_, s)| *s)
            .fold(f64::NEG_INFINITY, f64::max);
        let range = max - min;
        list.iter()
            .map(|&(nid, s)| {
                let norm = if range < f64::EPSILON {
                    1.0
                } else {
                    (s - min) / range
                };
                (nid, norm)
            })
            .collect()
    };

    let norm1 = normalise(&list1);
    let norm2 = normalise(&list2);

    use std::collections::HashMap;
    let map1: HashMap<u64, f64> = norm1.into_iter().collect();
    let map2: HashMap<u64, f64> = norm2.into_iter().collect();

    // Union of all IDs.
    let mut all_ids: Vec<u64> = map1.keys().copied().collect();
    for &k in map2.keys() {
        if !map1.contains_key(&k) {
            all_ids.push(k);
        }
    }

    let mut scored: Vec<(u64, f64)> = all_ids
        .into_iter()
        .map(|nid| {
            let s1 = map1.get(&nid).copied().unwrap_or(0.0);
            let s2 = map2.get(&nid).copied().unwrap_or(0.0);
            (nid, alpha * s1 + (1.0 - alpha) * s2)
        })
        .collect();

    // Sort: descending score, then ascending node_id for determinism.
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });

    Ok(build_result_list(&scored))
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(node_id: u64, score: f64) -> Value {
        Value::Map(vec![
            ("node_id".to_owned(), Value::Int64(node_id as i64)),
            ("score".to_owned(), Value::Float64(score)),
        ])
    }

    fn make_list(entries: Vec<Value>) -> Value {
        Value::List(entries)
    }

    fn extract_ids_in_order(result: &Value) -> Vec<u64> {
        match result {
            Value::List(items) => items
                .iter()
                .filter_map(|item| match item {
                    Value::Map(kvs) => {
                        kvs.iter()
                            .find(|(k, _)| k == "node_id")
                            .and_then(|(_, v)| match v {
                                Value::Int64(n) => Some(*n as u64),
                                _ => None,
                            })
                    }
                    _ => None,
                })
                .collect(),
            _ => panic!("expected List, got {result:?}"),
        }
    }

    fn score_for(result: &Value, node_id: u64) -> Option<f64> {
        match result {
            Value::List(items) => items.iter().find_map(|item| match item {
                Value::Map(kvs) => {
                    let nid = kvs
                        .iter()
                        .find(|(k, _)| k == "node_id")
                        .and_then(|(_, v)| match v {
                            Value::Int64(n) => Some(*n as u64),
                            _ => None,
                        });
                    let sc = kvs
                        .iter()
                        .find(|(k, _)| k == "score")
                        .and_then(|(_, v)| match v {
                            Value::Float64(f) => Some(*f),
                            _ => None,
                        });
                    if nid == Some(node_id) {
                        sc
                    } else {
                        None
                    }
                }
                _ => None,
            }),
            _ => None,
        }
    }

    // ── rrf_fusion ────────────────────────────────────────────────────────────

    #[test]
    fn rrf_fusion_basic_two_lists() {
        // list1: node 1 (rank 1), node 2 (rank 2), node 3 (rank 3)
        // list2: node 3 (rank 1), node 1 (rank 2), node 4 (rank 3)
        // With k=60:
        //   node 1: 1/61 + 1/62 ≈ 0.032522 (top)
        //   node 3: 1/63 + 1/61 ≈ 0.032266 (second)
        //   node 2: 1/62 + 1/64 ≈ 0.031754 (third)
        //   node 4: 1/64 + 1/63 ≈ 0.031498 (last)
        let list1 = make_list(vec![entry(1, 0.9), entry(2, 0.8), entry(3, 0.7)]);
        let list2 = make_list(vec![entry(3, 0.9), entry(1, 0.8), entry(4, 0.7)]);

        let result = dispatch_function("rrf_fusion", vec![list1, list2]).expect("rrf_fusion");
        let ids = extract_ids_in_order(&result);

        assert_eq!(ids.len(), 4, "all 4 unique nodes should appear");
        assert_eq!(ids[0], 1, "node 1 should rank first");
        assert_eq!(ids[1], 3, "node 3 should rank second");
        assert_eq!(ids[2], 2, "node 2 should rank third");
        assert_eq!(ids[3], 4, "node 4 should rank last");
    }

    #[test]
    fn rrf_fusion_custom_k() {
        // k=1: each node is rank-1 in one list, rank-2 in the other → symmetric tie
        // tie-broken by ascending node_id → 10 before 20
        let list1 = make_list(vec![entry(10, 1.0), entry(20, 0.5)]);
        let list2 = make_list(vec![entry(20, 1.0), entry(10, 0.5)]);

        let result = dispatch_function("rrf_fusion", vec![list1, list2, Value::Float64(1.0)])
            .expect("rrf_fusion k=1");
        let ids = extract_ids_in_order(&result);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], 10);
        assert_eq!(ids[1], 20);
    }

    #[test]
    fn rrf_fusion_empty_lists() {
        let empty = make_list(vec![]);
        let result =
            dispatch_function("rrf_fusion", vec![empty.clone(), empty]).expect("rrf_fusion empty");
        assert_eq!(
            extract_ids_in_order(&result).len(),
            0,
            "empty lists produce empty result"
        );
    }

    #[test]
    fn rrf_fusion_disjoint_lists() {
        // No overlap: rank-1 nodes in both lists tie → resolved by node_id asc.
        let list1 = make_list(vec![entry(1, 1.0), entry(2, 0.5)]);
        let list2 = make_list(vec![entry(3, 1.0), entry(4, 0.5)]);

        let result =
            dispatch_function("rrf_fusion", vec![list1, list2]).expect("rrf_fusion disjoint");
        let ids = extract_ids_in_order(&result);
        assert_eq!(ids.len(), 4);
        assert_eq!(ids[0], 1);
        assert_eq!(ids[1], 3);
    }

    #[test]
    fn rrf_fusion_arity_error() {
        let list = make_list(vec![entry(1, 1.0)]);
        assert!(
            dispatch_function("rrf_fusion", vec![list]).is_err(),
            "single-argument call must error"
        );
    }

    // ── weighted_fusion ───────────────────────────────────────────────────────

    #[test]
    fn weighted_fusion_alpha_one_favors_list1() {
        let list1 = make_list(vec![entry(1, 1.0), entry(2, 0.0)]);
        let list2 = make_list(vec![entry(2, 1.0), entry(1, 0.0)]);

        let result = dispatch_function("weighted_fusion", vec![list1, list2, Value::Float64(1.0)])
            .expect("weighted_fusion alpha=1");
        let ids = extract_ids_in_order(&result);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], 1, "alpha=1 should rank list1's top item first");
    }

    #[test]
    fn weighted_fusion_alpha_zero_favors_list2() {
        let list1 = make_list(vec![entry(1, 1.0), entry(2, 0.0)]);
        let list2 = make_list(vec![entry(2, 1.0), entry(1, 0.0)]);

        let result = dispatch_function("weighted_fusion", vec![list1, list2, Value::Float64(0.0)])
            .expect("weighted_fusion alpha=0");
        let ids = extract_ids_in_order(&result);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], 2, "alpha=0 should rank list2's top item first");
    }

    #[test]
    fn weighted_fusion_alpha_half_tie_breaks_by_node_id() {
        // Symmetric at alpha=0.5 → both nodes get 0.5 → tie → node_id asc
        let list1 = make_list(vec![entry(1, 1.0), entry(2, 0.0)]);
        let list2 = make_list(vec![entry(2, 1.0), entry(1, 0.0)]);

        let result = dispatch_function("weighted_fusion", vec![list1, list2, Value::Float64(0.5)])
            .expect("weighted_fusion alpha=0.5");
        let ids = extract_ids_in_order(&result);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], 1);
        assert_eq!(ids[1], 2);
    }

    #[test]
    fn weighted_fusion_known_scores() {
        // list1: node 100=0.8, node 200=0.4  → norm: 100→1.0, 200→0.0
        // list2: node 200=0.9, node 100=0.6  → norm: 200→1.0, 100→0.0
        // alpha=0.7: node100 = 0.7*1.0 + 0.3*0.0 = 0.7
        //            node200 = 0.7*0.0 + 0.3*1.0 = 0.3
        let list1 = make_list(vec![entry(100, 0.8), entry(200, 0.4)]);
        let list2 = make_list(vec![entry(200, 0.9), entry(100, 0.6)]);

        let result = dispatch_function("weighted_fusion", vec![list1, list2, Value::Float64(0.7)])
            .expect("weighted_fusion known scores");

        let score_a = score_for(&result, 100).expect("node 100 in result");
        let score_b = score_for(&result, 200).expect("node 200 in result");
        assert!(
            (score_a - 0.7).abs() < 1e-9,
            "node 100 expected 0.7, got {score_a}"
        );
        assert!(
            (score_b - 0.3).abs() < 1e-9,
            "node 200 expected 0.3, got {score_b}"
        );
        let ids = extract_ids_in_order(&result);
        assert_eq!(ids[0], 100, "node 100 should rank first");
    }

    #[test]
    fn weighted_fusion_arity_error() {
        let list = make_list(vec![entry(1, 1.0)]);
        assert!(
            dispatch_function("weighted_fusion", vec![list.clone(), list]).is_err(),
            "two-argument call must error (needs alpha)"
        );
    }

    #[test]
    fn weighted_fusion_invalid_alpha() {
        let list = make_list(vec![entry(1, 1.0)]);
        assert!(
            dispatch_function(
                "weighted_fusion",
                vec![list.clone(), list, Value::Float64(1.5)]
            )
            .is_err(),
            "alpha > 1.0 must error"
        );
    }
}
