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
