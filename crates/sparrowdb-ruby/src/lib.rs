//! Magnus Ruby bindings for SparrowDB.
//!
//! Exposes [`RbGraphDb`], [`RbReadTx`], and [`RbWriteTx`] to Ruby under the
//! module `SparrowDB`.
//!
//! The `#[magnus::init]` function registers the module, classes, and custom
//! exception hierarchy on load.
//!
//! # Ruby API
//!
//! ```ruby
//! require 'sparrowdb'
//!
//! db = SparrowDB::GraphDb.new('/tmp/my.db')
//! rows = db.execute('MATCH (n:Person) RETURN n.name, n.age')
//! # => [{"n.name" => "Alice", "n.age" => 30}, ...]
//!
//! tx = db.begin_write
//! txn_id = tx.commit   # => Integer
//!
//! db.checkpoint
//! db.optimize
//! db.close
//! ```

use magnus::{
    function, method,
    prelude::*,
    rb_sys::{AsRawValue, FromRawValue},
    Error as MagnusError, ExceptionClass, Ruby, Value,
};
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

// ── Cached exception class VALUEs ─────────────────────────────────────────────
//
// Ruby constants (class objects) are GC-roots pinned for the process lifetime,
// so their VALUE pointer is stable.  We store the raw `usize` so that the
// static itself is `Sync`.  Writing happens once in `init`; all subsequent
// reads are after that point.

static SPARROW_ERROR_VALUE: AtomicUsize = AtomicUsize::new(0);
static WRITER_BUSY_VALUE: AtomicUsize = AtomicUsize::new(0);

/// Reconstruct `SparrowDB::Error` as an `ExceptionClass` from the cached VALUE.
///
/// # Panics
/// Panics if called before `#[magnus::init]` has run.
fn sparrow_error() -> ExceptionClass {
    let raw = SPARROW_ERROR_VALUE.load(Ordering::Acquire) as rb_sys::VALUE;
    debug_assert!(raw != 0, "SparrowDB::Error not yet initialized");
    // SAFETY: `raw` was stored from a live ExceptionClass returned by
    // `define_error` inside `init`.  Ruby class constants are pinned GC roots.
    let val = unsafe { Value::from_raw(raw) };
    ExceptionClass::from_value(val)
        .expect("SparrowDB::Error is not an exception class — this is a bug")
}

/// Reconstruct `SparrowDB::WriterBusy` as an `ExceptionClass` from the cached VALUE.
fn writer_busy() -> ExceptionClass {
    let raw = WRITER_BUSY_VALUE.load(Ordering::Acquire) as rb_sys::VALUE;
    debug_assert!(raw != 0, "SparrowDB::WriterBusy not yet initialized");
    let val = unsafe { Value::from_raw(raw) };
    ExceptionClass::from_value(val)
        .expect("SparrowDB::WriterBusy is not an exception class — this is a bug")
}

// ── Error conversion ──────────────────────────────────────────────────────────

/// Convert a `sparrowdb_common::Error` into a Magnus `Error`.
///
/// * `WriterBusy` → `SparrowDB::WriterBusy`
/// * Everything else → `SparrowDB::Error`
fn db_err(e: sparrowdb_common::Error) -> MagnusError {
    let msg = e.to_string();
    match e {
        sparrowdb_common::Error::WriterBusy => MagnusError::new(writer_busy(), msg),
        _ => MagnusError::new(sparrow_error(), msg),
    }
}

// ── Value → Ruby conversion ───────────────────────────────────────────────────

/// Convert a `sparrowdb_execution::Value` to a Magnus Ruby `Value`.
fn value_to_rb(ruby: &Ruby, v: &sparrowdb_execution::Value) -> Value {
    use sparrowdb_execution::Value as Ev;
    match v {
        Ev::Null => ruby.qnil().as_value(),
        Ev::Int64(i) => ruby.integer_from_i64(*i).as_value(),
        Ev::Float64(f) => ruby.float_from_f64(*f).as_value(),
        Ev::Bool(b) => {
            if *b {
                ruby.qtrue().as_value()
            } else {
                ruby.qfalse().as_value()
            }
        }
        Ev::String(s) => ruby.str_new(s).as_value(),
        // NodeRef / EdgeRef: expose the packed u64 id as an Integer.
        Ev::NodeRef(n) => ruby.integer_from_u64(n.0).as_value(),
        Ev::EdgeRef(e) => ruby.integer_from_u64(e.0).as_value(),
        // List: produced by collect() aggregation — convert to Ruby Array.
        Ev::List(items) => {
            let arr = ruby.ary_new_capa(items.len());
            for item in items {
                let _ = arr.push(value_to_rb(ruby, item));
            }
            arr.as_value()
        }
    }
}

// ── RbGraphDb ─────────────────────────────────────────────────────────────────

/// Ruby wrapper around the top-level SparrowDB handle.
#[magnus::wrap(class = "SparrowDB::GraphDb", free_immediately, size)]
struct RbGraphDb {
    inner: RefCell<Option<::sparrowdb::GraphDb>>,
}

impl RbGraphDb {
    /// Open (or create) a SparrowDB database at *path*.
    fn open(path: String) -> Result<Self, MagnusError> {
        let db = ::sparrowdb::GraphDb::open(std::path::Path::new(&path)).map_err(db_err)?;
        Ok(RbGraphDb {
            inner: RefCell::new(Some(db)),
        })
    }

    fn with_db<F, T>(&self, f: F) -> Result<T, MagnusError>
    where
        F: FnOnce(&::sparrowdb::GraphDb) -> Result<T, MagnusError>,
    {
        let borrow = self.inner.borrow();
        match borrow.as_ref() {
            Some(db) => f(db),
            None => Err(MagnusError::new(sparrow_error(), "database is closed")),
        }
    }

    /// Fold the WAL delta log into the CSR base files.
    fn checkpoint(&self) -> Result<(), MagnusError> {
        self.with_db(|db| db.checkpoint().map_err(db_err))
    }

    /// Like `checkpoint` but also sorts neighbour lists for faster scans.
    fn optimize(&self) -> Result<(), MagnusError> {
        self.with_db(|db| db.optimize().map_err(db_err))
    }

    /// Open a read-only snapshot transaction.
    fn begin_read(&self) -> Result<RbReadTx, MagnusError> {
        self.with_db(|db| {
            let tx = db.begin_read().map_err(db_err)?;
            Ok(RbReadTx {
                inner: RefCell::new(Some(tx)),
            })
        })
    }

    /// Open a write transaction.
    ///
    /// Raises `SparrowDB::WriterBusy` if another writer is active.
    fn begin_write(&self) -> Result<RbWriteTx, MagnusError> {
        self.with_db(|db| {
            let tx = db.begin_write().map_err(db_err)?;
            Ok(RbWriteTx {
                inner: RefCell::new(Some(tx)),
            })
        })
    }

    /// Run a Cypher query and return an `Array` of `Hash` rows.
    ///
    /// Column names are `String` keys; values are `nil`, `Integer`, `Float`,
    /// `true`/`false`, or `String`.  `NodeRef` / `EdgeRef` are `Integer`.
    fn execute(&self, cypher: String) -> Result<Value, MagnusError> {
        let result = self.with_db(|db| db.execute(&cypher).map_err(db_err))?;

        let ruby = Ruby::get().map_err(|e| MagnusError::new(sparrow_error(), e.to_string()))?;

        let outer = ruby.ary_new_capa(result.rows.len());
        for row in &result.rows {
            let hash = ruby.hash_new();
            for (col, val) in result.columns.iter().zip(row.iter()) {
                hash.aset(ruby.str_new(col), value_to_rb(&ruby, val))
                    .map_err(|e| MagnusError::new(sparrow_error(), format!("{e}")))?;
            }
            outer
                .push(hash.as_value())
                .map_err(|e| MagnusError::new(sparrow_error(), format!("{e}")))?;
        }
        Ok(outer.as_value())
    }

    /// Close the database and release all resources.
    fn close(&self) -> Result<(), MagnusError> {
        *self.inner.borrow_mut() = None;
        Ok(())
    }

    fn inspect(&self) -> &'static str {
        "#<SparrowDB::GraphDb>"
    }
}

// ── RbReadTx ──────────────────────────────────────────────────────────────────

/// Ruby wrapper around a read-only snapshot transaction.
#[magnus::wrap(class = "SparrowDB::ReadTx", free_immediately, size)]
struct RbReadTx {
    inner: RefCell<Option<::sparrowdb::ReadTx>>,
}

impl RbReadTx {
    /// The committed `txn_id` this reader is pinned to.
    fn snapshot_txn_id(&self) -> Result<u64, MagnusError> {
        let borrow = self.inner.borrow();
        match borrow.as_ref() {
            Some(tx) => Ok(tx.snapshot_txn_id),
            None => Err(MagnusError::new(
                sparrow_error(),
                "read transaction already released",
            )),
        }
    }

    fn inspect(&self) -> String {
        match self.inner.borrow().as_ref() {
            Some(tx) => format!(
                "#<SparrowDB::ReadTx snapshot_txn_id={}>",
                tx.snapshot_txn_id
            ),
            None => "#<SparrowDB::ReadTx (released)>".to_string(),
        }
    }
}

// ── RbWriteTx ─────────────────────────────────────────────────────────────────

/// Ruby wrapper around a write transaction.
///
/// Call {#commit} to persist changes; dropping without committing is a
/// no-op rollback.
#[magnus::wrap(class = "SparrowDB::WriteTx", free_immediately, size)]
struct RbWriteTx {
    inner: RefCell<Option<::sparrowdb::WriteTx>>,
}

impl RbWriteTx {
    /// Commit the transaction and return the new `txn_id` as an `Integer`.
    ///
    /// Raises `RuntimeError` if already committed or rolled back.
    fn commit(&self) -> Result<u64, MagnusError> {
        match self.inner.borrow_mut().take() {
            Some(tx) => Ok(tx.commit().map_err(db_err)?.0),
            None => {
                let ruby = Ruby::get().map_err(|e| {
                    MagnusError::new(sparrow_error(), e.to_string())
                })?;
                Err(MagnusError::new(
                    ruby.exception_runtime_error(),
                    "transaction already committed or rolled back",
                ))
            }
        }
    }

    fn inspect(&self) -> &'static str {
        "#<SparrowDB::WriteTx>"
    }
}

// ── Module entry point ────────────────────────────────────────────────────────

/// Magnus init function — called by Ruby when the native extension is required.
#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), MagnusError> {
    let module = ruby.define_module("SparrowDB")?;

    // ── Exception hierarchy ───────────────────────────────────────────────────
    // SparrowDB::Error < StandardError
    let base_error = module.define_error("Error", ruby.exception_standard_error())?;
    SPARROW_ERROR_VALUE.store(base_error.as_raw() as usize, Ordering::Release);

    // SparrowDB::WriterBusy < SparrowDB::Error
    let writer_busy_cls = module.define_error("WriterBusy", base_error)?;
    WRITER_BUSY_VALUE.store(writer_busy_cls.as_raw() as usize, Ordering::Release);

    // ── SparrowDB::GraphDb ────────────────────────────────────────────────────
    let db_class = module.define_class("GraphDb", ruby.class_object())?;
    db_class.define_singleton_method("new", function!(RbGraphDb::open, 1))?;
    db_class.define_method("checkpoint", method!(RbGraphDb::checkpoint, 0))?;
    db_class.define_method("optimize", method!(RbGraphDb::optimize, 0))?;
    db_class.define_method("begin_read", method!(RbGraphDb::begin_read, 0))?;
    db_class.define_method("begin_write", method!(RbGraphDb::begin_write, 0))?;
    db_class.define_method("execute", method!(RbGraphDb::execute, 1))?;
    db_class.define_method("close", method!(RbGraphDb::close, 0))?;
    db_class.define_method("inspect", method!(RbGraphDb::inspect, 0))?;
    db_class.define_method("to_s", method!(RbGraphDb::inspect, 0))?;

    // ── SparrowDB::ReadTx ─────────────────────────────────────────────────────
    let read_tx_class = module.define_class("ReadTx", ruby.class_object())?;
    read_tx_class.define_method("snapshot_txn_id", method!(RbReadTx::snapshot_txn_id, 0))?;
    read_tx_class.define_method("inspect", method!(RbReadTx::inspect, 0))?;
    read_tx_class.define_method("to_s", method!(RbReadTx::inspect, 0))?;

    // ── SparrowDB::WriteTx ────────────────────────────────────────────────────
    let write_tx_class = module.define_class("WriteTx", ruby.class_object())?;
    write_tx_class.define_method("commit", method!(RbWriteTx::commit, 0))?;
    write_tx_class.define_method("inspect", method!(RbWriteTx::inspect, 0))?;
    write_tx_class.define_method("to_s", method!(RbWriteTx::inspect, 0))?;

    Ok(())
}

// ── Tests (no Ruby runtime needed) ───────────────────────────────────────────

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {
        // Verify the crate builds without a live Ruby VM.
    }
}
