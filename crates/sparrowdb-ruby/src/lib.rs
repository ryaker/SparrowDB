//! Magnus Ruby bindings for SparrowDB.
//!
//! Exposes [`RbGraphDb`], [`RbReadTx`], and [`RbWriteTx`] to Ruby under the
//! module `SparrowDB` with classes `GraphDb`, `ReadTx`, and `WriteTx`.
//!
//! Build with `bundle exec rake build` or `gem build sparrowdb.gemspec` to
//! produce the native gem.  The `ruby` feature is required; it is activated
//! automatically by `rb_sys` via `extconf.rb`.

// ── Ruby feature guard ────────────────────────────────────────────────────────

#[cfg(feature = "ruby")]
use magnus::{
    class, define_class, define_module, error::Error, exception, method,
    prelude::*, typed_data::Obj, value::Value as RubyValue, Ruby,
};

// ── Value → Ruby conversion ───────────────────────────────────────────────────

/// Convert a `sparrowdb_execution::Value` into a Ruby object.
#[cfg(feature = "ruby")]
fn value_to_ruby(ruby: &Ruby, v: &sparrowdb_execution::Value) -> RubyValue {
    use sparrowdb_execution::Value;
    match v {
        Value::Null => ruby.qnil().as_value(),
        Value::Int64(i) => ruby.integer_from_i64(*i).as_value(),
        Value::Float64(f) => ruby.float_from_f64(*f).as_value(),
        Value::Bool(b) => {
            if *b {
                ruby.qtrue().as_value()
            } else {
                ruby.qfalse().as_value()
            }
        }
        Value::String(s) => ruby.str_new(s).as_value(),
        // NodeRef / EdgeRef: expose the packed u64 id as an Integer.
        Value::NodeRef(n) => ruby.integer_from_u64(n.0).as_value(),
        Value::EdgeRef(e) => ruby.integer_from_u64(e.0).as_value(),
    }
}

// ── WriterBusy exception ──────────────────────────────────────────────────────

/// Define and return the `SparrowDB::WriterBusy` exception class.
#[cfg(feature = "ruby")]
fn writer_busy_class(ruby: &Ruby) -> Result<magnus::ExceptionClass, Error> {
    let module = ruby.get_inner_ref(&*MODULE);
    let runtime_error = exception::runtime_error();
    magnus::ExceptionClass::new_subclass(ruby, module, "WriterBusy", runtime_error)
        .or_else(|_| {
            // Already defined — retrieve it.
            let existing: magnus::ExceptionClass = module
                .const_get("WriterBusy")
                .map_err(|e| Error::new(exception::runtime_error(), format!("{e}")))?;
            Ok(existing)
        })
}

// ── Module handle (lazy static) ───────────────────────────────────────────────

#[cfg(feature = "ruby")]
static MODULE: std::sync::OnceLock<magnus::value::OpaqueObject> = std::sync::OnceLock::new();

// ── RbGraphDb ─────────────────────────────────────────────────────────────────

/// Ruby wrapper around the top-level SparrowDB handle.
///
/// ```ruby
/// require 'sparrowdb'
/// db = SparrowDB::GraphDb.new('/tmp/my.db')
/// db.checkpoint
/// db.optimize
/// ```
#[cfg(feature = "ruby")]
#[magnus::wrap(class = "SparrowDB::GraphDb", free_immediately, size)]
struct RbGraphDb {
    inner: ::sparrowdb::GraphDb,
}

#[cfg(feature = "ruby")]
impl RbGraphDb {
    /// Open (or create) a SparrowDB database at *path*.
    fn new(path: String) -> Result<Self, Error> {
        let db = ::sparrowdb::GraphDb::open(std::path::Path::new(&path))
            .map_err(|e| Error::new(exception::runtime_error(), e.to_string()))?;
        Ok(RbGraphDb { inner: db })
    }

    /// Run a WAL checkpoint — folds the delta log into the CSR base files.
    fn checkpoint(&self) -> Result<(), Error> {
        self.inner
            .checkpoint()
            .map_err(|e| Error::new(exception::runtime_error(), e.to_string()))
    }

    /// Run an OPTIMIZE — like checkpoint but also sorts neighbour lists.
    fn optimize(&self) -> Result<(), Error> {
        self.inner
            .optimize()
            .map_err(|e| Error::new(exception::runtime_error(), e.to_string()))
    }

    /// Open a read-only snapshot transaction.
    fn begin_read(ruby: &Ruby, rb_self: Obj<RbGraphDb>) -> Result<RbReadTx, Error> {
        let db = rb_self.get();
        let tx = db
            .inner
            .begin_read()
            .map_err(|e| Error::new(exception::runtime_error(), e.to_string()))?;
        Ok(RbReadTx { inner: tx })
    }

    /// Open a write transaction.
    ///
    /// Raises `SparrowDB::WriterBusy` if another write transaction is already
    /// active (single-writer semantics).
    fn begin_write(ruby: &Ruby, rb_self: Obj<RbGraphDb>) -> Result<RbWriteTx, Error> {
        let db = rb_self.get();
        // Safety: same rationale as the Python binding — we transmute
        // WriteTx<'_> to WriteTx<'static> so it can be stored in the Ruby
        // wrapper.  This is safe because:
        // 1. The `'db` lifetime on WriteTx only constrains the MutexGuard
        //    which holds the write-lock on `inner.write_lock`.
        // 2. `RbGraphDb` holds `inner: GraphDb` (containing Arc<DbInner>)
        //    which keeps the Mutex alive for the whole lifetime of the db
        //    object.
        // 3. Ruby's GC will drop RbWriteTx before RbGraphDb because
        //    RbWriteTx holds an Obj<RbGraphDb> reference keeping the db
        //    alive, so the guard is released before the Mutex is freed.
        let tx: ::sparrowdb::WriteTx<'static> = unsafe {
            std::mem::transmute(
                db.inner
                    .begin_write()
                    .map_err(|e| {
                        // Map WriterBusy to SparrowDB::WriterBusy exception.
                        let msg = e.to_string();
                        if msg.contains("WriterBusy") || msg.contains("writer") {
                            if let Ok(cls) = writer_busy_class(ruby) {
                                return Error::new(cls, msg);
                            }
                        }
                        Error::new(exception::runtime_error(), msg)
                    })?,
            )
        };
        Ok(RbWriteTx { inner: Some(tx) })
    }

    /// Execute a read-only Cypher query and return an `Array` of `Hash`es.
    ///
    /// Each hash maps column name (String) to value.  Supported value types:
    /// `nil`, `Integer`, `Float`, `true`/`false`, `String`.
    /// `NodeRef` and `EdgeRef` are returned as `Integer` (their packed id).
    ///
    /// Example:
    ///
    /// ```ruby
    /// rows = db.execute('MATCH (n:Person) RETURN n.name, n.age')
    /// rows.each { |row| puts row }
    /// ```
    fn execute(ruby: &Ruby, rb_self: Obj<RbGraphDb>, cypher: String) -> Result<RubyValue, Error> {
        let db = rb_self.get();
        let result = db
            .inner
            .execute(&cypher)
            .map_err(|e| Error::new(exception::runtime_error(), e.to_string()))?;

        let array = ruby.ary_new();
        for row in &result.rows {
            let hash = ruby.hash_new();
            for (col, val) in result.columns.iter().zip(row.iter()) {
                let key = ruby.str_new(col);
                let value = value_to_ruby(ruby, val);
                hash.aset(key, value)
                    .map_err(|e| Error::new(exception::runtime_error(), format!("{e}")))?;
            }
            array
                .push(hash)
                .map_err(|e| Error::new(exception::runtime_error(), format!("{e}")))?;
        }
        Ok(array.as_value())
    }

    fn inspect(&self) -> String {
        "#<SparrowDB::GraphDb>".to_string()
    }
}

// ── RbReadTx ──────────────────────────────────────────────────────────────────

/// Ruby wrapper around a read-only snapshot transaction.
///
/// Obtained via `GraphDb#begin_read`.
#[cfg(feature = "ruby")]
#[magnus::wrap(class = "SparrowDB::ReadTx", free_immediately, size)]
struct RbReadTx {
    inner: ::sparrowdb::ReadTx,
}

// ReadTx contains Arc<DbInner> (Send) + NodeStore (file handles opened
// per-operation).  Cross-thread use is safe in practice.
#[cfg(feature = "ruby")]
unsafe impl Send for RbReadTx {}

#[cfg(feature = "ruby")]
impl RbReadTx {
    /// The committed `txn_id` this reader is pinned to.
    fn snapshot_txn_id(&self) -> u64 {
        self.inner.snapshot_txn_id
    }

    fn inspect(&self) -> String {
        format!("#<SparrowDB::ReadTx snapshot_txn_id={}>", self.inner.snapshot_txn_id)
    }
}

// ── RbWriteTx ─────────────────────────────────────────────────────────────────

/// Ruby wrapper around a write transaction.
///
/// Obtained via `GraphDb#begin_write`.  Commit explicitly with `#commit`;
/// letting the object be garbage-collected without committing discards staged
/// changes (rollback semantics).
#[cfg(feature = "ruby")]
#[magnus::wrap(class = "SparrowDB::WriteTx", free_immediately, size)]
struct RbWriteTx {
    /// `None` after commit/rollback so use-after-commit is detected.
    inner: Option<::sparrowdb::WriteTx<'static>>,
}

#[cfg(feature = "ruby")]
impl RbWriteTx {
    /// Commit the transaction and return the new transaction id (`Integer`).
    ///
    /// Raises `RuntimeError` if already committed or rolled back.
    fn commit(rb_self: Obj<RbWriteTx>) -> Result<u64, Error> {
        // SAFETY: We need mutable access.  Magnus wraps values in Obj which
        // provides shared access; we use get_mut_unchecked here because the
        // WriteTx is !Send and the Ruby GVL ensures single-threaded access.
        let tx_ref = rb_self.get();
        // We can't call get_mut on Obj<RbWriteTx> directly, so we use a
        // raw pointer approach protected by the GVL.
        let tx_ptr = tx_ref as *const RbWriteTx as *mut RbWriteTx;
        let tx_mut = unsafe { &mut *tx_ptr };
        match tx_mut.inner.take() {
            Some(tx) => {
                let txn_id = tx
                    .commit()
                    .map_err(|e| Error::new(exception::runtime_error(), e.to_string()))?;
                Ok(txn_id.0)
            }
            None => Err(Error::new(
                exception::runtime_error(),
                "transaction already committed or rolled back",
            )),
        }
    }

    fn inspect(&self) -> String {
        "#<SparrowDB::WriteTx>".to_string()
    }
}

// ── Module entry point ────────────────────────────────────────────────────────

/// Ruby extension initializer.  Called by Ruby when `require 'sparrowdb'` loads
/// the native extension.
#[cfg(feature = "ruby")]
#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    // Define SparrowDB module.
    let module = define_module("SparrowDB")
        .map_err(|e| Error::new(exception::runtime_error(), format!("{e}")))?;

    // Store module handle for later use (e.g., WriterBusy definition).
    MODULE
        .set(module.as_value().into())
        .ok();

    // Define SparrowDB::WriterBusy exception (subclass of RuntimeError).
    let runtime_error = exception::runtime_error();
    let _writer_busy = module
        .define_class("WriterBusy", runtime_error)
        .map_err(|e| Error::new(exception::runtime_error(), format!("{e}")))?;

    // Define SparrowDB::GraphDb.
    let graph_db_class = module
        .define_class("GraphDb", class::object())
        .map_err(|e| Error::new(exception::runtime_error(), format!("{e}")))?;
    graph_db_class.define_singleton_method("new", method!(RbGraphDb::new, 1))?;
    graph_db_class.define_method("checkpoint", method!(RbGraphDb::checkpoint, 0))?;
    graph_db_class.define_method("optimize", method!(RbGraphDb::optimize, 0))?;
    graph_db_class.define_method("begin_read", method!(RbGraphDb::begin_read, 0))?;
    graph_db_class.define_method("begin_write", method!(RbGraphDb::begin_write, 0))?;
    graph_db_class.define_method("execute", method!(RbGraphDb::execute, 1))?;
    graph_db_class.define_method("inspect", method!(RbGraphDb::inspect, 0))?;
    graph_db_class.define_method("to_s", method!(RbGraphDb::inspect, 0))?;

    // Define SparrowDB::ReadTx.
    let read_tx_class = module
        .define_class("ReadTx", class::object())
        .map_err(|e| Error::new(exception::runtime_error(), format!("{e}")))?;
    read_tx_class.define_method("snapshot_txn_id", method!(RbReadTx::snapshot_txn_id, 0))?;
    read_tx_class.define_method("inspect", method!(RbReadTx::inspect, 0))?;
    read_tx_class.define_method("to_s", method!(RbReadTx::inspect, 0))?;

    // Define SparrowDB::WriteTx.
    let write_tx_class = module
        .define_class("WriteTx", class::object())
        .map_err(|e| Error::new(exception::runtime_error(), format!("{e}")))?;
    write_tx_class.define_method("commit", method!(RbWriteTx::commit, 0))?;
    write_tx_class.define_method("inspect", method!(RbWriteTx::inspect, 0))?;
    write_tx_class.define_method("to_s", method!(RbWriteTx::inspect, 0))?;

    Ok(())
}

// ── Tests (no magnus needed) ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {
        // Verify the crate builds without magnus installed (no `ruby` feature).
    }
}
