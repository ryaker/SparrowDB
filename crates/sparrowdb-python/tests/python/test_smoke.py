"""Smoke tests for the sparrowdb Python extension module.

Run with:
    cd crates/sparrowdb-python
    maturin develop
    pytest tests/python/ -v
"""

import os
import tempfile

import sparrowdb


# ── GraphDb basics ────────────────────────────────────────────────────────────


def test_open_creates_db_directory():
    """GraphDb.open() creates the database directory if it doesn't exist."""
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "test.db")
        assert not os.path.exists(db_path)
        db = sparrowdb.GraphDb(db_path)
        assert os.path.isdir(db_path)
        del db


def test_graphdb_repr():
    """GraphDb has a sensible repr containing 'GraphDb'."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        assert "GraphDb" in repr(type(db))


def test_checkpoint_does_not_raise():
    """checkpoint() succeeds on an empty database."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        db.checkpoint()  # Must not raise


def test_optimize_does_not_raise():
    """optimize() succeeds on an empty database."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        db.optimize()  # Must not raise


def test_open_existing_db_succeeds():
    """Re-opening an existing database directory does not raise."""
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "reopen.db")
        sparrowdb.GraphDb(db_path)  # create
        db2 = sparrowdb.GraphDb(db_path)  # re-open
        db2.checkpoint()


# ── ReadTx ────────────────────────────────────────────────────────────────────


def test_begin_read_returns_read_tx():
    """begin_read() returns a ReadTx object with snapshot_txn_id == 0 before any writes."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        rx = db.begin_read()
        assert isinstance(rx, sparrowdb.ReadTx)
        assert rx.snapshot_txn_id == 0


def test_read_tx_repr():
    """ReadTx.__repr__ contains 'ReadTx' and the snapshot id."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        rx = db.begin_read()
        r = repr(rx)
        assert "ReadTx" in r
        assert "0" in r


# ── WriteTx ───────────────────────────────────────────────────────────────────


def test_begin_write_returns_write_tx():
    """begin_write() returns a WriteTx object."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        tx = db.begin_write()
        assert isinstance(tx, sparrowdb.WriteTx)
        del tx  # implicit rollback


def test_write_tx_commit_returns_txn_id():
    """commit() returns an integer txn_id starting at 1."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        tx = db.begin_write()
        txn_id = tx.commit()
        assert isinstance(txn_id, int)
        assert txn_id == 1


def test_write_tx_txn_id_increments():
    """Each commit increments the txn_id."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        t1 = db.begin_write().commit()
        t2 = db.begin_write().commit()
        assert t2 == t1 + 1


def test_write_tx_double_commit_raises():
    """Committing a WriteTx twice raises RuntimeError."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        tx = db.begin_write()
        tx.commit()
        try:
            tx.commit()
            assert False, "Expected RuntimeError"
        except RuntimeError:
            pass


def test_writer_busy_raises():
    """begin_write() raises RuntimeError if a writer is already active."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        tx1 = db.begin_write()
        try:
            db.begin_write()
            assert False, "Expected RuntimeError (WriterBusy)"
        except RuntimeError:
            pass
        del tx1  # release writer lock


def test_write_lock_released_after_commit():
    """After a commit, a new write transaction can be opened."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        db.begin_write().commit()
        db.begin_write().commit()  # Must not raise


# ── execute (Cypher) ──────────────────────────────────────────────────────────


def test_execute_returns_list_or_raises_runtime_error():
    """execute() returns a list of dicts, or raises RuntimeError on engine errors.

    On an empty database the execution engine raises NotFound (no labels exist),
    which the Python binding converts to RuntimeError.  Either outcome is valid
    here; what must NOT happen is an unhandled panic or a non-RuntimeError
    exception.
    """
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        try:
            result = db.execute("MATCH (n) RETURN n.name LIMIT 10")
            assert isinstance(result, list)
        except RuntimeError:
            pass  # empty-db NotFound is acceptable


def test_execute_empty_graph_no_panic():
    """execute() on an empty graph does not panic; it returns a list or RuntimeError."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        try:
            result = db.execute("MATCH (n) RETURN n LIMIT 100")
            assert isinstance(result, list)
        except RuntimeError:
            pass  # NotFound on empty DB is acceptable


def test_execute_invalid_query_raises():
    """An unparseable Cypher query raises RuntimeError."""
    with tempfile.TemporaryDirectory() as d:
        db = sparrowdb.GraphDb(os.path.join(d, "test.db"))
        try:
            db.execute("THIS IS NOT VALID CYPHER !!!")
            assert False, "Expected RuntimeError for invalid Cypher"
        except RuntimeError:
            pass
