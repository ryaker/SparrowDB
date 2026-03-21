/**
 * Native Node.js/TypeScript bindings for SparrowDB.
 *
 * @example
 * ```typescript
 * import { SparrowDB } from 'sparrowdb'
 *
 * const db = SparrowDB.open('/path/to/my.db')
 *
 * // Read query
 * const result = db.execute('MATCH (n:Person) RETURN n.name LIMIT 5')
 * for (const row of result.rows) {
 *   console.log(row['n.name'])
 * }
 *
 * db.checkpoint()
 * ```
 */

/** A node reference returned from a Cypher query. */
export interface NodeRef {
  $type: 'node'
  /** Packed node id (upper 16 bits: label id, lower 48 bits: slot id). */
  id: number
}

/** An edge reference returned from a Cypher query. */
export interface EdgeRef {
  $type: 'edge'
  /** Monotonic edge id. */
  id: number
}

/** A scalar value in a query result row. */
export type Value = null | number | boolean | string | NodeRef | EdgeRef

/** One row of a query result, keyed by the column name used in the RETURN clause. */
export interface Row {
  [column: string]: Value
}

/** The materialized result of a Cypher query. */
export interface QueryResult {
  /** Column names in RETURN clause order. */
  columns: string[]
  /** Rows, each mapping column name → value. */
  rows: Row[]
}

/**
 * Top-level database handle.
 *
 * Wraps a SparrowDB database directory.  The database is opened with
 * Single-Writer / Multiple-Reader (SWMR) semantics: any number of concurrent
 * readers are allowed, but only one writer at a time.
 */
export declare class SparrowDB {
  /**
   * Open (or create) a SparrowDB database at `path`.
   *
   * @throws if the path cannot be created or the database files are corrupt.
   */
  static open(path: string): SparrowDB

  /**
   * Execute a Cypher query and return the materialized result.
   *
   * Both read queries (`MATCH … RETURN`) and write queries (`CREATE`, `MERGE`,
   * `SET`, `DELETE`) are supported.  Write queries execute in an implicit
   * auto-committed transaction.
   *
   * @throws on parse errors, execution errors, or write-write conflicts.
   */
  execute(cypher: string): QueryResult

  /**
   * Flush the WAL and compact the database.
   *
   * Folds in-flight delta records into the base CSR/node-store files.
   * Equivalent to `CHECKPOINT` in SQL databases.
   */
  checkpoint(): void

  /**
   * Checkpoint + sort neighbour lists.
   *
   * Performs a checkpoint and then sorts the CSR adjacency lists, which
   * improves traversal performance for highly-connected graphs.
   */
  optimize(): void

  /**
   * Open a read-only snapshot transaction.
   *
   * The reader is pinned to the current committed state and is immune to
   * subsequent writes.  Multiple readers may coexist with an active writer.
   */
  beginRead(): ReadTx

  /**
   * Open a write transaction.
   *
   * Only one writer may be active at a time.
   * @throws `WriterBusy` if another write transaction is already open.
   */
  beginWrite(): WriteTx
}

/**
 * Read-only snapshot transaction.
 *
 * Obtained via {@link SparrowDB.beginRead}.  Sees only data committed at or
 * before the snapshot point.
 */
export declare class ReadTx {
  /** The committed `txn_id` this reader is pinned to. */
  readonly snapshotTxnId: number
}

/**
 * Write transaction.
 *
 * Obtained via {@link SparrowDB.beginWrite}.  Commit explicitly with
 * {@link commit}; dropping (GC'd) without committing automatically rolls back
 * all staged changes.
 */
export declare class WriteTx {
  /**
   * Execute a Cypher mutation statement inside this transaction.
   *
   * @returns `{ columns, rows }` — typically empty for pure write statements.
   * @throws if the transaction is already committed or rolled back.
   */
  execute(cypher: string): QueryResult

  /**
   * Commit all staged changes and return the new transaction id.
   *
   * @throws on write-write conflict or if already committed / rolled back.
   */
  commit(): number

  /**
   * Roll back all staged changes explicitly.
   *
   * Equivalent to letting the transaction be garbage-collected without
   * calling {@link commit}.
   */
  rollback(): void
}
