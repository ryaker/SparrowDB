// Type-check smoke test for the published `.d.ts` — not executed, only
// compiled. Exercises the shapes real TypeScript consumers rely on: named
// import of the class exports, `execute`, `executeWithParams`, and iterating
// `QueryResult.rows`. CI type-checks this file against `index.d.ts` so a
// change to the declarations that breaks a realistic consumer fails the
// build instead of shipping silently (see issue #449).
import { SparrowDB, ReadTx, WriteTx } from 'sparrowdb'

// Stand-in for a real sink (console.log) so this file type-checks without
// pulling in @types/node just to know the shape of `console`.
declare function use(value: unknown): void

const db: SparrowDB = SparrowDB.open('/tmp/example.db')

const result = db.execute('MATCH (n:Person) RETURN n.name LIMIT 5')
for (const row of result.rows) {
  use(row['n.name'])
}

const paramResult = db.executeWithParams(
  'MERGE (k:Memory {id: $id}) SET k.embedding = $emb',
  { id: 'abc-123', emb: [0.1, 0.2, 0.3] },
)
use(paramResult.columns)

const health = db.vectorIndexHealth('Memory', 'embedding')
use([health.stored, health.reachable, health.unreachable])

const hits = db.hybridSearch('Memory', 'content', 'embedding', new Float32Array(768), 'query', 10, 0.7)
for (const hit of hits) {
  use([hit.id, hit.score])
}

const readTx: ReadTx = db.beginRead()
use(readTx.snapshotTxnId)

const writeTx: WriteTx = db.beginWrite()
writeTx.execute("CREATE (:Person {name: 'Ada'})")
const newTxnId: string = writeTx.commit()
use(newTxnId)

db.checkpoint()
db.optimize()
