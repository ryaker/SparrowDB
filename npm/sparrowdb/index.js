'use strict'

const { existsSync } = require('fs')
const { join } = require('path')

// There are no separate `@sparrowdb/<platform>` packages — the prebuilt
// binaries for every platform this package supports are bundled directly
// in this tarball (see .github/workflows/release.yml). Map platform+arch
// to the bundled filename so we only ever try to load a binary that was
// actually built for the running platform. See README for the supported
// platform list and why musl/Alpine isn't one of them.
const PLATFORM_BINARIES = {
  'linux-x64': 'sparrowdb.linux-x64-gnu.node',
  'darwin-arm64': 'sparrowdb.darwin-arm64.node',
}

function loadNative() {
  // 1. Try the binary bundled for this exact platform+arch (production / npm install).
  const key = `${process.platform}-${process.arch}`
  const bundled = PLATFORM_BINARIES[key]
  if (bundled) {
    const bundledPath = join(__dirname, bundled)
    if (existsSync(bundledPath)) {
      return require(bundledPath)
    }
  }

  // 2. Try a locally compiled sparrowdb.node in the same directory
  //    (development: `cargo build --release && cp target/release/libsparrowdb_node.so npm/sparrowdb/sparrowdb.node`).
  //    This is a dev-only convenience — CI never publishes a file by this
  //    name, so it can't shadow the platform-specific binaries above with
  //    the wrong platform's build (see issue #481).
  const local = join(__dirname, 'sparrowdb.node')
  if (existsSync(local)) {
    return require(local)
  }

  // 3. Try the workspace target directory (useful during development without copying).
  //    Node native addons must be loaded as .node files regardless of platform.
  //    Use `napi-cli` or manually rename the compiled artifact:
  //      Linux:   target/release/libsparrowdb_node.so  → sparrowdb.node
  //      macOS:   target/release/libsparrowdb_node.dylib → sparrowdb.node
  //      Windows: target/release/sparrowdb_node.dll    → sparrowdb.node
  const targets = [
    join(__dirname, '..', '..', 'target', 'release', 'sparrowdb.node'),
    join(__dirname, '..', '..', 'target', 'debug',   'sparrowdb.node'),
  ]
  for (const t of targets) {
    if (existsSync(t)) {
      return require(t)
    }
  }

  const supported = Object.keys(PLATFORM_BINARIES).join(', ')
  throw new Error(
    `sparrowdb: no prebuilt native module for ${process.platform}-${process.arch}.\n` +
    `Supported platforms: ${supported}\n` +
    `Run \`cargo build --release -p sparrowdb-node\` to build locally.`
  )
}

const native = loadNative()

module.exports = native

// Named exports must be assigned explicitly (not just spread onto
// `module.exports`) so that Node's ESM/CJS interop — which relies on static
// analysis via cjs-module-lexer, not evaluation — can see them. A dynamic
// `module.exports = loadNative()` alone is invisible to that analysis, so an
// ESM consumer's `import { SparrowDB } from 'sparrowdb'` fails with "Named
// export 'SparrowDB' not found" even though `require('sparrowdb').SparrowDB`
// works fine under CommonJS. See issue #449.
module.exports.SparrowDB = native.SparrowDB
module.exports.ReadTx = native.ReadTx
module.exports.WriteTx = native.WriteTx
