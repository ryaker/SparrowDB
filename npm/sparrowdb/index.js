'use strict'

const { existsSync } = require('fs')
const { join } = require('path')

// Map platform+arch to the optional dependency package name.
const PLATFORM_PACKAGES = {
  'linux-x64': '@sparrowdb/linux-x64-gnu',
  'linux-arm64': '@sparrowdb/linux-arm64-gnu',
  'darwin-x64': '@sparrowdb/darwin-x64',
  'darwin-arm64': '@sparrowdb/darwin-arm64',
  'win32-x64': '@sparrowdb/win32-x64-msvc',
}

function loadNative() {
  // 1. Try the platform-specific optional dependency (production / npm install).
  const key = `${process.platform}-${process.arch}`
  const pkg = PLATFORM_PACKAGES[key]
  if (pkg) {
    try {
      return require(pkg)
    } catch (_) {
      // Package not installed — fall through to local .node file.
    }
  }

  // 2. Try a locally compiled sparrowdb.node in the same directory
  //    (development: `cargo build --release && cp target/release/libsparrowdb_node.so npm/sparrowdb/sparrowdb.node`).
  const local = join(__dirname, 'sparrowdb.node')
  if (existsSync(local)) {
    return require(local)
  }

  // 3. Try the workspace target directory (useful during development without copying).
  const targets = [
    join(__dirname, '..', '..', 'target', 'release', 'libsparrowdb_node.so'),
    join(__dirname, '..', '..', 'target', 'release', 'libsparrowdb_node.dylib'),
    join(__dirname, '..', '..', 'target', 'release', 'sparrowdb_node.dll'),
  ]
  for (const t of targets) {
    if (existsSync(t)) {
      return require(t)
    }
  }

  const supported = Object.keys(PLATFORM_PACKAGES).join(', ')
  throw new Error(
    `sparrowdb: could not load native module for ${process.platform}-${process.arch}.\n` +
    `Supported platforms: ${supported}\n` +
    `Run \`cargo build --release -p sparrowdb-node\` to build locally.`
  )
}

module.exports = loadNative()
