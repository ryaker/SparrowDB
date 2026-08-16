'use strict'

// There are no separate `@sparrowdb/<platform>` packages — the prebuilt
// binaries for every platform this package supports are bundled directly
// in this tarball (see .github/workflows/release.yml). Map platform+arch
// to the bundled filename so we only ever try to load a binary that was
// actually built for the running platform. See README for the supported
// platform list and why musl/Alpine isn't one of them.
//
// This lives in its own module (rather than being exported from index.js)
// so test/platform-resolution.test.js can read the real naming convention
// without putting it on the package's public API. See issue #518.
const PLATFORM_BINARIES = {
  'linux-x64': 'sparrowdb.linux-x64-gnu.node',
  'darwin-arm64': 'sparrowdb.darwin-arm64.node',
}

module.exports = { PLATFORM_BINARIES }
