'use strict'

/**
 * Regression coverage for issue #481.
 *
 * integration.test.js only ever exercises index.js's *dev* fallback: a
 * generic `sparrowdb.node` built fresh from source on whichever machine
 * runs the suite (see index.js step 2, and the CI step that creates it).
 * That test passed in CI (Linux) and in local dev on every machine while
 * the *published* package was unloadable on darwin-arm64 — the dev
 * fallback is, by construction, always correct for the machine that built
 * it, so it can never see a mismatch between a platform-specific bundled
 * filename and what's actually inside it.
 *
 * These tests instead build an isolated copy of the npm package directory
 * and populate it the way `npm publish` does: nothing but a binary under
 * `sparrowdb.<platform>-<arch>.node`. Each require happens in a fresh child
 * process — a bad native binary can abort the process, and we don't want
 * that to take the test runner down or leak process.platform overrides
 * into other tests.
 */

const { describe, it } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')

const PACKAGE_DIR = path.join(__dirname, '..')
const INDEX_JS = path.join(PACKAGE_DIR, 'index.js')
const PLATFORMS_JS = path.join(PACKAGE_DIR, 'platforms.js')

// Read the real filename convention from platforms.js rather than
// reconstructing it here. A hand-copied convention is a second place for it
// to drift from the loader — which is exactly how #481 shipped: index.js
// looks for "sparrowdb.linux-x64-gnu.node" (the `-gnu` suffix matters), and
// an earlier version of this file built "sparrowdb.linux-x64.node" by
// concatenation, so it passed on darwin-arm64 by coincidence and failed on
// Linux CI. (platforms.js was split out of index.js in #518 so this map
// isn't part of the package's public API — see makeIsolatedPackageDir
// below, which now needs to copy it alongside index.js.)
const { PLATFORM_BINARIES } = require(PLATFORMS_JS)

// Locate a real, loadable compiled binary for the machine running this
// suite — the same places index.js's own dev fallback (steps 2/3) looks,
// plus the raw cargo artifact name under CARGO_TARGET_DIR for worktree
// setups where that isn't the default `target/`.
function findRealBinary() {
  const named = [
    path.join(PACKAGE_DIR, 'sparrowdb.node'),
    path.join(PACKAGE_DIR, '..', '..', 'target', 'release', 'sparrowdb.node'),
    path.join(PACKAGE_DIR, '..', '..', 'target', 'debug', 'sparrowdb.node'),
  ]
  for (const candidate of named) {
    if (fs.existsSync(candidate)) return candidate
  }
  const targetDir = process.env.CARGO_TARGET_DIR || path.join(PACKAGE_DIR, '..', '..', 'target')
  const rawArtifacts = [
    path.join(targetDir, 'release', 'libsparrowdb_node.so'),
    path.join(targetDir, 'release', 'libsparrowdb_node.dylib'),
    path.join(targetDir, 'release', 'sparrowdb_node.dll'),
  ]
  for (const candidate of rawArtifacts) {
    if (fs.existsSync(candidate)) return candidate
  }
  return null
}

function makeIsolatedPackageDir() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sparrowdb-pkgtest-'))
  fs.copyFileSync(INDEX_JS, path.join(dir, 'index.js'))
  // index.js requires('./platforms') — the isolated copy needs it too,
  // same as npm publish bundles it alongside index.js in the real tarball.
  fs.copyFileSync(PLATFORMS_JS, path.join(dir, 'platforms.js'))
  return dir
}

// Requires `<dir>/index.js` in a fresh child process, optionally overriding
// process.platform/arch first, and reports what happened via stdout so a
// crashed/uncaught child still yields a readable assertion failure.
function runInChild(dir, { platform, arch } = {}) {
  const overrides =
    (platform ? `Object.defineProperty(process, 'platform', { value: ${JSON.stringify(platform)} });` : '') +
    (arch ? `Object.defineProperty(process, 'arch', { value: ${JSON.stringify(arch)} });` : '')
  const script = `
    ${overrides}
    try {
      const native = require(${JSON.stringify(path.join(dir, 'index.js'))})
      console.log('LOADED:' + Object.keys(native).sort().join(','))
    } catch (err) {
      console.log('THREW:' + err.message.replace(/\\n/g, '\\\\n'))
    }
  `
  const result = spawnSync(process.execPath, ['-e', script], { encoding: 'utf8' })
  return (result.stdout || '').trim() || `(no output; stderr: ${result.stderr})`
}

describe('production binary resolution (issue #481)', () => {
  const key = `${process.platform}-${process.arch}`
  const expectedFilename = PLATFORM_BINARIES[key]
  const realBinary = findRealBinary()

  it('a binary bundled under the correct platform-specific filename loads and exposes the API', (t) => {
    if (!expectedFilename) {
      t.skip(`${key} is not a supported platform (see PLATFORM_BINARIES in index.js)`)
      return
    }
    if (!realBinary) {
      t.skip('no compiled sparrowdb.node available in this environment')
      return
    }
    const dir = makeIsolatedPackageDir()
    try {
      fs.copyFileSync(realBinary, path.join(dir, expectedFilename))
      const output = runInChild(dir)
      assert.ok(output.startsWith('LOADED:'), `expected a clean load for ${key}, got: ${output}`)
      // A subset check, not deepEqual against the full key list: index.js's
      // own module.exports can grow (e.g. PLATFORM_BINARIES, added for this
      // suite's own use), and this test's job is to confirm the *native*
      // API loaded, not to pin index.js's exact export list. Sorted in the
      // child (see runInChild) is irrelevant to a subset check but keeps
      // the raw output readable in a failure message.
      const exported = output.slice('LOADED:'.length).split(',')
      for (const name of ['ReadTx', 'SparrowDB', 'WriteTx']) {
        assert.ok(exported.includes(name), `expected ${name} among the loaded exports, got: ${exported.join(',')}`)
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  it('a bad binary under the expected platform filename does not crash uncaught — it falls through to the clear supported-platforms error', (t) => {
    if (!expectedFilename) {
      t.skip(`${key} is not a supported platform (see PLATFORM_BINARIES in index.js)`)
      return
    }
    const dir = makeIsolatedPackageDir()
    try {
      // Reproduces the actual shape of #481: a file that does not match
      // the running platform sitting at the name index.js loads
      // unconditionally. The content doesn't need to be a real foreign
      // binary — any bytes dlopen can't parse reproduce the failure mode
      // (an ERR_DLOPEN_FAILED-style error thrown out of `require`).
      fs.writeFileSync(path.join(dir, expectedFilename), 'not a real native module')
      const output = runInChild(dir)
      assert.ok(output.startsWith('THREW:'), `expected a clean thrown Error, got: ${output}`)
      const message = output.slice('THREW:'.length)
      assert.match(
        message,
        /no prebuilt native module for/,
        'a bad bundled binary must fall through to the catchable message, not an uncaught dlopen crash',
      )
      assert.match(message, /Supported platforms:/)
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  it('an explicitly unsupported platform+arch throws the catchable error naming it', () => {
    assert.ok(!PLATFORM_BINARIES['win32-x64'], 'this test assumes win32-x64 stays unsupported')
    const dir = makeIsolatedPackageDir()
    try {
      const output = runInChild(dir, { platform: 'win32', arch: 'x64' })
      assert.ok(output.startsWith('THREW:'), `expected a clean thrown Error, got: ${output}`)
      assert.match(output, /no prebuilt native module for win32-x64/)
      // Derived from PLATFORM_BINARIES, not hardcoded — same reasoning as
      // expectedFilename above: don't let this list drift from the loader's.
      const supportedList = Object.keys(PLATFORM_BINARIES).join(', ')
      assert.ok(
        output.includes(`Supported platforms: ${supportedList}`),
        `expected the supported-platform list to read "${supportedList}", got: ${output}`,
      )
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })
})
