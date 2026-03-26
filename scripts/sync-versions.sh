#!/usr/bin/env bash
#
# sync-versions.sh — Sync language binding package versions from Cargo workspace.
#
# Usage:
#   ./scripts/sync-versions.sh          # reads version from Cargo.toml [workspace.package]
#   ./scripts/sync-versions.sh 0.2.0    # override with explicit version
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Extract version from workspace Cargo.toml or use CLI argument
if [ -n "${1:-}" ]; then
  VERSION="$1"
else
  VERSION=$(grep -A1 '^\[workspace\.package\]' "$REPO_ROOT/Cargo.toml" \
    | grep '^version' \
    | sed 's/version = "\(.*\)"/\1/')
fi

if [ -z "$VERSION" ]; then
  echo "ERROR: Could not determine version" >&2
  exit 1
fi

echo "Syncing all bindings to version: $VERSION"

# ── npm/sparrowdb/package.json ─────────────────────────────────────────────
NPM_PKG="$REPO_ROOT/npm/sparrowdb/package.json"
if [ -f "$NPM_PKG" ]; then
  node -e "
    const pkg = require('$NPM_PKG');
    pkg.version = '$VERSION';
    if (pkg.optionalDependencies) {
      for (const dep of Object.keys(pkg.optionalDependencies)) {
        pkg.optionalDependencies[dep] = '$VERSION';
      }
    }
    require('fs').writeFileSync('$NPM_PKG', JSON.stringify(pkg, null, 2) + '\n');
  "
  echo "  npm/sparrowdb/package.json → $VERSION"
else
  echo "  SKIP npm/sparrowdb/package.json (not found)"
fi

# ── crates/sparrowdb-python/pyproject.toml ─────────────────────────────────
PYPROJECT="$REPO_ROOT/crates/sparrowdb-python/pyproject.toml"
if [ -f "$PYPROJECT" ]; then
  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s/^version = \".*\"/version = \"$VERSION\"/" "$PYPROJECT"
  else
    sed -i "s/^version = \".*\"/version = \"$VERSION\"/" "$PYPROJECT"
  fi
  echo "  crates/sparrowdb-python/pyproject.toml → $VERSION"
else
  echo "  SKIP crates/sparrowdb-python/pyproject.toml (not found)"
fi

# ── crates/sparrowdb-ruby/Cargo.toml (if not using workspace version) ─────
RUBY_CARGO="$REPO_ROOT/crates/sparrowdb-ruby/Cargo.toml"
if [ -f "$RUBY_CARGO" ]; then
  if grep -q '^version = "' "$RUBY_CARGO"; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
      sed -i '' "s/^version = \".*\"/version = \"$VERSION\"/" "$RUBY_CARGO"
    else
      sed -i "s/^version = \".*\"/version = \"$VERSION\"/" "$RUBY_CARGO"
    fi
    echo "  crates/sparrowdb-ruby/Cargo.toml → $VERSION"
  else
    echo "  SKIP crates/sparrowdb-ruby/Cargo.toml (uses workspace version)"
  fi
fi

echo ""
echo "Done. Verify with: git diff"
