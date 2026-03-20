#!/usr/bin/env bash
# worktree-remove.sh — Remove a SparrowDB agent worktree after PR merge
#
# Usage:
#   ./scripts/worktree-remove.sh <slug>
#
# Example:
#   ./scripts/worktree-remove.sh phase-1-catalog

set -euo pipefail

SLUG="${1:?Usage: $0 <slug>}"

MAIN_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORKTREES_DIR="$HOME/Dev/SparrowDB-worktrees"
WORKTREE_PATH="$WORKTREES_DIR/$SLUG"

if [ ! -d "$WORKTREE_PATH" ]; then
  echo "Worktree not found: $WORKTREE_PATH"
  exit 1
fi

echo "→ Removing worktree: $WORKTREE_PATH"
git -C "$MAIN_DIR" worktree remove --force "$WORKTREE_PATH"

echo "✓ Worktree removed"
echo ""
echo "To also delete the branch:"
echo "  git branch -d <branch-name>"
