#!/usr/bin/env bash
# worktree-create.sh — Create a SparrowDB agent worktree
#
# Usage:
#   ./scripts/worktree-create.sh <branch> <slug>
#
# Example:
#   ./scripts/worktree-create.sh phase/1-catalog phase-1-catalog
#
# The slug becomes the directory name under ~/Dev/SparrowDB-worktrees/
# The branch is created from main if it doesn't exist.
#
# After creation, launch an agent with:
#   claude --dangerously-skip-permissions \
#     --project ~/Dev/SparrowDB-worktrees/<slug>

set -euo pipefail

BRANCH="${1:?Usage: $0 <branch> <slug>}"
SLUG="${2:?Usage: $0 <branch> <slug>}"

MAIN_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORKTREES_DIR="$HOME/Dev/SparrowDB-worktrees"
WORKTREE_PATH="$WORKTREES_DIR/$SLUG"

echo "→ Main repo:   $MAIN_DIR"
echo "→ Worktree:    $WORKTREE_PATH"
echo "→ Branch:      $BRANCH"
echo ""

# Create worktrees directory if needed
mkdir -p "$WORKTREES_DIR"

# Create the worktree (branch from main if not exists)
if git -C "$MAIN_DIR" show-ref --verify --quiet "refs/heads/$BRANCH"; then
  git -C "$MAIN_DIR" worktree add "$WORKTREE_PATH" "$BRANCH"
else
  git -C "$MAIN_DIR" worktree add -b "$BRANCH" "$WORKTREE_PATH" main
fi

# .mcp.json and CLAUDE.md are committed — already present in the worktree checkout.
#
# settings.local.json is globally gitignored (not committed), so we symlink it
# from the main repo so Claude Code picks up the right permissions in the worktree.
mkdir -p "$WORKTREE_PATH/.claude"
ln -sf "$MAIN_DIR/.claude/settings.local.json" "$WORKTREE_PATH/.claude/settings.local.json"
echo "→ Symlinked settings.local.json from main repo"

echo ""
echo "✓ Worktree ready at: $WORKTREE_PATH"
echo ""
echo "Start an agent:"
echo "  claude --dangerously-skip-permissions --project \"$WORKTREE_PATH\""
echo ""
echo "Or via Claude-Ops, set CWD to: $WORKTREE_PATH"
