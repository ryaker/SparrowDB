#!/usr/bin/env bash
# orchestrator.sh — SparrowDB autonomous PR pipeline
#
# State machine:
#   Linear "In Progress" → create worktree → launch impl agent → open PR
#   PR open → poll reviews → if blocking: launch fix agent → push
#   PR clean (only nitpicks) → squash merge → Linear "Done" → next phase
#
# Dependencies: gh, jq, git, claude (Claude Code CLI)
# Usage: ./scripts/orchestrator.sh [--once] [--dry-run]

set -euo pipefail

MAIN_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORKTREES_DIR="$HOME/Dev/SparrowDB-worktrees"
REPO="ryaker/SparrowDB"
TEAM="SPA"
POLL_INTERVAL=300  # 5 minutes
MAX_FIX_ROUNDS=3

DRY_RUN=false
RUN_ONCE=false
for arg in "$@"; do
  case $arg in
    --dry-run) DRY_RUN=true ;;
    --once)    RUN_ONCE=true ;;
  esac
done

log() { echo "[$(date '+%H:%M:%S')] $*"; }
dry() { if $DRY_RUN; then echo "[DRY] $*"; else eval "$*"; fi; }

# ── Comment classifier ──────────────────────────────────────────────────────
# Returns 0 if blocking comments exist, 1 if only nitpicks/suggestions remain.
# CodeRabbit prefixes: "nitpick:" "suggestion:" "praise:" = non-blocking
# Everything else (actionable, issue, bug, etc) = blocking
pr_has_blocking_comments() {
  local pr_number=$1
  gh api "repos/$REPO/pulls/$pr_number/comments" --jq '
    [.[] | select(.body | test("^(nitpick|suggestion|praise|minor)"; "i") | not)
         | select(.body | test("^(<!--|\\[LGTM\\]|approved)"; "i") | not)
    ] | length > 0
  '
}

# Returns 0 if all required status checks pass
pr_checks_green() {
  local pr_number=$1
  local state
  state=$(gh pr view "$pr_number" --repo "$REPO" \
    --json statusCheckRollup --jq \
    '[.statusCheckRollup[] | select(.status != "COMPLETED" or .conclusion != "SUCCESS")] | length == 0')
  [[ "$state" == "true" ]]
}

# Count review rounds already done (by counting fix-round commits)
fix_round_count() {
  local branch=$1
  git -C "$MAIN_DIR" log --oneline "origin/main..origin/$branch" \
    | grep -c "fix-round" || true
}

# ── Worktree lifecycle ───────────────────────────────────────────────────────
create_worktree() {
  local branch=$1 slug=$2
  "$MAIN_DIR/scripts/worktree-create.sh" "$branch" "$slug"
}

remove_worktree() {
  local slug=$1
  "$MAIN_DIR/scripts/worktree-remove.sh" "$slug" 2>/dev/null || true
}

# ── Claude Code agent launcher ───────────────────────────────────────────────
# Launches a one-shot Claude Code session in the worktree with a prompt file.
launch_claude_agent() {
  local worktree=$1 prompt_file=$2 description=$3
  log "Launching Claude Code: $description"
  log "  Worktree: $worktree"
  log "  Prompt:   $prompt_file"
  dry "claude --dangerously-skip-permissions \
    --project \"$worktree\" \
    --print < \"$prompt_file\""
}

# ── Impl agent prompt ────────────────────────────────────────────────────────
write_impl_prompt() {
  local ticket=$1 title=$2 description=$3 prompt_file=$4
  cat > "$prompt_file" <<PROMPT
You are implementing a SparrowDB ticket in a git worktree.
Your branch is already checked out. Do NOT switch branches.

## Ticket
ID: $ticket
Title: $title

## Description
$description

## Your Task
1. Read the relevant sections of specs/sparrowdb-v3-implementation-spec.md
2. Implement the feature in the appropriate crate(s)
3. Write all tests specified in the acceptance criteria
4. Run: cargo test --workspace
5. Fix all test failures before continuing
6. Commit with message: "$ticket: $title"
7. Push the branch: git push -u origin HEAD
8. Open a PR: gh pr create --title "$ticket: $title" --body "Closes $ticket" --repo $REPO

Do NOT declare success until cargo test passes.
Do NOT open the PR until all tests are green.
PROMPT
}

# ── Fix agent prompt ─────────────────────────────────────────────────────────
write_fix_prompt() {
  local pr_number=$1 branch=$2 round=$3 prompt_file=$4
  local comments
  comments=$(gh api "repos/$REPO/pulls/$pr_number/comments" \
    --jq '[.[] | {user: .user.login, body: .body, path: .path, line: .line}]')

  cat > "$prompt_file" <<PROMPT
You are fixing review comments on a SparrowDB PR (round $round of $MAX_FIX_ROUNDS).
Branch: $branch
PR: #$pr_number

## Review Comments to Address
$(echo "$comments" | jq -r '.[] | "[\(.user)] \(.path):\(.line // "?") — \(.body)"')

## Your Task
1. Address every BLOCKING comment (actionable, bug, issue, error)
2. SKIP comments prefixed with: nitpick:, suggestion:, praise:, minor:
3. Run: cargo test --workspace — all tests must stay green
4. Commit: git commit -m "fix-round-$round: address review comments"
5. Push: git push

Do NOT open a new PR. Do NOT change the branch. Do NOT declare done until tests pass.
PROMPT
}

# ── Main loop ────────────────────────────────────────────────────────────────
process_open_prs() {
  log "Checking open PRs..."
  local prs
  prs=$(gh pr list --repo "$REPO" --json number,headRefName,title,reviews \
    --jq '.[] | @base64')

  while IFS= read -r pr_b64; do
    [[ -z "$pr_b64" ]] && continue
    local pr
    pr=$(echo "$pr_b64" | base64 --decode)
    local pr_number branch title
    pr_number=$(echo "$pr" | jq -r '.number')
    branch=$(echo "$pr"    | jq -r '.headRefName')
    title=$(echo "$pr"     | jq -r '.title')

    log "PR #$pr_number ($branch): $title"

    # Extract slug from branch name (phase/1-catalog → phase-1-catalog)
    local slug
    slug=$(echo "$branch" | tr '/' '-')
    local worktree="$WORKTREES_DIR/$slug"

    local has_blocking
    has_blocking=$(pr_has_blocking_comments "$pr_number")

    if [[ "$has_blocking" == "true" ]]; then
      local rounds
      rounds=$(fix_round_count "$branch")
      if (( rounds >= MAX_FIX_ROUNDS )); then
        log "  ⚠ PR #$pr_number hit max fix rounds ($MAX_FIX_ROUNDS) — needs human review"
        continue
      fi

      # Ensure worktree exists (may have been removed)
      if [[ ! -d "$worktree" ]]; then
        log "  Re-creating worktree for fix round..."
        create_worktree "$branch" "$slug"
      fi

      local prompt_file
      prompt_file=$(mktemp /tmp/sparrow-fix-XXXXXX.md)
      write_fix_prompt "$pr_number" "$branch" "$((rounds+1))" "$prompt_file"
      launch_claude_agent "$worktree" "$prompt_file" "Fix round $((rounds+1)) for PR #$pr_number"
      rm -f "$prompt_file"

    elif pr_checks_green "$pr_number"; then
      log "  ✓ PR #$pr_number clean — squash merging"
      dry "gh pr merge $pr_number --repo $REPO --squash --auto --delete-branch"
      remove_worktree "$slug"
    else
      log "  ⏳ PR #$pr_number: checks still running"
    fi
  done <<< "$prs"
}

process_linear_tickets() {
  log "Checking Linear for In Progress tickets..."
  # Query Linear for In Progress tickets via gh (no Linear CLI dependency)
  # Tickets are identified by branch convention spa-{N}-{slug}
  local branches_with_prs
  branches_with_prs=$(gh pr list --repo "$REPO" --json headRefName \
    --jq '[.[].headRefName]')

  # List open worktrees to avoid duplicates
  local active_worktrees
  active_worktrees=$(git -C "$MAIN_DIR" worktree list --porcelain \
    | grep "^branch" | awk '{print $2}')

  log "  Active branches with PRs: $branches_with_prs"
  log "  Active worktrees: $active_worktrees"
  # Impl agent spawning is manual for Phase 0 ramp-up.
  # Future: poll Linear API for "In Progress" + no open PR → spawn impl agent.
}

main() {
  log "SparrowDB Orchestrator starting"
  log "  Repo:     $REPO"
  log "  Main dir: $MAIN_DIR"
  log "  Worktrees: $WORKTREES_DIR"
  $DRY_RUN && log "  DRY RUN MODE"

  while true; do
    process_open_prs
    process_linear_tickets
    $RUN_ONCE && break
    log "Sleeping ${POLL_INTERVAL}s..."
    sleep "$POLL_INTERVAL"
  done
}

main
