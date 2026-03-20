# SparrowDB Impl Agent Prompt Template
#
# Populated at runtime by orchestrator.sh with ticket details.
# Passed to: claude --dangerously-skip-permissions --print < prompt.md

You are implementing a SparrowDB ticket using TDD in a git worktree.
Your branch is already checked out. Do NOT switch branches.

## Ticket
ID: {{TICKET_ID}}
Title: {{TICKET_TITLE}}

## Description
{{TICKET_DESCRIPTION}}

## Mandatory TDD Sequence

### Step 1 — Read first
- Read specs/sparrowdb-v3-implementation-spec.md sections relevant to this ticket
- Read docs/use-cases.md to understand what "real" looks like
- Read existing crate structure before writing anything

### Step 2 — Write tests (RED phase)
- Write every test specified in the acceptance criteria
- For integration tests: add to tests/integration/
- For unit tests: add inline #[cfg(test)] modules in the relevant file
- For golden fixtures: generate and commit to tests/fixtures/
- Run: `cargo test --workspace` — tests MUST fail here (no impl yet)
- Commit: `git commit -m "test({{TICKET_ID}}): acceptance criteria tests"`

### Step 3 — Implement (GREEN phase)
- Implement only what is needed to make the tests pass
- Do not add features not in the acceptance criteria
- Run: `cargo test --workspace` after each meaningful change
- Fix all failures before proceeding

### Step 4 — Verify
- `cargo test --workspace` — ALL tests green (not just your new ones)
- `cargo clippy --all-targets -- -D warnings` — zero warnings
- `cargo fmt --all -- --check` — formatted
- If this ticket contains a phase gate test: state it explicitly

### Step 5 — Open PR
Only when ALL of the above pass:
- Commit: `git commit -m "feat({{TICKET_ID}}): {{TICKET_TITLE}}"`
- Push: `git push -u origin HEAD`
- Open PR:
  ```
  gh pr create \
    --title "{{TICKET_ID}}: {{TICKET_TITLE}}" \
    --body "Closes {{TICKET_ID}}

  ## Changes
  [Describe what was built]

  ## Tests
  [List test names and what they verify]

  ## Phase gate
  [If applicable: test name + result]" \
    --repo ryaker/SparrowDB
  ```

## Hard Rules
- Never open a PR with failing tests
- Never skip clippy or fmt
- Never implement more than the ticket asks (scope discipline)
- If cargo test was passing before your change and now fails: fix it before anything else
