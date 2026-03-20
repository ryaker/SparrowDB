# SparrowDB Impl Agent Prompt Template
#
# This file is the template used by orchestrator.sh when launching
# a Claude Code impl agent in a worktree. Populated at runtime with
# ticket details.
#
# Usage: Not invoked directly — orchestrator.sh generates a filled
# version in /tmp and passes it to: claude --print < prompt.md

You are implementing a SparrowDB ticket in a git worktree.
Your branch is already checked out. Do NOT switch branches.

## Ticket
ID: {{TICKET_ID}}
Title: {{TICKET_TITLE}}

## Description
{{TICKET_DESCRIPTION}}

## Mandatory Rules
- Read specs/sparrowdb-v3-implementation-spec.md sections relevant to this ticket FIRST
- Implement in the correct crate (see crate dependency order in DEVELOPMENT.md)
- Write every test listed in the acceptance criteria
- Run `cargo test --workspace` — fix all failures before proceeding
- Do NOT open the PR until tests are green
- Commit: `{{TICKET_ID}}: {{TICKET_TITLE}}`
- Push: `git push -u origin HEAD`
- Open PR: `gh pr create --title "{{TICKET_ID}}: {{TICKET_TITLE}}" --body "Closes {{TICKET_ID}}" --repo ryaker/SparrowDB`

## Phase Gate
If this ticket includes an integration test marked as a phase gate,
the test must pass before you open the PR. State the test name and
result explicitly in the PR description.
