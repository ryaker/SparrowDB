#!/usr/bin/env bash
# AgentBus SessionStart hook — registers this session with the bus
# Install to: <project>/.claude/hooks/ (via agent-bus install-hooks)
# Fires at session start. Silent failure — bus may not be running.

# Derive project name from git repo root name (handles subdirectory sessions correctly).
# Falls back to basename "$PWD" if not in a git repo.
# If two distinct repos share the same folder name, set AGENT_BUS_PROJECT explicitly.
_GIT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null)
PROJECT_NAME="${AGENT_BUS_PROJECT:-${_GIT_ROOT:+$(basename "$_GIT_ROOT")}}"
PROJECT_NAME="${PROJECT_NAME:-$(basename "$PWD")}"
BUS_URL="http://localhost:8090/api/bus"

curl -s -X POST "$BUS_URL/register" \
  -H "Content-Type: application/json" \
  -d "{
    \"project\": \"$PROJECT_NAME\",
    \"folder_path\": \"$PWD\",
    \"runtime\": \"claude_code\",
    \"pid\": $PPID
  }" > /dev/null 2>&1 || true
