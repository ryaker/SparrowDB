#!/usr/bin/env bash
# AgentBus UserPromptSubmit hook — injects inbox messages as context
# Install to: <project>/.claude/hooks/ (via agent-bus install-hooks)
# Fires before LLM sees user message. Silent when inbox is empty.
# Silent failure throughout — never generates hook errors.
set +e

# Derive project name from git repo root name (handles subdirectory sessions correctly).
# Falls back to basename "$PWD" if not in a git repo.
# If two distinct repos share the same folder name, set AGENT_BUS_PROJECT explicitly.
_GIT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null)
PROJECT_NAME="${AGENT_BUS_PROJECT:-${_GIT_ROOT:+$(basename "$_GIT_ROOT")}}"
PROJECT_NAME="${PROJECT_NAME:-$(basename "$PWD")}"
INBOX_DIR="$HOME/.agent-bus/inbox/$PROJECT_NAME"
PROCESSED_DIR="$HOME/.agent-bus/processed"
BUS_URL="http://localhost:8090/api/bus"

# Heartbeat — keeps session alive in the registry (silent, fire-and-forget)
curl -s -X POST "$BUS_URL/heartbeat" \
  -H "Content-Type: application/json" \
  -d "{\"project\": \"$PROJECT_NAME\"}" \
  > /dev/null 2>&1 || true

# Exit silently if no inbox directory or no files
[ -d "$INBOX_DIR" ] || exit 0

# Collect JSON files
shopt -s nullglob
files=("$INBOX_DIR"/*.json)
shopt -u nullglob

[ ${#files[@]} -eq 0 ] && exit 0

# Ensure processed directory exists
mkdir -p "$PROCESSED_DIR"

# Sort files by priority (DESC) then created_at (ASC)
# Build sortable lines: "priority|created_at|filepath" then sort
sorted_files=()
while IFS= read -r line; do
  sorted_files+=("${line##*|}")
done < <(
  for f in "${files[@]}"; do
    pri=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get('priority',0))" "$f" 2>/dev/null || echo "0")
    cat=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get('created_at',''))" "$f" 2>/dev/null || echo "")
    # Negate priority for descending sort, then ascending created_at
    neg_pri=$(( 999 - pri ))
    printf '%03d|%s|%s\n' "$neg_pri" "$cat" "$f"
  done | sort
)

[ ${#sorted_files[@]} -eq 0 ] && exit 0

# Process each message. Claim FIRST by moving the file — the inbox file is the
# ownership token shared with the channel poller. If the move fails, the channel
# already claimed this message and will inject it itself: skip silently.
# Marking 'delivered' is all this hook does — the agent acks after doing the
# work, via send_reply/ack_message. The hook must NEVER ack.
output=""
count=0
for f in "${sorted_files[@]}"; do
  base=$(basename "$f")
  mv "$f" "$PROCESSED_DIR/" 2>/dev/null || continue
  pf="$PROCESSED_DIR/$base"

  # Extract fields (from the claimed copy)
  msg_id=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get('id','?'))" "$pf" 2>/dev/null)
  from=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get('from','unknown'))" "$pf" 2>/dev/null)
  pri=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get('priority',0))" "$pf" 2>/dev/null)
  content=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get('content',''))" "$pf" 2>/dev/null)

  # If the /deliver POST fails, move the file back to inbox/ so it is retried next turn.
  deliver_ok=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BUS_URL/deliver" \
    -H "Content-Type: application/json" \
    -d "{\"message_id\": $msg_id, \"delivered_by\": \"hook\"}" 2>/dev/null || echo "000")
  if [[ "$deliver_ok" != "200" ]]; then
    mv "$pf" "$INBOX_DIR/" 2>/dev/null || true
    continue
  fi

  count=$((count + 1))
  output+="--- Message from $from (id: $msg_id, priority: $pri) ---
$content

Reply when done: call mcp__agent_bus_channel__send_reply(message_id=$msg_id, content=\"<result>\")  OR agent-bus send $from \"<result>\"

"
done

[ "$count" -eq 0 ] && exit 0

echo "[AgentBus inbox — $count pending message$([ "$count" -gt 1 ] && echo "s")]"
echo ""
echo "$output"
