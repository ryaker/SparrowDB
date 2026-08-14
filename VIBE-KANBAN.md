# Vibe Kanban — SparrowDB Agent Guide

Vibe Kanban runs at `http://localhost:3333`. No auth needed from localhost.

## SparrowDB Project IDs

```python
SP_PROJECT_ID  = "01e6f151-71e8-4fd4-985e-25d78dcc4d3f"
SP_BACKLOG     = "045a7d5c-58c5-45e5-8fbc-4cbb9d281546"
SP_TODO        = "88f04207-8613-4456-9d46-5dfce6a19338"
SP_IN_PROGRESS = "a1483636-c65e-4935-9fcf-c15ae27dbe00"
SP_IN_REVIEW   = "07de6f61-ca97-4231-9402-eae0f459f917"
SP_DONE        = "b342c538-a2f8-496e-be91-6edc8db47900"
SP_CANCELLED   = "37e5c54b-8edf-4c77-80cc-a6fe04867e56"
```

## Agent Boilerplate

Paste into any agent prompt working on a SparrowDB issue:

```python
import json, urllib.request

def _vk_port():
    try:
        return json.loads(open("/var/folders/b5/d07g827s1l9fw40nk5l421xm0000gn/T/vibe-kanban/vibe-kanban.port").read())["main_port"]
    except Exception:
        return 3333

def vk_move(issue_id: str, status_id: str):
    port = _vk_port()
    body = json.dumps({"status_id": status_id}).encode()
    req = urllib.request.Request(f"http://localhost:{port}/api/remote/issues/{issue_id}",
        data=body, headers={"Content-Type": "application/json"}, method="PATCH")
    urllib.request.urlopen(req)

def vk_find_by_gh(gh_number: int):
    port = _vk_port()
    req = urllib.request.Request(f"http://localhost:{port}/api/remote/issues?project_id={SP_PROJECT_ID}")
    with urllib.request.urlopen(req) as r:
        issues = json.loads(r.read())["data"]["issues"]
    for i in issues:
        if i.get("title", "").startswith(f"#{gh_number} "):
            return i
    return None

SP_PROJECT_ID  = "01e6f151-71e8-4fd4-985e-25d78dcc4d3f"
SP_TODO        = "88f04207-8613-4456-9d46-5dfce6a19338"
SP_IN_PROGRESS = "a1483636-c65e-4935-9fcf-c15ae27dbe00"
SP_IN_REVIEW   = "07de6f61-ca97-4231-9402-eae0f459f917"
SP_DONE        = "b342c538-a2f8-496e-be91-6edc8db47900"
```

## Agent Lifecycle

```python
# 1. Find your card by GitHub issue number
card = vk_find_by_gh(309)          # replace with your issue number

# 2. Move through columns as you work
vk_move(card["id"], SP_IN_PROGRESS)  # when you start
vk_move(card["id"], SP_IN_REVIEW)    # when PR is open
vk_move(card["id"], SP_DONE)         # when merged
```

## Listing Issues

```python
# See what's on the board
port = _vk_port()
req = urllib.request.Request(f"http://localhost:{port}/api/remote/issues?project_id={SP_PROJECT_ID}")
with urllib.request.urlopen(req) as r:
    issues = json.loads(r.read())["data"]["issues"]

# Filter by column
todo = [i for i in issues if i["status_id"] == SP_TODO]
in_progress = [i for i in issues if i["status_id"] == SP_IN_PROGRESS]
```

## Auto-Sync

`scripts/vk-sync.py` runs hourly via cron:
- New GitHub issues → Backlog
- Merged issues → Done (unless an agent owns them)
- Log: `/tmp/sparrowdb-vk-sync.log`

Run manually: `python3 scripts/vk-sync.py`
