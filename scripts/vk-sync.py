#!/usr/bin/env python3
"""
SparrowDB ↔ Vibe Kanban sync script.

Keeps the VK board in sync with GitHub issues:
  - New open GitHub issues → create card in Backlog (if not already on board)
  - GitHub issues closed/merged → move card to Done (unless agent has it In Progress/Review)
  - Never touches cards that are In Progress or In Review (agent is working)

Run manually: python3 scripts/vk-sync.py
Cron (every hour): managed by cron, see bottom of this file
"""

import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────

GITHUB_REPO   = "ryaker/SparrowDB"
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN") or os.environ.get("VK_GITHUB_TOKEN")
if not GITHUB_TOKEN:
    sys.exit("Error: set GITHUB_TOKEN or VK_GITHUB_TOKEN env var before running vk-sync")

VK_PORT_FILE  = "/var/folders/b5/d07g827s1l9fw40nk5l421xm0000gn/T/vibe-kanban/vibe-kanban.port"
VK_PORT       = 3333  # fallback if port file not found

SP_PROJECT_ID  = "01e6f151-71e8-4fd4-985e-25d78dcc4d3f"
SP_BACKLOG     = "045a7d5c-58c5-45e5-8fbc-4cbb9d281546"
SP_TODO        = "88f04207-8613-4456-9d46-5dfce6a19338"
SP_IN_PROGRESS = "a1483636-c65e-4935-9fcf-c15ae27dbe00"
SP_IN_REVIEW   = "07de6f61-ca97-4231-9402-eae0f459f917"
SP_DONE        = "b342c538-a2f8-496e-be91-6edc8db47900"
SP_CANCELLED   = "37e5c54b-8edf-4c77-80cc-a6fe04867e56"

# Statuses agents own — never auto-close these
AGENT_STATUSES = {SP_IN_PROGRESS, SP_IN_REVIEW}

# ── Helpers ───────────────────────────────────────────────────────────────────

def vk_port():
    try:
        return json.loads(open(VK_PORT_FILE).read())["main_port"]
    except Exception:
        return VK_PORT


def vk_get(path):
    port = vk_port()
    req = urllib.request.Request(f"http://localhost:{port}/api{path}")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def vk_post(path, body):
    port = vk_port()
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://localhost:{port}/api{path}", data=data,
        headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def vk_patch(path, body):
    port = vk_port()
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://localhost:{port}/api{path}", data=data,
        headers={"Content-Type": "application/json"}, method="PATCH"
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def gh_get(path):
    req = urllib.request.Request(
        f"https://api.github.com{path}",
        headers={
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def priority_from_labels(labels):
    names = {l["name"] for l in labels}
    if "P0-data-integrity" in names: return "urgent"
    if "P1-correctness" in names:    return "high"
    if "P2-tech-debt" in names:      return "medium"
    if "performance" in names:       return "medium"
    return "low"


def issue_url(number):
    return f"https://github.com/{GITHUB_REPO}/issues/{number}"


def card_title(issue):
    return f"#{issue['number']} {issue['title']}"


def card_description(issue):
    labels = ", ".join(l["name"] for l in issue.get("labels", []))
    desc = f"GitHub: {issue_url(issue['number'])}"
    if labels:
        desc += f"\nLabels: {labels}"
    if issue.get("body"):
        body_preview = issue["body"][:300].strip()
        if body_preview:
            desc += f"\n\n{body_preview}"
    return desc


# ── GitHub ────────────────────────────────────────────────────────────────────

def fetch_github_issues(state="open"):
    issues = []
    page = 1
    while True:
        batch = gh_get(f"/repos/{GITHUB_REPO}/issues?state={state}&per_page=100&page={page}")
        if not batch:
            break
        # Filter out pull requests
        issues.extend(i for i in batch if "pull_request" not in i)
        if len(batch) < 100:
            break
        page += 1
    return issues


# ── VK board ──────────────────────────────────────────────────────────────────

def fetch_vk_issues():
    return vk_get(f"/remote/issues?project_id={SP_PROJECT_ID}")["data"]["issues"]


def extract_gh_number(vk_issue):
    """Extract GitHub issue number from VK card title like '#309 bug(csr):...'"""
    title = vk_issue.get("title", "")
    if title.startswith("#"):
        try:
            return int(title.split()[0][1:])
        except (ValueError, IndexError):
            pass
    return None


# ── Sync logic ────────────────────────────────────────────────────────────────

def sync():
    log = []

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] SparrowDB VK sync starting...")

    # Fetch current state
    gh_open   = {i["number"]: i for i in fetch_github_issues("open")}
    gh_closed = {i["number"]: i for i in fetch_github_issues("closed")}
    vk_issues = fetch_vk_issues()

    # Map GitHub number → VK card
    vk_by_gh_num = {}
    for vk in vk_issues:
        num = extract_gh_number(vk)
        if num:
            vk_by_gh_num[num] = vk

    created = 0
    closed  = 0
    skipped = 0

    # 1. New open GitHub issues not yet on board → create in Backlog
    for num, gh in gh_open.items():
        if num not in vk_by_gh_num:
            pri = priority_from_labels(gh.get("labels", []))
            result = vk_post("/remote/issues", {
                "project_id": SP_PROJECT_ID,
                "status_id":  SP_BACKLOG,
                "title":      card_title(gh),
                "description": card_description(gh),
                "priority":   pri,
                "sort_order": float(num),
                "extension_metadata": {}
            })
            simple_id = result["data"]["data"]["simple_id"]
            msg = f"  + Created {simple_id} for #{num}: {gh['title'][:55]}"
            print(msg); log.append(msg)
            created += 1

    # 2. GitHub issues now closed → move VK card to Done (if agent doesn't own it)
    for num, vk in vk_by_gh_num.items():
        if num in gh_closed and num not in gh_open:
            status_id = vk.get("status_id", "")
            if status_id in AGENT_STATUSES:
                msg = f"  ~ Skipped #{num} (agent owns it: {vk['simple_id']})"
                print(msg); log.append(msg)
                skipped += 1
            elif status_id != SP_DONE and status_id != SP_CANCELLED:
                vk_patch(f"/remote/issues/{vk['id']}", {"status_id": SP_DONE})
                msg = f"  ✓ Closed {vk['simple_id']} for #{num} (merged on GitHub)"
                print(msg); log.append(msg)
                closed += 1

    print(f"Done: {created} created, {closed} closed, {skipped} skipped (agent-owned).")
    return {"created": created, "closed": closed, "skipped": skipped}


# ── Agent helper (import this in agent prompts) ───────────────────────────────

def vk_move(issue_id: str, status_id: str):
    """Move a VK card to a new column. Call this from agent code."""
    vk_patch(f"/remote/issues/{issue_id}", {"status_id": status_id})


def vk_find_by_gh(gh_number: int):
    """Find VK card by GitHub issue number. Returns card dict or None."""
    for vk in fetch_vk_issues():
        if extract_gh_number(vk) == gh_number:
            return vk
    return None


if __name__ == "__main__":
    try:
        result = sync()
        sys.exit(0)
    except urllib.error.URLError as e:
        print(f"ERROR: Cannot reach VK server — is it running on port {vk_port()}?")
        print(f"  {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        raise
