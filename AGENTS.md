# Agents: SSH Workflow with Per-User Unix Accounts (No Containers)

This document is the canonical spec for how agents collaborate on a shared host over **SSH** using **dedicated Unix user accounts** for isolation. It **removes the containerized option** and standardizes on per-user isolation only.

## Core Principles

- Always verify **git state** matches between local and remote before work begins.
- Do **all development/testing on the remote host** as the claimed Unix user.
- At the end, **pull the committed changes locally** and present a code-only PR.
- Use a **race-free user claim protocol** (lock + heartbeat) so agents never collide.

---

## Quick Start (TL;DR)

```bash
# 0) Ensure we start from the same git ref
./bin/agent git:compare  # runs locally

# 1) SSH and claim a user, then drop into a session
ssh "$REMOTE" "cd $TARGET_REPO && ./bin/agent user:claim --ref '$GIT_REF' --branch '$BRANCH' && ./bin/agent user:shell"

# 2) Inside the session, work normally (build/test/commit/push)
# ...

# 3) Finalize locally and render a code-only PR
./bin/agent finalize --ref "$GIT_REF" --remote "$REMOTE" --branch "$BRANCH"
```

---

## Host Prerequisites

- A pool of dedicated users: `agent-0`, `agent-1`, `agent-2`, and `agent-3` (expandable if needed).
- Orchestrator user can switch with passwordless `sudo -iu <agent>`.
- Standard tools: `flock`, `pgrep`, `who`, `date`, `awk`, `sudo` (and optionally `loginctl`).

### Files & Directories

```
/etc/agents/users.txt   # pool: one username per line
/var/lock/agents/       # lockfiles (root-owned)
/var/run/agents/        # leases (root-owned, tmpfs recommended)
```

### Sudoers

```
# /etc/sudoers.d/agents
Cmnd_Alias AGENT_SWITCH = /usr/bin/sudo -iu *, /bin/su -
%agents ALL=(ALL:ALL) NOPASSWD: AGENT_SWITCH
```

Add the orchestrator user to the `agents` group.

---

## Active Session Detection

A user is **in use** if any of the following are true:

- A **fresh lease** exists and its PID is alive.
- `who` shows the username as logged in.
- `pgrep -u "$user" -fa "sshd: $user@"` finds an active SSH session.
- (Optional) `loginctl list-sessions` shows an active session.

Helper script `bin/agent-user-active`:

```bash
#!/usr/bin/env bash
set -euo pipefail
u=${1:?user}
lease="/var/run/agents/${u}.lease"

is_alive() { kill -0 "$1" 2>/dev/null; }
if [[ -f "$lease" ]]; then
  read -r pid ts < "$lease" || true
  now=$(date +%s)
  if [[ -n "${pid:-}" ]] && is_alive "$pid" && (( now - ${ts:-0} < 120 )); then exit 0; fi
fi
pgrep -u "$u" -fa "sshd: $u@" >/dev/null && exit 0
who | awk '{print $1}' | grep -qx "$u" && exit 0
exit 1
```

---

## Race-Free User Claiming (Lock + Heartbeat)

We rely on **per-user lockfiles** guarded by `flock` and a **heartbeat lease** to avoid collisions and stale claims.

### Acquire (`bin/agent-user-acquire`)

```bash
#!/usr/bin/env bash
set -euo pipefail
POOL_FILE=${POOL_FILE:-/etc/agents/users.txt}
LEASE_DIR=${LEASE_DIR:-/var/run/agents}
LOCK_DIR=${LOCK_DIR:-/var/lock/agents}
HEARTBEAT=${HEARTBEAT:-30}

# Shuffle so agents spread evenly
mapfile -t pool < <(shuf "$POOL_FILE")

for u in "${pool[@]}"; do
  lock="$LOCK_DIR/$u.lock"
  exec {fd}<>"$lock" || continue
  if flock -n "$fd"; then
    # Double-check other activity
    if bin/agent-user-active "$u"; then
      flock -u "$fd"; continue
    fi
    lease="$LEASE_DIR/$u.lease"
    echo "$$ $(date +%s)" > "$lease"
    # Heartbeat while parent lives
    (
      while kill -0 $$ 2>/dev/null; do
        echo "$$ $(date +%s)" > "$lease"; sleep "$HEARTBEAT"
      done
    ) & disown
    # Output: username and lock fd path so caller can release
    echo "$u $fd $lease"
    exit 0
  fi
done

echo "No available users in pool" >&2
exit 2
```

### Release & GC

`bin/agent-user-release`:

```bash
#!/usr/bin/env bash
set -euo pipefail
u=${1:?user}
rm -f "/var/run/agents/${u}.lease" || true
```

`bin/agent-user-gc`:

```bash
#!/usr/bin/env bash
set -euo pipefail
LEASE_DIR=${LEASE_DIR:-/var/run/agents}
MAX_AGE=${MAX_AGE:-600}
now=$(date +%s)
for f in "$LEASE_DIR"/*.lease; do
  [[ -e "$f" ]] || continue
  read -r pid ts < "$f" || continue
  if ! kill -0 "$pid" 2>/dev/null || (( now - ts > MAX_AGE )); then
    rm -f "$f"
  fi
done
```

---

## Orchestration CLI (`bin/agent`)

A single entrypoint agents/humans call for the full workflow.

```bash
#!/usr/bin/env bash
set -euo pipefail
cmd=${1:-help}; shift || true

case "$cmd" in
  git:compare)
    : "${BRANCH:?Set BRANCH}";
    LOCAL=$(git rev-parse "$BRANCH")
    REMOTE=$(git ls-remote --heads origin "$BRANCH" | awk '{print $1}')
    test "$LOCAL" = "$REMOTE" || { echo "Branch mismatch $LOCAL != $REMOTE" >&2; exit 1; }
    ;;

  user:claim)
    read USER FD LEASE < <(bin/agent-user-acquire)
    echo "$USER" > .agent.user
    # Tiny window: re-check then proceed
    if bin/agent-user-active "$USER"; then
      flock -u "$FD"; rm -f "$LEASE"; exec "$0" user:claim "$@"
    fi
    ;;

  user:shell)
    USER=$(cat .agent.user)
    sudo -iu "$USER" bash -lc '
      set -euo pipefail
      if [ ! -d "$TARGET_REPO" ]; then git clone $REPO_URL $TARGET_REPO; fi
      cd "$TARGET_REPO"
      git fetch --all --tags
      git checkout -B "$BRANCH" "$GIT_REF"
      # Your bootstrap here (deps, tests, etc.)
      exec bash
    '
    ;;

  run)
    USER=$(cat .agent.user)
    sudo -iu "$USER" bash -lc "$*" ;;

  commit)
    USER=$(cat .agent.user)
    sudo -iu "$USER" bash -lc "cd \"$TARGET_REPO\" && git add -A && git commit -m 'agent: update' && git push -u origin \"$BRANCH\"" ;;

  finalize)
    : "${BRANCH:?Set BRANCH}"; git fetch && git checkout "$BRANCH" && git pull --ff-only
    git --no-pager diff --name-status origin/main...HEAD
    ;;

  user:release)
    USER=$(cat .agent.user)
    bin/agent-user-release "$USER" || true
    rm -f .agent.user || true
    ;;

  *)
    echo "Usage: bin/agent [git:compare|user:claim|user:shell|run|commit|finalize|user:release]" ;;
 esac
```

---

## End-to-End SSH Flow

1. **Local**: `./bin/agent git:compare` to assert local vs remote branch/SHAs match.
2. **Remote** (via SSH): `./bin/agent user:claim` to atomically acquire an available user.
3. **Remote**: `./bin/agent user:shell` to enter a login shell as the claimed user and bootstrap the repo at the target ref.
4. **Remote**: develop, test, `./bin/agent commit`.
5. **Local**: `./bin/agent finalize` to pull and present a code-only diff/PR.
6. **Remote**: `./bin/agent user:release` (also handled automatically if the orchestrator dies due to FD-tied lock + heartbeat GC).

---

## Security & Policy

- **Least privilege**: Claimed users have only the permissions they need in their home and the repo workspace.
- **Visibility**: Non-root users see only their own processes; orchestrator can audit via `sudo` if necessary.
- **Auditing**: Tag commits with `user.name = Agent <agent-X>` where `X` is 0–3.
- **Quotas**: Optional per-user disk quotas or per-user ZFS/LVM datasets.
- **Network policy**: Egress allowlist if running third-party agents.

---

## FAQ

**Why not containers?**
We standardized on Unix users to reduce complexity and avoid container runtime dependencies while keeping isolation via file permissions and process visibility.

**How do we avoid races?**
Atomic `flock` per user, immediate re-check, and a heartbeat lease + GC keep claims correct even under crashes.

**Can we scale beyond four users?**
Yes—append to `/etc/agents/users.txt` (e.g., `agent-4`, `agent-5`, …). The claim loop shuffles to spread load.

**How do we add language toolchains?**
Install per-user or system-wide pinned toolchains; or add Nix (`nix develop`) for reproducible dev shells without containers.

---

*End of spec.*
