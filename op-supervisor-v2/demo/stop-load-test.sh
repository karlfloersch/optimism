#!/usr/bin/env bash

set -euo pipefail

echo "=== Stopping All Load Test Workers ==="

# Find and kill all load test related processes
LOAD_TEST_PIDS=$(pgrep -f "load-test.sh" 2>/dev/null || true)
MSGCHECK_PIDS=$(pgrep -f "msgcheck.*-mode.*-from" 2>/dev/null || true)
GO_RUN_PIDS=$(pgrep -f "go run.*msgcheck" 2>/dev/null || true)

# Count total processes to kill
TOTAL_PIDS=$(echo "$LOAD_TEST_PIDS $MSGCHECK_PIDS $GO_RUN_PIDS" | tr ' ' '\n' | grep -v '^$' | sort -u | wc -l | tr -d ' ')

if [ "$TOTAL_PIDS" -eq 0 ]; then
    echo "✅ No load test workers found running"
    exit 0
fi

echo "Found $TOTAL_PIDS processes to stop:"

# Show what we're about to kill
if [ -n "$LOAD_TEST_PIDS" ]; then
    echo "Load test scripts: $LOAD_TEST_PIDS"
fi
if [ -n "$MSGCHECK_PIDS" ]; then
    echo "Msgcheck processes: $MSGCHECK_PIDS"
fi
if [ -n "$GO_RUN_PIDS" ]; then
    echo "Go run processes: $GO_RUN_PIDS"
fi

echo ""
echo "Stopping processes..."

# Kill load test scripts first
if [ -n "$LOAD_TEST_PIDS" ]; then
    for pid in $LOAD_TEST_PIDS; do
        if kill -TERM "$pid" 2>/dev/null; then
            echo "Sent SIGTERM to load-test.sh (PID: $pid)"
        fi
    done
fi

# Kill msgcheck processes
if [ -n "$MSGCHECK_PIDS" ]; then
    for pid in $MSGCHECK_PIDS; do
        if kill -TERM "$pid" 2>/dev/null; then
            echo "Sent SIGTERM to msgcheck (PID: $pid)"
        fi
    done
fi

# Kill go run processes
if [ -n "$GO_RUN_PIDS" ]; then
    for pid in $GO_RUN_PIDS; do
        if kill -TERM "$pid" 2>/dev/null; then
            echo "Sent SIGTERM to go run (PID: $pid)"
        fi
    done
fi

# Wait a moment for graceful shutdown
sleep 2

# Force kill any remaining processes
REMAINING_LOAD=$(pgrep -f "load-test.sh" 2>/dev/null || true)
REMAINING_MSG=$(pgrep -f "msgcheck.*-mode.*-from" 2>/dev/null || true)
REMAINING_GO=$(pgrep -f "go run.*msgcheck" 2>/dev/null || true)

if [ -n "$REMAINING_LOAD" ]; then
    for pid in $REMAINING_LOAD; do
        if kill -KILL "$pid" 2>/dev/null; then
            echo "Force killed load-test.sh (PID: $pid)"
        fi
    done
fi

if [ -n "$REMAINING_MSG" ]; then
    for pid in $REMAINING_MSG; do
        if kill -KILL "$pid" 2>/dev/null; then
            echo "Force killed msgcheck (PID: $pid)"
        fi
    done
fi

if [ -n "$REMAINING_GO" ]; then
    for pid in $REMAINING_GO; do
        if kill -KILL "$pid" 2>/dev/null; then
            echo "Force killed go run (PID: $pid)"
        fi
    done
fi

echo ""
echo "✅ All load test workers have been stopped"

# Show any remaining related processes
FINAL_CHECK=$(pgrep -f "load-test\|msgcheck.*-mode" 2>/dev/null || true)
if [ -n "$FINAL_CHECK" ]; then
    echo "⚠️  Warning: Some processes may still be running:"
    ps -p $FINAL_CHECK -o pid,ppid,command 2>/dev/null || true
else
    echo "✅ Confirmed: No related processes remain"
fi
