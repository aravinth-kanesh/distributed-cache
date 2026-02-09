#!/usr/bin/env bash
#
# bench.sh — runs redis-benchmark against a local dcache instance.
# Requires redis-benchmark to be installed (brew install redis / apt install redis-tools).
#
set -euo pipefail

PORT="${1:-6379}"
BINARY="./dcache"
REQUESTS=1000000
CLIENTS=50

if ! command -v redis-benchmark &>/dev/null; then
    echo "error: redis-benchmark not found. Install redis-tools first."
    exit 1
fi

if [ ! -f "$BINARY" ]; then
    echo "building dcache..."
    go build -o "$BINARY" cmd/server/main.go
fi

# Start dcache in the background
"$BINARY" --port "$PORT" --appendonly=false --save-interval 0 --metrics-port 0 &
SERVER_PID=$!

cleanup() {
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for server to be ready
echo "waiting for dcache on port $PORT..."
for i in $(seq 1 30); do
    if redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG; then
        break
    fi
    sleep 0.1
done

echo ""
echo "=== DCache Benchmark (port $PORT, $CLIENTS clients, $REQUESTS requests) ==="
echo ""

redis-benchmark -p "$PORT" \
    -t set,get,lpush,lpop,sadd,hset \
    -n "$REQUESTS" \
    -c "$CLIENTS" \
    -q \
    --csv

echo ""
echo "benchmark complete"
