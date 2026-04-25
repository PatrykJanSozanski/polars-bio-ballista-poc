#!/usr/bin/env bash
# E4-A - Weryfikacja podejscia A (provider bez session + gettery + zewnetrzny codec)
# Oczekiwany wynik: sukces i tabela overlap, semantycznie jak E2.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT/target/ballista-logs"
mkdir -p "$LOG_DIR"

scheduler_pid=""
executor1_pid=""
executor2_pid=""

cleanup() {
  for pid in "$executor2_pid" "$executor1_pid" "$scheduler_pid"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM

cd "$ROOT"

echo "[E4-A] building POC binaries"
cargo build --bins

echo "[E4-A] starting local Ballista cluster"
./target/debug/scheduler --bind-port 50050 >"$LOG_DIR/e4a-scheduler.log" 2>&1 &
scheduler_pid=$!
sleep 2

./target/debug/executor --port 50051 --grpc-port 50052 --work-dir target/ballista/e4a-executor-1 >"$LOG_DIR/e4a-executor-1.log" 2>&1 &
executor1_pid=$!

./target/debug/executor --port 50061 --grpc-port 50062 --work-dir target/ballista/e4a-executor-2 >"$LOG_DIR/e4a-executor-2.log" 2>&1 &
executor2_pid=$!
sleep 4

echo "[E4-A] running approach-a overlap query"
./target/debug/query --provider-mode approach-a --limit 20

echo "[E4-A] cluster logs written to $LOG_DIR"
