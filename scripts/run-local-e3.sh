#!/usr/bin/env bash
# E3 - Bezposredni upstream OverlapProvider (checkpoint regresyjny)
# Uruchamia lokalny klaster Ballista i wykonuje zapytanie overlap (tryb direct).
# Oczekiwany wynik: BLAD serializacji "LogicalExtensionCodec is not provided".
# Skrypt konczy sie kodem 0 jesli pojawil sie oczekiwany blad, 1 w przypadku
# nieoczekiwanego przebiegu.
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

echo "[E3] building POC binaries"
cargo build --bins

echo "[E3] starting local Ballista cluster"
./target/debug/scheduler --bind-port 50050 >"$LOG_DIR/e3-scheduler.log" 2>&1 &
scheduler_pid=$!
sleep 2

./target/debug/executor --port 50051 --grpc-port 50052 --work-dir target/ballista/e3-executor-1 >"$LOG_DIR/e3-executor-1.log" 2>&1 &
executor1_pid=$!

./target/debug/executor --port 50061 --grpc-port 50062 --work-dir target/ballista/e3-executor-2 >"$LOG_DIR/e3-executor-2.log" 2>&1 &
executor2_pid=$!
sleep 4

echo "[E3] running direct upstream OverlapProvider (expected to fail)"
set +e
./target/debug/query --provider-mode direct --limit 5 >"$LOG_DIR/e3-query.log" 2>&1
QUERY_EXIT=$?
set -e

cat "$LOG_DIR/e3-query.log"

if grep -q "LogicalExtensionCodec is not provided" "$LOG_DIR/e3-query.log"; then
  echo ""
  echo "[E3] CHECKPOINT PASSED: received expected serialization error."
  echo "[E3] This confirms that upstream OverlapProvider cannot be used directly"
  echo "[E3] through Ballista without a custom LogicalExtensionCodec."
  exit 0
else
  echo ""
  echo "[E3] UNEXPECTED RESULT: expected serialization error was not found." >&2
  echo "[E3] Exit code was: $QUERY_EXIT" >&2
  exit 1
fi
