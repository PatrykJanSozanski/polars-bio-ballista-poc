#!/usr/bin/env bash
# Zbiorcza weryfikacja E2 / E4-A / E5-B.
# Buduje binaria raz, uruchamia jeden klaster Ballista, wykonuje trzy tryby overlap
# i porownuje wynikowe tabele 1:1.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT/target/ballista-logs"
OUT_DIR="$ROOT/target/ballista-compare"
mkdir -p "$LOG_DIR" "$OUT_DIR"

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

extract_table() {
  local input_file="$1"
  local output_file="$2"
  grep -E '^[+|]' "$input_file" > "$output_file"
}

run_mode() {
  local mode="$1"
  local tag="$2"
  local raw_out="$OUT_DIR/${tag}.raw.txt"
  local table_out="$OUT_DIR/${tag}.table.txt"

  echo "[COMPARE] running mode ${mode}"
  ./target/debug/query --provider-mode "$mode" --limit 20 | tee "$raw_out"
  extract_table "$raw_out" "$table_out"
}

cd "$ROOT"

echo "[COMPARE] building POC binaries"
cargo build --bins

echo "[COMPARE] starting local Ballista cluster"
./target/debug/scheduler --bind-port 50050 >"$LOG_DIR/compare-scheduler.log" 2>&1 &
scheduler_pid=$!
sleep 2

./target/debug/executor --port 50051 --grpc-port 50052 --work-dir target/ballista/compare-executor-1 >"$LOG_DIR/compare-executor-1.log" 2>&1 &
executor1_pid=$!

./target/debug/executor --port 50061 --grpc-port 50062 --work-dir target/ballista/compare-executor-2 >"$LOG_DIR/compare-executor-2.log" 2>&1 &
executor2_pid=$!
sleep 4

run_mode serialized e2
run_mode approach-a e4a
run_mode approach-b e5b

if ! diff -u "$OUT_DIR/e2.table.txt" "$OUT_DIR/e4a.table.txt" >/dev/null; then
  echo "[COMPARE] E2 and E4-A differ" >&2
  diff -u "$OUT_DIR/e2.table.txt" "$OUT_DIR/e4a.table.txt" || true
  exit 1
fi

if ! diff -u "$OUT_DIR/e2.table.txt" "$OUT_DIR/e5b.table.txt" >/dev/null; then
  echo "[COMPARE] E2 and E5-B differ" >&2
  diff -u "$OUT_DIR/e2.table.txt" "$OUT_DIR/e5b.table.txt" || true
  exit 1
fi

echo "[COMPARE] SUCCESS: E2, E4-A and E5-B returned identical tables."
echo "[COMPARE] saved outputs in $OUT_DIR"
