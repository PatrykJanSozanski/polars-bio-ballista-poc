# polars-bio-ballista-poc

Osobne repo do lokalnego spike'a rozszerzania Ballisty o własną TVF/UDTF.

Aktualny zakres POC:
- lokalny klaster `1 scheduler + 2 executors`
- `Ballista` jako zależność Cargo
- jedna własna TVF `overlap_demo(left_path, right_path)`
- dwa pliki `Parquet` z toy danymi interwałowymi
- uruchamianie przez `cargo run`

## Co robi PoC

`overlap_demo` przyjmuje ścieżki do dwóch plików `Parquet`, buduje logiczny plan `DataFusion` z joinem interwałowym i zwraca go jako `ViewTable`. Dzięki temu pierwszy spike testuje ścieżkę rozszerzania o własną TVF bez dokładania własnych codeców na starcie.

Fixture'y mają stały schemat:
- `id: Utf8`
- `contig: Utf8`
- `start: Int64`
- `end: Int64`

## Layout repo

- `src/lib.rs` - wspólne moduły PoC
- `src/fixtures.rs` - generowanie dwóch plików `Parquet`
- `src/overlap.rs` - implementacja `overlap_demo`
- `src/bin/scheduler.rs` - lokalny scheduler Ballisty
- `src/bin/executor.rs` - lokalny executor Ballisty
- `src/bin/query.rs` - runner zapytania do klastra
- `scripts/run-local.sh` - prosty harness uruchomieniowy

## Szybki start

1. Uruchom scheduler:
   `cargo run --bin scheduler`
2. Uruchom pierwszy executor:
   `cargo run --bin executor -- --port 50051 --grpc-port 50052 --work-dir target/ballista/executor-1`
3. Uruchom drugi executor:
   `cargo run --bin executor -- --port 50061 --grpc-port 50062 --work-dir target/ballista/executor-2`
4. W osobnym terminalu uruchom query runner:
   `cargo run --bin query`

Albo użyj harnessu:

`./scripts/run-local.sh`

## Oczekiwany wynik

Dla domyślnych fixture'ów powinny pojawić się trzy pary overlapów:
- `L1` z `R1`
- `L2` z `R2`
- `L3` z `R4`
