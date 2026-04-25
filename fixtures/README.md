# Fixtures

Parquet fixtures dla eksperymentow przechowywane lokalnie w repo.

## fixtures/polars-bio/

Kopie plikow Parquet z `polars-bio` uzywane przez E2 i E3.
Schemat: `contig`, `pos_start`, `pos_end` (zgodny z `polars-bio`).
Pliki nie sa generowane w runtime — sa statycznymi kopiami.

## fixtures/e1/

Deterministyczne toy pliki Parquet dla E1.
Schemat: `id`, `contig`, `start`, `end`.
Generowane automatycznie przez `ensure_e1_toy_parquet_fixtures()` w `src/fixtures.rs`,
jezeli nie istnieja. Dane: 3 rekordy left, 4 rekordy right, 3 oczekiwane pary overlap.
