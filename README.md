# polars-bio-ballista-poc

POC uruchamiania operacji overlap z `polars-bio` przez rozproszony klaster
Apache Ballista bez modyfikacji Ballisty.

## Cel POC

Sprawdzić, czy operację `overlap` z `polars-bio` można dystrybuować przez
Ballistę przy minimalnych zmianach w stosunku do istniejącego kodu upstream.

## Stos technologiczny

- Apache Ballista `52.0.0`
- DataFusion `52.5.0`
- `datafusion-bio-function-ranges` branch `feat/bump-datafusion-52`
- lokalny klaster: `1 scheduler + 2 executors`

## Layout repo

```text
src/
  bin/
    scheduler.rs   — lokalny scheduler Ballisty z codec override
    executor.rs    — lokalny executor Ballisty z codec override
    query.rs       — runner zapytania; obsługuje tryby e1 / serialized / approach-a / approach-b / direct
  codec.rs         — PolarsBioBallistaLogicalCodec (serializacja providera overlap)
  operation.rs     — providery E2 / E4-A / E5-B + direct overlap dla E3
  overlap.rs       — OverlapParquetFunction (UDTF dla E1)
  fixtures.rs      — interval_schema() i generowanie toy parquet dla E1
  lib.rs           — deklaracje modułów
scripts/
  run-local-e1.sh  — harness E1
  run-local-e2.sh  — harness E2
  run-local-e3.sh  — harness E3 (weryfikuje oczekiwany błąd)
  run-local-e4-a.sh — harness E4-A (weryfikacja podejścia A)
  run-local-e5-b.sh — harness E5-B (weryfikacja podejścia B)
  run-local-compare-overlap-modes.sh — zbiorcze porownanie E2 / E4-A / E5-B
fixtures/
  e1/              — deterministyczne toy parquet (id/contig/start/end)
  polars-bio/      — kopie parquet z polars-bio (contig/pos_start/pos_end)
EXPERIMENTS.md     — pelny rejestr eksperymentow E1/E2/E3/E4-A/E5-B z instrukcjami
docs/              — uporzadkowana dokumentacja koncowa POC
```

## Eksperymenty

Repo zawiera piec eksperymentow. Szczegółowy opis, instrukcje uruchomienia
i różnice względem `polars-bio` są w `EXPERIMENTS.md`.

| Eksperyment | Opis | Wynik |
| --- | --- | --- |
| E1 | Toy UDTF `overlap_demo` przez Ballistę | sukces — 3 pary overlap |
| E2 | Overlap w stylu `polars-bio` + serializowalny adapter | sukces — 20 wierszy |
| E4-A | Symulacja podejścia A (provider bez `session` + gettery + codec zewnętrzny) | sukces — 20 wierszy |
| E5-B | Symulacja podejścia B (provider + wersjonowany kontrakt serializacji) | sukces — 20 wierszy |
| E3 | Bezpośredni upstream `OverlapProvider` (checkpoint regresyjny) | oczekiwany błąd serializacji |

## Szybki start

Każdy eksperyment ma dedykowany skrypt:

```bash
./scripts/run-local-e1.sh
./scripts/run-local-e2.sh
./scripts/run-local-e3.sh
./scripts/run-local-e4-a.sh
./scripts/run-local-e5-b.sh
./scripts/run-local-compare-overlap-modes.sh
```

Skrypty budują binaria, uruchamiają klaster lokalny i zamykają go po wykonaniu.

Dane wejściowe dla E2, E3, E4-A i E5-B to kopie plików Parquet z `polars-bio`
przechowywane w `fixtures/polars-bio/`. Ścieżki można nadpisać:

```bash
./target/debug/query --provider-mode serialized --left <left.parquet> --right <right.parquet>
```

## Wyniki

E2, E4-A i E5-B zwracają ten sam wynik tabelaryczny dla tych samych plików
Parquet z `polars-bio`. Zbiorczo weryfikuje to `run-local-compare-overlap-modes.sh`.

Przykładowy wynik:

```text
+----------+-------------+-----------+----------+-------------+-----------+
| contig_1 | pos_start_1 | pos_end_1 | contig_3 | pos_start_3 | pos_end_3 |
+----------+-------------+-----------+----------+-------------+-----------+
| chr9     | 14313480    | 14314045  | chr9     | 14313566    | 14317405  |
| ...
```

Plan przechodzi przez: zdalny `SessionContext` Ballisty, serializację providera
przez `PolarsBioBallistaLogicalCodec`, scheduler, dwa executory i wynik końcowy.

E3 pozostaje kontrolą negatywną i kończy się oczekiwanym błędem:

```text
Error: Internal error: failed to serialize logical plan:
  NotImplemented("LogicalExtensionCodec is not provided")
```

## Wniosek architektoniczny

Bezpośrednie uruchomienie upstream `OverlapProvider` przez Ballistę jest
niemożliwe bez zmian w `datafusion-bio-function-ranges`, ponieważ:

1. Pole `session: Arc<SessionContext>` jest nieserializowalne.
2. Wszystkie pola `OverlapProvider` są prywatne, więc zewnętrzny codec nie ma czego odczytać.
3. Upstream nie zawiera implementacji `LogicalExtensionCodec`.

`SerializableOverlapProvider` z E2 jest więc działającą architekturą przejściową
bez forka upstream. E4-A i E5-B pokazują dwie sensowne ścieżki dojścia do
lepszego modelu upstream:

- Podejście A: usunięcie `session` z pól i wystawienie potrzebnych danych przez gettery.
- Podejście B: pełny, wersjonowany kontrakt serializacji providera.

Znaczenie produkcyjne:

- E2 jest bezpiecznym adapterem przejściowym, który działa już teraz.
- E4-A jest najtańszą ścieżką upstream do ograniczenia długu technicznego.
- E5-B jest docelowo najlepsze dla utrzymania, ale wymaga większej odpowiedzialności po stronie upstream.

Szczegóły, instrukcje i porównanie z kodem oryginalnym są w `EXPERIMENTS.md`,
`docs/README.md` oraz `docs/add-ballista-distributed-execution-poc/report.md`.

## Ograniczenia POC

- Scope: tylko Parquet; API Pythona `polars-bio` jest poza zakresem.
- E4-A i E5-B są symulacjami strategii upstream, nie patchami upstream.
- Produkcyjne wdrożenie A/B wymaga zmian w `datafusion-bio-function-ranges`.

