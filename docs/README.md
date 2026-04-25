# Dokumentacja POC

Ten katalog zawiera tylko dokumenty zgodne z aktualnym zakresem `polars-bio-ballista-poc`.

## Kanoniczny zestaw dokumentow

- `../README.md` - przeglad repo, szybki start i wnioski architektoniczne
- `../EXPERIMENTS.md` - komplet instrukcji wykonania E1/E2/E3/E4-A/E5-B
- `add-ballista-distributed-execution-poc/report.md` - raport koncowy POC, w tym analiza A/B i odniesienie do produkcji

## Zakres dokumentacji

Repo utrzymuje piec eksperymentow:

- E1 - toy UDTF bootstrap
- E2 - adapter serializowalny (aktualny fallback produkcyjny)
- E3 - direct upstream checkpoint regresyjny
- E4-A - symulacja podejscia A
- E5-B - symulacja podejscia B

## Skrypty wykonawcze

- `../scripts/run-local-e1.sh`
- `../scripts/run-local-e2.sh`
- `../scripts/run-local-e3.sh`
- `../scripts/run-local-e4-a.sh`
- `../scripts/run-local-e5-b.sh`
- `../scripts/run-local-compare-overlap-modes.sh`

Ostatni skrypt uruchamia zbiorczo E2, E4-A i E5-B oraz porownuje tabele wynikowe.
