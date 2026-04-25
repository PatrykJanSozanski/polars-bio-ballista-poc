# Rejestr wersji eksperymentów

Ten plik gwarantuje, ze kazda wersja eksperymentu opisana w dokumentacji ma
odpowiadajaca implementacje w repo. Wszystkie ponizsze wersje sa utrzymywane
rownolegle jako material porownawczy.

## E1 - Toy UDTF bootstrap (`overlap_demo`)

Cel:

- Pierwsza walidacja: czy da sie uruchomic custom TVF/UDTF przez lokalny klaster
  Ballisty.

Kod obecny w repo:

- `src/overlap.rs` - implementacja `overlap_demo(left_path, right_path)`
- `src/fixtures.rs` - schemat `interval_schema()` i deterministyczne toy fixture'y Parquet
- `src/bin/query.rs` - tryb `--provider-mode e1`
- `scripts/run-local-e1.sh` - harness uruchomieniowy E1

Relacja do oryginalnego `polars-bio`:

- Nie jest to bezposredni port `src/operation.rs` z `polars-bio`.
- Uzywa uproszczonego joina na kolumnach `start/end` i nie korzysta z
  `datafusion-bio-function-ranges::OverlapProvider`.
- Traktowac jako etap bootstrap, nie jako docelowa integracje.

Instrukcja wykonania:

**Wariant skryptowy:**

```bash
./scripts/run-local-e1.sh
```

**Wariant reczny** (wszystkie komendy w osobnych terminalach):

```bash
# Terminal 1 — build
cargo build --bins

# Terminal 1 — scheduler
./target/debug/scheduler --bind-port 50050
```

```bash
# Terminal 2 — executor 1
./target/debug/executor --port 50051 --grpc-port 50052 --work-dir target/ballista/e1-executor-1
```

```bash
# Terminal 3 — executor 2
./target/debug/executor --port 50061 --grpc-port 50062 --work-dir target/ballista/e1-executor-2
```

```bash
# Terminal 4 — query (po uruchomieniu klastra)
./target/debug/query --provider-mode e1 --limit 10
```

Oczekiwany wynik:

- query konczy sie sukcesem,
- wypisywane sa 3 pary overlap: `L1-R1`, `L2-R2`, `L3-R4`,
- logi klastra trafiaja do `target/ballista-logs/`.

Co dodano wzgledem oryginalnego `polars-bio`:

- Calkowicie nowa implementacja: `OverlapParquetFunction` jako `TableFunctionImpl` w `src/overlap.rs`.
  W `polars-bio` nie istnieje odpowiednik — to izolowany bootstrap do walidacji mechanizmu TVF w Balliscie.
- Deterministyczne toy fixture'y Parquet (schemat `id/contig/start/end`) generowane programatycznie
  przez `ensure_e1_toy_parquet_fixtures()` w `src/fixtures.rs`. Dane nie pochodza z `polars-bio`.
- Tryb `--provider-mode e1` w `src/bin/query.rs` oraz skrypt `scripts/run-local-e1.sh`.
- Eksperyment celowo nie uzywa `datafusion-bio-function-ranges` — join interwałowy zbudowany
  bezposrednio na `LogicalPlanBuilder`.

## E2 - Port overlap w stylu `polars-bio` z adapterem serializowalnym

Cel:

- Przeniesienie logiki overlap na dane Parquet skopiowane z `polars-bio` i
  uruchomienie przez Balliste.

Kod obecny w repo:

- `src/operation.rs` - `do_polars_bio_style_overlap(...)`
- `src/operation.rs` - `SerializableOverlapProvider`
- `src/codec.rs` - `PolarsBioBallistaLogicalCodec`
- `src/bin/query.rs` - tryb `--provider-mode serialized`
- `src/bin/scheduler.rs` i `src/bin/executor.rs` - zaladowanie tego samego codec

Relacja do oryginalnego `polars-bio`:

- To najblizsza funkcjonalnie wersja wzgledem `polars-bio/src/operation.rs`.
- Zachowuje semantyke overlap i kolumny `contig`, `pos_start`, `pos_end`.
- Dodaje jedynie warstwe dystrybucyjna wymagana przez Balliste:
  serializowalny provider + `LogicalExtensionCodec`.

Instrukcja wykonania:

**Wariant skryptowy:**

```bash
./scripts/run-local-e2.sh
```

**Wariant reczny** (wszystkie komendy w osobnych terminalach):

```bash
# Terminal 1 — build
cargo build --bins

# Terminal 1 — scheduler
./target/debug/scheduler --bind-port 50050
```

```bash
# Terminal 2 — executor 1
./target/debug/executor --port 50051 --grpc-port 50052 --work-dir target/ballista/e2-executor-1
```

```bash
# Terminal 3 — executor 2
./target/debug/executor --port 50061 --grpc-port 50062 --work-dir target/ballista/e2-executor-2
```

```bash
# Terminal 4 — query (po uruchomieniu klastra)
./target/debug/query --provider-mode serialized --limit 20
```

Oczekiwany wynik:

- query konczy sie sukcesem,
- wypisywana jest tabela overlap (domyslnie 20 wierszy),
- logi klastra trafiaja do `target/ballista-logs/`.

Co dodano wzgledem oryginalnego `polars-bio`:

- `SerializableOverlapProvider` w `src/operation.rs` — adapter wokol logiki overlap z `polars-bio/src/operation.rs`.
  Semantyka i kolumny (`contig`, `pos_start`, `pos_end`) sa identyczne; jedyna zmiana to mozliwosc
  serializacji przez Balliste.
- `PolarsBioBallistaLogicalCodec` w `src/codec.rs` — minimalny codec kodujacy i dekodujacy provider
  przez protokol Ballisty. W `polars-bio` nie istnieje zadna ścieżka serializacji.
- Scheduler i executory (`src/bin/scheduler.rs`, `src/bin/executor.rs`) laduja ten sam codec,
  aby plan byl spójny we wszystkich procesach klastra.
- Lokalne kopie fixture'ow Parquet z `polars-bio` w `fixtures/polars-bio/` — bez zaleznosci
  miedzy repozytoriami w czasie wykonania.
- Tryb `--provider-mode serialized` w `src/bin/query.rs` oraz skrypt `scripts/run-local-e2.sh`.

Mapa zmian funkcja-po-funkcji wzgledem `polars-bio/src/operation.rs`:

- Oryginalne `do_overlap(...)` ma w POC dwa odpowiedniki:
  `do_polars_bio_direct_overlap(...)` jako checkpoint E3 oraz `do_polars_bio_style_overlap(...)`
  jako wejscie do dzialajacej sciezki E2.
- Oryginalny etap `ctx.table(...).schema()` zostal zachowany niemal 1:1 i nadal sluzy do
  zbudowania tego samego ukladu kolumn wynikowych.
- Oryginalne `OverlapProvider::new(...)` zostalo w E2 zastapione przez
  `SerializableOverlapProvider::new(...)`, bo provider upstream nie moze przejsc przez granice procesu.
- Oryginalne `ctx.register_table(...)` pozostaje, ale rejestrowany obiekt jest serializowalnym
  providerem POC zamiast upstreamowego providera.
- Oryginalne budowanie `SELECT left_* ..., right_* ...` zostalo zachowane praktycznie bez zmian
  w `do_overlap(...)` w POC, z tym samym aliasowaniem kolumn.
- Logika wykonawcza, ktora w oryginale byla ukryta wewnatrz upstreamowego providera, w E2 zostala
  jawnie odtworzona w `TableProvider::scan(...)` oraz `build_overlap_plan_from_paths(...)`.
- Nowy element tylko dla trybu rozproszonego: `PolarsBioBallistaLogicalCodec` w `src/codec.rs`,
  ktory koduje i odtwarza providera na schedulerze i executorach.

## E4-A - Weryfikacja podejscia A (minimalny upstream refactor)

Cel:

- Zweryfikowac, czy model "provider bez `session` + publiczne gettery +
  codec zewnetrzny" daje ten sam efekt wykonawczy co E2.

Kod obecny w repo:

- `src/operation.rs` - `do_approach_a_overlap(...)`
- `src/operation.rs` - `ApproachAOverlapProvider`
- `src/codec.rs` - obsluga `OVERLAP_PROVIDER_A_MAGIC`
- `src/bin/query.rs` - tryb `--provider-mode approach-a`
- `scripts/run-local-e4-a.sh` - harness uruchomieniowy E4-A

Relacja do oryginalnego `polars-bio`:

- To eksperymentalna symulacja tego, jak wygladalby upstream po wdrozeniu
  podejscia A.
- Runtime i topologia klastra pozostaja identyczne jak w E2.

Instrukcja wykonania:

**Wariant skryptowy:**

```bash
./scripts/run-local-e4-a.sh
```

**Wariant reczny** (wszystkie komendy w osobnych terminalach):

```bash
# Terminal 1 — build
cargo build --bins

# Terminal 1 — scheduler
./target/debug/scheduler --bind-port 50050
```

```bash
# Terminal 2 — executor 1
./target/debug/executor --port 50051 --grpc-port 50052 --work-dir target/ballista/e4a-executor-1
```

```bash
# Terminal 3 — executor 2
./target/debug/executor --port 50061 --grpc-port 50062 --work-dir target/ballista/e4a-executor-2
```

```bash
# Terminal 4 — query
./target/debug/query --provider-mode approach-a --limit 20
```

Oczekiwany wynik:

- query konczy sie sukcesem,
- zwracana jest tabela overlap (20 wierszy przy domyslnym limicie),
- brak regresji semantycznej wzgledem E2.

Co dodano wzgledem E2:

- Osobny provider eksperymentalny `ApproachAOverlapProvider` bez stanu sesji,
  z publicznymi getterami i zewnętrznym codec.
- Odrębny tag serializacji (`OVERLAP_PROVIDER_A_MAGIC`) dla walidacji
  niezaleznego kontraktu.

Zasadnosc i potencjal produkcyjny:

- Najbardziej realistyczna i najmniej inwazyjna sciezka upstream.
- Utrzymuje kompatybilnosc z obecnym modelem deploymentu Ballisty.
- Ogranicza dlug techniczny adaptera E2, ale nadal wymaga codec po stronie
  systemu dystrybucyjnego.

Co zostalo zmienione wzgledem oryginalnego `polars-bio/src/operation.rs`:

- Usunieto zaleznosc od `Arc<SessionContext>` jako pola providera.
- Dodano jawnie dostepne dane potrzebne do odtworzenia planu na executorze:
  `left_table`, `right_table`, `left_path`, `right_path`, `columns_1`, `columns_2`, `strict`.
- Zamiast bezposrednio tworzyc upstream `OverlapProvider`, provider A odtwarza
  plan z samych danych serializowalnych.
- Nadal wymagany jest zewnetrzny codec po stronie Ballisty.
- Odpowiednikiem lokalnej logiki wykonawczej providera staje sie wspolne
  `build_overlap_plan_from_paths(...)`; roznica wobec E2 polega na tym, ze A modeluje
  sytuacje, w ktorej upstream sam wystawia dane potrzebne codecowi przez gettery.

## E5-B - Weryfikacja podejscia B (pelny kontrakt serializacji)

Cel:

- Zweryfikowac model, w ktorym provider ma jawnie wersjonowany kontrakt
  serializacji (symulacja pelnego podejscia B).

Kod obecny w repo:

- `src/operation.rs` - `do_approach_b_overlap(...)`
- `src/operation.rs` - `ApproachBOverlapProvider` (z `codec_version`)
- `src/codec.rs` - obsluga `OVERLAP_PROVIDER_B_MAGIC`
- `src/bin/query.rs` - tryb `--provider-mode approach-b`
- `scripts/run-local-e5-b.sh` - harness uruchomieniowy E5-B

Relacja do oryginalnego `polars-bio`:

- To eksperymentalna symulacja tego, jak wygladalaby integracja po wdrozeniu
  pelnego podejscia B (natywna, wersjonowana serializacja providera).

Instrukcja wykonania:

**Wariant skryptowy:**

```bash
./scripts/run-local-e5-b.sh
```

**Wariant reczny** (wszystkie komendy w osobnych terminalach):

```bash
# Terminal 1 — build
cargo build --bins

# Terminal 1 — scheduler
./target/debug/scheduler --bind-port 50050
```

```bash
# Terminal 2 — executor 1
./target/debug/executor --port 50051 --grpc-port 50052 --work-dir target/ballista/e5b-executor-1
```

```bash
# Terminal 3 — executor 2
./target/debug/executor --port 50061 --grpc-port 50062 --work-dir target/ballista/e5b-executor-2
```

```bash
# Terminal 4 — query
./target/debug/query --provider-mode approach-b --limit 20
```

Oczekiwany wynik:

- query konczy sie sukcesem,
- zwracana jest tabela overlap (20 wierszy przy domyslnym limicie),
- kontrakt serializacji jest jawnie wersjonowany (`codec_version`).

Co dodano wzgledem E2:

- Provider `ApproachBOverlapProvider` z polem `codec_version`.
- Dedykowany tag serializacji (`OVERLAP_PROVIDER_B_MAGIC`) i walidacja
  wersjonowania payloadu.

Zasadnosc i potencjal produkcyjny:

- Najlepsza baza do dlugoterminowej kompatybilnosci i migracji protokolu.
- Najbardziej kosztowna organizacyjnie opcja, bo wymaga utrzymania stabilnego
  API i polityki wersji serializacji po stronie upstream.
- W praktyce: najwyzsza dojrzalosc architektoniczna, ale najwiekszy koszt
  wejscia.

Co zostalo zmienione wzgledem oryginalnego `polars-bio/src/operation.rs`:

- Zachowano model providera serializowalnego jak w A, ale dodano jawny kontrakt
  wersjonowania payloadu (`codec_version`).
- Serializacja nie jest juz tylko lokalnym detalem adaptera, lecz staje sie
  czescia kontraktu runtime.
- To odpowiada produkcyjnemu scenariuszowi, w ktorym upstream utrzymuje wlasny,
  stabilny format przenoszenia providera przez scheduler i executory.
- Na poziomie funkcji odpowiedniki pozostaja te same co w E4-A, ale `src/codec.rs`
  rozszerza mapowanie o wersjonowany encode/decode dla `ApproachBOverlapProvider`.

## Skrypt zbiorczy porownawczy

Repo zawiera dodatkowy skrypt:

```bash
./scripts/run-local-compare-overlap-modes.sh
```

Skrypt:

- buduje binaria raz,
- uruchamia jeden lokalny klaster,
- wykonuje kolejno E2, E4-A i E5-B,
- zapisuje tabele wynikowe,
- porownuje je 1:1.

Oczekiwany wynik:

- komunikat `SUCCESS`,
- identyczna tabela wynikowa dla E2, E4-A i E5-B,
- artefakty porownania w `target/ballista-compare/`.

## E3 - Bezposredni upstream `OverlapProvider` (checkpoint regresyjny)

Cel:

- Sprawdzic, czy mozna usunac adapter i uruchomic upstream
  `datafusion-bio-function-ranges::OverlapProvider` bezposrednio.

Kod obecny w repo:

- `src/operation.rs` - `do_polars_bio_direct_overlap(...)`
- `src/bin/query.rs` - tryb `--provider-mode direct`
- `scripts/run-local-e3.sh` - harness uruchomieniowy E3

Relacja do oryginalnego `polars-bio`:

- To prawie 1:1 translacja wywolania `OverlapProvider::new(...)` z
  `polars-bio/src/operation.rs`.
- W runtime ten eksperyment celowo pada na serializacji planu (`LogicalExtensionCodec
  is not provided`) i pozostaje trwalym testem regresyjnym.

Instrukcja wykonania:

**Wariant skryptowy** (automatycznie weryfikuje oczekiwany blad):

```bash
./scripts/run-local-e3.sh
```

Skrypt konczy sie kodem 0 jesli pojawil sie oczekiwany blad serializacji,
kodem 1 jesli wynik byl nieoczekiwany.

**Wariant reczny** (wszystkie komendy w osobnych terminalach):

```bash
# Terminal 1 — build
cargo build --bins

# Terminal 1 — scheduler
./target/debug/scheduler --bind-port 50050
```

```bash
# Terminal 2 — executor 1
./target/debug/executor --port 50051 --grpc-port 50052 --work-dir target/ballista/e3-executor-1
```

```bash
# Terminal 3 — executor 2
./target/debug/executor --port 50061 --grpc-port 50062 --work-dir target/ballista/e3-executor-2
```

```bash
# Terminal 4 — query (po uruchomieniu klastra)
./target/debug/query --provider-mode direct --limit 5
```

Oczekiwany wynik:

- komenda konczy sie bledem (exit code 1),
- w wyjsciu pojawia sie `LogicalExtensionCodec is not provided`,
- to zachowanie jest intencjonalne i potwierdza wymagana zmiane upstream.

Co dodano wzgledem oryginalnego `polars-bio`:

- Praktycznie zadnych zmian kodu logiki overlap — wywolanie `OverlapProvider::new(...)` jest
  niemal 1:1 zgodne z `polars-bio/src/operation.rs`.
- Jedyna roznica strukturalna: zapytanie trafia przez zdalny `SessionContext` Ballisty zamiast
  lokalnego, co ujawnia brakujaca sciezke serializacji w upstream.
- Dodano tryb `--provider-mode direct` w `src/bin/query.rs` oraz skrypt `scripts/run-local-e3.sh`
  z automatyczna weryfikacja oczekiwanego bledu.
- Eksperyment nie wymaga zadnych zmian w `src/codec.rs` — brak codec jest tu zamierzony.
