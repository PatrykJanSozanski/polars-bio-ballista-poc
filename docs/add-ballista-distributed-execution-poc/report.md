# Final POC Report: polars-bio-ballista-poc

## Scope

This repository is a local proof of concept for running overlap-like queries
through Apache Ballista without forking Ballista.

Implemented experiment set:

- E1: toy UDTF bootstrap
- E2: serializable adapter provider
- E3: direct upstream checkpoint (expected failure)
- E4-A: approach A simulation
- E5-B: approach B simulation

Topology and data scope:

- 1 scheduler
- 2 executors
- local Parquet fixtures only

## Validated DF53 baseline

This POC was revalidated on 2026-05-19 against the newer `polars-bio`
DataFusion 53 line.

Runtime sources:

- `polars-bio` branch `upgrade-datafusion-53-1`
- `polars-bio` commit `bfc67d3d822040ddb2dfa949010dbb7ea00968cf`
- DataFusion `53.1.0`
- Arrow `58.3.0`
- `datafusion-bio-function-ranges`
  `35a6a6e41c6212c8e031d3beb7f917591e589475`
- Apache Ballista from `apache/datafusion-ballista` branch `main`
- Ballista commit `38ef6004f64b5aa14a5d8e8765d94f716b796fbc`
  (crate version `53.0.0`)

The POC uses the `polars-bio` branch as the source of the function contract:
its Cargo pins define the DataFusion/Arrow/function-crate baseline, and its
`src/operation.rs` remains the reference for overlap behavior. The POC still
keeps a local serializable adapter because the upstream provider cannot be
serialized directly by Ballista.

Build note:

- Building Ballista directly from GitHub requires `protoc`, because
  `ballista-core` generates protobuf bindings in its build script.

Validated commands:

```bash
cargo check --bins
./scripts/run-local-e1.sh
./scripts/run-local-compare-overlap-modes.sh
./scripts/run-local-e3.sh
```

Observed result:

- E1 returned the expected 3 toy overlap rows.
- E2, E4-A and E5-B returned identical tables through one local cluster.
- E3 returned the expected serialization error:
  `LogicalExtensionCodec is not provided`.

## Verified runtime behavior

### E1

- custom table function executes through Ballista
- deterministic toy fixtures are generated automatically
- expected result: 3 overlap pairs

### E2

- overlap query executes end-to-end through scheduler and executors
- custom provider is serialized via `PolarsBioBallistaLogicalCodec`
- expected display result: 20 rows in default run
- on DF53, the runner also truncates collected batches before printing, because
  the remote Ballista path did not reliably preserve the client-side display
  limit for this custom-provider plan

### E3

- direct upstream `OverlapProvider` fails with expected serialization error
- error proves that upstream currently lacks the data and codec path required
  by Ballista

### E4-A

- approach A simulation executes successfully through the same runtime path as E2
- result table matches E2 in the validated local runs

### E5-B

- approach B simulation executes successfully through the same runtime path as E2
- result table matches E2 in the validated local runs
- payload versioning is carried explicitly in the provider contract

## What E2 changes relative to original polars-bio

Reference point: `polars-bio/src/operation.rs`, function `do_overlap(...)`.

Original local path:

- creates upstream `OverlapProvider::new(...)`
- stores `Arc<SessionContext>` inside the provider
- registers provider locally in one process
- does not need any Ballista serialization support

E2 changes:

- replaces upstream provider instance with `SerializableOverlapProvider`
- stores only serializable reconstruction data:
  - table names
  - parquet paths
  - overlap columns
  - strict flag
  - output schema
- reconstructs executable overlap plan inside `scan()` from those values
- adds `PolarsBioBallistaLogicalCodec` and wires it into:
  - client
  - scheduler
  - executors

Result:

- E2 is the current working integration layer in this repository
- E2 is the safe fallback architecture as long as upstream remains unchanged
- This remains true for the DataFusion 53.1.0 / Ballista main validation.

### Function-level mapping

- Original `do_overlap(...)` maps to two paths in the POC:
  - `do_polars_bio_direct_overlap(...)` for E3, which preserves the original call shape
  - `do_polars_bio_style_overlap(...)` for E2, which enters the serializable path
- Original schema fetches via `ctx.table(...).schema()` are preserved and still define
  the same output projection layout.
- Original `OverlapProvider::new(...)` is replaced in E2 by
  `SerializableOverlapProvider::new(...)`.
- Original `ctx.register_table(...)` is preserved, but the registered provider is now
  serializable.
- Original SQL-style renaming of `left_*` and `right_*` columns is preserved in the
  POC helper `do_overlap(...)`.
- The execution logic that upstream keeps inside `OverlapProvider` is reconstructed in
  `TableProvider::scan(...)` and `build_overlap_plan_from_paths(...)`.
- The distributed-only addition is `PolarsBioBallistaLogicalCodec`, which encodes and
  decodes the provider across client, scheduler, and executors.

## What E4-A changes relative to original polars-bio

Approach A models the smallest realistic upstream refactor.

Compared with original `polars-bio/src/operation.rs`:

- removes dependency on `Arc<SessionContext>` as provider state
- keeps provider data in a serializable shape
- exposes reconstruction data through public getters
- still relies on external codec integration in Ballista runtime
- keeps the same execution reconstruction path as E2 via `build_overlap_plan_from_paths(...)`

Interpretation:

- this is the most realistic short-term upstream path
- it reduces adapter debt relative to E2
- it does not remove the need for codec support in distributed execution

## What E5-B changes relative to original polars-bio

Approach B models a full serialization contract.

Compared with original `polars-bio/src/operation.rs`:

- keeps the same serializable provider shape as A
- adds explicit payload versioning (`codec_version`)
- treats provider serialization as part of a maintained runtime contract
- extends the encode/decode mapping in `src/codec.rs` with a versioned payload branch

Interpretation:

- this is the most production-ready shape long-term
- it is best for protocol evolution and compatibility management
- it has the highest ownership and maintenance cost in upstream

## Relationship between E2, E4-A and E5-B

- E2 is the currently working local integration layer
- E4-A shows how E2 could be simplified if upstream removes non-serializable
  provider state and exposes the necessary data
- E5-B shows how the same path could evolve into a versioned, maintainable
  serialization contract owned by upstream

These are not competing runtime behaviors in this repository.
They are staged architecture examples built on the same validated overlap slice.

## Aggregate verification

The repository contains a comparison script:

```bash
./scripts/run-local-compare-overlap-modes.sh
```

It:

- builds binaries once
- starts one local Ballista cluster
- runs E2, E4-A and E5-B sequentially
- extracts result tables
- compares them 1:1

Expected result:

- success message confirming identical tables
- comparison artifacts written to `target/ballista-compare/`

DF53 validation result:

```text
[COMPARE] SUCCESS: E2, E4-A and E5-B returned identical tables.
```

## Production guidance

Recommended path:

1. keep E2 as the stable fallback while upstream remains unchanged
2. drive upstream toward approach A first
3. adopt approach B only when upstream is ready to own codec and payload
   version lifecycle

Operational conclusion:

- no Ballista fork is required
- distributed overlap is viable for this slice
- the main upstream gap is provider serialization ownership, not Ballista itself
- the same conclusion still holds after moving the POC to DataFusion `53.1.0`
  and Ballista `main`

## Canonical documentation set

- `README.md`
- `EXPERIMENTS.md`
- `docs/README.md`
- `docs/add-ballista-distributed-execution-poc/report.md`
